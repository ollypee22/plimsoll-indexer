package worker

import (
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
)

type fakeWebhookDelivery struct {
	coverage.WebhookDelivery
	delivered bool
	nextAt    time.Time
	status    int
	lastErr   string
}

type fakeWebhookStore struct {
	hooks      []coverage.Webhook
	supply     map[string]*coverage.Supply
	reports    map[string]map[uint32]coverage.Report
	deliveries []*fakeWebhookDelivery
}

func (s *fakeWebhookStore) ListWebhooks(context.Context) ([]coverage.Webhook, error) {
	return s.hooks, nil
}

func (s *fakeWebhookStore) LatestSupply(_ context.Context, sac string) (*coverage.Supply, error) {
	return s.supply[sac], nil
}

func (s *fakeWebhookStore) LatestReports(_ context.Context, sac string) (map[uint32]coverage.Report, error) {
	if r := s.reports[sac]; r != nil {
		return r, nil
	}
	return map[uint32]coverage.Report{}, nil
}

func (s *fakeWebhookStore) SetWebhookAlerted(_ context.Context, id int64, alerted bool) error {
	for i := range s.hooks {
		if s.hooks[i].ID == id {
			s.hooks[i].Alerted = alerted
		}
	}
	return nil
}

func (s *fakeWebhookStore) EnqueueWebhookDelivery(_ context.Context, webhookID int64, payload []byte) error {
	url := ""
	for _, h := range s.hooks {
		if h.ID == webhookID {
			url = h.URL
		}
	}
	id := int64(len(s.deliveries) + 1)
	s.deliveries = append(s.deliveries, &fakeWebhookDelivery{
		WebhookDelivery: coverage.WebhookDelivery{
			ID: id, WebhookID: webhookID, URL: url, Payload: append([]byte(nil), payload...),
		},
	})
	return nil
}

func (s *fakeWebhookStore) DueWebhookDeliveries(_ context.Context, limit, maxAttempts int) ([]coverage.WebhookDelivery, error) {
	var out []coverage.WebhookDelivery
	for _, d := range s.deliveries {
		if d.delivered || d.Attempts >= maxAttempts {
			continue
		}
		out = append(out, d.WebhookDelivery)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (s *fakeWebhookStore) RecordWebhookAttempt(_ context.Context, id int64, status int, errMsg string, delivered bool, nextAt time.Time) error {
	for _, d := range s.deliveries {
		if d.ID != id {
			continue
		}
		d.Attempts++
		d.status, d.lastErr, d.delivered, d.nextAt = status, errMsg, delivered, nextAt
	}
	return nil
}

// receiver records every delivery it is sent and answers with status.
type receiver struct {
	mu         sync.Mutex
	bodies     [][]byte
	signatures []string
	deliveries []string
	status     int
	failFirst  int
}

func newReceiver(status int) (*receiver, *httptest.Server) {
	rc := &receiver{status: status}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rc.mu.Lock()
		rc.bodies = append(rc.bodies, body)
		rc.signatures = append(rc.signatures, r.Header.Get(SignatureHeader))
		rc.deliveries = append(rc.deliveries, r.Header.Get("X-Plimsoll-Delivery"))
		fail := rc.status
		if rc.failFirst > 0 {
			rc.failFirst--
			fail = http.StatusInternalServerError
			defer rc.mu.Unlock()
			w.WriteHeader(fail)
			return
		}
		rc.mu.Unlock()
		w.WriteHeader(fail)
	}))
	return rc, srv
}

func (r *receiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bodies)
}

func testWebhookStore(url string) *fakeWebhookStore {
	base := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	return &fakeWebhookStore{
		hooks: []coverage.Webhook{{
			ID: 7, URL: url, SAC: "CSAC", MinBps: 9500, MinTier: coverage.TierIssuerSigned,
		}},
		supply: map[string]*coverage.Supply{
			"CSAC": {Amount: big.NewInt(1000), Ledger: 10, PostedAt: base},
		},
		reports: map[string]map[uint32]coverage.Report{
			"CSAC": {coverage.TierIssuerSigned: {
				Tier: coverage.TierIssuerSigned, Amount: big.NewInt(900), AsOf: base,
			}},
		},
	}
}

func setReportBps(store *fakeWebhookStore, amount int64, at time.Time) {
	store.reports["CSAC"] = map[uint32]coverage.Report{
		coverage.TierIssuerSigned: {
			Tier: coverage.TierIssuerSigned, Amount: big.NewInt(amount), AsOf: at,
		},
	}
}

func TestWebhookNotifierAlertsOncePerCrossingAndSigns(t *testing.T) {
	rc, srv := newReceiver(http.StatusOK)
	defer srv.Close()
	store := testWebhookStore(srv.URL)
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	n := &WebhookNotifier{
		Store: store, Secret: "s3cret", Log: discardLog(),
		Now: func() time.Time { return now },
	}

	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rc.count() != 1 {
		t.Fatalf("receiver got %d deliveries, want 1", rc.count())
	}
	if !store.hooks[0].Alerted {
		t.Fatal("webhook was not marked alerted")
	}
	if len(store.deliveries) != 1 || !store.deliveries[0].delivered || store.deliveries[0].Attempts != 1 {
		t.Fatalf("delivery log = %+v", store.deliveries)
	}

	// Verify the exact bytes the receiver got against the signature.
	body := rc.bodies[0]
	if got, want := rc.signatures[0], signWebhook("s3cret", body); got != want {
		t.Fatalf("signature = %q, want %q", got, want)
	}
	if rc.deliveries[0] != "1" {
		t.Fatalf("delivery header = %q, want 1", rc.deliveries[0])
	}
	var payload struct {
		Event    string    `json:"event"`
		SAC      string    `json:"sac"`
		MinBps   uint32    `json:"min_bps"`
		MinTier  string    `json:"min_tier"`
		Bps      uint32    `json:"bps"`
		Supply   string    `json:"supply"`
		Reserves string    `json:"reserves"`
		SentAt   time.Time `json:"sent_at"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Event != "coverage_dropped" || payload.SAC != "CSAC" || payload.MinBps != 9500 ||
		payload.MinTier != "issuer_signed" || payload.Bps != 9000 ||
		payload.Supply != "1000" || payload.Reserves != "900" || !payload.SentAt.Equal(now) {
		t.Fatalf("payload = %+v", payload)
	}

	// Still below the threshold: no second alert for the same crossing.
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rc.count() != 1 {
		t.Fatalf("sustained shortfall produced %d deliveries, want 1", rc.count())
	}

	// Recovery re-arms the webhook.
	setReportBps(store, 960, now.Add(time.Hour))
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.hooks[0].Alerted {
		t.Fatal("webhook was not re-armed after coverage recovered")
	}

	// The next drop is a new crossing and alerts again.
	setReportBps(store, 900, now.Add(2*time.Hour))
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rc.count() != 2 {
		t.Fatalf("second crossing produced %d deliveries in total, want 2", rc.count())
	}
}

func TestWebhookNotifierRetriesWithBackoffAtMostFiveTimes(t *testing.T) {
	rc, srv := newReceiver(http.StatusServiceUnavailable)
	defer srv.Close()
	store := testWebhookStore(srv.URL)
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	n := &WebhookNotifier{
		Store: store, Secret: "s3cret", Log: discardLog(),
		Now:     func() time.Time { return now },
		Backoff: 30 * time.Second,
	}

	for attempt := 1; attempt <= webhookMaxAttempts; attempt++ {
		if err := n.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		if rc.count() != attempt {
			t.Fatalf("after attempt %d the receiver has %d requests", attempt, rc.count())
		}
		got := store.deliveries[0]
		if got.Attempts != attempt {
			t.Fatalf("attempts = %d, want %d", got.Attempts, attempt)
		}
		if got.delivered {
			t.Fatalf("delivery marked delivered after a %d response", http.StatusServiceUnavailable)
		}
		wantNext := now.Add(30 * time.Second << (attempt - 1))
		if !got.nextAt.Equal(wantNext) {
			t.Fatalf("attempt %d next_attempt_at = %v, want %v", attempt, got.nextAt, wantNext)
		}
		now = got.nextAt
	}

	// The cap is reached: another tick must not send a sixth attempt.
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rc.count() != webhookMaxAttempts {
		t.Fatalf("receiver got %d requests after the cap, want %d", rc.count(), webhookMaxAttempts)
	}
}

func TestWebhookNotifierStopsAfterSuccess(t *testing.T) {
	rc, srv := newReceiver(http.StatusOK)
	rc.failFirst = 1 // fail the first attempt, accept the retry
	defer srv.Close()
	store := testWebhookStore(srv.URL)
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	n := &WebhookNotifier{
		Store: store, Secret: "s3cret", Log: discardLog(),
		Now:     func() time.Time { return now },
		Backoff: 30 * time.Second,
	}

	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Second)
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rc.count() != 2 {
		t.Fatalf("receiver got %d requests, want 2", rc.count())
	}
	if !store.deliveries[0].delivered || store.deliveries[0].Attempts != 2 {
		t.Fatalf("delivery = %+v, want delivered on attempt 2", store.deliveries[0])
	}

	// A delivered alert is never sent again.
	now = now.Add(24 * time.Hour)
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rc.count() != 2 {
		t.Fatalf("receiver got %d requests after delivery, want 2", rc.count())
	}
}

func TestWebhookNotifierIgnoresUnknownCoverage(t *testing.T) {
	rc, srv := newReceiver(http.StatusOK)
	defer srv.Close()
	store := testWebhookStore(srv.URL)
	store.supply = nil // no snapshot: coverage unknown, not zero
	n := &WebhookNotifier{
		Store: store, Secret: "s3cret", Log: discardLog(),
		Now: func() time.Time { return time.Now() },
	}
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rc.count() != 0 || len(store.deliveries) != 0 {
		t.Fatalf("unknown coverage produced %d deliveries", len(store.deliveries))
	}
}
