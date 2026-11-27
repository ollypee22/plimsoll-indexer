package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
)

func TestWebhookRegistrationAndDeliveryLog(t *testing.T) {
	s, url := openStore(t)
	ctx := context.Background()

	w, err := s.CreateWebhook(ctx, coverage.Webhook{
		URL: "https://integrator.example/hook", SAC: "CSAC", MinBps: 9500, MinTier: coverage.TierIssuerSigned,
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.ID <= 0 || w.CreatedAt.IsZero() || w.Alerted {
		t.Fatalf("created webhook = %+v", w)
	}

	hooks, err := s.ListWebhooks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(hooks) != 1 || hooks[0].URL != w.URL || hooks[0].MinBps != 9500 || hooks[0].MinTier != 2 {
		t.Fatalf("ListWebhooks = %+v", hooks)
	}

	if err := s.SetWebhookAlerted(ctx, w.ID, true); err != nil {
		t.Fatal(err)
	}
	hooks, err = s.ListWebhooks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hooks[0].Alerted {
		t.Fatal("alerted flag was not stored")
	}

	payload := []byte(`{"event":"coverage_dropped","bps":9000}`)
	if err := s.EnqueueWebhookDelivery(ctx, w.ID, payload); err != nil {
		t.Fatal(err)
	}
	due, err := s.DueWebhookDeliveries(ctx, 10, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].WebhookID != w.ID || due[0].URL != w.URL {
		t.Fatalf("due deliveries = %+v", due)
	}
	// JSONB normalises whitespace, so compare the decoded value: the worker
	// signs the exact bytes it reads back, not the bytes queued.
	var gotPayload map[string]any
	if err := json.Unmarshal(due[0].Payload, &gotPayload); err != nil {
		t.Fatalf("payload %q is not JSON: %v", due[0].Payload, err)
	}
	if gotPayload["event"] != "coverage_dropped" || gotPayload["bps"].(float64) != 9000 {
		t.Fatalf("payload = %s", due[0].Payload)
	}
	deliveryID := due[0].ID

	// Five attempts exhaust the delivery: it must stop being due.
	dueAt := time.Now().Add(-time.Second)
	for i := 0; i < 5; i++ {
		if err := s.RecordWebhookAttempt(ctx, deliveryID, 500, "status 500", false, dueAt); err != nil {
			t.Fatal(err)
		}
	}
	due, err = s.DueWebhookDeliveries(ctx, 10, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("delivery still due after 5 attempts: %+v", due)
	}

	var attempts, status int
	var delivered *time.Time
	var lastErr string
	if err := testPool(t, url).QueryRow(ctx,
		`SELECT attempts, last_status, last_error, delivered_at FROM webhook_deliveries WHERE id = $1`,
		deliveryID).Scan(&attempts, &status, &lastErr, &delivered); err != nil {
		t.Fatal(err)
	}
	if attempts != 5 || status != 500 || lastErr != "status 500" || delivered != nil {
		t.Fatalf("delivery log = attempts %d, status %d, err %q, delivered %v", attempts, status, lastErr, delivered)
	}

	// The delivery log is deleted with the registration.
	if err := s.DeleteWebhook(ctx, w.ID); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := testPool(t, url).QueryRow(ctx, `SELECT count(*) FROM webhook_deliveries`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("webhook_deliveries has %d rows after deleting the webhook, want 0", rows)
	}
	if err := s.DeleteWebhook(ctx, w.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete err = %v, want ErrNotFound", err)
	}
}
