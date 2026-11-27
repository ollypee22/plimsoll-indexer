package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// The tests in this file need a real Postgres. postgres_test.go starts one for
// the package and each test skips with a message under `go test -short`.

const testSAC = "CSACSTORETEST"

func eventMeta(id string, ledger uint32, at time.Time) soroban.Meta {
	return soroban.Meta{ID: id, Ledger: ledger, ClosedAt: at.UTC(), TxHash: "tx-" + id}
}

// tableCounts returns the row count of every table named, so two states of the
// database can be compared without knowing which table a bug touched.
func tableCounts(t *testing.T, url string, tables ...string) map[string]int {
	t.Helper()
	p := testPool(t, url)
	out := make(map[string]int, len(tables))
	for _, table := range tables {
		var n int
		if err := p.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		out[table] = n
	}
	return out
}

func TestApplyEventsIsIdempotent(t *testing.T) {
	s, url := openStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	events := []any{
		soroban.AssetListed{Meta: eventMeta("ev-asset", 10, base), SAC: testSAC, Code: "PUSD", Issuer: "GISS"},
		soroban.SupplyPosted{
			Meta: eventMeta("ev-supply", 11, base.Add(time.Minute)), SAC: testSAC,
			Amount: big.NewInt(1_000_000), AtLedger: 9, BreakdownHash: "hash-1", Poster: "GPOST",
		},
		soroban.ReservePosted{
			Meta: eventMeta("ev-report", 12, base.Add(2*time.Minute)), SAC: testSAC,
			Tier: 2, Amount: big.NewInt(1_020_000), AsOf: base, Reporter: "GREP",
			DocHash: "doc-1", DocURI: "https://issuer.example/report",
		},
		soroban.ReporterSet{Meta: eventMeta("ev-reporter", 13, base.Add(3*time.Minute)), Reporter: "GREP", Role: 2, Name: "Reporter"},
		soroban.ReporterRevoked{Meta: eventMeta("ev-revoked", 14, base.Add(4*time.Minute)), Reporter: "GREP"},
	}

	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-1", 12, events); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	tables := []string{"assets", "supply_snapshots", "reserve_reports", "reporters", "ingest_cursor"}
	first := tableCounts(t, url, tables...)
	if first["assets"] != 1 || first["supply_snapshots"] != 1 || first["reserve_reports"] != 1 || first["reporters"] != 1 {
		t.Fatalf("first apply stored %v, want one row per table", first)
	}
	progress, err := s.Progress(ctx, "plimsoll-events")
	if err != nil {
		t.Fatal(err)
	}
	firstSupply, err := s.SupplyHistory(ctx, testSAC, 10)
	if err != nil {
		t.Fatal(err)
	}
	firstReports, err := s.ReportHistory(ctx, testSAC, 10)
	if err != nil {
		t.Fatal(err)
	}

	// The same page delivered twice by a retried RPC call must be a no-op.
	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-1", 12, events); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	second := tableCounts(t, url, tables...)
	for _, table := range tables {
		if first[table] != second[table] {
			t.Errorf("%s has %d rows after re-applying, want %d", table, second[table], first[table])
		}
	}
	again, err := s.Progress(ctx, "plimsoll-events")
	if err != nil {
		t.Fatal(err)
	}
	if again != progress {
		t.Errorf("progress after re-applying = %+v, want %+v", again, progress)
	}
	secondSupply, err := s.SupplyHistory(ctx, testSAC, 10)
	if err != nil {
		t.Fatal(err)
	}
	secondReports, err := s.ReportHistory(ctx, testSAC, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondSupply) != len(firstSupply) || len(secondReports) != len(firstReports) {
		t.Fatalf("history changed after re-applying: supply %d→%d, reports %d→%d",
			len(firstSupply), len(secondSupply), len(firstReports), len(secondReports))
	}
	if len(secondSupply) != 1 || secondSupply[0].Amount.Cmp(big.NewInt(1_000_000)) != 0 {
		t.Fatalf("supply history = %+v", secondSupply)
	}
}

func TestLatestReportsReturnsNewestPerTier(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	events := []any{
		soroban.ReservePosted{
			Meta: eventMeta("ev-t1-old", 20, base), SAC: testSAC, Tier: 1,
			Amount: big.NewInt(100), AsOf: base, Reporter: "GOLD", DocHash: "doc-1",
		},
		soroban.ReservePosted{
			Meta: eventMeta("ev-t1-new", 21, base.Add(time.Hour)), SAC: testSAC, Tier: 1,
			Amount: big.NewInt(200), AsOf: base.Add(2 * time.Hour), Reporter: "GNEW", DocHash: "doc-2",
		},
		soroban.ReservePosted{
			Meta: eventMeta("ev-t3", 22, base.Add(2*time.Hour)), SAC: testSAC, Tier: 3,
			Amount: big.NewInt(300), AsOf: base.Add(3 * time.Hour), Reporter: "GAUD", DocHash: "doc-3",
		},
		// A report for another asset must not leak into this one's tiers.
		soroban.ReservePosted{
			Meta: eventMeta("ev-other", 23, base), SAC: "COSAC", Tier: 2,
			Amount: big.NewInt(999), AsOf: base.Add(4 * time.Hour), Reporter: "GOTHER", DocHash: "doc-4",
		},
	}
	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-2", 23, events); err != nil {
		t.Fatal(err)
	}

	reports, err := s.LatestReports(ctx, testSAC)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 2 {
		t.Fatalf("LatestReports returned %d tiers, want 2: %+v", len(reports), reports)
	}
	if got := reports[1]; got.Reporter != "GNEW" || got.Amount.Cmp(big.NewInt(200)) != 0 {
		t.Errorf("tier 1 newest = %+v, want GNEW/200", got)
	}
	if got := reports[3]; got.Reporter != "GAUD" || got.Amount.Cmp(big.NewInt(300)) != 0 {
		t.Errorf("tier 3 newest = %+v, want GAUD/300", got)
	}
	if _, ok := reports[2]; ok {
		t.Errorf("tier 2 came from another asset: %+v", reports[2])
	}
}

func TestBreakdownBytesRoundTrip(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	// Whitespace, an escaped string and multi-byte characters: anything that a
	// JSONB round-trip would normalise.
	body := []byte("{\n  \"asset\": \"PUSD:GISS\",\n  \"note\": \"caf\u00e9 \u2014 \u00fcber\",\n  \"total\": \"10\"\n}\n")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])

	if err := s.SaveBreakdown(ctx, hash, testSAC, body); err != nil {
		t.Fatal(err)
	}
	got, err := s.Breakdown(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("breakdown came back changed:\n got: %q\nwant: %q", got, body)
	}
	// The served bytes must still hash to the value posted on chain.
	check := sha256.Sum256(got)
	if hex.EncodeToString(check[:]) != hash {
		t.Fatalf("sha256(served) = %x, want %s", check, hash)
	}
}
