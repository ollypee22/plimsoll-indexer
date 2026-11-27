package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/stellartoml"
)

type fakeTomlStore struct {
	recorded []recordedAttestation
}

type recordedAttestation struct {
	sac, url, hash string
	at             time.Time
}

func (s *fakeTomlStore) ListAssets(context.Context) ([]coverage.Asset, error) { return nil, nil }
func (s *fakeTomlStore) SaveToml(context.Context, string, []byte, string, time.Time) error {
	return nil
}
func (s *fakeTomlStore) RecordAttestation(_ context.Context, sac, url, hash string, at time.Time) error {
	s.recorded = append(s.recorded, recordedAttestation{sac: sac, url: url, hash: hash, at: at})
	return nil
}

type fakeAttestationFetcher struct {
	hash string
	err  error
	uri  string
}

func (f *fakeAttestationFetcher) Fetch(_ context.Context, uri string) (string, error) {
	f.uri = uri
	return f.hash, f.err
}

func TestCheckAttestationRecordsHash(t *testing.T) {
	store := &fakeTomlStore{}
	fetcher := &fakeAttestationFetcher{hash: "abc123"}
	at := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	sync := &TomlSync{Store: store, Attestation: fetcher, Log: discardLog()}
	info := stellartoml.Info{Currency: &stellartoml.Currency{
		Code: "PUSD", AttestationOfReserve: "https://issuer.example/audit.pdf",
	}}
	asset := coverage.Asset{SAC: "CSAC", Code: "PUSD", Issuer: "GISS"}

	sync.checkAttestation(context.Background(), asset, info, at)

	if fetcher.uri != "https://issuer.example/audit.pdf" {
		t.Fatalf("fetched %q, want the stellar.toml URL", fetcher.uri)
	}
	if len(store.recorded) != 1 {
		t.Fatalf("recorded %d attestations, want 1", len(store.recorded))
	}
	got := store.recorded[0]
	if got.sac != "CSAC" || got.url != "https://issuer.example/audit.pdf" || got.hash != "abc123" || !got.at.Equal(at) {
		t.Fatalf("recorded %+v", got)
	}
}

func TestCheckAttestationSkipsMissingOrUnreachable(t *testing.T) {
	at := time.Now()
	for name, tc := range map[string]struct {
		info    stellartoml.Info
		fetcher *fakeAttestationFetcher
	}{
		"no currency":     {info: stellartoml.Info{}},
		"no attestation":  {info: stellartoml.Info{Currency: &stellartoml.Currency{Code: "PUSD"}}},
		"fetch fails":     {info: stellartoml.Info{Currency: &stellartoml.Currency{AttestationOfReserve: "https://x"}}, fetcher: &fakeAttestationFetcher{err: errors.New("boom")}},
		"fetcher not set": {info: stellartoml.Info{Currency: &stellartoml.Currency{AttestationOfReserve: "https://x"}}},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeTomlStore{}
			sync := &TomlSync{Store: store, Log: discardLog()}
			if tc.fetcher != nil {
				sync.Attestation = tc.fetcher
			}
			sync.checkAttestation(context.Background(), coverage.Asset{SAC: "CSAC"}, tc.info, at)
			if len(store.recorded) != 0 {
				t.Fatalf("recorded %+v, want nothing", store.recorded)
			}
		})
	}
}
