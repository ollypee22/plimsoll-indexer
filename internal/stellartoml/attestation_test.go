package stellartoml

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDocumentFetcherHashesBody(t *testing.T) {
	body := []byte("reserve attestation\n")
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	f := &DocumentFetcher{client: srv.Client()}
	got, err := f.Fetch(context.Background(), srv.URL+"/audit.pdf")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	if want := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("hash = %s, want %s", got, want)
	}
}

func TestDocumentFetcherRejectsBadURL(t *testing.T) {
	for _, uri := range []string{"ftp://issuer.example/a.pdf", "/relative/a.pdf", "https://"} {
		if _, err := NewDocumentFetcher().Fetch(context.Background(), uri); err == nil {
			t.Errorf("Fetch(%q) = nil error, want refusal", uri)
		}
	}
}

func TestDocumentFetcherRejectsNon2xx(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(http.NotFound))
	defer srv.Close()

	f := &DocumentFetcher{client: srv.Client()}
	if _, err := f.Fetch(context.Background(), srv.URL+"/missing"); err == nil {
		t.Fatal("Fetch of a 404 = nil error, want an error")
	}
}

// A document over the cap is rejected, not truncated: a truncated prefix would
// hash to a value no one could reproduce.
func TestDocumentFetcherRejectsOversizeDocument(t *testing.T) {
	body := strings.Repeat("A", attestationMaxBytes+1)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	f := &DocumentFetcher{client: srv.Client()}
	if _, err := f.Fetch(context.Background(), srv.URL+"/big.pdf"); err == nil {
		t.Fatal("Fetch of a document over 10 MB = nil error, want an error")
	}
}

func TestIsDocumentPrivate(t *testing.T) {
	cases := []struct {
		ip   string
		want bool
	}{
		{"10.0.0.1", true},
		{"172.16.0.1", true},
		{"172.32.0.0", false},
		{"192.168.1.1", true},
		{"127.0.0.1", true},
		{"169.254.0.1", true},
		{"::1", true},
		{"8.8.8.8", false},
		{"2001:db8::1", false},
	}
	for _, tc := range cases {
		addr := &testAddr{ip: tc.ip}
		if got := isDocumentPrivate(addr); got != tc.want {
			t.Errorf("isDocumentPrivate(%s) = %v, want %v", tc.ip, got, tc.want)
		}
	}
}

type testAddr struct{ ip string }

func (a *testAddr) Network() string { return "tcp" }
func (a *testAddr) String() string {
	if strings.Contains(a.ip, ":") {
		return "[" + a.ip + "]:443"
	}
	return a.ip + ":443"
}
