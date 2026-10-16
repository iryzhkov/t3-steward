package claudequota

import (
	"context"
	"errors"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestPrivateCredentialFileAndExpiryRequired(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		edit func(*testing.T, *Collector)
		code string
	}{
		{"public file", func(t *testing.T, c *Collector) {
			if err := os.Chmod(filepath.Join(c.options.CredentialDir, ".credentials.json"), 0644); err != nil {
				t.Fatal(err)
			}
		}, "credential-unavailable"},
		{"expired", func(t *testing.T, c *Collector) {
			read := c.readCredential
			c.readCredential = func() (credentialSnapshot, error) {
				s, e := read()
				x := float64(testNow.Add(-time.Minute).UnixMilli())
				s.expiresAt = &x
				return s, e
			}
		}, "credential-expired"},
		{"missing expiry", func(t *testing.T, c *Collector) {
			read := c.readCredential
			c.readCredential = func() (credentialSnapshot, error) { s, e := read(); s.expiresAt = nil; return s, e }
		}, "credential-expiry-invalid"},
		{"infinite expiry", func(t *testing.T, c *Collector) {
			read := c.readCredential
			c.readCredential = func() (credentialSnapshot, error) { s, e := read(); x := math.Inf(1); s.expiresAt = &x; return s, e }
		}, "credential-expiry-invalid"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := testCollector(t)
			calls := serveGood(t, c, goodUsage, nil)
			tt.edit(t, c)
			_, err := c.collect(context.Background(), testNow)
			requireFailure(t, err, tt.code)
			if *calls != 0 {
				t.Fatal("invalid credential contacted provider")
			}
		})
	}
}
func TestIdenticalCredentialBytesNewGenerationAreRejected(t *testing.T) {
	t.Parallel()
	c := testCollector(t)
	path := filepath.Join(c.options.CredentialDir, ".credentials.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	serveGood(t, c, goodUsage, func(r *http.Request) {
		if r.URL.Path != "/api/oauth/usage" {
			return
		}
		next := path + ".next"
		if err := os.WriteFile(next, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(next, path); err != nil {
			t.Fatal(err)
		}
	})
	got, err := c.collect(context.Background(), testNow)
	requireFailure(t, err, "credential-changed")
	if len(got) != 0 {
		t.Fatal("new generation emitted observations")
	}
}
func TestCancellationBetweenRequestsStopsPair(t *testing.T) {
	t.Parallel()
	c := testCollector(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := serveGood(t, c, goodUsage, func(*http.Request) { cancel() })
	got, err := c.collect(ctx, testNow)
	requireFailure(t, err, "deadline")
	if *calls != 1 || len(got) != 0 {
		t.Fatal("cancelled shared acquisition continued")
	}
}
func TestTimingOverflowAndHorizonFailClosed(t *testing.T) {
	t.Parallel()
	c := testCollector(t)
	o := c.options
	o.Interval = time.Duration(math.MaxInt64)
	requireFailure(t, ValidateOptions(o), "insufficient-scoped-age")
	o = c.options
	o.MaxObservationAge = 375 * time.Second
	requireFailure(t, ValidateOptions(o), "insufficient-scoped-age")
	o = c.options
	o.MaxObservationAge = 360 * time.Second
	requireFailure(t, ValidateOptions(o), "insufficient-scoped-age")
	// Cancellation is returned through Run, never persisted as healthy data.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Run(ctx, make(chan domain.QuotaSnapshot)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
