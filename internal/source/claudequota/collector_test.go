package claudequota

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const testAccount = "11111111-2222-3333-4444-555555555555"

var testNow = time.Date(2026, 10, 13, 19, 0, 0, 0, time.UTC)

const goodUsage = `{"five_hour":{"utilization":0,"resets_at":"2026-10-13T23:00:00Z"},"seven_day":{"utilization":94,"resets_at":"2026-10-15T04:00:00Z"},"seven_day_opus":null,"seven_day_sonnet":null,"seven_day_oauth_apps":null,"limits":[{"kind":"weekly_scoped","scope":{"model":{"display_name":"Fable"}},"percent":20,"resets_at":"2026-10-15T04:00:00+00:00"}]}`

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func testCollector(t *testing.T) *Collector {
	t.Helper()
	dir := t.TempDir()
	o := Options{ProviderInstanceID: "claudeAgent", AccountID: testAccount, CredentialDir: dir,
		StatusPath: filepath.Join(dir, "status.json"), MaxObservationAge: 390 * time.Second, PipelineBudget: 50 * time.Second,
		ModelBindings: []ModelBinding{{DisplayName: "Fable", Window: "seven_day_overage_included", Label: "Fable weekly", Selector: "fable", Models: []string{"claude-fable-5-1"}}}}
	c, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return testNow }
	c.jitter = func(time.Duration) time.Duration { return 0 }
	writeCredential(t, c, "test-only-access-token", []string{"user:profile", "user:inference"})
	return c
}
func writeCredential(t *testing.T, c *Collector, token string, scopes []string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"claudeAiOauth": map[string]any{"accessToken": token,
		"scopes": scopes, "expiresAt": testNow.Add(time.Hour).UnixMilli(), "refreshToken": "test-only-refresh-secret"}})
	if err := os.WriteFile(filepath.Join(c.options.CredentialDir, ".credentials.json"), body, 0600); err != nil {
		t.Fatal(err)
	}
}
func serveGood(t *testing.T, c *Collector, usage string, hook func(*http.Request)) *int {
	t.Helper()
	calls := new(int)
	var deadline time.Time
	c.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		*calls++
		if r.Method != http.MethodGet || r.URL.Scheme != "https" || r.URL.Host != "api.anthropic.com" || r.Body != nil {
			t.Error("request is not the bounded read-only contract")
		}
		if r.Header.Get("Authorization") != "Bearer test-only-access-token" || r.Header.Get("User-Agent") != "claude-code/2.1.296" {
			t.Error("wrong source-pinned headers")
		}
		d, ok := r.Context().Deadline()
		if !ok {
			t.Error("missing shared deadline")
		}
		if deadline.IsZero() {
			deadline = d
		} else if !deadline.Equal(d) {
			t.Error("requests do not share one deadline")
		}
		if hook != nil {
			hook(r)
		}
		switch r.URL.Path {
		case "/api/oauth/profile":
			if r.Header.Get("Cache-Control") != "no-cache" {
				t.Error("missing profile cache control")
			}
			return response(200, `{"account":{"uuid":"`+testAccount+`","email":"private@example.invalid"}}`), nil
		case "/api/oauth/usage":
			if r.Header.Get("anthropic-beta") != "oauth-2025-04-20" {
				t.Error("missing usage beta header")
			}
			return response(200, usage), nil
		default:
			t.Error("unexpected route")
			return response(404, ""), nil
		}
	})
	return calls
}
func requireFailure(t *testing.T, err error, code string) {
	t.Helper()
	var f *failure
	if !errors.As(err, &f) || f.code != code {
		t.Fatalf("error=%v, want sanitized %s", err, code)
	}
}

func TestCollectJointIdentityUsageAndGeneration(t *testing.T) {
	t.Parallel()
	c := testCollector(t)
	calls := serveGood(t, c, goodUsage, nil)
	got, err := c.collect(context.Background(), testNow)
	if err != nil || *calls != 2 || len(got) != 3 {
		t.Fatalf("count=%d windows=%d err=%v", *calls, len(got), err)
	}
	want := map[string]float64{"five_hour": 0, "seven_day": 94, "seven_day_overage_included": 20}
	event := got[0].SourceEventID
	for _, s := range got {
		if s.Key.AccountID != testAccount || s.Key.LimitID != "claude" || s.Key.ProviderInstanceID != "claudeAgent" || s.ObservedAt != testNow || s.SourceEventID == "" || s.SourceEventID != event || s.UsedPercent != want[s.Key.Window] {
			t.Fatalf("wrong observation: %+v", s)
		}
		if s.ResetsAt == nil || s.ResetsAt.Location() != time.UTC {
			t.Error("reset not normalized")
		}
	}
	if !errors.Is(c.client.CheckRedirect(nil, nil), http.ErrUseLastResponse) {
		t.Fatal("redirect policy permits redirect")
	}
}

func TestCollectRejectsIncompleteOrInvalidUsageBeforeAnyEmission(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, body, code string }{
		{"missing weekly", `{"five_hour":{"utilization":0,"resets_at":"2026-10-15T00:00:00Z"}}`, "partial-window"},
		{"null weekly", strings.Replace(goodUsage, `{"utilization":94,"resets_at":"2026-10-15T04:00:00Z"}`, "null", 1), "partial-window"},
		{"missing model", strings.Replace(goodUsage, `"limits":[{"kind":"weekly_scoped","scope":{"model":{"display_name":"Fable"}},"percent":20,"resets_at":"2026-10-15T04:00:00+00:00"}]`, `"limits":[]`, 1), "partial-model-window"},
		{"string percent", strings.Replace(goodUsage, `"utilization":94`, `"utilization":"94"`, 1), "invalid-window"},
		{"over100", strings.Replace(goodUsage, `"utilization":94`, `"utilization":101`, 1), "invalid-window"},
		{"negative", strings.Replace(goodUsage, `"utilization":94`, `"utilization":-1`, 1), "invalid-window"},
		{"past reset", strings.Replace(goodUsage, "2026-10-15T04:00:00Z", "2026-10-12T04:00:00Z", 1), "invalid-window"},
		{"no timezone", strings.Replace(goodUsage, "2026-10-15T04:00:00Z", "2026-10-15T04:00:00", 1), "invalid-window"},
		{"unbound model", strings.Replace(goodUsage, `"display_name":"Fable"`, `"display_name":"Other"`, 1), "unbound-model-window"},
		{"unbound legacy", strings.Replace(goodUsage, `"seven_day_opus":null`, `"seven_day_opus":{"utilization":4,"resets_at":"2026-10-15T00:00:00Z"}`, 1), "unbound-model-window"},
		{"trailing JSON", goodUsage + " {}", "invalid-usage"},
		{"oversize", strings.Repeat(" ", MaximumBodyBytes+1), "response-too-large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := testCollector(t)
			calls := serveGood(t, c, tt.body, nil)
			got, err := c.collect(context.Background(), testNow)
			requireFailure(t, err, tt.code)
			if len(got) != 0 || *calls != 2 {
				t.Fatalf("invalid batch count=%d calls=%d", len(got), *calls)
			}
		})
	}
}
func TestOptionalMissingIsNotZeroAndDuplicateModelFails(t *testing.T) {
	t.Parallel()
	c := testCollector(t)
	c.options.ModelBindings[0].Optional = true
	body := strings.Replace(goodUsage, `"limits":[{"kind":"weekly_scoped","scope":{"model":{"display_name":"Fable"}},"percent":20,"resets_at":"2026-10-15T04:00:00+00:00"}]`, `"limits":null`, 1)
	serveGood(t, c, body, nil)
	got, err := c.collect(context.Background(), testNow)
	if err != nil || len(got) != 2 {
		t.Fatalf("optional windows=%d err=%v", len(got), err)
	}
	body = strings.Replace(goodUsage, `"limits":[`, `"limits":[{"kind":"weekly_scoped","scope":{"model":{"display_name":"FABLE"}},"percent":22,"resets_at":"2026-10-15T04:00:00Z"},`, 1)
	serveGood(t, c, body, nil)
	_, err = c.collect(context.Background(), testNow)
	requireFailure(t, err, "duplicate-model-window")
}

func TestAccountMismatchStopsBeforeUsage(t *testing.T) {
	t.Parallel()
	c := testCollector(t)
	calls := 0
	c.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return response(200, `{"account":{"uuid":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}}`), nil
	})
	got, err := c.collect(context.Background(), testNow)
	requireFailure(t, err, "account-mismatch")
	if calls != 1 || len(got) != 0 {
		t.Fatal("usage was requested for a mismatched account")
	}
}
func TestCredentialChangeRejectsCompletePair(t *testing.T) {
	t.Parallel()
	c := testCollector(t)
	calls := serveGood(t, c, goodUsage, func(r *http.Request) {
		if r.URL.Path == "/api/oauth/usage" {
			writeCredential(t, c, "changed-test-only-token", []string{"user:profile"})
		}
	})
	got, err := c.collect(context.Background(), testNow)
	requireFailure(t, err, "credential-changed")
	if *calls != 2 || len(got) != 0 {
		t.Fatal("credential change emitted observations")
	}
}
func TestInferenceOnlyScopeAndCancellationNeverRequest(t *testing.T) {
	t.Parallel()
	c := testCollector(t)
	calls := serveGood(t, c, goodUsage, nil)
	writeCredential(t, c, "test-only-access-token", []string{"user:inference"})
	_, err := c.collect(context.Background(), testNow)
	requireFailure(t, err, "credential-profile-scope-missing")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.collect(ctx, testNow)
	requireFailure(t, err, "deadline")
	if *calls != 0 {
		t.Fatal("unauthorized/cancelled acquisition contacted provider")
	}
}
func TestTransportFailureIsSanitizedAndNotRetried(t *testing.T) {
	t.Parallel()
	c := testCollector(t)
	calls := 0
	c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("Bearer test-only-access-token private@example.invalid")
	})
	_, err := c.collect(context.Background(), testNow)
	requireFailure(t, err, "transport")
	if calls != 1 || strings.Contains(err.Error(), "test-only") || strings.Contains(err.Error(), "private@") {
		t.Fatal("failure leaked or retried")
	}
	c2 := testCollector(t)
	transport := c2.client.Transport.(*http.Transport)
	if !transport.DisableKeepAlives || !transport.Protocols.HTTP1() || transport.Protocols.HTTP2() {
		t.Fatal("native transport permits hidden connection or stream retry")
	}
}

func TestRunStatusRestartRetainsSourceTimeAndBackoff(t *testing.T) {
	t.Parallel()
	c := testCollector(t)
	serveGood(t, c, goodUsage, func(r *http.Request) {
		if r.URL.Path == "/api/oauth/usage" {
			c.now = func() time.Time { return testNow.Add(2 * time.Second) }
		}
	})
	stop := errors.New("test stop")
	var waited time.Duration
	c.wait = func(_ context.Context, d time.Duration) error { waited = d; return stop }
	output := make(chan domain.QuotaSnapshot, 10)
	if err := c.Run(context.Background(), output); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if len(output) != 3 || waited != 298*time.Second {
		t.Fatalf("batch=%d delay=%s", len(output), waited)
	}
	start := c.status.LastSuccessStartedAt
	if start != testNow || c.status.LastSuccessCompletedAt != testNow.Add(2*time.Second) {
		t.Fatal("source timestamps restamped")
	}
	body, err := os.ReadFile(c.options.StatusPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"test-only", "private@example", testAccount, "refreshToken", "accessToken"} {
		if strings.Contains(string(body), secret) {
			t.Fatal("unsanitized status")
		}
	}
	info, _ := os.Stat(c.options.StatusPath)
	if info.Mode().Perm() != 0600 {
		t.Fatal("status not private")
	}
	// A new process loads successful history, records failure, and preserves
	// the original success timestamp and a server-directed cooldown.
	c.now = func() time.Time { return testNow.Add(5 * time.Minute) }
	c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		r := response(429, "sensitive body")
		r.Header.Set("Retry-After", "900")
		return r, nil
	})
	if err := c.Run(context.Background(), output); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if len(output) != 3 || c.status.State != "failed" || c.status.ErrorCode != "rate-limited" || c.status.LastSuccessStartedAt != start || waited != 900*time.Second {
		t.Fatalf("bad failure status=%+v delay=%s", c.status, waited)
	}
	calls := 0
	c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("should not execute") })
	if err := c.Run(context.Background(), output); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if calls != 0 || waited != 900*time.Second {
		t.Fatal("restart ignored durable cooldown")
	}
}
func TestStatusFailureSuppressesValidBatch(t *testing.T) {
	t.Parallel()
	c := testCollector(t)
	serveGood(t, c, goodUsage, func(r *http.Request) {
		// Create the blocker after loading status, before atomic persistence.
		if r.URL.Path == "/api/oauth/usage" {
			if err := os.Mkdir(c.options.StatusPath, 0700); err != nil {
				t.Fatal(err)
			}
		}
	})
	output := make(chan domain.QuotaSnapshot, 10)
	if err := c.Run(context.Background(), output); err == nil || len(output) != 0 {
		t.Fatal("status failure emitted a batch")
	}
}
func TestHTTPFailuresNoRedirectRetryOrHealthyBatch(t *testing.T) {
	t.Parallel()
	for _, status := range []int{301, 401, 403, 429, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			c := testCollector(t)
			calls := 0
			c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				r := response(status, "secret")
				r.Header.Set("Location", "https://other.invalid")
				return r, nil
			})
			output := make(chan domain.QuotaSnapshot, 10)
			c.wait = func(context.Context, time.Duration) error { return context.Canceled }
			if err := c.Run(context.Background(), output); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if calls != 1 || len(output) != 0 || c.status.HTTPStatus != status || c.status.State != "failed" {
				t.Fatalf("calls=%d batch=%d status=%+v", calls, len(output), c.status)
			}
		})
	}
}
