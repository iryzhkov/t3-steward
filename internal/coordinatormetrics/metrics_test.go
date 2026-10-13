package coordinatormetrics

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fixtureLoader struct {
	snapshot Snapshot
	err      error
	calls    int
}

func (f *fixtureLoader) LoadMetricsSnapshot(context.Context) (Snapshot, error) {
	f.calls++
	return f.snapshot, f.err
}
func scrape(c *Cache) (int, string) {
	r := httptest.NewRecorder()
	c.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return r.Code, r.Body.String()
}
func TestCacheImmutableFailedRefreshAndStaleness(t *testing.T) {
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	loader := &fixtureLoader{snapshot: Snapshot{Tasks: map[string]int64{"active": 1}}}
	cache := NewCache(loader, time.Minute, time.Minute)
	cache.now = func() time.Time { return now }
	if code, _ := scrape(cache); code != 503 {
		t.Fatal(code)
	}
	cache.Refresh(context.Background())
	loader.snapshot.Tasks["active"] = 99
	for i := 0; i < 3; i++ {
		code, body := scrape(cache)
		if code != 200 || !strings.Contains(body, "steward_tasks{state=\"active\"} 1") {
			t.Fatal(code, body)
		}
	}
	if loader.calls != 1 {
		t.Fatal("HTTP performed durable reads", loader.calls)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	cache.Refresh(cancelled)
	_, cancelledBody := scrape(cache)
	if !strings.Contains(cancelledBody, "steward_metrics_refresh_success 0") || !strings.Contains(cancelledBody, "steward_tasks{state=\"active\"} 1") {
		t.Fatal("cancelled snapshot published", cancelledBody)
	}
	loader.err = errors.New("private source details")
	cache.Refresh(context.Background())
	_, body := scrape(cache)
	if !strings.Contains(body, "steward_metrics_refresh_success 0") || strings.Contains(body, "private") {
		t.Fatal(body)
	}
	now = now.Add(MaxSnapshotAge + time.Second)
	code, body := scrape(cache)
	if code != 503 || strings.Contains(body, "steward_tasks") {
		t.Fatal(code, body)
	}
}
func TestFreshQuotaMergeDedupAndPrivacy(t *testing.T) {
	now := time.Now().UTC()
	key := domain.BucketKey{ProviderInstanceID: "secret-instance", AccountID: "private-account", Window: "five_hour"}
	old := now.Add(-30 * time.Second)
	s := Snapshot{Buckets: []domain.BucketState{{Key: key, UsedPercent: 99, ObservedAt: old}},
		Workers: []domain.WorkerSnapshot{
			{WorkerID: "private-host-a", Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute), QuotaObservations: []domain.WorkerQuotaObservation{{Key: key, UsedPercent: 20, ObservedAt: now}}},
			{WorkerID: "private-host-b", Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute), QuotaObservations: []domain.WorkerQuotaObservation{{Key: key, UsedPercent: 90, ObservedAt: old}}},
			{WorkerID: "expired", Connected: true, ObservedAt: now.Add(-time.Hour), ValidUntil: now.Add(-time.Minute), QuotaObservations: []domain.WorkerQuotaObservation{{Key: key, UsedPercent: 100, ObservedAt: now.Add(time.Second)}}},
		}}
	body := render(s, now, time.Minute, time.Minute)
	for _, want := range []string{"steward_quota_observations{window=\"five_hour\"} 1", "steward_quota_worst_used_ratio{window=\"five_hour\"} 0.2", "steward_workers{state=\"fresh\"} 2", "steward_token_aggregation_supported 0"} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in %s", want, body)
		}
	}
	for _, secret := range []string{"private-host-a", "private-host-b", "private-account", "secret-instance"} {
		if strings.Contains(body, secret) {
			t.Fatal("identity exposed", body)
		}
	}
	body = render(s, now.Add(2*time.Minute), time.Minute, time.Minute)
	if strings.Contains(body, "steward_quota_worst_used_ratio{window=") {
		t.Fatal("stale quota exposed", body)
	}
}
func TestBindAndDisabledListener(t *testing.T) {
	for _, bind := range []string{"0.0.0.0:9469", "localhost:9469", "192.168.1.5:9469", "[::]:9469", "127.0.0.1:bad"} {
		if ValidateBind(bind) == nil {
			t.Fatal(bind)
		}
	}
	for _, bind := range []string{"", "127.0.0.1:9469", "[::1]:9469"} {
		if err := ValidateBind(bind); err != nil {
			t.Fatal(err)
		}
	}
	l, err := Start(context.Background(), "", nil, 0, 0)
	if l != nil || err != nil {
		t.Fatal(l, err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestOnlyMetricsGet(t *testing.T) {
	cache := NewCache(&fixtureLoader{}, time.Minute, time.Minute)
	for _, tc := range []struct {
		method, path string
		code         int
	}{{"GET", "/admin", 404}, {"POST", "/metrics", 405}} {
		r := httptest.NewRecorder()
		cache.ServeHTTP(r, httptest.NewRequest(tc.method, tc.path, nil))
		if r.Code != tc.code {
			t.Fatal(r.Code)
		}
	}
}
