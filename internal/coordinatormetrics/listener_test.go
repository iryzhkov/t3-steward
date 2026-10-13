package coordinatormetrics

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type readyLoader struct{ ready chan struct{} }

func (f readyLoader) LoadMetricsSnapshot(context.Context) (Snapshot, error) {
	close(f.ready)
	return Snapshot{}, nil
}
func TestLoopbackListenerLifecycle(t *testing.T) {
	ready := make(chan struct{})
	listener, err := Start(context.Background(), "127.0.0.1:0", readyLoader{ready}, time.Minute, time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	<-ready
	response, err := http.Get("http://" + listener.listener.Addr().String() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	// The loader signals before the cache is committed, so first readiness may
	// still be503; either response must have the metrics freshness contract.
	if response.StatusCode != 200 && response.StatusCode != 503 {
		t.Fatal(response.StatusCode)
	}
	if !strings.Contains(string(body), "steward_metrics_snapshot_fresh") {
		t.Fatal(string(body))
	}
}
func TestConcurrentScrapesAndRefresh(t *testing.T) {
	cache := NewCache(&fixtureLoader{snapshot: Snapshot{Tasks: map[string]int64{"active": 1}}}, time.Minute, time.Minute, nil)
	cache.Refresh(context.Background())
	var wg sync.WaitGroup
	for n := 0; n < 10; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				code, body := scrape(cache)
				if code != 200 || !strings.Contains(body, "steward_tasks{state=\"active\"} 1") {
					t.Errorf("incomplete cache published: %d", code)
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		cache.Refresh(context.Background())
	}
	wg.Wait()
}
