// Package coordinatormetrics exposes bounded native coordinator observations.
// Scrapes read an immutable cache; they never query or reconcile durable state.
package coordinatormetrics

import (
	"context"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const RefreshInterval = 30 * time.Second
const MaxSnapshotAge = 90 * time.Second
const LoadTimeout = 5 * time.Second

type Snapshot struct {
	Tasks       map[string]int64
	Assignments map[string]int64
	Admissions  map[string]int64
	Workers     []domain.WorkerSnapshot
	Buckets     []domain.BucketState
	// UsageEvidence counts retained rows, not normalized token consumption.
	UsageEvidence          map[string]int64
	UsageFields            map[string]int64
	UsageEvidenceTruncated bool
	UsageEvidenceOldest    time.Time
}
type Loader interface {
	LoadMetricsSnapshot(context.Context) (Snapshot, error)
}

func ValidateBind(bind string) error {
	if bind == "" {
		return nil
	}
	host, port, err := net.SplitHostPort(bind)
	if err != nil {
		return errors.New("metrics.bind requires a loopback IP and port")
	}
	ip := net.ParseIP(host)
	n, err := strconv.Atoi(port)
	if ip == nil || !ip.IsLoopback() || err != nil || n < 0 || n > 65535 {
		return errors.New("metrics.bind requires a literal loopback IP and valid port")
	}
	return nil
}

type Cache struct {
	loader                    Loader
	workerMaxAge, quotaMaxAge time.Duration
	now                       func() time.Time
	mu                        sync.RWMutex
	body                      string
	at                        time.Time
	success                   bool
}

func NewCache(loader Loader, workerMaxAge, quotaMaxAge time.Duration) *Cache {
	return &Cache{loader: loader, workerMaxAge: workerMaxAge, quotaMaxAge: quotaMaxAge, now: time.Now}
}

// Refresh is called by the timer, never by an HTTP request. Only a successful
// complete read replaces the cache; a failed read cannot publish a partial view.
func (c *Cache) Refresh(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, LoadTimeout)
	defer cancel()
	snapshot, err := c.loader.LoadMetricsSnapshot(ctx)
	if err == nil {
		err = ctx.Err()
	}
	var body string
	at := c.now().UTC()
	if err == nil {
		body = render(snapshot, at, c.workerMaxAge, c.quotaMaxAge)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.success = err == nil
	if err == nil {
		c.body = body
		c.at = at
	}
}
func (c *Cache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/metrics" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	c.mu.RLock()
	body, at, success := c.body, c.at, c.success
	c.mu.RUnlock()
	age := c.now().Sub(at)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if at.IsZero() || age > MaxSnapshotAge || age < 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "# TYPE steward_metrics_snapshot_fresh gauge\nsteward_metrics_snapshot_fresh 0\n")
		return
	}
	fmt.Fprint(w, body)
	fresh := 1
	refresh := 0
	if success {
		refresh = 1
	}
	fmt.Fprintf(w, "# TYPE steward_metrics_snapshot_fresh gauge\nsteward_metrics_snapshot_fresh %d\n# TYPE steward_metrics_refresh_success gauge\nsteward_metrics_refresh_success %d\n# TYPE steward_metrics_snapshot_age_seconds gauge\nsteward_metrics_snapshot_age_seconds %g\n", fresh, refresh, age.Seconds())
}

type Listener struct {
	server   *http.Server
	listener net.Listener
	cancel   context.CancelFunc
	done     chan struct{}
}

func Start(ctx context.Context, bind string, loader Loader, workerMaxAge, quotaMaxAge time.Duration) (*Listener, error) {
	if bind == "" {
		return nil, nil
	}
	if err := ValidateBind(bind); err != nil {
		return nil, err
	}
	if loader == nil || workerMaxAge <= 0 || quotaMaxAge <= 0 {
		return nil, errors.New("metrics requires a loader and positive freshness limits")
	}
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	cache := NewCache(loader, workerMaxAge, quotaMaxAge)
	server := &http.Server{Handler: cache, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	result := &Listener{server: server, listener: listener, cancel: cancel, done: make(chan struct{})}
	go func() { _ = server.Serve(listener) }()
	go func() {
		defer close(result.done)
		cache.Refresh(ctx)
		timer := time.NewTicker(RefreshInterval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				cache.Refresh(ctx)
			}
		}
	}()
	return result, nil
}
func (l *Listener) Close() error {
	if l == nil {
		return nil
	}
	l.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), LoadTimeout)
	defer cancel()
	err := l.server.Shutdown(ctx)
	<-l.done
	return err
}

func render(s Snapshot, now time.Time, workerAge, quotaAge time.Duration) string {
	var b strings.Builder
	metric := func(name, help string) { fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name) }
	counts := func(name, help, label string, values map[string]int64, known []string) {
		metric(name, help)
		var other int64
		for key, n := range values {
			found := false
			for _, v := range known {
				if key == v {
					found = true
					break
				}
			}
			if !found {
				other += n
			}
		}
		for _, v := range known {
			fmt.Fprintf(&b, "%s{%s=%q} %d\n", name, label, v, values[v])
		}
		fmt.Fprintf(&b, "%s{%s=%q} %d\n", name, label, "other", other)
	}
	metric("steward_metrics_snapshot_timestamp_seconds", "Time of the last complete native snapshot.")
	fmt.Fprintf(&b, "steward_metrics_snapshot_timestamp_seconds %g\n", float64(now.UnixNano())/1e9)
	counts("steward_tasks", "Latest retained declared task attempts, not every retry.", "state", s.Tasks, []string{"queued", "blocked", "ready", "active", "needs-input", "waiting-external", "verifying", "succeeded", "failed", "cancelled", "skipped"})
	counts("steward_assignments", "Retained native assignment records.", "state", s.Assignments, []string{"offered", "claimed", "completed", "released"})
	counts("steward_quota_admissions", "Native pool admission records, without pool identities.", "state", s.Admissions, []string{"open", "constrained", "draining", "closed", "recovering", "unknown"})
	var connected, fresh int64
	var workers []domain.WorkerSnapshot
	for _, w := range s.Workers {
		if w.Connected {
			connected++
		}
		age := now.Sub(w.ObservedAt)
		if w.Connected && !w.ObservedAt.IsZero() && age >= 0 && age <= workerAge && w.ValidUntil.After(now) {
			fresh++
			workers = append(workers, w)
		}
	}
	metric("steward_workers", "Retained worker snapshots by freshness.")
	fmt.Fprintf(&b, "steward_workers{state=\"reported\"} %d\nsteward_workers{state=\"connected\"} %d\nsteward_workers{state=\"fresh\"} %d\n", len(s.Workers), connected, fresh)
	var stale, invalid int64
	used := map[string]float64{}
	observed := map[string]int64{}
	for _, v := range domain.MergeQuotaObservations(s.Buckets, workers) {
		age := now.Sub(v.ObservedAt)
		if v.ObservedAt.IsZero() || age < 0 || age > quotaAge {
			stale++
			continue
		}
		if math.IsNaN(v.UsedPercent) || math.IsInf(v.UsedPercent, 0) || v.UsedPercent < 0 || v.UsedPercent > 100 {
			invalid++
			continue
		}
		window := v.Key.Window
		switch window {
		case "five_hour", "seven_day", "seven_day_opus", "seven_day_sonnet", "weekly", "daily":
		default:
			window = "other"
		}
		observed[window]++
		if v.UsedPercent > used[window] {
			used[window] = v.UsedPercent
		}
	}
	metric("steward_quota_observations", "Fresh unique native buckets; stale and invalid observations are omitted.")
	metric("steward_quota_worst_used_ratio", "Largest provider-reported used percentage among fresh buckets for this window; not a sum.")
	for _, window := range []string{"five_hour", "seven_day", "seven_day_opus", "seven_day_sonnet", "weekly", "daily", "other"} {
		fmt.Fprintf(&b, "steward_quota_observations{window=%q} %d\n", window, observed[window])
		if observed[window] > 0 {
			fmt.Fprintf(&b, "steward_quota_worst_used_ratio{window=%q} %g\n", window, used[window]/100)
		}
	}
	metric("steward_quota_stale_observations", "Buckets excluded because freshness is unknown or expired.")
	fmt.Fprintf(&b, "steward_quota_stale_observations %d\n", stale)
	metric("steward_quota_invalid_observations", "Buckets excluded because the native percentage is invalid.")
	fmt.Fprintf(&b, "steward_quota_invalid_observations %d\n", invalid)
	counts("steward_usage_evidence_rows", "Latest bounded evidence rows after in-window local-forwarded duplicate removal; call and turn rows may overlap and are not tokens.", "kind", s.UsageEvidence, []string{"call", "turn", "diagnostic"})
	counts("steward_usage_field_coverage_rows", "Presence of token fields in retained evidence, not measured token totals.", "coverage", s.UsageFields, []string{"complete", "partial", "absent"})
	metric("steward_usage_evidence_truncated", "One when older evidence was excluded by the latest10000-row bound.")
	truncated := 0
	if s.UsageEvidenceTruncated {
		truncated = 1
	}
	fmt.Fprintf(&b, "steward_usage_evidence_truncated %d\n", truncated)
	if !s.UsageEvidenceOldest.IsZero() {
		metric("steward_usage_evidence_oldest_timestamp_seconds", "Oldest valid observation timestamp among the bounded evidence rows.")
		fmt.Fprintf(&b, "steward_usage_evidence_oldest_timestamp_seconds %g\n", float64(s.UsageEvidenceOldest.UnixNano())/1e9)
	}
	metric("steward_token_aggregation_supported", "Zero until a bounded native fleet usage normalization contract exists.")
	fmt.Fprint(&b, "steward_token_aggregation_supported 0\n")
	return b.String()
}
