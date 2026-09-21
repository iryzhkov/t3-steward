package main

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// TestConsultationsPrefeatureCoordinatorMeasurements records the existing
// coordinator boundary over ordinary ready work and supervision. Each sample
// owns a migrated SQLite fixture; fixture construction is outside the timed
// region.
func TestConsultationsPrefeatureCoordinatorMeasurements(t *testing.T) {
	const samples = 30
	type sample struct {
		fixture *activationLeaseFixture
		cycle   coordinatorBoundaryCycle
	}
	fixtures := make([]sample, 0, samples)
	for i := 0; i < samples; i++ {
		fixture := newActivationLeaseFixture(t)
		records, err := fixture.store.LoadCoordinatorRecords(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for index := range records.Tasks {
			if records.Tasks[index].ID == "task-protected" {
				records.Tasks[index].Needs = []string{"producer"}
			}
		}
		if err := fixture.store.SaveCoordinatorRecords(context.Background(), records); err != nil {
			t.Fatal(err)
		}
		fixture.superviseRun(t)
		addActivationFairnessAttempt(t, fixture, "ordinary", "worker-ordinary", "ordinary-pool", fixture.now.Add(-time.Minute), false)
		quota := activationFairnessQuota(fixture.now,
			domain.QuotaPool{ID: "claude-main", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"}, Admission: domain.AdmissionOpen, MaxConcurrent: 1},
			domain.QuotaPool{ID: "ordinary-pool", Provider: "ordinary", ProviderInstanceIDs: []string{"instance-ordinary"}, Admission: domain.AdmissionOpen, MaxConcurrent: 1},
		)
		cycle := activationFairnessCycle(t, fixture, quota)
		fixtures = append(fixtures, sample{fixture: fixture, cycle: cycle})
	}

	ctx := context.Background()
	latencies := make([]int64, 0, samples)
	rssBeforeKB, rssBeforeOK := processRSSKB()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for _, sample := range fixtures {
		start := time.Now()
		sample.cycle.Tick(ctx)
		latencies = append(latencies, time.Since(start).Nanoseconds())
	}
	runtime.ReadMemStats(&after)
	rssAfterKB, rssAfterOK := processRSSKB()

	for _, sample := range fixtures {
		records, err := sample.fixture.store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var ordinaryOffered bool
		for _, assignment := range records.Assignments {
			if assignment.AttemptID == "attempt-ordinary" && assignment.State == domain.AssignmentOffered {
				ordinaryOffered = true
			}
		}
		if !ordinaryOffered {
			t.Fatalf("ordinary ready attempt received no offered assignment: %+v", records.Assignments)
		}
		state, err := sample.fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
		if err != nil {
			t.Fatal(err)
		}
		if state.Activation.State != domain.ActivationPendingDispatch {
			t.Fatalf("supervision activation = %q, want %q", state.Activation.State, domain.ActivationPendingDispatch)
		}
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var total int64
	for _, latency := range latencies {
		total += latency
	}
	load, _ := os.ReadFile("/proc/loadavg")
	receipt := map[string]any{
		"schema":                        "consultations-prefeature-coordinator-v1",
		"samples":                       samples,
		"fixture_creation_timed":        false,
		"coordinator_passes_per_sample": 1,
		"ordinary_ready_per_sample":     1,
		"supervised_runs_per_sample":    1,
		"median_ns":                     latencies[len(latencies)/2],
		"p95_ns":                        latencies[(len(latencies)*95-1)/100],
		"throughput_passes_per_second":  float64(samples) / (float64(total) / float64(time.Second)),
		"mallocs_delta":                 after.Mallocs - before.Mallocs,
		"total_alloc_bytes_delta":       after.TotalAlloc - before.TotalAlloc,
		"process_rss_before_kb":         nullableInt64(rssBeforeKB, rssBeforeOK),
		"process_rss_after_kb":          nullableInt64(rssAfterKB, rssAfterOK),
		"sql_statement_count":           nil,
		"transaction_count":             nil,
		"go_version":                    runtime.Version(),
		"gomaxprocs":                    runtime.GOMAXPROCS(0),
		"host_loadavg":                  strings.TrimSpace(string(load)),
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(raw))
}

func processRSSKB() (int64, bool) {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "VmRSS:" && fields[2] == "kB" {
			value, err := strconv.ParseInt(fields[1], 10, 64)
			return value, err == nil
		}
	}
	return 0, false
}

func nullableInt64(value int64, ok bool) any {
	if !ok {
		return nil
	}
	return value
}
