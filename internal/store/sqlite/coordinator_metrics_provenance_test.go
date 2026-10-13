package sqlite

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/coordinatormetrics"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestMetricsProjectionPreservesAuthenticatedColumnAndAcquisition(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC()
	acquired, reset := now.Add(-2*time.Minute), now.Add(time.Hour)
	key := domain.BucketKey{ProviderInstanceID: "fixture-instance", AccountID: "00000000-0000-0000-0000-000000000000", Window: "five_hour"}
	provenance := &domain.QuotaProvenance{SourceID: "fixture-native", AcquisitionID: "fixture-acquisition", OriginWorkerID: "claimed-origin", AcquiredAt: acquired}
	worker := domain.WorkerSnapshot{
		WorkerID: "claimed-record-worker", Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute),
		QuotaObservations: []domain.WorkerQuotaObservation{{Key: key, Provenance: provenance, UsedPercent: 20, ObservedAt: acquired, ResetsAt: &reset}},
	}
	raw, err := json.Marshal(worker)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.db.Exec("INSERT INTO coordinator_worker_snapshots(worker_id,worker_epoch,coordinator_epoch,sequence,connected,valid_until,record)VALUES(?,?,?,?,?,?,?)",
		"authenticated-column-worker", "epoch", 1, 1, 1, worker.ValidUntil.Format(time.RFC3339Nano), string(raw))
	if err != nil {
		t.Fatal(err)
	}
	observer, err := OpenQueryOnly(store.path)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	snapshot, err := observer.LoadMetricsSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Workers) != 1 || snapshot.Workers[0].WorkerID != "authenticated-column-worker" {
		t.Fatalf("wrong trusted worker projection: %#v", snapshot.Workers)
	}
	merged := domain.MergeQuotaObservations(snapshot.Buckets, snapshot.Workers)
	if len(merged) != 1 || merged[0].Provenance == nil || merged[0].Provenance.OriginWorkerID != "authenticated-column-worker" ||
		merged[0].Provenance.AcquisitionID != "fixture-acquisition" || !merged[0].ObservedAt.Equal(acquired) ||
		!merged[0].Provenance.AcquiredAt.Equal(acquired) || merged[0].OriginValidUntil == nil ||
		!merged[0].OriginValidUntil.Equal(worker.ValidUntil) {
		t.Fatalf("acquisition/authenticated origin lost: %#v", merged)
	}
	pool := domain.QuotaPool{AccountID: key.AccountID, ProviderInstanceIDs: []string{key.ProviderInstanceID},
		Collector: &domain.QuotaCollectorCoverage{SourceID: "fixture-native", AccountID: key.AccountID,
			Origins:           []domain.QuotaCollectorOrigin{{WorkerID: "authenticated-column-worker", ProviderInstanceID: key.ProviderInstanceID}},
			MaxObservationAge: 390 * time.Second, RequiredWindows: []domain.QuotaCollectorWindow{{Window: "five_hour"}}}}
	cache := coordinatormetrics.NewCache(observer, time.Minute, time.Minute, []domain.QuotaPool{pool})
	cache.Refresh(context.Background())
	response := httptest.NewRecorder()
	cache.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	if response.Code != 200 || !strings.Contains(body, "steward_quota_observations{window=\"five_hour\"} 1\n") {
		t.Fatal(response.Code, body)
	}
	for _, private := range []string{"authenticated-column-worker", "claimed-record-worker", "claimed-origin", "fixture-acquisition", "fixture-native", key.AccountID, key.ProviderInstanceID} {
		if strings.Contains(body, private) {
			t.Fatal("identity exported", private)
		}
	}
	// The observer is already open: a later committed WAL update must be visible,
	// without replacing the read-only connection or restamping acquisition.
	worker.QuotaObservations[0].UsedPercent = 70
	raw, err = json.Marshal(worker)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec("UPDATE coordinator_worker_snapshots SET record=? WHERE worker_id=?", string(raw), "authenticated-column-worker"); err != nil {
		t.Fatal(err)
	}
	updated, err := observer.LoadMetricsSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if updated.Workers[0].QuotaObservations[0].UsedPercent != 70 || !updated.Workers[0].QuotaObservations[0].Provenance.AcquiredAt.Equal(acquired) {
		t.Fatal("WAL update or immutable acquisition lost", updated.Workers)
	}
}
