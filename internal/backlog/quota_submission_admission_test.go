package backlog

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"testing"
	"time"
)

func quotaSubmissionFixture() (SubmissionQuotaAdmission, sqlite.QuotaAdmissionSnapshot, sqlite.CoordinatorRecords) {
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	key := domain.BucketKey{ProviderInstanceID: "codex", Window: domain.WindowPrimary}
	pool := domain.QuotaPool{ID: "pool", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, Admission: domain.AdmissionOpen}
	state := domain.BucketState{Key: key, ObservedAt: now, UsedPercent: 70}
	cost := 20.0
	records := sqlite.CoordinatorRecords{Tasks: []domain.Task{{ID: "task", Name: "root", EstimatedCost: &cost, Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", QuotaPoolID: "pool"}}}}, Attempts: []domain.Attempt{{TaskID: "task", Progress: domain.ProgressReady, Control: domain.ControlUnassigned}}}
	return SubmissionQuotaAdmission{Bridge: QuotaBridge{SafetyMargin: 5, LongWindowCap: 100}, Now: func() time.Time { return now }}, sqlite.QuotaAdmissionSnapshot{Records: sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{pool}, WorkflowRuns: []domain.WorkflowRun{{}}}, States: []domain.BucketState{state}, StaleAfter: time.Hour}, records
}

func TestSubmissionQuotaFuturePendingDemandDoesNotReserveHeadroom(t *testing.T) {
	admission, snapshot, records := quotaSubmissionFixture()
	task := records.Tasks[0]
	future := admission.Now().Add(time.Hour)
	cost := 10.0
	task.ID = "pending"
	task.NotBefore = &future
	task.EstimatedCost = &cost
	snapshot.Records.Tasks = []domain.Task{task}
	snapshot.Records.Attempts = []domain.Attempt{{TaskID: task.ID, Progress: domain.ProgressReady, Control: domain.ControlUnassigned}}
	receipt, err := admission.evaluate(records, snapshot)
	if err != nil {
		t.Fatalf("future pending work reserved current headroom: %v", err)
	}
	if receipt.Pools[0].AlreadyAdmittedDemand != 0 || receipt.Pools[0].RootCost != 20 {
		t.Fatalf("future demand receipt: %#v", receipt)
	}
}

func TestSubmissionQuotaSharedHeadroomAndTruth(t *testing.T) {
	for _, mode := range []string{"demand", "stale", "missing", "exhausted"} {
		t.Run(mode, func(t *testing.T) {
			admission, snapshot, records := quotaSubmissionFixture()
			switch mode {
			case "demand":
				snapshot.Records.Tasks = records.Tasks
				snapshot.Records.Attempts = records.Attempts
			case "stale":
				snapshot.States[0].ObservedAt = snapshot.States[0].ObservedAt.Add(-2 * time.Hour)
			case "missing":
				snapshot.States = nil
			case "exhausted":
				snapshot.States[0].UsedPercent = 100
			}
			if _, err := admission.evaluate(records, snapshot); err == nil {
				t.Fatal("quota-invalid submission accepted")
			}
		})
	}
}

func TestSubmissionQuotaReadyStateDefinesRoots(t *testing.T) {
	admission, snapshot, records := quotaSubmissionFixture()
	records.Tasks[0].ExternalNeeds = []domain.NodeRef{{}}
	records.Attempts[0].Progress = domain.ProgressBlocked
	snapshot.States[0].UsedPercent = 94
	receipt, err := admission.evaluate(records, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipt.Roots) != 0 || len(receipt.Pools) != 1 || receipt.Pools[0].RootCost != 0 || receipt.Pools[0].CampaignBudgetCeiling != 20 {
		t.Fatalf("external-only task reserved headroom: %#v", receipt)
	}
}

func TestSubmissionQuotaRouteFallbackAndRecordedDemand(t *testing.T) {
	admission, snapshot, records := quotaSubmissionFixture()
	records.Tasks[0].Routes = append([]domain.ProviderRoute{{ProviderInstanceID: "missing", QuotaPoolID: "missing"}}, records.Tasks[0].Routes...)
	receipt, err := admission.evaluate(records, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipt.Roots) != 1 || receipt.Roots[0].Route.QuotaPoolID != "pool" {
		t.Fatalf("route fallback: %#v", receipt)
	}
	snapshot.Records.Tasks = records.Tasks
	snapshot.Records.Attempts = records.Attempts
	snapshot.Records.Attempts[0].Control = domain.ControlRunning
	if _, err := admission.evaluate(records, snapshot); err != nil {
		t.Fatalf("assigned demand counted: %v", err)
	}
	snapshot.Records.Attempts[0].Control = domain.ControlUnassigned
	snapshot.Records.Attempts[0].Progress = domain.ProgressSucceeded
	if _, err := admission.evaluate(records, snapshot); err != nil {
		t.Fatalf("terminal demand counted: %v", err)
	}
}
