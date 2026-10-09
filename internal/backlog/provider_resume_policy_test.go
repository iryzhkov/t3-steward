package backlog

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type providerResumeSource struct {
	snapshots  []domain.WorkerSnapshot
	admissions []domain.QuotaAdmissionRecord
}

func (s providerResumeSource) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	return s.snapshots, nil
}

func (s providerResumeSource) LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error) {
	return s.admissions, nil
}

// A worker that resumes sessions after provider errors is sent the
// coordinator's maximum and every pool whose admission is closed or
// draining; an older worker is sent nothing new.
func TestParkedAssignmentsCarryTheProviderResumePolicy(t *testing.T) {
	source := providerResumeSource{
		snapshots: []domain.WorkerSnapshot{
			{WorkerID: "homelab", Inventory: domain.WorkerInventory{Capabilities: []string{workerproto.CapabilityProviderResume}}},
			{WorkerID: "old", Inventory: domain.WorkerInventory{Capabilities: []string{workerproto.CapabilityTurnEndCommands}}},
		},
		admissions: []domain.QuotaAdmissionRecord{
			{QuotaPoolID: "codex-main", Admission: domain.AdmissionDraining, Reason: "five_hour at 91%"},
			{QuotaPoolID: "claude-main", Admission: domain.AdmissionClosed, Reason: "seven_day at 99%"},
			{QuotaPoolID: "open-pool", Admission: domain.AdmissionOpen},
		},
	}
	coordinator := FleetCoordinator{Store: nil}
	request, err := ParkedAssignmentsFor(context.Background(), source, "homelab")
	if err != nil {
		t.Fatal(err)
	}
	policy := request.ProviderResume
	if policy == nil || policy.MaxResumes != domain.DefaultProviderResumeMax || policy.MaxDelaySeconds != int64(domain.MaxProviderResumeDelay/time.Second) {
		t.Fatalf("policy = %+v", policy)
	}
	if len(policy.ClosedPools) != 2 || policy.ClosedPools[0].PoolID != "claude-main" || policy.ClosedPools[1].Admission != domain.AdmissionDraining {
		t.Fatalf("closed pools = %+v", policy.ClosedPools)
	}
	if err := workerproto.ValidateSnapshotRequest(request); err != nil {
		t.Fatal(err)
	}

	// The configured maximum replaces the default, zero included.
	coordinator.ProviderResume = ProviderResumeLimits{Configured: true, MaxResumes: 0, MaxDelay: 10 * time.Minute}
	coordinator.ProviderResume.apply(policy)
	if policy.MaxResumes != 0 || policy.MaxDelaySeconds != 600 {
		t.Fatalf("configured policy = %+v", policy)
	}

	older, err := ParkedAssignmentsFor(context.Background(), source, "old")
	if err != nil {
		t.Fatal(err)
	}
	if older.ProviderResume != nil {
		t.Fatalf("an older worker was sent a resume policy: %+v", older.ProviderResume)
	}
}
