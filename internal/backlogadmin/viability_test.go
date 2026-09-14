package backlogadmin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const (
	viabilityDesiredDigest  = "1111111111111111111111111111111111111111111111111111111111111111"
	viabilityAcceptedDigest = "2222222222222222222222222222222222222222222222222222222222222222"
)

var viabilityNow = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

func viabilityCatalog(t *testing.T, credentials ...string) ViabilitySettings {
	t.Helper()
	return ViabilitySettings{
		Projects: []backlog.ProjectDefinition{{
			Name: "t3-steward", Repository: "https://github.com/iryzhkov/t3-steward",
			DefaultRef: "main", SetupProfile: "go", RequiredCredentials: credentials,
		}},
		SetupProfiles: []backlog.SetupProfile{{
			Name: "go", Commands: []string{"go build ./..."}, Timeout: time.Minute,
		}},
	}
}

// withRepositoryObserver adds one observer to the settings under test.
func withRepositoryObserver(settings ViabilitySettings, observer RepositoryObserver) ViabilitySettings {
	settings.Repository = observer
	return settings
}

// withBundleLimits adds the coordinator's message limits.
func withBundleLimits(settings ViabilitySettings, bytes int64, files int) ViabilitySettings {
	settings.MaxBundleBytes, settings.MaxBundleFiles = bytes, files
	return settings
}

// viabilityWorkerSnapshot is a fresh, connected, fully capable worker.
func viabilityWorkerSnapshot() domain.WorkerSnapshot {
	return domain.WorkerSnapshot{
		WorkerID: "homelab", WorkerEpoch: "worker-1", CoordinatorEpoch: 7, Connected: true,
		ObservedAt: viabilityNow, ValidUntil: viabilityNow.Add(time.Minute),
		Inventory: domain.WorkerInventory{
			ID: "homelab", AcceptBacklog: true, Health: domain.WorkerHealthReady,
			CatalogRevision: viabilityDesiredDigest,
			Epoch:           "worker-1",
			Capabilities:    []string{"git", "huyang"},
			Projects:        []domain.WorkerProjectInventory{{Name: "t3-steward", Available: true}},
			Providers: []domain.WorkerProviderInventory{{
				InstanceID: "t3-primary", Models: []string{"opus"},
				QuotaPoolID: "pool-1", Available: true,
			}},
			ObservedAt: viabilityNow,
		},
	}
}

func viabilityView(t *testing.T, mutate func(*view)) view {
	return viabilityViewWith(t, nil, mutate)
}

// viabilityViewWith builds the fleet the matrix is composed from. The record
// mutator runs before the view is built, because the view indexes the records
// once at construction.
func viabilityViewWith(t *testing.T, records func(*sqlite.CoordinatorRecords), mutate func(*view)) view {
	t.Helper()
	stored := sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{{
		ID: "pool-1", ProviderInstanceIDs: []string{"t3-primary"},
		MaxConcurrent: 4, Admission: domain.AdmissionOpen,
	}}}
	if records != nil {
		records(&stored)
	}
	v := newView(stored, []domain.WorkerSnapshot{viabilityWorkerSnapshot()}, nil,
		RuntimeInfo{Epoch: 7, MaxWorkerSnapshotAge: time.Minute}, viabilityNow)
	v.requirements = []domain.WorkerRequirement{{
		WorkerID: "homelab", WorkerEpoch: "worker-1", CatalogRevision: viabilityDesiredDigest,
		CredentialRef: "secretref:f02-protocol/homelab", Connection: "persistent-ssh",
	}}
	v.enrollments = []domain.WorkerEnrollment{{
		Request:  domain.WorkerEnrollmentRequest{WorkerID: "homelab", CatalogRevision: viabilityDesiredDigest},
		Revision: 3, WorkerEpoch: "worker-1",
		CredentialRef: "secretref:f02-protocol/homelab", Connection: "persistent-ssh",
	}}
	if mutate != nil {
		mutate(&v)
	}
	return v
}

func viabilityTaskRequest() ViabilityTask {
	return ViabilityTask{
		Name: "implement", Project: "t3-steward", Ref: "main",
		Class:        domain.TaskClassSurplus,
		Capabilities: []string{"git"},
		Routes: []domain.ProviderRoute{{
			ProviderInstanceID: "t3-primary", Model: "opus", QuotaPoolID: "pool-1",
		}},
	}
}

func candidateReason(t *testing.T, matrix ViabilityMatrix, code string) (ViabilityReason, bool) {
	t.Helper()
	for _, task := range matrix.Tasks {
		for _, reason := range task.Reasons {
			if reason.Code == code {
				return reason, true
			}
		}
		for _, candidate := range task.Candidates {
			for _, reason := range candidate.Reasons {
				if reason.Code == code {
					return reason, true
				}
			}
		}
	}
	for _, reason := range matrix.Reasons {
		if reason.Code == code {
			return reason, true
		}
	}
	return ViabilityReason{}, false
}

func TestViabilityReportsAReadyFleet(t *testing.T) {
	v := viabilityView(t, nil)
	matrix := v.viability(context.Background(), viabilityCatalog(t),
		ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
	if matrix.Outcome != ViabilityReady {
		t.Fatalf("outcome = %q, reasons %+v", matrix.Outcome, matrix.Tasks[0].Candidates)
	}
	if matrix.SchemaVersion != ViabilityMatrixSchemaVersion {
		t.Fatalf("schema version = %d", matrix.SchemaVersion)
	}
	if len(matrix.Tasks) != 1 || len(matrix.Tasks[0].Candidates) != 1 {
		t.Fatalf("matrix = %+v", matrix)
	}
}

// TestViabilityReportsDriftAsDrift is the blind spot this query exists to
// close. A worker whose accepted catalog digest no longer matches its
// requirement must be reported as drift, with both digests and the expected
// revision, and never as an absent or ineligible worker.
func TestViabilityReportsDriftAsDrift(t *testing.T) {
	v := viabilityView(t, func(v *view) {
		v.enrollments[0].Request.CatalogRevision = viabilityAcceptedDigest
	})
	matrix := v.viability(context.Background(), viabilityCatalog(t),
		ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})

	reason, found := candidateReason(t, matrix, ReasonCatalogDigestMismatch)
	if !found {
		t.Fatalf("drift was not reported: %+v", matrix.Tasks[0].Candidates)
	}
	if reason.Desired != viabilityDesiredDigest || reason.Observed != viabilityAcceptedDigest {
		t.Fatalf("digests = desired %q observed %q", reason.Desired, reason.Observed)
	}
	if reason.Revision != 3 {
		t.Fatalf("expected revision = %d, want 3", reason.Revision)
	}
	if reason.Permanent {
		t.Fatal("drift must be temporary: re-enrolling the worker fixes it")
	}
	if _, found := candidateReason(t, matrix, ReasonWorkerNotEligible); found {
		t.Fatal("drift was also reported as no eligible worker")
	}
	if _, found := candidateReason(t, matrix, ReasonNoConfiguredRoute); found {
		t.Fatal("drift was reported as a missing route")
	}
	if matrix.Outcome != ViabilityAcceptedWaiting {
		t.Fatalf("outcome = %q, want %q", matrix.Outcome, ViabilityAcceptedWaiting)
	}
}

func TestViabilityFindings(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*view)
		task    func(*ViabilityTask)
		code    string
		outcome ViabilityOutcome
	}{
		{
			name: "an unknown project is permanent",
			task: func(task *ViabilityTask) { task.Project = "absent" },
			code: ReasonUnknownProject, outcome: ViabilityImpossible,
		},
		{
			name: "a missing capability is permanent",
			task: func(task *ViabilityTask) { task.Capabilities = []string{"cuda"} },
			code: ReasonCapabilityMissing, outcome: ViabilityImpossible,
		},
		{
			name: "an unreachable cpu class is permanent",
			task: func(task *ViabilityTask) {
				task.Resources = domain.ResourceDemand{MinCPUClass: domain.CPUClassHigh}
			},
			code: ReasonCPUClassImpossible, outcome: ViabilityImpossible,
		},
		{
			name: "an unavailable host allowlist is permanent",
			task: func(task *ViabilityTask) { task.Hosts = []string{"other-host"} },
			code: ReasonWorkerNotEligible, outcome: ViabilityImpossible,
		},
		{
			name: "an unknown provider instance is permanent",
			task: func(task *ViabilityTask) {
				task.Routes = []domain.ProviderRoute{{ProviderInstanceID: "absent", Model: "opus"}}
			},
			code: ReasonUnknownProviderInstance, outcome: ViabilityImpossible,
		},
		{
			name: "an unknown model is permanent",
			task: func(task *ViabilityTask) {
				task.Routes = []domain.ProviderRoute{{ProviderInstanceID: "t3-primary", Model: "absent"}}
			},
			code: ReasonUnknownModel, outcome: ViabilityImpossible,
		},
		{
			name: "an unknown quota pool is permanent",
			task: func(task *ViabilityTask) {
				task.Routes = []domain.ProviderRoute{{
					ProviderInstanceID: "t3-primary", Model: "opus", QuotaPoolID: "absent",
				}}
			},
			code: ReasonUnknownQuotaPool, outcome: ViabilityImpossible,
		},
		{
			name: "a closed timing window is permanent",
			task: func(task *ViabilityTask) {
				expired := viabilityNow.Add(-time.Hour)
				task.ExpiresAt = &expired
			},
			code: ReasonTimingWindowClosed, outcome: ViabilityImpossible,
		},
		{
			name: "a start window that has not opened is temporary",
			task: func(task *ViabilityTask) {
				later := viabilityNow.Add(time.Hour)
				task.NotBefore = &later
			},
			code: ReasonTimingWindowNotOpen, outcome: ViabilityAcceptedWaiting,
		},
		{
			name: "a closed quota pool is temporary",
			mutate: func(v *view) {
				v.records.QuotaPools[0].Admission = domain.AdmissionClosed
			},
			code: ReasonQuotaClosed, outcome: ViabilityAcceptedWaiting,
		},
		{
			name: "an exhausted worker is temporary",
			task: func(task *ViabilityTask) {
				task.Resources = domain.ResourceDemand{CPUUnits: 64}
			},
			code: ReasonWorkerAtCapacity, outcome: ViabilityAcceptedWaiting,
		},
		{
			name:   "a disconnected worker is temporary",
			mutate: func(v *view) { v.workers[0].Connected = false },
			code:   ReasonWorkerStale, outcome: ViabilityAcceptedWaiting,
		},
		{
			name: "a degraded worker is temporary",
			mutate: func(v *view) {
				v.workers[0].Inventory.Health = domain.WorkerHealthDegraded
			},
			code: ReasonWorkerOffline, outcome: ViabilityAcceptedWaiting,
		},
		{
			name:   "a worker that never enrolled is permanent",
			mutate: func(v *view) { v.enrollments = nil },
			code:   ReasonWorkerNotEligible, outcome: ViabilityImpossible,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := viabilityView(t, test.mutate)
			task := viabilityTaskRequest()
			if test.task != nil {
				test.task(&task)
			}
			matrix := v.viability(context.Background(), viabilityCatalog(t),
				ViabilityRequest{Tasks: []ViabilityTask{task}})
			reason, found := candidateReason(t, matrix, test.code)
			if !found {
				t.Fatalf("reason %q was not reported: task %+v candidates %+v",
					test.code, matrix.Tasks[0].Reasons, matrix.Tasks[0].Candidates)
			}
			if reason.Permanent != PermanentViabilityReason(test.code) {
				t.Fatalf("reason %q permanence = %t, want %t",
					test.code, reason.Permanent, PermanentViabilityReason(test.code))
			}
			if matrix.Outcome != test.outcome {
				t.Fatalf("outcome = %q, want %q", matrix.Outcome, test.outcome)
			}
		})
	}
}

// stubRepositoryObserver answers with one fixed classification.
type stubRepositoryObserver struct {
	class backlog.RepositoryReachability
	keys  []backlog.RepositoryProbeKey
}

func (o *stubRepositoryObserver) ObserveRepository(_ context.Context, key backlog.RepositoryProbeKey) (backlog.RepositoryProbeObservation, error) {
	o.keys = append(o.keys, key)
	return backlog.RepositoryProbeObservation{Key: key, Class: o.class}, nil
}

func TestViabilityReportsRepositoryReachability(t *testing.T) {
	tests := []struct {
		class   backlog.RepositoryReachability
		code    string
		outcome ViabilityOutcome
	}{
		{class: backlog.RepositoryAuthenticatedOK, code: "", outcome: ViabilityReady},
		{class: backlog.RepositoryAuthenticationFailed, code: ReasonRepositoryAuthFailed, outcome: ViabilityImpossible},
		{class: backlog.RepositoryNotFound, code: ReasonRepositoryNotFound, outcome: ViabilityImpossible},
		{class: backlog.RepositoryRefNotFound, code: ReasonRefNotFound, outcome: ViabilityImpossible},
		{class: backlog.RepositoryProbeTimeout, code: ReasonProbeTimeout, outcome: ViabilityAcceptedWaiting},
		{class: backlog.RepositoryDNSFailure, code: ReasonDNSFailure, outcome: ViabilityAcceptedWaiting},
		{class: backlog.RepositoryNetworkUnavailable, code: ReasonNetworkUnavailable, outcome: ViabilityAcceptedWaiting},
	}
	for _, test := range tests {
		t.Run(string(test.class), func(t *testing.T) {
			observer := &stubRepositoryObserver{class: test.class}
			v := viabilityView(t, nil)
			matrix := v.viability(context.Background(),
				withRepositoryObserver(viabilityCatalog(t, "git-github"), observer),
				ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
			if matrix.Outcome != test.outcome {
				t.Fatalf("outcome = %q, want %q (%+v)", matrix.Outcome, test.outcome, matrix.Tasks[0].Candidates)
			}
			if test.code != "" {
				if _, found := candidateReason(t, matrix, test.code); !found {
					t.Fatalf("reason %q was not reported", test.code)
				}
			}
			if len(observer.keys) != 1 {
				t.Fatalf("probe keys = %d, want 1", len(observer.keys))
			}
			key := observer.keys[0]
			if key.WorkerID != "homelab" || key.CatalogDigest != viabilityDesiredDigest ||
				key.Repository != "https://github.com/iryzhkov/t3-steward" || key.Ref != "main" ||
				len(key.CredentialRefs) != 1 {
				t.Fatalf("probe key = %+v", key)
			}
		})
	}
}

// TestViabilityDoesNotProbeADriftedWorker states that a worker the coordinator
// is already replacing is not asked about a repository: the answer would
// describe an execution identity that is on its way out.
func TestViabilityDoesNotProbeADriftedWorker(t *testing.T) {
	observer := &stubRepositoryObserver{class: backlog.RepositoryAuthenticatedOK}
	v := viabilityView(t, func(v *view) {
		v.enrollments[0].Request.CatalogRevision = viabilityAcceptedDigest
	})
	v.viability(context.Background(),
		withRepositoryObserver(viabilityCatalog(t), observer),
		ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
	if len(observer.keys) != 0 {
		t.Fatalf("a drifted worker was probed: %+v", observer.keys)
	}
}

// TestViabilityReportsAHeldLock states that a resource lock another attempt
// already holds only delays the work.
func TestViabilityReportsAHeldLock(t *testing.T) {
	v := viabilityViewWith(t, func(records *sqlite.CoordinatorRecords) {
		records.Workflows = []domain.Workflow{{ID: "wf-1", Project: "t3-steward"}}
		records.WorkflowRuns = []domain.WorkflowRun{{ID: "run-1", WorkflowID: "wf-1"}}
		records.Tasks = []domain.Task{{
			ID: "task-9", WorkflowID: "wf-1", RunID: "run-1", Name: "holder",
			ResourceLocks: []string{"repo:t3-steward"},
		}}
		records.Attempts = []domain.Attempt{{
			ID: "attempt-9", WorkflowRunID: "run-1", TaskID: "task-9", AssignmentID: "assignment-9",
			Progress: domain.ProgressActive, Control: domain.ControlRunning,
		}}
		records.Assignments = []domain.Assignment{{
			ID: "assignment-9", AttemptID: "attempt-9", WorkerID: "homelab",
			State: domain.AssignmentClaimed,
		}}
	}, nil)
	task := viabilityTaskRequest()
	task.ResourceLocks = []string{"repo:t3-steward"}
	matrix := v.viability(context.Background(), viabilityCatalog(t),
		ViabilityRequest{Tasks: []ViabilityTask{task}})
	reason, found := candidateReason(t, matrix, ReasonLockHeld)
	if !found || reason.Permanent {
		t.Fatalf("lock finding = %+v (found %t)", reason, found)
	}
	if matrix.Outcome != ViabilityAcceptedWaiting {
		t.Fatalf("outcome = %q", matrix.Outcome)
	}
}

func TestViabilityRefusesAnOversizedBundle(t *testing.T) {
	v := viabilityView(t, nil)
	matrix := v.viability(context.Background(),
		withBundleLimits(viabilityCatalog(t), 1024, 10),
		ViabilityRequest{
			Tasks:       []ViabilityTask{viabilityTaskRequest()},
			BundleBytes: 4096, BundleFiles: 40,
		})
	if matrix.Outcome != ViabilityImpossible {
		t.Fatalf("outcome = %q", matrix.Outcome)
	}
	if len(matrix.Reasons) != 2 {
		t.Fatalf("reasons = %+v", matrix.Reasons)
	}
	for _, reason := range matrix.Reasons {
		if reason.Code != ReasonMessageLimitExceeded || !reason.Permanent {
			t.Fatalf("reason = %+v", reason)
		}
	}
	if len(matrix.PermanentReasons()) != 2 {
		t.Fatalf("permanent reasons = %+v", matrix.PermanentReasons())
	}
}

func TestViabilityRefusesACredentiallessWorker(t *testing.T) {
	v := viabilityView(t, func(v *view) { v.enrollments[0].CredentialRef = "" })
	matrix := v.viability(context.Background(), viabilityCatalog(t, "git-github"),
		ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
	reason, found := candidateReason(t, matrix, ReasonCredentialMissing)
	if !found || !reason.Permanent {
		t.Fatalf("credential finding = %+v (found %t)", reason, found)
	}
	if strings.Contains(reason.Detail, "secretref:") {
		t.Fatalf("a credential reference leaked into the detail: %q", reason.Detail)
	}
}

// TestViabilityPermanenceTable pins every frozen reason code to the side of the
// permanent/temporary line the ADR puts it on.
func TestViabilityPermanenceTable(t *testing.T) {
	permanent := []string{
		ReasonUnknownProject, ReasonUnknownSetupProfile, ReasonUnknownProviderInstance,
		ReasonUnknownModel, ReasonUnknownQuotaPool, ReasonWorkerNotEligible,
		ReasonCapabilityMissing, ReasonCPUClassImpossible, ReasonResourcesImpossible,
		ReasonDirectoryImpossible, ReasonCredentialMissing, ReasonRepositorySyntaxInvalid,
		ReasonRepositoryAuthFailed, ReasonRepositoryNotFound, ReasonRefNotFound,
		ReasonNoConfiguredRoute, ReasonTimingWindowClosed, ReasonMessageLimitExceeded,
	}
	temporary := []string{
		ReasonQuotaClosed, ReasonWorkerAtCapacity, ReasonWorkerOffline, ReasonWorkerStale,
		ReasonNetworkUnavailable, ReasonDNSFailure, ReasonProbeTimeout, ReasonSnapshotStale,
		ReasonLockHeld, ReasonCatalogDigestMismatch, ReasonTimingWindowNotOpen,
	}
	for _, code := range permanent {
		if !PermanentViabilityReason(code) {
			t.Fatalf("%q must be permanent", code)
		}
	}
	for _, code := range temporary {
		if PermanentViabilityReason(code) {
			t.Fatalf("%q must be temporary", code)
		}
	}
	// A code this build has never heard of must not refuse a submission.
	if PermanentViabilityReason("a-code-from-a-later-release") {
		t.Fatal("an unknown code was treated as permanent")
	}
}
