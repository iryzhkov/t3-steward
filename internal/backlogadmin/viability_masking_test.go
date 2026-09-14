package backlogadmin

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// normandyDigest is a third catalog digest, for the drifted-second-worker case.
const normandyDigest = "3333333333333333333333333333333333333333333333333333333333333333"

// perWorkerObserver answers differently per worker, which is the shape the
// masking bug needs: one worker observes a permanent failure and the other
// cannot be observed at all.
type perWorkerObserver struct {
	answers map[string]backlog.RepositoryReachability
	errors  map[string]error
	asked   []string
}

func (o *perWorkerObserver) ObserveRepository(_ context.Context, key backlog.RepositoryProbeKey) (backlog.RepositoryProbeObservation, error) {
	o.asked = append(o.asked, key.WorkerID)
	if err, ok := o.errors[key.WorkerID]; ok {
		return backlog.RepositoryProbeObservation{}, err
	}
	class, ok := o.answers[key.WorkerID]
	if !ok {
		return backlog.RepositoryProbeObservation{}, errors.New("no answer configured")
	}
	return backlog.RepositoryProbeObservation{Key: key, Class: class}, nil
}

// normandySnapshot is the second worker: a full peer of homelab.
func normandySnapshot() domain.WorkerSnapshot {
	snapshot := viabilityWorkerSnapshot()
	snapshot.WorkerID = "normandy"
	snapshot.Inventory.ID = "normandy"
	return snapshot
}

// twoWorkerView is the fleet shape the review reproduced on: homelab answers,
// normandy is present and, in each subtest, unobservable for a different
// ordinary reason.
func twoWorkerView(t *testing.T, mutate func(*view)) view {
	t.Helper()
	stored := sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{{
		ID: "pool-1", ProviderInstanceIDs: []string{"t3-primary"},
		MaxConcurrent: 4, Admission: domain.AdmissionOpen,
	}}}
	v := newView(stored,
		[]domain.WorkerSnapshot{viabilityWorkerSnapshot(), normandySnapshot()}, nil,
		RuntimeInfo{Epoch: 7, MaxWorkerSnapshotAge: time.Minute}, viabilityNow)
	for _, id := range []string{"homelab", "normandy"} {
		v.requirements = append(v.requirements, domain.WorkerRequirement{
			WorkerID: id, WorkerEpoch: "worker-1", CatalogRevision: viabilityDesiredDigest,
			CredentialRef: "secretref:f02-protocol/" + id, Connection: "persistent-ssh",
		})
		v.enrollments = append(v.enrollments, domain.WorkerEnrollment{
			Request:  domain.WorkerEnrollmentRequest{WorkerID: id, CatalogRevision: viabilityDesiredDigest},
			Revision: 3, WorkerEpoch: "worker-1",
			CredentialRef: "secretref:f02-protocol/" + id, Connection: "persistent-ssh",
		})
	}
	if mutate != nil {
		mutate(&v)
	}
	return v
}

// TestOneUnobservedWorkerDoesNotMaskAPermanentVerdict is the regression.
//
// homelab observes repository-not-found. normandy contributes no observation,
// for each of the ordinary reasons a real fleet produces. Before the fix the
// task outcome was accepted_waiting, submit returned no error and a workflow
// run was created for a campaign that could never prepare its workspace.
func TestOneUnobservedWorkerDoesNotMaskAPermanentVerdict(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*view)
		observer   func(*perWorkerObserver)
		wantReason string
	}{
		{
			name: "the second worker cannot be dialled",
			observer: func(o *perWorkerObserver) {
				o.errors["normandy"] = errors.New("dial normandy: connection refused")
			},
			wantReason: "could not be reached",
		},
		{
			name: "the second worker has reported no inventory",
			mutate: func(v *view) {
				v.workers = v.workers[:1]
			},
			wantReason: "reported no inventory",
		},
		{
			name: "the second worker has drifted",
			mutate: func(v *view) {
				for index := range v.enrollments {
					if v.enrollments[index].Request.WorkerID == "normandy" {
						v.enrollments[index].Request.CatalogRevision = normandyDigest
					}
				}
			},
			wantReason: "catalog the coordinator has replaced",
		},
		{
			name: "the second worker runs an older build that cannot answer",
			observer: func(o *perWorkerObserver) {
				o.errors["normandy"] = errors.New("worker does not support the repository probe")
			},
			wantReason: "could not be reached",
		},
		{
			name: "the second worker is stale and disconnected",
			mutate: func(v *view) {
				for index := range v.workers {
					if v.workers[index].WorkerID == "normandy" {
						v.workers[index].Connected = false
					}
				}
			},
			observer: func(o *perWorkerObserver) {
				o.errors["normandy"] = errors.New("dial normandy: no route to host")
			},
			wantReason: "could not be reached",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observer := &perWorkerObserver{
				answers: map[string]backlog.RepositoryReachability{
					"homelab": backlog.RepositoryNotFound,
				},
				errors: map[string]error{},
			}
			if test.observer != nil {
				test.observer(observer)
			}
			v := twoWorkerView(t, test.mutate)
			matrix := v.viability(context.Background(),
				withRepositoryObserver(viabilityCatalog(t), observer),
				ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})

			if matrix.Outcome != ViabilityImpossible {
				t.Fatalf("outcome = %q, want %q: one unobserved worker masked a permanent verdict",
					matrix.Outcome, ViabilityImpossible)
			}
			reason, found := candidateReason(t, matrix, ReasonRepositoryNotFound)
			if !found || !reason.Permanent {
				t.Fatalf("verdict = %+v (found %t)", reason, found)
			}
			// The task-level verdict has to name its basis: who observed the
			// failure, and who was not observed and why.
			taskReason, ok := taskLevelReason(matrix, ReasonRepositoryNotFound)
			if !ok {
				t.Fatal("the permanent verdict was reported only on a candidate, not for the task")
			}
			for _, want := range []string{"homelab", "normandy", test.wantReason, "no candidate observed success"} {
				if !strings.Contains(taskReason.Detail, want) {
					t.Fatalf("verdict detail %q does not name %q", taskReason.Detail, want)
				}
			}
			if len(matrix.PermanentReasons()) == 0 {
				t.Fatal("the matrix reported no permanent reason to refuse with")
			}
		})
	}
}

// taskLevelReason finds a reason attached to a task rather than to a candidate.
func taskLevelReason(matrix ViabilityMatrix, code string) (ViabilityReason, bool) {
	for _, task := range matrix.Tasks {
		for _, reason := range task.Reasons {
			if reason.Code == code {
				return reason, true
			}
		}
	}
	return ViabilityReason{}, false
}

// TestOneObservedSuccessSettlesTheTask states the other half of the rule: a
// candidate that can read the repository settles it, whatever another candidate
// reported, because the work can be placed there.
func TestOneObservedSuccessSettlesTheTask(t *testing.T) {
	observer := &perWorkerObserver{
		answers: map[string]backlog.RepositoryReachability{
			"homelab":  backlog.RepositoryNotFound,
			"normandy": backlog.RepositoryAuthenticatedOK,
		},
		errors: map[string]error{},
	}
	v := twoWorkerView(t, nil)
	matrix := v.viability(context.Background(),
		withRepositoryObserver(viabilityCatalog(t), observer),
		ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
	if matrix.Outcome != ViabilityReady {
		t.Fatalf("outcome = %q, want %q", matrix.Outcome, ViabilityReady)
	}
	if _, found := taskLevelReason(matrix, ReasonRepositoryNotFound); found {
		t.Fatal("a task with a working candidate was given a permanent repository verdict")
	}
}

// TestEveryCandidateUnobservedIsNotPermanent states the floor: with no observed
// candidate at all there is no evidence, and no evidence is not a refusal.
func TestEveryCandidateUnobservedIsNotPermanent(t *testing.T) {
	observer := &perWorkerObserver{
		answers: map[string]backlog.RepositoryReachability{},
		errors: map[string]error{
			"homelab":  errors.New("dial homelab: connection refused"),
			"normandy": errors.New("dial normandy: connection refused"),
		},
	}
	v := twoWorkerView(t, nil)
	matrix := v.viability(context.Background(),
		withRepositoryObserver(viabilityCatalog(t), observer),
		ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
	if matrix.Outcome != ViabilityAcceptedWaiting {
		t.Fatalf("outcome = %q, want %q", matrix.Outcome, ViabilityAcceptedWaiting)
	}
	if len(matrix.PermanentReasons()) != 0 {
		t.Fatalf("absence of evidence produced a permanent refusal: %+v", matrix.PermanentReasons())
	}
}

// TestATemporaryObservationDoesNotCountAsSuccess states that only an observed
// success settles the task. A DNS failure on one worker is not evidence that
// the repository exists, so a confirmed permanent verdict on another stands.
func TestATemporaryObservationDoesNotCountAsSuccess(t *testing.T) {
	observer := &perWorkerObserver{
		answers: map[string]backlog.RepositoryReachability{
			"homelab":  backlog.RepositoryRefNotFound,
			"normandy": backlog.RepositoryDNSFailure,
		},
		errors: map[string]error{},
	}
	v := twoWorkerView(t, nil)
	matrix := v.viability(context.Background(),
		withRepositoryObserver(viabilityCatalog(t), observer),
		ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
	if matrix.Outcome != ViabilityImpossible {
		t.Fatalf("outcome = %q, want %q", matrix.Outcome, ViabilityImpossible)
	}
	if _, found := taskLevelReason(matrix, ReasonRefNotFound); !found {
		t.Fatal("the observed missing ref did not produce a task verdict")
	}
}

// TestEveryCandidateRecordsWhetherItWasObserved is the legibility property: an
// operator reading any outcome can tell a checked candidate from an unchecked
// one, in every one of the four ways a candidate can go unchecked.
func TestEveryCandidateRecordsWhetherItWasObserved(t *testing.T) {
	tests := []struct {
		name     string
		settings func(*testing.T) ViabilitySettings
		mutate   func(*view)
		want     string
	}{
		{
			name:     "no observer is configured",
			settings: func(t *testing.T) ViabilitySettings { return viabilityCatalog(t) },
			want:     "no repository observer configured",
		},
		{
			name: "the worker has drifted",
			settings: func(t *testing.T) ViabilitySettings {
				return withRepositoryObserver(viabilityCatalog(t), &stubRepositoryObserver{
					class: backlog.RepositoryAuthenticatedOK,
				})
			},
			mutate: func(v *view) {
				v.enrollments[0].Request.CatalogRevision = viabilityAcceptedDigest
			},
			want: "catalog the coordinator has replaced",
		},
		{
			name: "the project prepares a fresh workspace",
			settings: func(t *testing.T) ViabilitySettings {
				settings := withRepositoryObserver(viabilityCatalog(t), &stubRepositoryObserver{
					class: backlog.RepositoryAuthenticatedOK,
				})
				settings.Projects[0].Type = backlog.EnvironmentFresh
				settings.Projects[0].Repository = ""
				settings.Projects[0].DefaultRef = ""
				return settings
			},
			want: "no repository to reach",
		},
		{
			name: "the worker has reported no inventory",
			settings: func(t *testing.T) ViabilitySettings {
				return withRepositoryObserver(viabilityCatalog(t), &stubRepositoryObserver{
					class: backlog.RepositoryAuthenticatedOK,
				})
			},
			mutate: func(v *view) { v.workers = nil },
			want:   "reported no inventory",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := viabilityView(t, test.mutate)
			task := viabilityTaskRequest()
			if test.name == "the project prepares a fresh workspace" {
				task.Ref = ""
			}
			matrix := v.viability(context.Background(), test.settings(t),
				ViabilityRequest{Tasks: []ViabilityTask{task}})
			if len(matrix.Tasks) != 1 || len(matrix.Tasks[0].Candidates) != 1 {
				t.Fatalf("matrix = %+v", matrix)
			}
			candidate := matrix.Tasks[0].Candidates[0]
			if candidate.Repository == nil {
				t.Fatal("the candidate recorded no repository observation at all")
			}
			if candidate.Repository.Observed {
				t.Fatalf("the candidate claims it was observed: %+v", candidate.Repository)
			}
			if !strings.Contains(candidate.Repository.Unobserved, test.want) {
				t.Fatalf("unobserved reason %q does not say %q",
					candidate.Repository.Unobserved, test.want)
			}
		})
	}
}

// TestAnObservedCandidateSaysSo is the positive half of the same property.
func TestAnObservedCandidateSaysSo(t *testing.T) {
	v := viabilityView(t, nil)
	matrix := v.viability(context.Background(),
		withRepositoryObserver(viabilityCatalog(t), &stubRepositoryObserver{
			class: backlog.RepositoryAuthenticatedOK,
		}),
		ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
	candidate := matrix.Tasks[0].Candidates[0]
	if candidate.Repository == nil || !candidate.Repository.Observed ||
		candidate.Repository.Class != string(backlog.RepositoryAuthenticatedOK) {
		t.Fatalf("observation = %+v", candidate.Repository)
	}
	if matrix.Outcome != ViabilityReady {
		t.Fatalf("outcome = %q", matrix.Outcome)
	}
}

// TestCredentialAvailabilityIsReportedAsUnobserved states the honest answer for
// a question this coordinator cannot ask. It must not read as a passed check,
// and it must not be a temporary reason that tells an operator to wait for
// something that will never change on its own.
func TestCredentialAvailabilityIsReportedAsUnobserved(t *testing.T) {
	v := viabilityView(t, nil)
	matrix := v.viability(context.Background(), viabilityCatalog(t, "git-github"),
		ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
	candidate := matrix.Tasks[0].Candidates[0]
	found := false
	for _, note := range candidate.Unchecked {
		if strings.Contains(note, "git-github") && strings.Contains(note, "was not observed") {
			found = true
		}
	}
	if !found {
		t.Fatalf("unchecked notes = %+v", candidate.Unchecked)
	}
	for _, reason := range candidate.Reasons {
		if reason.Code == ReasonCredentialMissing || reason.Code == ReasonSnapshotStale {
			t.Fatalf("an unobservable question produced a finding: %+v", reason)
		}
	}
}

// TestAWorkerRunningAnUncomparedCatalogSaysSo covers the drift gap: a worker
// with a snapshot and no requirement row has no desired digest to compare
// against, and the probe key is built from the digest nobody compared.
func TestAWorkerRunningAnUncomparedCatalogSaysSo(t *testing.T) {
	v := viabilityView(t, func(v *view) {
		v.requirements = nil
		v.enrollments = nil
	})
	matrix := v.viability(context.Background(), viabilityCatalog(t),
		ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
	candidate := matrix.Tasks[0].Candidates[0]
	found := false
	for _, note := range candidate.Unchecked {
		if strings.Contains(note, "no requirement row") {
			found = true
		}
	}
	if !found {
		t.Fatalf("unchecked notes = %+v", candidate.Unchecked)
	}
}

// TestTheRunningCatalogIsComparedNotOnlyTheAcceptedOne covers the second drift
// gap: a worker that enrolled against the right catalog and is observably
// running a different one is drift, and the probe key is built from the digest
// it is running.
func TestTheRunningCatalogIsComparedNotOnlyTheAcceptedOne(t *testing.T) {
	v := viabilityView(t, func(v *view) {
		v.workers[0].Inventory.CatalogRevision = normandyDigest
	})
	matrix := v.viability(context.Background(), viabilityCatalog(t),
		ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
	reason, found := candidateReason(t, matrix, ReasonCatalogDigestMismatch)
	if !found {
		t.Fatal("a worker running an unexpected catalog was not reported as drift")
	}
	if reason.Observed != normandyDigest || reason.Desired != viabilityDesiredDigest {
		t.Fatalf("drift digests = observed %q desired %q", reason.Observed, reason.Desired)
	}
	if !strings.Contains(reason.Detail, "is running catalog") {
		t.Fatalf("drift detail does not distinguish running from accepted: %q", reason.Detail)
	}
}
