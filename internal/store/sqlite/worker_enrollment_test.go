package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"reflect"
	"strings"
	"testing"
	"time"
)

func enrollmentFixture(t *testing.T) (*Store, domain.WorkerRequirement, domain.WorkerSnapshot, domain.WorkerEnrollment) {
	t.Helper()
	s := openFleetTestStore(t)
	requirement := domain.WorkerRequirement{WorkerID: "normandy", WorkerEpoch: "worker-epoch-1", CatalogRevision: strings.Repeat("a", 64), CredentialRef: "secretref:f02-protocol/normandy", Connection: "persistent-ssh"}
	if err := s.SaveWorkerRequirements(context.Background(), []domain.WorkerRequirement{requirement}); err != nil {
		t.Fatal(err)
	}
	snapshot := fleetSnapshot(1, requirement.WorkerEpoch, 1, true, fleetTestTime.Add(time.Minute))
	snapshot.Inventory.CatalogRevision = requirement.CatalogRevision
	saveFleetSnapshot(t, s, snapshot)
	value := domain.WorkerEnrollment{
		Request:     domain.WorkerEnrollmentRequest{ID: "enroll-1", WorkerID: requirement.WorkerID, CatalogRevision: requirement.CatalogRevision, Reason: "verified host"},
		WorkerEpoch: requirement.WorkerEpoch, CoordinatorID: "coordinator", CredentialRef: requirement.CredentialRef, Principal: "ssh:normandy", Connection: requirement.Connection, Actor: "operator", EnrolledAt: fleetTestTime,
	}
	return s, requirement, snapshot, value
}

func TestEnrollmentFencesOffersAndRetainsExactReplay(t *testing.T) {
	ctx := context.Background()
	s, requirement, snapshot, value := enrollmentFixture(t)
	saveFleetAttempt(t, s, fleetAttempt("attempt-1"))
	plan := fleetPlanCommit(1, "assignment-1", "attempt-1", requirement.WorkerEpoch, 1)
	if got, err := s.CommitAssignmentPlan(ctx, plan); err != nil || len(got) != 0 {
		t.Fatalf("configured worker received offer: %v %v", got, err)
	}
	enrolled, err := s.CommitWorkerEnrollment(ctx, value, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if enrolled.Revision != 1 {
		t.Fatal(enrolled)
	}
	replay, err := s.CommitWorkerEnrollment(ctx, value, snapshot)
	if err != nil || !reflect.DeepEqual(replay, enrolled) {
		t.Fatalf("replay: %v %v", replay, err)
	}
	changed := value
	changed.Actor = "another-operator"
	if _, err := s.CommitWorkerEnrollment(ctx, changed, snapshot); err == nil {
		t.Fatal("actor-changing replay accepted")
	}
	if got, err := s.CommitAssignmentPlan(ctx, plan); err != nil || len(got) != 1 {
		t.Fatalf("enrolled worker offer: %v %v", got, err)
	}
	assertNativeAuditEvent(t, s, "worker-enrolled:enroll-1", "operator", requirement.CatalogRevision)
}

func TestEnrollmentRejectsChangedObservationAndEpoch(t *testing.T) {
	for _, kind := range []string{"observation", "coordinator", "stale"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s, _, snapshot, value := enrollmentFixture(t)
			switch kind {
			case "observation":
				fresh := snapshot
				fresh.Sequence++
				fresh.ObservedAt = fresh.ObservedAt.Add(time.Second)
				fresh.Inventory.ObservedAt = fresh.ObservedAt
				saveFleetSnapshot(t, s, fresh)
			case "coordinator":
				if _, err := s.AdvanceCoordinatorEpoch(ctx, 1); err != nil {
					t.Fatal(err)
				}
			case "stale":
				value.EnrolledAt = snapshot.ValidUntil
			}
			if _, err := s.CommitWorkerEnrollment(ctx, value, snapshot); err == nil {
				t.Fatal("unsafe enrollment accepted")
			}
		})
	}
}

func TestEnrollmentRequirementChangesDrainAdmission(t *testing.T) {
	for _, kind := range []string{"catalog", "credential", "epoch", "removed", "drain"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s, requirement, snapshot, value := enrollmentFixture(t)
			if _, err := s.CommitWorkerEnrollment(ctx, value, snapshot); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "drain":
				requirement.Draining = true
			case "catalog":
				requirement.CatalogRevision = strings.Repeat("b", 64)
			case "credential":
				requirement.CredentialRef = "secretref:f02-protocol/rotated"
			case "epoch":
				requirement.WorkerEpoch = "epoch-2"
			}
			requirements := []domain.WorkerRequirement{requirement}
			if kind == "removed" {
				requirements = nil
			}
			if err := s.SaveWorkerRequirements(ctx, requirements); err != nil {
				t.Fatal(err)
			}
			if ok, err := s.WorkerEnrolled(ctx, "normandy"); err != nil || ok {
				t.Fatalf("changed requirement admitted: %v %v", ok, err)
			}
		})
	}
}
