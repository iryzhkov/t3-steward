package backlog

import (
	"context"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestReviewDeclaredFreshEpochAndOriginalSnapshot(t *testing.T) {
	ctx := context.Background()
	f := newDeclaredAdmissionFixture(t)
	original, err := f.service.ResolveDeclared(ctx, declaredRequest(f))
	if err != nil {
		t.Fatal(err)
	}
	as := f.records.Assignments[0]
	as.Epoch++
	if err = f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{as}}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.FreezeDeclaredReviewAuthority(ctx, original.Authority); err == nil {
		t.Fatal("original epoch snapshot froze")
	}
	_, found, err := f.store.GetFrozenReviewAuthority(ctx, f.request.RunID, f.request.TaskID)
	if err != nil || found {
		t.Fatal("stale epoch allocated authority", err)
	}
	fresh, err := f.service.FreezeDeclared(ctx, declaredRequest(f))
	if err != nil || fresh.Authority.Parent.AssignmentEpoch != as.Epoch {
		t.Fatal("coherent fresh epoch did not freeze", err)
	}
	// Legitimate revision and catalog advancement still reuse that exact issuance.
	a := f.records.Attempts[0]
	a.Revision++
	if err = f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
		t.Fatal(err)
	}
	f.catalog.catalog.Classifications = nil
	replay, err := f.service.FreezeDeclared(ctx, declaredRequest(f))
	if err != nil || !reflect.DeepEqual(fresh, replay) {
		t.Fatal("valid original issue/provenance replay changed", err)
	}
}
