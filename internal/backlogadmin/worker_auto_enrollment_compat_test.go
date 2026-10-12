package backlogadmin

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestAutomaticEnrollmentWorkersReadNegotiatesOlderCoordinator(t *testing.T) {
	var versions []string
	response, err := queryExtended(context.Background(), Query{Version: Version, Kind: QueryWorkers}, func(_ context.Context, q Query) (Response, error) {
		versions = append(versions, q.Version)
		if q.Version == AutomaticEnrollmentReadVersion {
			return Response{}, &TransportError{Class: ClassRejected, Err: fmt.Errorf("%w: got %q, want %q", ErrUnsupportedVersion, q.Version, Version)}
		}
		return Response{Version: q.Version}, nil
	})
	if err != nil || response.Version != CurrentReadVersion || !reflect.DeepEqual(versions, []string{AutomaticEnrollmentReadVersion, CurrentReadVersion}) {
		t.Fatalf("negotiation: %+v %v %v", response, err, versions)
	}
}

func TestAutomaticEnrollmentWorkersReadPreservesOlderShape(t *testing.T) {
	// Projection runs after the ordinary query, and must not mutate the stored attempt.
	store := openAdminTestStore(t)
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	requirement := domain.WorkerRequirement{WorkerID: "configured", WorkerEpoch: "epoch", CatalogRevision: viabilityDesiredDigest, Connection: "unix"}
	if err := store.SaveWorkerRequirements(context.Background(), []domain.WorkerRequirement{requirement}); err != nil {
		t.Fatal(err)
	}
	attempt := domain.AutomaticWorkerEnrollment{Reason: "automatic enrollment refused"}
	if err := store.SaveAutomaticWorkerEnrollment(context.Background(), "configured", attempt); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{CurrentReadVersion, AutomaticEnrollmentReadVersion} {
		response, err := service.Query(context.Background(), Query{Version: version, Kind: QueryWorkers})
		if err != nil {
			t.Fatal(err)
		}
		var got *domain.AutomaticWorkerEnrollment
		for _, worker := range response.Workers {
			if worker.Requirement != nil && worker.Requirement.WorkerID == "configured" {
				got = worker.Requirement.AutomaticEnrollment
			}
		}
		if (got != nil) != (version == AutomaticEnrollmentReadVersion) {
			t.Fatalf("version %q attempt %+v", version, got)
		}
	}
}
