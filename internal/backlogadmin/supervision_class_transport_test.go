package backlogadmin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// supervisionClassRuns names one run per refusal class, so a service on the far
// side of a carrier can produce exactly the refusal the test asked for without
// any out-of-band channel.
var supervisionClassRuns = map[SupervisionErrorClass]string{
	SupervisionErrorStaleEvidence:     "run-stale-evidence",
	SupervisionErrorUnauthorizedScope: "run-unauthorized-scope",
	SupervisionErrorPrerequisite:      "run-unmet-prerequisite",
	SupervisionErrorUnavailable:       "run-temporarily-unavailable",
	SupervisionErrorMalformed:         "run-malformed-request",
}

// supervisionErrorForRun is the refusal a test run name stands for, built from
// the sentinels ClassifySupervisionError matches in process.
func supervisionErrorForRun(runID string) error {
	switch runID {
	case supervisionClassRuns[SupervisionErrorStaleEvidence]:
		return fmt.Errorf("%w: the gate moved under this decision", domain.ErrSupervisionStaleRevision)
	case supervisionClassRuns[SupervisionErrorUnauthorizedScope]:
		return fmt.Errorf("%w: this capability is bound to another run", domain.ErrSupervisionUnauthorizedActor)
	case supervisionClassRuns[SupervisionErrorPrerequisite]:
		return fmt.Errorf("%w: the gate has no evidence yet", domain.ErrSupervisionPrerequisite)
	case supervisionClassRuns[SupervisionErrorUnavailable]:
		return fmt.Errorf("%w: the supervision store is not reachable", ErrSupervisionUnavailable)
	case supervisionClassRuns[SupervisionErrorMalformed]:
		return errors.New("this supervision request is not a request")
	default:
		return nil
	}
}

// supervisionClassService answers a show with the refusal its run name names.
type supervisionClassService struct {
	*localTransportService
}

func (s *supervisionClassService) Supervise(_ context.Context, _ Principal, request SupervisionRequest) (SupervisionResponse, error) {
	if err := supervisionErrorForRun(request.RunID); err != nil {
		return SupervisionResponse{}, err
	}
	return SupervisionResponse{
		Version: SupervisionVersion, Operation: request.Operation, RunID: request.RunID,
		GeneratedAt: supervisionTestNow,
	}, nil
}

// Supervise lets the remote harness's coordinator-side service refuse the same
// way the local one does, so one table covers both carriers.
func (s *remoteFakeService) Supervise(_ context.Context, principal Principal, request SupervisionRequest) (SupervisionResponse, error) {
	s.record(principal)
	if err := supervisionErrorForRun(request.RunID); err != nil {
		return SupervisionResponse{}, err
	}
	return SupervisionResponse{
		Version: SupervisionVersion, Operation: request.Operation, RunID: request.RunID,
		GeneratedAt: supervisionTestNow,
	}, nil
}

// The five refusal classes exist so a client can branch on them. The sentinels
// they are derived from do not survive serialization, so each class has to
// arrive as itself on both carriers; before the class travelled in the response
// frame, every remote refusal read as "rejected" and the distinction existed
// only in process.
func TestSupervisionErrorClassSurvivesBothCarriers(t *testing.T) {
	classes := []SupervisionErrorClass{
		SupervisionErrorStaleEvidence,
		SupervisionErrorUnauthorizedScope,
		SupervisionErrorPrerequisite,
		SupervisionErrorUnavailable,
		SupervisionErrorMalformed,
	}
	show := func(runID string) SupervisionRequest {
		return SupervisionRequest{Version: SupervisionVersion, Operation: SupervisionShow, RunID: runID}
	}

	t.Run("local", func(t *testing.T) {
		service := &supervisionClassService{localTransportService: &localTransportService{}}
		client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), service)
		defer stopLocalTransport(t, cancel, done)
		for _, class := range classes {
			_, err := client.Supervise(context.Background(), show(supervisionClassRuns[class]))
			if got := ClassifySupervisionError(err); got != class {
				t.Fatalf("local carrier: class = %q, want %q (%v)", got, class, err)
			}
		}
		// An answer is still an answer: a run that names no refusal succeeds.
		if _, err := client.Supervise(context.Background(), show("run-1")); err != nil {
			t.Fatalf("local carrier refused a permitted show: %v", err)
		}
	})

	t.Run("remote", func(t *testing.T) {
		harness := newRemoteHarness(t, false)
		for _, class := range classes {
			_, err := harness.client.Supervise(context.Background(), show(supervisionClassRuns[class]))
			if got := ClassifySupervisionError(err); got != class {
				t.Fatalf("remote carrier: class = %q, want %q (%v)", got, class, err)
			}
			// The transport class is unchanged: the coordinator answered and
			// refused, which is a rejection whatever supervision decided.
			if transport := ClassOf(err); transport != ClassRejected {
				t.Fatalf("remote carrier: transport class = %q for %q", transport, class)
			}
		}
		if _, err := harness.client.Supervise(context.Background(), show("run-1")); err != nil {
			t.Fatalf("remote carrier refused a permitted show: %v", err)
		}
	})
}
