package backlogadmin

import (
	"context"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Every answer this carrier serves from its replay cache and that carries a
// replay flag must say so, or the caller is told that work happened now which
// happened before. "backlog recover" is the case the first fix missed: its
// request id is derived from the recovery ID, so a repeat is answered from the
// cache, and it printed "outcome applied" for a recovery it did not apply.
func TestARepeatedUnknownRecoveryIsReportedAsAReplay(t *testing.T) {
	harness := newRemoteHarness(t, true)
	recover := func(client *SSHClient) (bool, error) {
		decision, err := client.RecoverUnknown(context.Background(), Principal{},
			UnknownRecoveryRequest{ID: "recovery-1", AssignmentID: "assignment-1"})
		return decision.Replay, err
	}
	first, err := recover(harness.client)
	if err != nil {
		t.Fatal(err)
	}
	if first {
		t.Fatal("the first recovery reported itself a replay")
	}
	// A retry is a new CLI invocation with a new session id and the same
	// recovery ID, which is what makes it the same request identity.
	second, err := recover(harness.newClient(t))
	if err != nil {
		t.Fatalf("retry from a new client: %v", err)
	}
	if !second {
		t.Fatal("a recovery answered from the carrier's cache reported replay=false")
	}
}

// Which answers carry a replay flag is a fact about the response types, and the
// list in markReplayedAnswer has to keep up with them. This reads the fields of
// localResponse and fails when one grows a Replay flag that nothing marks,
// which is how the recovery answer was missed.
func TestEveryAnswerWithAReplayFlagIsMarkedOnAReplay(t *testing.T) {
	full := localResponse{
		SubmissionResponse:         &LocalSubmissionResponse{},
		ScheduleDefinitionResponse: &LocalScheduleDefinitionResponse{},
		GraphAmendment:             &domain.GraphAmendmentResult{},
		SupervisionResponse:        &SupervisionResponse{},
		UnknownRecoveryResponse:    &domain.UnknownAssignmentRecoveryDecision{},
		MutationResponse:           &MutationResponse{},
		QuarantineReleaseResponse:  &domain.QuarantineRelease{},
		NodeWait:                   &NodeWaitResponse{},
		Response:                   &Response{},
		WorkerEnrollment:           &domain.WorkerEnrollment{},
	}
	marked := markReplayedAnswer(full)
	value := reflect.ValueOf(marked)
	for i := range value.NumField() {
		field := value.Field(i)
		if field.Kind() != reflect.Ptr || field.IsNil() {
			continue
		}
		flag := field.Elem().FieldByName("Replay")
		if !flag.IsValid() || flag.Kind() != reflect.Bool {
			continue
		}
		if !flag.Bool() {
			t.Fatalf("%s carries a replay flag that markReplayedAnswer leaves false",
				value.Type().Field(i).Name)
		}
	}
}
