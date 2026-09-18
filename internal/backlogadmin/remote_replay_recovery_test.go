package backlogadmin

import (
	"context"
	"reflect"
	"testing"
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
// list in markReplayedAnswer has to keep up with them. This asks the type which
// answers there are and fails when one carries a Replay flag that nothing
// marks, which is how the recovery answer was missed.
//
// The enumeration comes from localResponse itself and never from a literal
// written here. A literal is only as complete as the memory of whoever last
// edited it: the one this test used to carry named ten of the eleven pointer
// fields, having already missed ArtifactMetadata, so a response type added to
// the struct later would have been skipped in silence -- the exact failure this
// test exists to prevent.
func TestEveryAnswerWithAReplayFlagIsMarkedOnAReplay(t *testing.T) {
	full := reflect.New(reflect.TypeFor[localResponse]()).Elem()
	var populated int
	for i := range full.NumField() {
		field := full.Field(i)
		if field.Kind() != reflect.Ptr {
			continue
		}
		// A zero value of whatever this field points at. markReplayedAnswer has
		// to reach every answer the carrier can serve, so every answer is
		// present at once.
		field.Set(reflect.New(field.Type().Elem()))
		populated++
	}
	if populated == 0 {
		t.Fatal("localResponse has no pointer answers, so this test proves nothing")
	}
	marked := reflect.ValueOf(markReplayedAnswer(full.Interface().(localResponse)))
	var checked int
	for i := range marked.NumField() {
		field := marked.Field(i)
		if field.Kind() != reflect.Ptr || field.IsNil() || field.Elem().Kind() != reflect.Struct {
			continue
		}
		flag := field.Elem().FieldByName("Replay")
		if !flag.IsValid() || flag.Kind() != reflect.Bool {
			continue
		}
		checked++
		if !flag.Bool() {
			t.Fatalf("%s carries a replay flag that markReplayedAnswer leaves false",
				marked.Type().Field(i).Name)
		}
	}
	// The five answers markReplayedAnswer names. A count stated here turns the
	// addition of a response type that carries a replay flag into a failure even
	// if the walk above were ever weakened, and the addition of one that does
	// not into a deliberate edit rather than a silent pass.
	if checked != 5 {
		t.Fatalf("walked %d answers carrying a replay flag, want the 5 markReplayedAnswer names", checked)
	}
}
