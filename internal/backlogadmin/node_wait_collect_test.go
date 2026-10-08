package backlogadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestNodeWaitCollectRunAndList(t *testing.T) {
	ctx := context.Background()
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), nativeTransport{&localTransportService{}, service})
	defer stopLocalTransport(t, cancel, done)
	register := NodeWaitOperation{Action: "register", Request: domain.NodeWaitRequest{
		ID: "nw-campaign-1", ThreadID: "thread-mine", Name: "implement", Target: domain.NodeRef{RunID: "run-1", TaskID: "implement"}, Timeout: time.Hour,
	}}
	if _, err := client.NodeWait(ctx, register); err != nil {
		t.Fatal(err)
	}
	collect := func(thread string) (NodeWaitResponse, error) {
		return client.NodeWait(ctx, NodeWaitOperation{Action: RunCollectAction, Request: domain.NodeWaitRequest{
			ThreadID: thread, Target: domain.NodeRef{RunID: "run-1"},
		}})
	}

	// run-1 is still active: refused by the coordinator, as a rejection.
	if _, err := collect("thread-mine"); err == nil || ClassOf(err) != ClassRejected || !strings.Contains(err.Error(), "run run-1 is active and cannot be collected") {
		t.Fatalf("active run: err = %v (class %q)", err, ClassOf(err))
	}

	finished := adminTestNow.Add(-time.Minute)
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run := records.WorkflowRuns[0]
	run.Progress, run.CompletedAt, run.Revision = domain.ProgressSucceeded, &finished, run.Revision+1
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
		t.Fatal(err)
	}

	// A thread with no notification on the run is refused.
	if _, err := collect("thread-other"); err == nil || ClassOf(err) != ClassRejected || !strings.Contains(err.Error(), "has no notification for thread thread-other") {
		t.Fatalf("unowned: err = %v", err)
	}

	first, err := collect("thread-mine")
	if err != nil {
		t.Fatal(err)
	}
	if !first.Changed || len(first.Collections) != 1 {
		t.Fatalf("first collection = %+v", first)
	}
	got := first.Collections[0]
	if got.RunID != "run-1" || got.ThreadID != "thread-mine" || got.Progress != domain.ProgressSucceeded ||
		got.CompletedAt == nil || !got.CompletedAt.Equal(finished) || got.CollectedAt.IsZero() || got.Actor == "" {
		t.Fatalf("collection = %+v", got)
	}
	again, err := collect("thread-mine")
	if err != nil {
		t.Fatal(err)
	}
	if again.Changed || len(again.Collections) != 1 || !again.Collections[0].CollectedAt.Equal(got.CollectedAt) {
		t.Fatalf("replay = %+v", again)
	}

	list := func(thread string) []domain.RunCollection {
		t.Helper()
		response, err := client.NodeWait(ctx, NodeWaitOperation{Action: RunCollectionListAction, Request: domain.NodeWaitRequest{ThreadID: thread}})
		if err != nil {
			t.Fatal(err)
		}
		return response.Collections
	}
	if listed := list("thread-mine"); len(listed) != 1 || listed[0].RunID != "run-1" {
		t.Fatalf("list thread-mine = %+v", listed)
	}
	if listed := list("thread-other"); len(listed) != 0 {
		t.Fatalf("list thread-other = %+v", listed)
	}
	// Malformed requests are refused before the store.
	for _, op := range []NodeWaitOperation{
		{Action: RunCollectAction, Request: domain.NodeWaitRequest{ThreadID: "thread-mine"}},
		{Action: RunCollectAction, Request: domain.NodeWaitRequest{Target: domain.NodeRef{RunID: "run-1"}}},
		{Action: RunCollectionListAction},
		{Action: RunCollectionListAction, Request: domain.NodeWaitRequest{ThreadID: "a/b"}},
	} {
		if _, err := client.NodeWait(ctx, op); err == nil || ClassOf(err) != ClassRejected {
			t.Fatalf("%+v: err = %v", op, err)
		}
	}
}

// A reader without run collections answers plainly rather than panicking or
// pretending to have recorded something.
func TestNodeWaitCollectionUnavailableWithoutStore(t *testing.T) {
	service, err := New(&countingReader{}, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{RunCollectAction, RunCollectionListAction} {
		_, err := service.NodeWait(context.Background(), Principal{ID: "tester"}, NodeWaitOperation{Action: action,
			Request: domain.NodeWaitRequest{ThreadID: "thread", Target: domain.NodeRef{RunID: "run-1"}}})
		if err == nil || err.Error() != "run collection unavailable" {
			t.Fatalf("%s: err = %v", action, err)
		}
	}
}

func TestRunCollectionOperationFitsRC70Envelope(t *testing.T) {
	for _, op := range []NodeWaitOperation{
		{Action: RunCollectAction, Request: domain.NodeWaitRequest{ThreadID: "thread", Target: domain.NodeRef{RunID: "run-1"}}},
		{Action: RunCollectionListAction, Request: domain.NodeWaitRequest{ThreadID: "thread"}},
	} {
		raw, err := json.Marshal(op)
		if err != nil {
			t.Fatal(err)
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		var old rc70NodeWaitOperation
		if err := decoder.Decode(&old); err != nil {
			t.Fatalf("%s does not fit the rc.70 envelope: %v\n%s", op.Action, err, raw)
		}
		if old.Action != op.Action || old.Request.ThreadID != op.Request.ThreadID || old.Request.Target.RunID != op.Request.Target.RunID {
			t.Fatalf("%s decoded as %+v", op.Action, old)
		}
	}
}

func TestListCollectionsIsNotMutating(t *testing.T) {
	list := NodeWaitOperation{Action: RunCollectionListAction, Request: domain.NodeWaitRequest{ThreadID: "thread"}}
	if mutatingRequest(localOperationNodeWait, localRequest{Operation: localOperationNodeWait, NodeWait: &list}) {
		t.Fatal("list-collections spends the admin replay store")
	}
	collect := NodeWaitOperation{Action: RunCollectAction, Request: domain.NodeWaitRequest{ThreadID: "thread", Target: domain.NodeRef{RunID: "run-1"}}}
	if !mutatingRequest(localOperationNodeWait, localRequest{Operation: localOperationNodeWait, NodeWait: &collect}) {
		t.Fatal("collect-run lost its replay protection")
	}
}

// The coordinator's answer to a node-wait action it does not know is the exact
// text a client matches to recognise a coordinator older than run collection.
// Changing it would make every newer client misread an older coordinator.
func TestRC117NodeWaitRefusesCollectionActions(t *testing.T) {
	store := openAdminTestStore(t)
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.NodeWait(context.Background(), Principal{ID: "tester"}, NodeWaitOperation{Action: "collect-run-from-the-future"})
	if err == nil || err.Error() != olderCoordinatorNodeWaitAnswer || olderCoordinatorNodeWaitAnswer != "unknown native wait action" {
		t.Fatalf("unknown action answered %v", err)
	}

	direct := &TransportError{Class: ClassRejected, Operation: localOperationNodeWait, Coordinator: "old", Err: err}
	if !RunCollectionUnsupported(direct) {
		t.Fatal("the direct answer of an older coordinator was not recognised")
	}
	wrapped := &TransportError{Class: ClassRejected, Operation: localOperationNodeWait, Err: fmt.Errorf("%w: %s", errors.New(olderCoordinatorNodeWaitAnswer), "Warning: Permanently added 'coordinator' to the list of known hosts.")}
	if !RunCollectionUnsupported(wrapped) {
		t.Fatal("the SSH-wrapped answer of an older coordinator was not recognised")
	}
	if !RunCollectionUnsupported(errors.New(olderCoordinatorNodeWaitAnswer)) {
		t.Fatal("the bare answer was not recognised")
	}
	for _, unrelated := range []error{
		nil,
		errors.New("unknown task-bound wait action"),
		errors.New(`run x/unknown native wait action is unknown`),
		&TransportError{Class: ClassRejected, Err: errors.New("unknown native wait action \"collect-run\"")},
		&TransportError{Class: ClassUnavailable, Err: errors.New("connect to backlog-v2 coordinator: refused")},
		&TransportError{Class: ClassRejected, Err: fmt.Errorf("%w: detail", errors.New("run collection unavailable"))},
	} {
		if RunCollectionUnsupported(unrelated) {
			t.Fatalf("%v was read as an older coordinator", unrelated)
		}
	}
}
