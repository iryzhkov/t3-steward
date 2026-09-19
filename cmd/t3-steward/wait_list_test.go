package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

var waitListNow = time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

// pendingNodeWait is the wait "task run" registers: a node wait on a run sink,
// held by the coordinator, invisible to the local store.
func pendingNodeWait(id, thread, host string) domain.NodeWait {
	return domain.NodeWait{
		Request: domain.NodeWaitRequest{
			ID: id, ThreadID: thread, Name: "run-1/sink",
			Target:  domain.NodeRef{RunID: "run-1", TaskID: domain.SinkTaskName},
			Timeout: 24 * time.Hour,
		},
		Host: host, Delivery: "pending", CreatedAt: waitListNow,
		Deadline: waitListNow.Add(24 * time.Hour),
	}
}

func localCheck(id, thread string) wait.Wait {
	return wait.Wait{
		ID: id, ThreadID: thread, Name: "ci", Kind: domain.WaitKindShell,
		Command: []string{"true"}, Status: wait.StatusWaiting,
		CreatedAt: waitListNow, Timeout: time.Hour,
	}
}

func listWaits(t *testing.T, sources waitListSources, options waitListOptions) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := runWaitList(context.Background(), sources, options, &out)
	return out.String(), err
}

// B-2. "wait list" said "No waits." while the node wait registered seconds
// earlier was pending, because it read one of the two places a wait lives.
func TestWaitListJoinsTheLocalStoreAndTheCoordinator(t *testing.T) {
	sources := waitListSources{
		host: "omarchy-pc",
		local: func(context.Context, string) ([]wait.Wait, error) {
			return nil, nil
		},
		coordinator: func(context.Context) ([]domain.NodeWait, []domain.TaskWait, error) {
			return []domain.NodeWait{pendingNodeWait("nw-campaign-run-abc", "thread-1", "omarchy-pc")}, nil, nil
		},
	}
	text, err := listWaits(t, sources, waitListOptions{thread: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text, "No waits.") || strings.Contains(text, "Nothing is pending") {
		t.Fatalf("a pending coordinator-held wait was reported as nothing:\n%s", text)
	}
	for _, want := range []string{"nw-campaign-run-abc", "state=waiting", "delivery=pending", "host=omarchy-pc"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the line does not carry %q:\n%s", want, text)
		}
	}
}

// No ambiguous zero: "this thread has nothing pending" and "a source could not
// be read" are different answers, and the second exits non-zero.
func TestWaitListDistinguishesNothingPendingFromAnUnreadableSource(t *testing.T) {
	empty := waitListSources{
		host:  "omarchy-pc",
		local: func(context.Context, string) ([]wait.Wait, error) { return nil, nil },
		coordinator: func(context.Context) ([]domain.NodeWait, []domain.TaskWait, error) {
			return nil, nil, nil
		},
	}
	text, err := listWaits(t, empty, waitListOptions{thread: "thread-1"})
	if err != nil {
		t.Fatalf("an answered empty list is not a failure: %v", err)
	}
	if !strings.Contains(text, "Nothing is pending") || !strings.Contains(text, "sources read:") {
		t.Fatalf("an empty answer does not say what was read:\n%s", text)
	}

	unreadable := empty
	unreadable.local = func(context.Context, string) ([]wait.Wait, error) {
		return []wait.Wait{localCheck("w-1", "thread-1")}, nil
	}
	unreadable.coordinator = func(context.Context) ([]domain.NodeWait, []domain.TaskWait, error) {
		return nil, nil, errors.New("no coordinator answered")
	}
	text, err = listWaits(t, unreadable, waitListOptions{thread: "thread-1"})
	if err == nil {
		t.Fatal("a source that could not be read exited zero")
	}
	if !strings.Contains(text, "NOT READ: coordinator-held waits") {
		t.Fatalf("the output does not name the source that failed:\n%s", text)
	}
	if !strings.Contains(text, "w-1") {
		t.Fatalf("the degraded answer dropped what could be read:\n%s", text)
	}
	if !strings.Contains(text, "sources read: local checks on this host") {
		t.Fatalf("the degraded answer is not labelled local-only:\n%s", text)
	}
}

// Scope is applied before anything is hidden: --thread, --host and --all.
func TestWaitListScopesByThreadAndHostBeforeHiding(t *testing.T) {
	settled := pendingNodeWait("nw-settled", "thread-1", "omarchy-pc")
	settled.Delivery = "delivered"
	settledAt := waitListNow.Add(time.Minute)
	settled.SettledAt = &settledAt
	sources := waitListSources{
		host:  "omarchy-pc",
		local: func(context.Context, string) ([]wait.Wait, error) { return nil, nil },
		coordinator: func(context.Context) ([]domain.NodeWait, []domain.TaskWait, error) {
			return []domain.NodeWait{
				pendingNodeWait("nw-mine", "thread-1", "omarchy-pc"),
				pendingNodeWait("nw-theirs", "thread-2", "omarchy-pc"),
				pendingNodeWait("nw-elsewhere", "thread-1", "normandy"),
				settled,
			}, nil, nil
		},
	}

	mine, err := listWaits(t, sources, waitListOptions{thread: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]bool{
		"nw-mine": true, "nw-elsewhere": true, "nw-theirs": false, "nw-settled": false,
	} {
		if strings.Contains(mine, id) != want {
			t.Fatalf("%s present = %t, want %t:\n%s", id, !want, want, mine)
		}
	}
	if !strings.Contains(mine, "1 settled or delivered wait(s) hidden") {
		t.Fatalf("the hidden count is not reported:\n%s", mine)
	}

	here, err := listWaits(t, sources, waitListOptions{thread: "thread-1", host: "omarchy-pc"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(here, "nw-elsewhere") {
		t.Fatalf("--host did not scope out a wait recorded for another host:\n%s", here)
	}

	all, err := listWaits(t, sources, waitListOptions{all: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"nw-mine", "nw-theirs", "nw-elsewhere", "nw-settled"} {
		if !strings.Contains(all, id) {
			t.Fatalf("--all does not show %s:\n%s", id, all)
		}
	}
}

// M-2. The help page this verb ships declares the transport exit codes, and the
// t3-wait skill tells agents to branch on the code rather than on the message.
// The joined list stringified the failure of each source, so the class was gone
// by the time the process exited and every unreadable source exited 1.
func TestWaitListExitsWithTheTransportClassOfTheSourceThatFailed(t *testing.T) {
	unavailable := &backlogadmin.TransportError{
		Class: backlogadmin.ClassUnavailable, Operation: "node-wait",
		Err: errors.New("no coordinator answered"),
	}
	sources := waitListSources{
		host:  "omarchy-pc",
		local: func(context.Context, string) ([]wait.Wait, error) { return nil, nil },
		coordinator: func(context.Context) ([]domain.NodeWait, []domain.TaskWait, error) {
			return nil, nil, unavailable
		},
	}
	text, err := listWaits(t, sources, waitListOptions{thread: "thread-1"})
	if err == nil {
		t.Fatal("a source that could not be read exited zero")
	}
	if class := backlogadmin.ClassOf(err); class != backlogadmin.ClassUnavailable {
		t.Fatalf("class = %q, want %q (%v)", class, backlogadmin.ClassUnavailable, err)
	}
	if code := backlogadmin.ExitCodeFor(err); code != 5 {
		t.Fatalf("exit code = %d, want the 5 the help page promises (%v)", code, err)
	}
	// The text half of the answer is unchanged: an empty list whose source
	// failed is not "nothing is pending".
	if !strings.Contains(err.Error(), "this list is incomplete: coordinator-held waits:") {
		t.Fatalf("the message no longer names the incomplete list: %v", err)
	}
	for _, want := range []string{
		"No waits could be listed from the sources that answered",
		"NOT READ: coordinator-held waits",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("the empty degraded answer does not say %q:\n%s", want, text)
		}
	}

	// A failure that was never transport-classified keeps the generic exit 1,
	// so nothing that used to exit 1 now exits something else.
	plain := sources
	plain.coordinator = func(context.Context) ([]domain.NodeWait, []domain.TaskWait, error) {
		return nil, nil, errors.New("no coordinator answered")
	}
	_, err = listWaits(t, plain, waitListOptions{thread: "thread-1"})
	if code := backlogadmin.ExitCodeFor(err); code != 1 {
		t.Fatalf("an unclassified failure exits %d, want 1 (%v)", code, err)
	}

	// Two failed sources: the classified one decides the code, because the
	// unclassified one would only have produced the 1 this already falls back to.
	both := sources
	both.local = func(context.Context, string) ([]wait.Wait, error) {
		return nil, errors.New("state database is locked")
	}
	_, err = listWaits(t, both, waitListOptions{thread: "thread-1"})
	if code := backlogadmin.ExitCodeFor(err); code != 5 {
		t.Fatalf("exit code = %d, want the classified source's 5 (%v)", code, err)
	}
}

// S-1. A thread that cannot be resolved leaves the scope wide, so the answer is
// every thread on every host. The empty answer said so; a non-empty one read as
// this thread's waits when it was not.
func TestWaitListSaysWhenItCouldNotScopeToTheCallingThread(t *testing.T) {
	sources := waitListSources{
		host:  "omarchy-pc",
		local: func(context.Context, string) ([]wait.Wait, error) { return nil, nil },
		coordinator: func(context.Context) ([]domain.NodeWait, []domain.TaskWait, error) {
			return []domain.NodeWait{
				pendingNodeWait("nw-mine", "thread-1", "omarchy-pc"),
				pendingNodeWait("nw-theirs", "thread-2", "normandy"),
			}, nil, nil
		},
	}
	unresolved := errors.New("no T3 thread could be resolved from the caller's provider session")
	text, err := listWaits(t, sources, waitListOptions{threadResolutionErr: unresolved})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "nw-theirs") {
		t.Fatalf("the unscoped list dropped another thread's wait:\n%s", text)
	}
	for _, want := range []string{
		"No thread could be resolved for the caller",
		"it is every thread on every host",
		"--thread",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("a silently widened scope does not say %q:\n%s", want, text)
		}
	}

	// A list that was scoped says nothing of the kind.
	scoped, err := listWaits(t, sources, waitListOptions{thread: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(scoped, "No thread could be resolved") {
		t.Fatalf("a scoped list claimed it could not scope:\n%s", scoped)
	}
}

// B-4. The native inventory and "wait add" printed a JSON document with no
// --json asked for, with the registration and request blocks duplicated and
// every duration as a nanosecond count.
func TestNativeWaitOutputIsTextByDefaultAndJSONOnDemand(t *testing.T) {
	registration := pendingNodeWait("nw-1", "thread-1", "omarchy-pc")
	registration.Registration = registration.Request
	result := backlogadmin.NodeWaitResponse{Waits: []domain.NodeWait{registration}}

	var text bytes.Buffer
	if err := renderNativeWaitResult(&text, result, waitListOptions{all: true}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text.String(), "86400000000000") {
		t.Fatalf("a duration was printed as a nanosecond count:\n%s", text.String())
	}
	for _, unwanted := range []string{"\"registration\"", "\"request\"", "{"} {
		if strings.Contains(text.String(), unwanted) {
			t.Fatalf("the default output is still a JSON document (%q):\n%s", unwanted, text.String())
		}
	}
	if !strings.Contains(text.String(), "nw-1") || !strings.Contains(text.String(), "deadline=2026-09-20T10:00:00Z") {
		t.Fatalf("the text form does not carry the wait and its deadline:\n%s", text.String())
	}

	var machine bytes.Buffer
	if err := renderNativeWaitResult(&machine, result, waitListOptions{all: true, asJSON: true}); err != nil {
		t.Fatal(err)
	}
	var answer waitListAnswer
	if err := json.Unmarshal(machine.Bytes(), &answer); err != nil {
		t.Fatalf("--json is not one document: %v\n%s", err, machine.String())
	}
	if len(answer.Rows) != 1 || answer.Rows[0].ID != "nw-1" {
		t.Fatalf("answer = %+v", answer)
	}
	if strings.Contains(machine.String(), "86400000000000") {
		t.Fatalf("the JSON form still carries a raw nanosecond duration:\n%s", machine.String())
	}
}

// The native inventory accepts the scope it used to refuse with
// `unknown native wait list flag "--thread"`.
func TestNativeWaitListAcceptsThreadAndHost(t *testing.T) {
	result := backlogadmin.NodeWaitResponse{Waits: []domain.NodeWait{
		pendingNodeWait("nw-mine", "thread-1", "omarchy-pc"),
		pendingNodeWait("nw-theirs", "thread-2", "normandy"),
	}}
	var out bytes.Buffer
	if err := renderNativeWaitResult(&out, result, waitListOptions{all: true, thread: "thread-1"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "nw-mine") || strings.Contains(out.String(), "nw-theirs") {
		t.Fatalf("--thread did not scope the inventory:\n%s", out.String())
	}

	out.Reset()
	if err := renderNativeWaitResult(&out, result, waitListOptions{all: true, host: "normandy"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "nw-theirs") || strings.Contains(out.String(), "nw-mine") {
		t.Fatalf("--host did not scope the inventory:\n%s", out.String())
	}
}
