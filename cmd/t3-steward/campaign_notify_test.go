package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// campaignNotifyCLI is a submit CLI with the node-wait seam recorded. The
// admin seam stays armed to fail, because registering a notification must not
// reach any mutation path other than the wait itself.
func campaignNotifyCLI(
	t *testing.T,
	out io.Writer,
	fake *fakeSubmissionService,
	registered *[]backlogadmin.NodeWaitOperation,
	thread func(string) (string, error),
	failure error,
) campaignCLI {
	t.Helper()
	return campaignCLI{
		limits:      campaignTestLimits,
		stdout:      out,
		submissions: func() (adminSubmissionService, error) { return fake, nil },
		admin: func(args []string) error {
			t.Fatalf("a notification reached the admin path with %v", args)
			return nil
		},
		amend: func(context.Context, domain.GraphAmendment) (domain.GraphAmendmentResult, error) {
			t.Fatal("a notification amended the graph")
			return domain.GraphAmendmentResult{}, nil
		},
		viability: func(_ context.Context, request backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
			return campaignReadyMatrix(request), nil
		},
		notify: func(_ context.Context, operation backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
			*registered = append(*registered, operation)
			if failure != nil {
				return backlogadmin.NodeWaitResponse{}, failure
			}
			return backlogadmin.NodeWaitResponse{Waits: []domain.NodeWait{{
				Request: operation.Request, Delivery: "pending",
			}}}, nil
		},
		resolveThread: thread,
	}
}

func TestCampaignSubmitNotifiesTheResolvedThreadOnTheRunSink(t *testing.T) {
	root := campaignFixture(t)
	var out bytes.Buffer
	var registered []backlogadmin.NodeWaitOperation
	var asked []string
	cli := campaignNotifyCLI(t, &out, &fakeSubmissionService{}, &registered,
		func(explicit string) (string, error) {
			asked = append(asked, explicit)
			return "thread-42", nil
		}, nil)
	if err := cli.run(context.Background(), []string{
		"submit", root, "--idempotency-key", "campaign-1", "--notify-thread", "current",
	}); err != nil {
		t.Fatal(err)
	}
	// "current" is handed to the resolver as an empty explicit id: it is a
	// request to resolve, never a thread id in its own right.
	if len(asked) != 1 || asked[0] != "" {
		t.Fatalf("the resolver was asked %v", asked)
	}
	if len(registered) != 1 {
		t.Fatalf("registered %d waits, want 1", len(registered))
	}
	operation := registered[0]
	if operation.Action != "register" {
		t.Fatalf("action = %q", operation.Action)
	}
	want := domain.NodeRef{RunID: "run-1", TaskID: domain.SinkTaskName}
	if operation.Request.Target != want {
		t.Fatalf("target = %+v, want the run sink %+v", operation.Request.Target, want)
	}
	if operation.Request.ThreadID != "thread-42" {
		t.Fatalf("thread = %q", operation.Request.ThreadID)
	}
	// The registration ID is derived from the submission key, so re-running
	// the same command registers the same wait rather than a second one.
	if operation.Request.ID != "nw-campaign-campaign-1" {
		t.Fatalf("registration id = %q", operation.Request.ID)
	}
	if operation.Request.Timeout != campaignNotifyTimeout {
		t.Fatalf("timeout = %s", operation.Request.Timeout)
	}
	if !strings.Contains(out.String(), "End this turn now") {
		t.Fatalf("submit did not tell the agent to end its turn:\n%s", out.String())
	}
}

func TestCampaignSubmitNotifiesAnExplicitThreadWithoutResolving(t *testing.T) {
	root := campaignFixture(t)
	var registered []backlogadmin.NodeWaitOperation
	var out bytes.Buffer
	cli := campaignNotifyCLI(t, &out, &fakeSubmissionService{}, &registered,
		func(explicit string) (string, error) { return explicit, nil }, nil)
	if err := cli.run(context.Background(), []string{
		"submit", root, "--idempotency-key", "campaign-1", "--notify-thread", "thread-9", "--json",
	}); err != nil {
		t.Fatal(err)
	}
	if len(registered) != 1 || registered[0].Request.ThreadID != "thread-9" {
		t.Fatalf("registered %+v", registered)
	}
	var document campaignSubmission
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if document.Notify == nil || document.Notify.ThreadID != "thread-9" ||
		document.Notify.WaitID != "nw-campaign-campaign-1" {
		t.Fatalf("notify = %+v", document.Notify)
	}
	if document.RunID != "run-1" {
		t.Fatalf("the submission answer changed shape: %+v", document)
	}
}

// A submission without --notify-thread must not register anything, so the flag
// is the only thing that creates a wait.
func TestCampaignSubmitRegistersNothingWithoutNotifyThread(t *testing.T) {
	root := campaignFixture(t)
	var registered []backlogadmin.NodeWaitOperation
	cli := campaignNotifyCLI(t, io.Discard, &fakeSubmissionService{}, &registered,
		func(string) (string, error) {
			t.Fatal("a submission without --notify-thread resolved a thread")
			return "", nil
		}, nil)
	if err := cli.run(context.Background(), []string{
		"submit", root, "--idempotency-key", "campaign-1",
	}); err != nil {
		t.Fatal(err)
	}
	if len(registered) != 0 {
		t.Fatalf("registered %+v", registered)
	}
}

// The registration is a wait and nothing else: one node-wait operation, no
// second submission, no mutation, no graph amendment.
func TestCampaignNotificationCreatesNoWorkflowState(t *testing.T) {
	root := campaignFixture(t)
	var registered []backlogadmin.NodeWaitOperation
	fake := &fakeSubmissionService{}
	cli := campaignNotifyCLI(t, io.Discard, fake, &registered,
		func(string) (string, error) { return "thread-42", nil }, nil)
	if err := cli.run(context.Background(), []string{
		"submit", root, "--idempotency-key", "campaign-1", "--notify-thread", "current",
	}); err != nil {
		t.Fatal(err)
	}
	if len(registered) != 1 {
		t.Fatalf("node-wait operations = %d, want exactly one", len(registered))
	}
	if registered[0].Task != nil {
		t.Fatal("the notification parked a task attempt")
	}
	if registered[0].Request.Target.TaskID != domain.SinkTaskName {
		t.Fatal("the notification targeted something other than the run sink")
	}
	// The submission itself is unchanged by the flag: same key, same bytes.
	if fake.request.IdempotencyKey != "campaign-1" || len(fake.raw) == 0 {
		t.Fatalf("submission request = %+v", fake.request)
	}
}

// An unresolvable "current" must fail before anything is submitted: a campaign
// nobody is listening for is worse than a campaign that was not submitted.
func TestCampaignSubmitRefusesAnUnresolvableCurrentThreadBeforeSubmitting(t *testing.T) {
	root := campaignFixture(t)
	var registered []backlogadmin.NodeWaitOperation
	fake := &fakeSubmissionService{}
	unresolved := errors.New("no T3 thread could be resolved from the caller's provider session")
	cli := campaignNotifyCLI(t, io.Discard, fake, &registered,
		func(string) (string, error) { return "", unresolved }, nil)
	err := cli.run(context.Background(), []string{
		"submit", root, "--idempotency-key", "campaign-1", "--notify-thread", "current",
	})
	if err == nil {
		t.Fatal("an unresolvable --notify-thread was accepted")
	}
	for _, fragment := range []string{
		"--notify-thread current could not be resolved",
		"nothing was submitted",
		"Pass --notify-thread <id>",
	} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("the message does not say %q: %v", fragment, err)
		}
	}
	if fake.raw != nil || fake.size != 0 {
		t.Fatal("a campaign was submitted for a thread that could not be resolved")
	}
	if len(registered) != 0 {
		t.Fatal("a wait was registered for a thread that could not be resolved")
	}
}

// If the run exists and only the registration failed, the error has to say so
// and name the command that repairs it. Reporting a plain failure would leave
// an agent believing nothing happened.
func TestCampaignSubmitReportsARegistrationFailureWithTheRunItCreated(t *testing.T) {
	root := campaignFixture(t)
	var registered []backlogadmin.NodeWaitOperation
	cli := campaignNotifyCLI(t, io.Discard, &fakeSubmissionService{}, &registered,
		func(string) (string, error) { return "thread-42", nil },
		errors.New("the coordinator is unavailable"))
	err := cli.run(context.Background(), []string{
		"submit", root, "--idempotency-key", "campaign-1", "--notify-thread", "current",
	})
	if err == nil {
		t.Fatal("a failed registration was reported as success")
	}
	for _, fragment := range []string{
		"submitted as run run-1",
		"t3-steward wait add --run run-1 --thread thread-42",
	} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("the message does not say %q: %v", fragment, err)
		}
	}
}

// campaign submit registers through the same path as task run and has the same
// promise to keep. A wake that cannot reach this host is reported as what it
// is, and the caller is not told to end its turn on it.
func TestCampaignSubmitDoesNotPromiseAWakeItCannotDeliver(t *testing.T) {
	root := campaignFixture(t)
	var out bytes.Buffer
	var registered []backlogadmin.NodeWaitOperation
	cli := campaignNotifyCLI(t, &out, &fakeSubmissionService{}, &registered,
		func(string) (string, error) { return "thread-42", nil }, nil)
	cli.wakeHost = func() (string, error) { return "omarchy-pc", nil }
	cli.release = func(context.Context) (string, error) { return "v0.11.0-rc.70", nil }
	cli.notify = func(_ context.Context, operation backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
		registered = append(registered, operation)
		host := operation.Host
		if host == "" {
			host = "normandy"
		}
		return backlogadmin.NodeWaitResponse{Waits: []domain.NodeWait{{
			Request: operation.Request, Delivery: "pending", Host: host,
		}}}, nil
	}
	if err := cli.run(context.Background(), []string{
		"submit", root, "--idempotency-key", "campaign-1", "--notify-thread", "current",
	}); err != nil {
		t.Fatal(err)
	}
	if len(registered) != 1 || registered[0].Host != "" {
		t.Fatalf("an rc.70 coordinator was told the calling host: %+v", registered)
	}
	text := out.String()
	if strings.Contains(text, "End this turn now") {
		t.Fatalf("a wake that cannot be delivered was promised anyway:\n%s", text)
	}
	for _, want := range []string{"undeliverable", "normandy", "omarchy-pc", "do not end this turn"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the report does not say %q:\n%s", want, text)
		}
	}
}

// What each undeliverable wake says, and which of them says nothing at all.
// The distinction that matters is between a coordinator that cannot record the
// calling host and one that did record a different one, because only the first
// is fixed by upgrading the coordinator.
func TestUndeliverableWakeNamesTheCauseItCanProve(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		recorded, local, release string
		want                     []string
	}{
		{"delivered here", "omarchy-pc", "omarchy-pc", nodeWakeDeliveryHostRelease, nil},
		{"no host recorded", "", "omarchy-pc", nodeWakeDeliveryHostRelease, nil},
		{"this host unnameable", "normandy", "", nodeWakeDeliveryHostRelease, nil},
		{"an older coordinator", "normandy", "omarchy-pc", "v0.11.0-rc.70",
			[]string{"normandy", "omarchy-pc", nodeWakeDeliveryHostRelease, "v0.11.0-rc.70"}},
		{"a release this client cannot read", "normandy", "omarchy-pc", "dev-build",
			[]string{"no release this client can read", `"dev-build"`}},
		{"registered earlier elsewhere", "normandy", "omarchy-pc", "v0.12.0",
			[]string{"does record the calling host", "registered earlier from normandy"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := undeliverableWake(tc.recorded, tc.local, tc.release)
			if len(tc.want) == 0 {
				if got != "" {
					t.Fatalf("a deliverable wake was reported as undeliverable: %q", got)
				}
				return
			}
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Fatalf("the reason does not say %q: %q", want, got)
				}
			}
		})
	}
}

func TestCampaignNotifyThreadIsOnlyValidOnSubmit(t *testing.T) {
	for _, command := range []string{"validate", "plan", "check"} {
		if _, err := parseCampaignArgs(command, []string{"dir", "--notify-thread", "current"}, true, false); err == nil {
			t.Fatalf("campaign %s accepted --notify-thread", command)
		}
	}
	if _, err := parseCampaignArgs("submit", []string{"dir", "--idempotency-key", "k", "--notify-thread"}, false, true); err == nil {
		t.Fatal("--notify-thread was accepted without a value")
	}
	parsed, err := parseCampaignArgs("submit",
		[]string{"dir", "--idempotency-key", "k", "--notify-thread", "current"}, false, true)
	if err != nil || parsed.notify != "current" {
		t.Fatalf("parsed = %+v, err = %v", parsed, err)
	}
}
