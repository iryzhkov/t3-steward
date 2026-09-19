package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Contract 1 on the sibling verb.
//
// "campaign submit" attaches its wake through the same path "task run" uses,
// so the same rule decides what it may promise. It was fixed on "task run"
// alone and went on printing "End this turn now" here for a wait the
// coordinator had answered as delivered, as cancelled, or as held for another
// thread -- and the same release made this verb notify by default, so a plain
// re-submission of a finished campaign took that path.
//
// Every state the rule exists for is driven below through the real submit
// command line.

// campaignWakeCase is one coordinator, stated by the answers it gives.
type campaignWakeCase struct {
	name string
	// delivery and waitThread are what the coordinator answers for every
	// registration, including the fresh one a spent answer provokes. A
	// coordinator that answers spent for the derived ID and live for a new one
	// is the re-registration case, which has its own test below.
	delivery   string
	waitThread string
	// replay says the idempotency key resolved to a run that already exists,
	// and progress and progressErr are what that run's own progress turns out
	// to be.
	replay      bool
	progress    domain.ProgressState
	progressErr error
	// want and unwanted are fragments of the printed record.
	want     []string
	unwanted []string
	// registrations is how many node-wait operations the submission sent.
	registrations int
}

// healthyWakeDelivery is a host whose steward daemon is delivering node wakes
// now, so that nothing in these cases is undeliverable and the answer under
// test is the coordinator's, not the daemon's.
func healthyWakeDelivery() nodeWakeDelivery {
	return deliveryReceiptSeam(nodeWakeDeliveryReceipt{
		SchemaVersion: nodeWakeDeliveryReceiptSchema, Release: nodeWakeDeliveryHostRelease,
		Host: "omarchy-pc", Interval: "15s", UpdatedAt: time.Now().UTC(),
	}, nil)
}

func (tc campaignWakeCase) submit(t *testing.T, args ...string) (string, []backlogadmin.NodeWaitOperation) {
	t.Helper()
	root := campaignFixture(t)
	var out bytes.Buffer
	var registered []backlogadmin.NodeWaitOperation
	cli := campaignNotifyCLI(t, &out, &fakeSubmissionService{replay: tc.replay}, &registered,
		func(string) (string, error) { return "thread-1", nil }, nil)
	cli.wakeHost = func() (string, error) { return "omarchy-pc", nil }
	cli.release = func(context.Context) (string, error) { return nodeWakeDeliveryHostRelease, nil }
	cli.delivery = healthyWakeDelivery()
	cli.notify = func(_ context.Context, operation backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
		registered = append(registered, operation)
		request := operation.Request
		if tc.waitThread != "" {
			// The coordinator holds this registration for a thread other than the
			// one that asked, which is a wake this caller will never receive.
			request.ThreadID = tc.waitThread
		}
		return backlogadmin.NodeWaitResponse{Waits: []domain.NodeWait{{
			Request: request, Delivery: tc.delivery, Host: "omarchy-pc",
		}}}, nil
	}
	cli.describe = func(context.Context, string) (backlogadmin.WorkflowSummary, error) {
		if tc.progressErr != nil {
			return backlogadmin.WorkflowSummary{}, tc.progressErr
		}
		return backlogadmin.WorkflowSummary{
			Run: domain.WorkflowRun{ID: "run-1", WorkflowID: "workflow-1", Progress: tc.progress},
		}, nil
	}
	command := append([]string{"submit", root, "--idempotency-key", "campaign-1"}, args...)
	if err := cli.run(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	return out.String(), registered
}

func TestCampaignSubmitPromisesAWakeOnlyWhenOneWillFire(t *testing.T) {
	for _, tc := range []campaignWakeCase{
		{
			name: "a fresh submission with a live wait", delivery: "pending", registrations: 1,
			want: []string{
				"notify thread thread-1 (wait nw-campaign-campaign-1)",
				"delivery=pending", "host=omarchy-pc", wakePromise,
			},
		},
		{
			// A-1 on this verb: the coordinator says the wake was already sent,
			// and a wake is sent once.
			name: "a wake the coordinator has already delivered", delivery: "delivered", registrations: 2,
			want: []string{
				"delivery=delivered", "No wake is attached to this run",
				"t3-steward wait add --run run-1",
			},
			unwanted: []string{wakePromise},
		},
		{
			name: "a wake the coordinator cancelled", delivery: "cancelled", registrations: 2,
			want:     []string{"delivery=cancelled", "No wake is attached to this run"},
			unwanted: []string{wakePromise},
		},
		{
			name: "a wait the coordinator holds for another thread", delivery: "pending",
			waitThread: "thread-other", registrations: 2,
			want:     []string{"held-for-thread=thread-other", "No wake is attached to this run"},
			unwanted: []string{wakePromise},
		},
		{
			// A re-submission of a campaign that already finished: nothing is
			// registered, because a wait on a settled sink would never fire.
			name: "a key that replayed onto a run which already ended", delivery: "pending",
			replay: true, progress: domain.ProgressSucceeded, registrations: 0,
			want:     []string{"This run already ended succeeded", "Collect it now"},
			unwanted: []string{wakePromise, "notify thread"},
		},
		{
			name: "a key that replayed onto a run still running", delivery: "pending",
			replay: true, progress: domain.ProgressActive, registrations: 1,
			want: []string{"delivery=pending", wakePromise},
		},
		{
			name: "a replay whose progress cannot be read", delivery: "pending",
			replay: true, progressErr: errors.New("coordinator unavailable"), registrations: 1,
			want: []string{
				"This run's progress could not be read", "coordinator unavailable",
				"t3-steward diagnose run-1",
			},
			unwanted: []string{wakePromise},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text, registered := tc.submit(t)
			for _, want := range tc.want {
				if !strings.Contains(text, want) {
					t.Fatalf("the record does not say %q:\n%s", want, text)
				}
			}
			for _, unwanted := range tc.unwanted {
				if strings.Contains(text, unwanted) {
					t.Fatalf("the record still says %q:\n%s", unwanted, text)
				}
			}
			if len(registered) != tc.registrations {
				t.Fatalf("registrations = %d, want %d: %+v", len(registered), tc.registrations, registered)
			}
		})
	}
}

// The other half of the spent-registration rule, as on "task run": when the
// wait the key derives is spent, a fresh one is registered under a new ID, and
// the promise is printed for that one because it will fire.
func TestCampaignSubmitRegistersAFreshWaitWhenTheDerivedOneIsSpent(t *testing.T) {
	root := campaignFixture(t)
	var out bytes.Buffer
	var registered []backlogadmin.NodeWaitOperation
	cli := campaignNotifyCLI(t, &out, &fakeSubmissionService{}, &registered,
		func(string) (string, error) { return "thread-1", nil }, nil)
	cli.wakeHost = func() (string, error) { return "omarchy-pc", nil }
	cli.release = func(context.Context) (string, error) { return nodeWakeDeliveryHostRelease, nil }
	cli.delivery = healthyWakeDelivery()
	cli.notify = func(_ context.Context, operation backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
		registered = append(registered, operation)
		delivery := "pending"
		if operation.Request.ID == "nw-campaign-campaign-1" {
			delivery = "delivered"
		}
		return backlogadmin.NodeWaitResponse{Waits: []domain.NodeWait{{
			Request: operation.Request, Delivery: delivery, Host: "omarchy-pc",
		}}}, nil
	}
	if err := cli.run(context.Background(), []string{
		"submit", root, "--idempotency-key", "campaign-1",
	}); err != nil {
		t.Fatal(err)
	}
	if len(registered) != 2 {
		t.Fatalf("registrations = %d, want the spent one and a fresh one: %+v", len(registered), registered)
	}
	if !strings.HasPrefix(registered[1].Request.ID, "nw-campaign-campaign-1-w-") {
		t.Fatalf("the second registration reused the spent ID: %q", registered[1].Request.ID)
	}
	text := out.String()
	if !strings.Contains(text, wakePromise) {
		t.Fatalf("a fresh wait was registered and the record did not say to end the turn:\n%s", text)
	}
	if !strings.Contains(text, registered[1].Request.ID) {
		t.Fatalf("the record names a wait other than the one that will fire:\n%s", text)
	}
}

// --no-notify is honest rather than silent: it says nothing will wake a
// thread, and it does not tell the caller to end its turn.
func TestCampaignSubmitWithNoNotifySaysNothingWillWakeAThread(t *testing.T) {
	text, registered := campaignWakeCase{delivery: "pending"}.submit(t, "--no-notify")
	if len(registered) != 0 {
		t.Fatalf("registrations = %+v", registered)
	}
	if !strings.Contains(text, "notify none: nothing will wake a thread when this run ends") {
		t.Fatalf("the record does not say that nothing will wake a thread:\n%s", text)
	}
	if strings.Contains(text, wakePromise) {
		t.Fatalf("a submission nobody is woken for told the caller to end its turn:\n%s", text)
	}
}

// Both verbs render the wake through one function, so the promise cannot be
// fixed on one of them and left standing on the other. This is the structural
// half of the same contract: if a second renderer appears, this fails.
func TestBothStartVerbsRenderTheWakeThroughTheSameRule(t *testing.T) {
	wake := startedRunWake{
		Run: "run-1",
		Notify: &campaignNotification{
			WaitID: "nw-campaign-k", ThreadID: "thread-1", Target: "run-1/__sink",
			Delivery: "delivered", Host: "omarchy-pc",
		},
	}
	var shared bytes.Buffer
	renderWake(&shared, wake)
	var viaTaskRun bytes.Buffer
	renderWake(&viaTaskRun, taskRunRecord{Run: wake.Run, Notify: wake.Notify}.wake())
	if shared.String() != viaTaskRun.String() {
		t.Fatalf("the two verbs disagree about one wake:\n%s\n---\n%s", shared.String(), viaTaskRun.String())
	}
	if strings.Contains(shared.String(), wakePromise) {
		t.Fatalf("a spent wake was promised:\n%s", shared.String())
	}
}
