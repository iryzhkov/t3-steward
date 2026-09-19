package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// B-6 and B-7, which are one screen of output.
//
// B-6's headline -- "the text renderers carry no clock" -- is false: diagnose
// already prints its generation time, a per-worker observation time and a
// per-wait deadline. What was missing is the attempt's own timeline, so that
// "it started an hour ago, why is it not finished" is answerable from the text
// path in one call instead of a second one in JSON.
//
// B-7 is the line under it. The provider's words "thread stopped, control
// stopped, phase completed" mean the provider thread was not generating at
// that instant, and they were printed bare, four lines under the same
// attempt's "progress: active" and "control: running".

var clockNow = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)

// runningAttemptDetail is the state the audit observed: an attempt the
// coordinator holds as active and running, started an hour ago, whose worker
// last reported a provider session that had stopped.
func runningAttemptDetail() *backlogadmin.TaskDetail {
	return &backlogadmin.TaskDetail{
		Task: domain.Task{ID: "task-1", Name: "implement", WorkflowID: "workflow-1"},
		Attempt: &domain.Attempt{
			ID: "attempt-1", Number: 1, Revision: 4,
			Progress: domain.ProgressActive, Control: domain.ControlRunning,
			UpdatedAt: clockNow.Add(-2 * time.Minute),
		},
		Assignment: &backlogadmin.Assignment{
			ID: "asg-1", AttemptID: "attempt-1", WorkerID: "normandy",
			Route:     domain.ProviderRoute{ProviderInstanceID: "t3-primary", Model: "opus-5"},
			State:     domain.AssignmentClaimed,
			CreatedAt: clockNow.Add(-time.Hour),
			UpdatedAt: clockNow.Add(-2 * time.Minute),
			// The lease is the one clock that says whether anything still holds
			// this attempt, which is the question after "how long has it been".
			LeaseExpiresAt: clockNow.Add(5 * time.Minute),
		},
		Evidence: &backlogadmin.AttemptEvidence{
			ThreadID: "thread-abc", WorkerID: "normandy",
			ThreadState: "stopped", Control: domain.ControlStopped, Phase: "completed",
			ObservedAt: clockNow.Add(-2 * time.Minute),
		},
	}
}

func runTaskShow(t *testing.T, detail *backlogadmin.TaskDetail) string {
	t.Helper()
	var out bytes.Buffer
	fake := &fakeAdminQueryService{response: backlogadmin.Response{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryTask, GeneratedAt: clockNow, Task: detail,
	}}
	cli := backlogAdminCLI{service: fake, principal: backlogadmin.Principal{ID: "operator"}, stdout: &out}
	if err := cli.runBacklog(context.Background(), []string{"task", "show", "run-1/task-1"}); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func runDiagnose(t *testing.T, detail backlogadmin.TaskDetail) string {
	t.Helper()
	return runDiagnoseTasks(t, []backlogadmin.TaskDetail{detail})
}

func runDiagnoseTasks(t *testing.T, tasks []backlogadmin.TaskDetail) string {
	t.Helper()
	var out bytes.Buffer
	fake := &fakeAdminQueryService{response: backlogadmin.Response{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryDiagnose, GeneratedAt: clockNow,
		Diagnosis: &backlogadmin.Diagnosis{
			GraphRevision: 3, GeneratedAt: clockNow,
			Workflow: backlogadmin.WorkflowDetail{
				Summary: backlogadmin.WorkflowSummary{
					Run:      domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressActive},
					Workflow: domain.Workflow{ID: "workflow-1", Name: "rebuild"},
				},
				Tasks: tasks,
			},
		},
	}}
	cli := backlogAdminCLI{service: fake, principal: backlogadmin.Principal{ID: "operator"}, stdout: &out}
	if err := cli.runBacklog(context.Background(), []string{"diagnose", "run-1"}); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func TestBothTextRenderersCarryTheAttemptsOwnClock(t *testing.T) {
	for _, form := range []struct {
		name   string
		text   string
		indent string
	}{
		{"task show", runTaskShow(t, runningAttemptDetail()), ""},
		{"diagnose", runDiagnose(t, *runningAttemptDetail()), "    "},
	} {
		t.Run(form.name, func(t *testing.T) {
			for _, want := range []string{
				form.indent + "started: 2026-09-19T11:00:00Z\n",
				form.indent + "elapsed: 1h0m0s (as of the coordinator's answer 2026-09-19T12:00:00Z)\n",
				form.indent + "lease: expires 2026-09-19T12:05:00Z (5m0s left)\n",
			} {
				if !strings.Contains(form.text, want) {
					t.Errorf("%s does not print %q:\n%s", form.name, want, form.text)
				}
			}
		})
	}
}

// A lapsed lease is the difference between "a worker is holding this" and
// "the coordinator is about to recover it", so it is said in words.
func TestTheLeaseLineSaysWhenTheLeaseHasLapsed(t *testing.T) {
	detail := runningAttemptDetail()
	detail.Assignment.LeaseExpiresAt = clockNow.Add(-90 * time.Second)
	if text := runTaskShow(t, detail); !strings.Contains(text, "lease: expires 2026-09-19T11:58:30Z (expired 1m30s ago)\n") {
		t.Errorf("the lease line does not say it lapsed:\n%s", text)
	}
}

// A finished attempt claims no lease: nothing holds it, and a deadline
// printed beside a terminal state reads as work still running.
func TestATerminalAttemptClaimsNoLease(t *testing.T) {
	detail := runningAttemptDetail()
	detail.Attempt.Progress = domain.ProgressSucceeded
	detail.Attempt.Control = domain.ControlStopped
	detail.Attempt.UpdatedAt = clockNow.Add(-10 * time.Minute)
	text := runTaskShow(t, detail)
	if strings.Contains(text, "lease:") {
		t.Errorf("a succeeded attempt still claims a lease:\n%s", text)
	}
	for _, want := range []string{
		"started: 2026-09-19T11:00:00Z\n",
		"elapsed: 50m0s (to the attempt's last update 2026-09-19T11:50:00Z)\n",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("a terminal attempt does not print %q:\n%s", want, text)
		}
	}
}

// Degraded: a record without the timestamps prints what is known and names
// the field that was absent, rather than a zero time rendered as a date.
func TestAMissingTimestampIsNamedRatherThanPrintedAsAZeroTime(t *testing.T) {
	detail := runningAttemptDetail()
	detail.Assignment = nil
	text := runTaskShow(t, detail)
	for _, want := range []string{
		"started: unknown (no assignment record, which is where assignment.createdAt lives)\n",
		"lease: none held (no assignment record; nothing holds this attempt)\n",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the degraded form does not print %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "0001-01-01") {
		t.Errorf("a zero time was rendered as a date:\n%s", text)
	}
}

// B-7. The provider session line cannot contradict the two lines above it.
func TestTheProviderSessionLineCannotContradictAnActiveAttempt(t *testing.T) {
	text := runTaskShow(t, runningAttemptDetail())
	if !strings.Contains(text, "progress: active\ncontrol: running\n") {
		t.Fatalf("the fixture is not the state the audit observed:\n%s", text)
	}
	// The bare concatenation is gone: no line begins with the word session,
	// and none of them says the work stopped.
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "session:") {
			t.Errorf("the unlabelled session line is still printed: %q", line)
		}
	}
	for _, want := range []string{
		"provider session: idle when the worker last looked",
		"thread stopped, control stopped, phase completed",
		"the attempt is progress active, control running",
		"not the task stopping",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the provider session line does not say %q:\n%s", want, text)
		}
	}

	// When the words agree with the attempt they are printed as the worker
	// reported them, still named as the provider session's own state.
	agreeing := runningAttemptDetail()
	agreeing.Attempt.Control = domain.ControlPaused
	agreeing.Evidence.Control = domain.ControlPaused
	agreeing.Evidence.Phase = "running"
	agreeing.Evidence.PauseReason = "t3-primary/claude/seven_day at 97%"
	text = runTaskShow(t, agreeing)
	if !strings.Contains(text, "provider session: thread stopped, control paused, phase running, observed 2026-09-19T11:58:00Z\n") {
		t.Errorf("a session the attempt agrees with is not printed plainly:\n%s", text)
	}
	if strings.Contains(text, "idle when the worker last looked") {
		t.Errorf("a paused attempt whose thread is stopped was reported as a contradiction:\n%s", text)
	}
}

// queuedAttemptDetail is the shape every task of a run has before it is
// dispatched: an attempt that exists because the run was planned, with no
// assignment and nothing holding it. It is the repository's own example of a
// task the coordinator has not started -- see cancel_run_test.go -- and it is
// what a freshly submitted campaign, a run held behind quota and a fleet with
// no eligible worker are full of.
func queuedAttemptDetail(id string) backlogadmin.TaskDetail {
	return backlogadmin.TaskDetail{
		Task: domain.Task{ID: id, Name: id, WorkflowID: "workflow-1"},
		Attempt: &domain.Attempt{
			ID: "attempt-" + id, Number: 1, Revision: 1,
			Progress: domain.ProgressQueued, Control: domain.ControlUnassigned,
		},
	}
}

// An attempt that has not started has no timeline, and saying "unknown" three
// times about it reports a defect in a record that is intact: the work has not
// begun. The degraded lines belong to a record that lost a timestamp, which is
// why they are still printed for a started attempt whose assignment is missing.
func TestAnAttemptThatHasNotStartedPrintsNoTimeline(t *testing.T) {
	queued := queuedAttemptDetail("task-0")
	for _, form := range []struct {
		name string
		text string
	}{
		{"diagnose", runDiagnose(t, queued)},
		{"task show", runTaskShow(t, &queued)},
	} {
		for _, unwanted := range []string{"started:", "elapsed:", "lease:", "unknown"} {
			if strings.Contains(form.text, unwanted) {
				t.Errorf("%s prints %q for a queued, unassigned attempt:\n%s", form.name, unwanted, form.text)
			}
		}
		if !strings.Contains(form.text, "attempt-task-0") {
			t.Errorf("%s does not name the attempt at all:\n%s", form.name, form.text)
		}
	}

	// A running attempt still prints all three lines: the gate is "has this
	// started", not "is this worth printing".
	running := runDiagnose(t, *runningAttemptDetail())
	for _, want := range []string{"    started: ", "    elapsed: ", "    lease: "} {
		if !strings.Contains(running, want) {
			t.Errorf("diagnose of a running attempt no longer prints %q:\n%s", want, running)
		}
	}
}

// TestQueuedDiagnoseMeasured records the size of the state diagnose is most
// often run in. The assertion is a proportion rather than a byte count: an
// eight-task queued run is the run's own five lines, the tasks heading and one
// line per task, and nothing else.
func TestQueuedDiagnoseMeasured(t *testing.T) {
	tasks := make([]backlogadmin.TaskDetail, 0, 8)
	for index := 0; index < 8; index++ {
		tasks = append(tasks, queuedAttemptDetail(fmt.Sprintf("task-%d", index)))
	}
	text := runDiagnoseTasks(t, tasks)
	lines := strings.Count(text, "\n")
	t.Logf("diagnose of eight queued tasks %d bytes, %d lines", len(text), lines)
	if lines != 14 {
		t.Errorf("diagnose of eight queued tasks is %d lines, want 14 -- five for the run, one heading and one per task:\n%s", lines, text)
	}
}
