package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// recordingWakeControl is waitTestControl keeping the text of every wake.
type recordingWakeControl struct {
	waitTestControl
	texts []string
}

func (c *recordingWakeControl) SendNodeWake(ctx context.Context, thread domain.Thread, messageID, text string) error {
	c.texts = append(c.texts, text)
	return c.waitTestControl.SendNodeWake(ctx, thread, messageID, text)
}

// registerTaskConditions registers each argument list as a task-bound wait
// through the command and returns what it printed.
func registerTaskConditions(t *testing.T, ctx context.Context, cfg config.Config, conditions ...[]string) []string {
	t.Helper()
	var outputs []string
	for _, args := range conditions {
		var err error
		output := captureStdout(t, func() { err = cmdTaskWaitAdd(ctx, cfg, args) })
		if err != nil {
			t.Fatalf("registering %v: %v", args, err)
		}
		outputs = append(outputs, output)
	}
	return outputs
}

// tickTaskWaits runs the steward's wait runner once, with the checks whose
// local wait id is in met exiting 0 and every other one not yet.
func tickTaskWaits(t *testing.T, ctx context.Context, store *sqlite.Store, at time.Time, met map[string]bool) *recordingWakeControl {
	t.Helper()
	control := &recordingWakeControl{waitTestControl: waitTestControl{threads: map[string]*domain.Thread{
		"thread-1": {ID: "thread-1", ProviderInstanceID: "claudeAgent"},
	}}}
	runner := wait.New(store, control, nil)
	runner.DisableQuotaChecks = true
	runner.SetClock(func() time.Time { return at })
	runner.Exec = func(_ context.Context, w wait.Wait) (string, int, error) {
		if met[w.ID] {
			return "done " + w.ID, 0, nil
		}
		return "not yet", 1, nil
	}
	runner.Tick(ctx, nil, nil)
	return control
}

// taskWaitFixtureNow is the coordinator clock of taskWaitCLIFixture. The
// registration probe stamps the local checks with the real clock, so they are
// moved onto this one before the runner ticks: otherwise a run on any later
// day sees the coordinator deadline already passed.
var taskWaitFixtureNow = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

func rebaseLocalChecks(t *testing.T, ctx context.Context, store *sqlite.Store) []wait.Wait {
	t.Helper()
	locals, err := store.ListWaits(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	for index := range locals {
		locals[index].CreatedAt = taskWaitFixtureNow
		locals[index].LastRunAt = &taskWaitFixtureNow
		if err := store.SaveWait(ctx, locals[index]); err != nil {
			t.Fatal(err)
		}
	}
	return locals
}

func attemptProgress(t *testing.T, ctx context.Context, store *sqlite.Store) domain.ProgressState {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return records.Attempts[0].Progress
}

// W2, the field case: a second `wait add --task current` in the same park
// used to print the first wait's id and "registered", and keep only the first
// condition. Now each condition gets its own wait, the default is all, the
// first settlement alone does not wake the task, and the wake that does
// reports both conditions.
func TestTaskWaitSecondConditionJoinsTheParkUnderAll(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	outputs := registerTaskConditions(t, ctx, cfg,
		[]string{"--task", "current", "--name", "run A", "--", "false"},
		[]string{"--task", "current", "--name", "run B", "--", "false"},
	)
	records, err := store.ListTaskWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].ID == records[1].ID {
		t.Fatalf("the park holds %+v, want two distinct conditions", records)
	}
	for index, record := range records {
		if record.Wake != domain.WakeAll {
			t.Fatalf("wait %s took no wake flag and is %q, want all", record.ID, record.Wake)
		}
		if !strings.Contains(outputs[0]+outputs[1], record.ID) {
			t.Fatalf("wait %s was never printed: %q", record.ID, outputs)
		}
		if !strings.Contains(outputs[index], "Wake: all.") {
			t.Fatalf("registration %d does not say how the park wakes: %q", index, outputs[index])
		}
	}
	locals := rebaseLocalChecks(t, ctx, store)
	if len(locals) != 2 {
		t.Fatalf("local checks = %v", locals)
	}

	start := taskWaitFixtureNow
	first := map[string]bool{locals[0].ID: true}
	if control := tickTaskWaits(t, ctx, store, start.Add(2*time.Minute), first); len(control.texts) != 0 {
		t.Fatalf("the task was woken after one of two conditions: %q", control.texts)
	}
	if progress := attemptProgress(t, ctx, store); progress != domain.ProgressWaitingExternal {
		t.Fatalf("the attempt left the park after one of two conditions: %q", progress)
	}

	both := map[string]bool{locals[0].ID: true, locals[1].ID: true}
	control := tickTaskWaits(t, ctx, store, start.Add(10*time.Minute), both)
	if len(control.texts) != 1 {
		t.Fatalf("the task was told %d times, want once", len(control.texts))
	}
	if progress := attemptProgress(t, ctx, store); progress != domain.ProgressActive {
		t.Fatalf("the attempt did not resume once both conditions settled: %q", progress)
	}
	text := control.texts[0]
	trailer, ok := wait.ParseWakeTrailer(text)
	if !ok || trailer["count"] != "2" {
		t.Fatalf("the trailer does not count both conditions: %v", trailer)
	}
	for _, record := range records {
		if !strings.Contains(trailer["waits"], record.ID+":met") {
			t.Fatalf("the trailer does not list %s: %v", record.ID, trailer)
		}
	}
	for _, want := range []string{"run A: met", "run B: met"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the wake does not report %q:\n%s", want, text)
		}
	}
}

// With --any on each condition the first settlement wakes the task, and the
// registration says so.
func TestTaskWaitSecondConditionWithAnyWakesOnTheFirst(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	outputs := registerTaskConditions(t, ctx, cfg,
		[]string{"--task", "current", "--any", "--name", "run A", "--", "false"},
		[]string{"--task", "current", "--any", "--name", "run B", "--", "false"},
	)
	if !strings.Contains(outputs[1], "Wake: any.") {
		t.Fatalf("an --any registration does not say how it wakes: %q", outputs[1])
	}
	records, _ := store.ListTaskWaits(ctx)
	if len(records) != 2 || records[0].Wake != domain.WakeEach || records[1].Wake != domain.WakeEach {
		t.Fatalf("the park holds %+v, want two each conditions", records)
	}
	locals := rebaseLocalChecks(t, ctx, store)
	if len(locals) != 2 {
		t.Fatalf("local checks = %v", locals)
	}
	control := tickTaskWaits(t, ctx, store, taskWaitFixtureNow.Add(2*time.Minute), map[string]bool{locals[1].ID: true})
	if len(control.texts) != 1 {
		t.Fatalf("the task was told %d times, want once", len(control.texts))
	}
	if progress := attemptProgress(t, ctx, store); progress != domain.ProgressActive {
		t.Fatalf("--any did not wake the task on the first settlement: %q", progress)
	}
}

// A retry of the same command is still the same wait: the derived request id
// depends on the condition, not on when the command ran, so --for 30m and a
// later --json both replay.
func TestTaskWaitRetryOfTheSameConditionReplays(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	outputs := registerTaskConditions(t, ctx, cfg,
		[]string{"--task", "current", "--for", "30m"},
		[]string{"--task", "current", "--for", "30m"},
	)
	records, _ := store.ListTaskWaits(ctx)
	if len(records) != 1 || !strings.Contains(outputs[1], records[0].ID) {
		t.Fatalf("a retry made %d waits: %+v", len(records), records)
	}
	if taskWaitArgsDigest([]string{"--task", "current", "--json", "--for", "30m"}) != taskWaitArgsDigest([]string{"--task", "current", "--for", "30m"}) {
		t.Fatal("--json changed the derived request id")
	}
	if taskWaitArgsDigest([]string{"--task", "current", "--", "a", "--json"}) == taskWaitArgsDigest([]string{"--task", "current", "--", "a"}) {
		t.Fatal("an argument of the shell check itself was dropped from the derived request id")
	}
}

// The refusal paths: a different condition under a reused --request-id, and a
// coordinator kind joining a local all set, are both refused by name and park
// nothing new.
func TestTaskWaitSecondConditionRefusals(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	registerTaskConditions(t, ctx, cfg, []string{"--task", "current", "--request-id", "ci", "--", "false"})

	var err error
	captureStdout(t, func() {
		err = cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "ci", "--", "sh", "-c", "exit 1"})
	})
	if err == nil || !strings.Contains(err.Error(), "already registered a different condition") || !strings.Contains(err.Error(), "tw-ci") {
		t.Fatalf("a different condition under a reused request id was not refused by name: %v", err)
	}

	captureStdout(t, func() {
		err = cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--node", "other-run/other-task"})
	})
	if err == nil || !strings.Contains(err.Error(), "--any") || !strings.Contains(err.Error(), "tw-ci") {
		t.Fatalf("a coordinator kind joined a local all set without a refusal naming --any: %v", err)
	}
	if records, _ := store.ListTaskWaits(ctx); len(records) != 1 {
		t.Fatalf("the refusals left %d waits, want the first alone", len(records))
	}
}

// A single condition behaves as it always has: it parks the attempt, its
// settlement alone resumes it, and the trailer is the single-wait trailer.
func TestTaskWaitSingleConditionIsUnchanged(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	registerTaskConditions(t, ctx, cfg, []string{"--task", "current", "--name", "only", "--", "false"})
	if progress := attemptProgress(t, ctx, store); progress != domain.ProgressWaitingExternal {
		t.Fatalf("one condition did not park the attempt: %q", progress)
	}
	locals := rebaseLocalChecks(t, ctx, store)
	if len(locals) != 1 {
		t.Fatalf("local checks = %v", locals)
	}
	control := tickTaskWaits(t, ctx, store, taskWaitFixtureNow.Add(2*time.Minute), map[string]bool{locals[0].ID: true})
	if len(control.texts) != 1 {
		t.Fatalf("the task was told %d times, want once", len(control.texts))
	}
	if progress := attemptProgress(t, ctx, store); progress != domain.ProgressActive {
		t.Fatalf("one settled condition did not resume the attempt: %q", progress)
	}
	text := control.texts[0]
	trailer, _ := wait.ParseWakeTrailer(text)
	if _, grouped := trailer["count"]; grouped {
		t.Fatalf("a single wake carries group pairs: %v", trailer)
	}
	if _, grouped := trailer["waits"]; grouped {
		t.Fatalf("a single wake carries group pairs: %v", trailer)
	}
	if !strings.Contains(text, "its registered wait has settled") {
		t.Fatalf("the single wake changed its wording:\n%s", text)
	}
}

// --all and --any are task-bound spellings of --wake; two spellings at once,
// or either on an interactive wait, are refused. A task-bound wait defaults
// to all and an interactive one to each.
func TestTaskWaitWakeFlags(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		args []string
		want string
		err  string
	}{
		{args: []string{"--task", "current", "--", "false"}, want: "all"},
		{args: []string{"--task", "current", "--all", "--", "false"}, want: "all"},
		{args: []string{"--task", "current", "--any", "--", "false"}, want: "each"},
		{args: []string{"--task", "current", "--wake", "each", "--", "false"}, want: "each"},
		{args: []string{"--", "false"}, want: "each"},
		{args: []string{"--task", "current", "--all", "--any", "--", "false"}, err: "give one"},
		{args: []string{"--task", "current", "--any", "--wake", "each", "--", "false"}, err: "give one"},
		{args: []string{"--any", "--", "false"}, err: "apply to --task current"},
		{args: []string{"--task", "current", "--wake", "some", "--", "false"}, err: "--wake must be each or all"},
	} {
		spec, err := parseLocalWaitSpec(tc.args, now)
		switch {
		case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
			t.Errorf("%v: err = %v, want %q", tc.args, err, tc.err)
		case tc.err == "" && err != nil:
			t.Errorf("%v: %v", tc.args, err)
		case tc.err == "" && spec.WakeMode != tc.want:
			t.Errorf("%v: wake = %q, want %q", tc.args, spec.WakeMode, tc.want)
		}
	}
	for _, tc := range []struct {
		args []string
		want string
		err  string
	}{
		{args: []string{"--task", "current", "--node", "r/t"}, want: "all"},
		{args: []string{"--task", "current", "--any", "--node", "r/t"}, want: "each"},
		{args: []string{"--node", "r/t"}, want: "each"},
		{args: []string{"--all", "--node", "r/t"}, err: "apply to --task current"},
		{args: []string{"--task", "current", "--all", "--wake", "all", "--node", "r/t"}, err: "give one"},
	} {
		spec, err := parseCoordinatorWaitSpec(tc.args)
		switch {
		case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
			t.Errorf("%v: err = %v, want %q", tc.args, err, tc.err)
		case tc.err == "" && err != nil:
			t.Errorf("%v: %v", tc.args, err)
		case tc.err == "" && spec.WakeMode != tc.want:
			t.Errorf("%v: wake = %q, want %q", tc.args, spec.WakeMode, tc.want)
		}
	}
}
