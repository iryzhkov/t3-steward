package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

func TestParseAskArgs(t *testing.T) {
	spec, err := parseAskArgs([]string{"M6b field test: pick one", "--option", "alpha", "--option", "beta", "--deadline", "2h", "--default", "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if spec.Request.Question != "M6b field test: pick one" || len(spec.Request.Options) != 2 || spec.Deadline != 2*time.Hour ||
		spec.Request.OnDeadline != domain.AskDeadlineDefault || spec.Request.Default[0] != "alpha" {
		t.Fatalf("spec = %+v", spec)
	}
	// The question may follow the flags, and no deadline means a week, then fail.
	spec, err = parseAskArgs([]string{"--option", "a", "--option", "b", "Which?", "--multi"})
	if err != nil || spec.Request.Question != "Which?" || !spec.Request.Multi ||
		spec.Deadline != domain.AskDefaultMaxWaiting || spec.Request.OnDeadline != domain.AskDeadlineFail {
		t.Fatalf("spec=%+v err=%v", spec, err)
	}
	contextFile := filepath.Join(t.TempDir(), "plan.md")
	if err := os.WriteFile(contextFile, []byte("the plan\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	spec, err = parseAskArgs([]string{"Ship it?", "--option", "yes", "--option", "no", "--context", contextFile, "--requires", "approver",
		"--deadline", "1h", "--on-deadline", "fail"})
	if err != nil || spec.Request.Context != "the plan" || spec.Request.ContextName != "plan.md" ||
		spec.Request.Requires != domain.AskRequiresApprover || spec.Request.OnDeadline != domain.AskDeadlineFail {
		t.Fatalf("spec=%+v err=%v", spec, err)
	}
	for name, args := range map[string][]string{
		"no question":           {"--option", "a", "--option", "b"},
		"two questions":         {"one", "two", "--option", "a", "--option", "b"},
		"one option":            {"q", "--option", "a"},
		"deadline no outcome":   {"q", "--option", "a", "--option", "b", "--deadline", "1h"},
		"default no deadline":   {"q", "--option", "a", "--option", "b", "--default", "a"},
		"default and fail":      {"q", "--option", "a", "--option", "b", "--deadline", "1h", "--default", "a", "--on-deadline", "fail"},
		"on-deadline not fail":  {"q", "--option", "a", "--option", "b", "--deadline", "1h", "--on-deadline", "default"},
		"default not an option": {"q", "--option", "a", "--option", "b", "--deadline", "1h", "--default", "c"},
	} {
		if _, err := parseAskArgs(args); err == nil {
			t.Errorf("%s: %v was accepted", name, args)
		}
	}
}

// Outside a task, ask refuses and points at the session's own question tool.
func TestAskOutsideATaskPointsAtTheSessionQuestionTool(t *testing.T) {
	for _, name := range domain.TaskWaitEnvironmentNames() {
		t.Setenv(name, "")
	}
	t.Chdir(t.TempDir())
	err := cmdAsk(globalFlags{configPath: filepath.Join(t.TempDir(), "missing.yaml")}, []string{"q?", "--option", "a", "--option", "b"})
	if !errors.Is(err, errAskNotInsideTask) || !strings.Contains(err.Error(), "AskUserQuestion") {
		t.Fatalf("err = %v", err)
	}
}

// The whole CLI path in one process: the task asks through the real transport,
// the owner answers with "ask answer", and the steward's wait runner writes
// ask-answer.json into the reported workspace and resumes the thread with the
// answer in its message.
func TestAskAnswerFromTheCLIResumesTheTaskWithTheAnswerFile(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	if err := store.SaveWorkerSnapshot(ctx, domain.WorkerSnapshot{
		WorkerID: "worker", WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 2,
		Connected: true, ObservedAt: now.Add(time.Second), ValidUntil: now.Add(30 * 24 * time.Hour),
		Inventory:   domain.WorkerInventory{ID: "worker", AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now},
		Assignments: []domain.WorkerAssignmentObservation{{AssignmentID: "assign-1", WorkspacePath: workspace}},
	}); err != nil {
		t.Fatal(err)
	}
	identity, err := resolveTaskIdentity(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := parseAskArgs([]string{"M6b field test: pick one", "--option", "alpha", "--option", "beta", "--deadline", "2h", "--default", "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runAsk(ctx, cfg, spec, identity, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "End this turn now") {
		t.Fatalf("the agent was not told to end its turn: %s", out.String())
	}
	// Asking again with the same question is the same ask, not a second one.
	out.Reset()
	if err := runAsk(ctx, cfg, spec, identity, &out); err != nil {
		t.Fatalf("retrying the same ask: %v", err)
	}
	waits, err := store.ListTaskWaits(ctx)
	if err != nil || len(waits) != 1 || waits[0].Kind != domain.WaitKindAsk {
		t.Fatalf("waits=%+v err=%v", waits, err)
	}
	ask := waits[0]

	// triage offers one ready-to-run command per option.
	report := &triageReport{}
	triageTaskWait(report, ask, now)
	if len(report.Items) != 1 || report.Items[0].Kind != "needs-input" ||
		report.Items[0].Commands[1].Run != "t3-steward ask answer "+ask.ID+" --option 'beta'" {
		t.Fatalf("triage = %+v", report.Items)
	}

	answer, err := parseAskAnswerArgs([]string{ask.ID, "--option", "beta"})
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := runAskAnswer(ctx, cfg, answer, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "answered: beta") {
		t.Fatalf("answer output: %s", out.String())
	}

	control := &waitTestControl{threads: map[string]*domain.Thread{"thread-1": {ID: "thread-1", ProviderInstanceID: "claudeAgent"}}}
	messages := map[string]string{}
	runner := wait.New(store, &messageRecordingControl{waitTestControl: control, messages: messages}, nil)
	runner.DisableQuotaChecks = true
	runner.SetClock(func() time.Time { return now.Add(time.Minute) })
	runner.Tick(ctx, nil, nil)
	if len(control.sends) != 1 {
		t.Fatalf("the task was resumed %d times, want once", len(control.sends))
	}
	for _, message := range messages {
		if !strings.Contains(message, "Answer: beta") || !strings.Contains(message, "ask-answer.json") {
			t.Fatalf("wake message:\n%s", message)
		}
	}
	raw, err := os.ReadFile(filepath.Join(workspace, domain.AskAnswerFile))
	if err != nil {
		t.Fatalf("ask-answer.json was not written: %v", err)
	}
	var document domain.AskAnswer
	if err := json.Unmarshal(raw, &document); err != nil || document.Schema != domain.AskAnswerSchema ||
		len(document.Options) != 1 || document.Options[0] != "beta" || document.Source != domain.AskSourceCLI {
		t.Fatalf("ask-answer.json = %s (%v)", raw, err)
	}
	exclude, _ := os.ReadFile(filepath.Join(workspace, ".git", "info", "exclude"))
	if !strings.Contains(string(exclude), "/ask-answer.json") {
		t.Fatalf("the answer file is not excluded from git: %q", exclude)
	}
}

// messageRecordingControl keeps each wake message by its delivery ID.
type messageRecordingControl struct {
	*waitTestControl
	messages map[string]string
}

func (c *messageRecordingControl) SendNodeWake(ctx context.Context, thread domain.Thread, messageID, message string) error {
	c.messages[messageID] = message
	return c.waitTestControl.SendNodeWake(ctx, thread, messageID, message)
}

// The local authorizer admits an approver to ask answers and to nothing else
// new; whether a particular ask needs the approver is the coordinator's call.
func TestTheApproverRoleMayAnswerAsks(t *testing.T) {
	approver := backlogadmin.Principal{ID: "approver", Roles: []string{backlogadmin.ApproverRole}}
	if err := (localAdminAuthorizer{}).Authorize(context.Background(), approver, backlogadmin.Action{Kind: backlogadmin.AskAnswerKind}); err != nil {
		t.Fatalf("an approver could not answer an ask: %v", err)
	}
	if err := (localAdminAuthorizer{}).Authorize(context.Background(), approver, backlogadmin.Action{Kind: backlogadmin.QueryKind("node-wait")}); err == nil {
		t.Fatal("the approver role reached an ordinary wait operation")
	}
	remote := backlogadmin.Principal{ID: "laptop", Roles: []string{backlogadmin.RemoteAdminRole}}
	if err := (localAdminAuthorizer{}).Authorize(context.Background(), remote, backlogadmin.Action{Kind: backlogadmin.AskAnswerKind}); err != nil {
		t.Fatalf("a remote administrator could not answer an ask: %v", err)
	}
}
