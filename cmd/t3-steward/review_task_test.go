package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// bareRemoteRefs answers the coordinator's head resolution from a local bare
// repository, which is what the task's checkpoint push writes to. It stands in
// for the assigned worker reading the project remote.
type bareRemoteRefs struct{ dir string }

func (b bareRemoteRefs) ResolveRef(_ context.Context, query repositoryRefQuery) (repositoryRefAnswer, error) {
	answer := repositoryRefAnswer{WorkerID: query.WorkerID, Ref: query.Ref, ObservedAt: time.Now().UTC()}
	out, err := exec.Command("git", "--git-dir", b.dir, "rev-parse", "--verify", "--quiet", query.Ref+"^{commit}").Output()
	if err != nil {
		answer.Status = workerproto.RefResolutionNotFound
		return answer, nil
	}
	answer.Status, answer.ObjectID = workerproto.RefResolutionResolved, strings.TrimSpace(string(out))
	return answer, nil
}

// adminNodeWaits is the coordinator transport reduced to the in-process admin
// service, so a test drives the real coordinator operation.
type adminNodeWaits struct {
	admin *backlogadmin.Service
	calls []string
}

func (c *adminNodeWaits) NodeWait(ctx context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
	c.calls = append(c.calls, op.Action)
	return c.admin.NodeWait(ctx, checkpointPrincipal, op)
}

// scriptedNodeWaits is a fake coordinator: it answers each action from a
// script and records what it was asked.
type scriptedNodeWaits struct {
	calls    []backlogadmin.NodeWaitOperation
	answers  map[string]backlogadmin.NodeWaitResponse
	transErr error
}

func (c *scriptedNodeWaits) NodeWait(_ context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
	c.calls = append(c.calls, op)
	if c.transErr != nil {
		return backlogadmin.NodeWaitResponse{}, c.transErr
	}
	return c.answers[op.Action], nil
}

func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "commit.gpgsign=false", "-c", "user.email=task@example.invalid", "-c", "user.name=task"}, args...)...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func commitIn(t *testing.T, dir, name, content string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	gitIn(t, dir, "add", name)
	gitIn(t, dir, "commit", "-q", "-m", "work "+content)
	return gitIn(t, dir, "rev-parse", "HEAD")
}

// reviewTaskWorkspace lays a task workspace out the way workspace preparation
// does: origin fetches from a mirror and pushes to the project remote, here a
// local bare repository.
func reviewTaskWorkspace(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	bare := filepath.Join(root, "remote.git")
	gitIn(t, root, "init", "-q", "--bare", bare)
	dir := filepath.Join(root, "workspace")
	gitIn(t, root, "init", "-q", dir)
	commitIn(t, dir, "base.txt", "base")
	gitIn(t, dir, "remote", "add", "origin", filepath.Join(root, "mirror.git"))
	gitIn(t, dir, "remote", "set-url", "--push", "origin", bare)
	return dir, bare
}

func remoteHead(t *testing.T, bare, ref string) string {
	t.Helper()
	out, err := exec.Command("git", "--git-dir", bare, "rev-parse", "--verify", "--quiet", ref).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func (h *reviewCheckpointHarness) identityEnv(name string) string {
	return map[string]string{
		domain.TaskWaitEnvWorkflowRunID: h.request.WorkflowRunID, domain.TaskWaitEnvTaskID: h.request.TaskID,
		domain.TaskWaitEnvAttemptID: h.request.AttemptID, domain.TaskWaitEnvAttemptRevision: fmt.Sprint(h.request.IssuedRevision),
		domain.TaskWaitEnvAssignmentID: h.request.AssignmentID, domain.TaskWaitEnvThreadID: h.request.ThreadID,
	}[name]
}

func (h *reviewCheckpointHarness) taskCLI(dir string, client coordinatorNodeWaitClient, stdout *bytes.Buffer) reviewTaskCLI {
	return reviewTaskCLI{
		dir: dir, getenv: h.identityEnv, git: "git", client: client, stdout: stdout, stderr: stdout,
		query: func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
			q.Version, q.Principal = backlogadmin.Version, checkpointPrincipal
			return h.admin.Query(ctx, q)
		},
	}
}

func (h *reviewCheckpointHarness) attempt(t *testing.T) domain.Attempt {
	t.Helper()
	records, err := h.db.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range records.Attempts {
		if attempt.ID == h.parent.ID {
			return attempt
		}
	}
	t.Fatal("parent attempt is missing")
	return domain.Attempt{}
}

func (h *reviewCheckpointHarness) liveWaits(t *testing.T) []domain.TaskWait {
	t.Helper()
	waits, err := h.db.ListTaskWaits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var live []domain.TaskWait
	for _, w := range waits {
		if w.Live() {
			live = append(live, w)
		}
	}
	return live
}

// reviewVerdictJSON is a valid verdict.json for one member of a round.
func reviewVerdictJSON(t *testing.T, round review.Round, route string, blocking int) []byte {
	t.Helper()
	verdict := review.Verdict{Schema: review.Schema, Verdict: "accept", Findings: []review.Finding{}, InputManifestDigest: round.InputManifestDigest, ReviewerRoute: route}
	for i := 0; i < blocking; i++ {
		verdict.Verdict = "reject"
		verdict.Findings = append(verdict.Findings, review.Finding{ID: fmt.Sprintf("f%d", i+1), Severity: "high", Blocking: true,
			Title: fmt.Sprintf("blocking finding %d", i+1), Evidence: []string{"artifact:evidence"}, Recommendation: "fix it"})
	}
	raw, err := json.Marshal(verdict)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// finishRound ends a round's child run and collects its reviews.
func (h *reviewCheckpointHarness) finishRound(t *testing.T, roundID string, blocking map[string]int) {
	t.Helper()
	h.endChildRun(t, roundID)
	h.collectRound(t, roundID, blocking)
}

// endChildRun ends every reviewer task of a round's child run, so its sink
// settles, without collecting any review.
func (h *reviewCheckpointHarness) endChildRun(t *testing.T, roundID string) {
	t.Helper()
	ctx := context.Background()
	records, err := h.db.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	var run domain.WorkflowRun
	for _, candidate := range records.WorkflowRuns {
		if candidate.ID == roundID {
			run = candidate
		}
	}
	var attempts []domain.Attempt
	for _, attempt := range records.Attempts {
		if attempt.WorkflowRunID != roundID {
			continue
		}
		attempt.Progress, attempt.Control = domain.ProgressSucceeded, domain.ControlStopped
		attempt.Revision++
		attempt.CompletedAt, attempt.UpdatedAt = &now, now
		attempts = append(attempts, attempt)
	}
	run, err = domain.ProjectRunSink(run, domain.TasksForRun(run, records.Tasks), attempts, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Attempts: attempts}); err != nil {
		t.Fatal(err)
	}
}

// collectRound records each reviewer's documents, as the coordinator's
// collector does from the retained outputs. blocking maps a member ID to its
// blocking finding count.
func (h *reviewCheckpointHarness) collectRound(t *testing.T, roundID string, blocking map[string]int) {
	t.Helper()
	ctx := context.Background()
	round, err := h.db.GetReviewRound(ctx, roundID)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range round.Reviewers {
		result := review.Result{State: "succeeded", ReviewMD: "# Review by " + member.ID + "\n", VerdictJSON: reviewVerdictJSON(t, round, member.Route, blocking[member.ID])}
		if round, err = h.db.RecordReviewResult(ctx, roundID, member.ID, round.Revision, result); err != nil {
			t.Fatal(err)
		}
	}
	if !round.Terminal() {
		t.Fatalf("round %s is not terminal after every reviewer finished", roundID)
	}
}

// reportWorkspace is the worker snapshot that tells the coordinator where the
// attempt's workspace is, which a wake needs to place review evidence in it.
func (h *reviewCheckpointHarness) reportWorkspace(t *testing.T, workspace string) {
	t.Helper()
	snapshots, err := h.db.LoadWorkerSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var snapshot domain.WorkerSnapshot
	for _, candidate := range snapshots {
		if candidate.WorkerID == h.assign.WorkerID {
			snapshot = candidate
		}
	}
	now := time.Now().UTC()
	snapshot.WorkerID, snapshot.WorkerEpoch, snapshot.CoordinatorEpoch = h.assign.WorkerID, h.assign.WorkerEpoch, h.epoch
	snapshot.Sequence++
	snapshot.Connected, snapshot.ObservedAt, snapshot.ValidUntil = true, now, now.Add(time.Hour)
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: h.assign.ID, WorkspacePath: workspace}}
	// The worker still offers the attempt's route and project, which a wake's
	// admission evidence requires.
	snapshot.Inventory.Providers = append(snapshot.Inventory.Providers, domain.WorkerProviderInventory{InstanceID: h.assign.Route.ProviderInstanceID, Available: true, Models: []string{h.assign.Route.Model}})
	snapshot.Inventory.Projects = append(snapshot.Inventory.Projects, domain.WorkerProjectInventory{Name: h.assign.Project, Available: true})
	if err := h.db.SaveWorkerSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
}

// commitWake resumes the parked attempt the way the coordinator loop does,
// with current admission evidence for the attempt's worker; the steward's
// wait runner then only delivers the wake.
func (h *reviewCheckpointHarness) commitWake(t *testing.T) {
	t.Helper()
	snapshots, err := h.db.LoadWorkerSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	authorized := map[string]sqlite.TaskWakeWorkerAuthorization{}
	for _, s := range snapshots {
		authorized[s.WorkerID] = sqlite.TaskWakeWorkerAuthorization{WorkerEpoch: s.WorkerEpoch, SnapshotSequence: s.Sequence,
			CatalogRevision: s.Inventory.CatalogRevision, ValidUntil: s.ValidUntil, Providers: s.Inventory.Providers, Projects: s.Inventory.Projects}
	}
	wakes, err := h.db.WakeTaskWaitsBefore(context.Background(), time.Now().UTC(), sqlite.TaskWakeCutoffs{AuthorizedWorkers: authorized})
	if err != nil || len(wakes) != 1 || !wakes[0].Resumption() {
		t.Fatalf("the settled review wait did not resume the attempt: %+v %v", wakes, err)
	}
}

// queriedRoundStore is the coordinator store as a worker host's steward sees
// review rounds: through the admin query, one document at a time, the way
// remoteTaskWaitStore reads them.
type queriedRoundStore struct {
	*sqlite.Store
	query func(context.Context, backlogadmin.Query) (backlogadmin.Response, error)
}

func (s queriedRoundStore) GetReviewRound(ctx context.Context, id string) (review.Round, error) {
	return fetchReviewRound(ctx, s.query, id)
}

// reviewWakeControl is a fake T3: one thread, and every wake message sent to it.
type reviewWakeControl struct {
	threads  map[string]*domain.Thread
	sentTo   []string
	texts    []string
	observed map[string]bool
}

func (c *reviewWakeControl) GetThread(_ context.Context, id string) (*domain.Thread, error) {
	return c.threads[id], nil
}
func (c *reviewWakeControl) ResumeThread(context.Context, domain.Thread, string) error { return nil }
func (c *reviewWakeControl) SendNodeWake(_ context.Context, thread domain.Thread, messageID, text string) error {
	c.sentTo, c.texts = append(c.sentTo, thread.ID), append(c.texts, text)
	if c.observed == nil {
		c.observed = map[string]bool{}
	}
	c.observed[messageID] = true
	return nil
}
func (c *reviewWakeControl) ObserveNodeWake(_ context.Context, _, messageID string) (bool, error) {
	return c.observed[messageID], nil
}

func requireReviewTaskExit(t *testing.T, err error, code int, refusal domain.ReviewCheckpointCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("want exit %d (%s), got success", code, refusal)
	}
	if got := exitCodeFor(err); got != code {
		t.Fatalf("want exit %d, got %d: %v", code, got, err)
	}
	if refusal != "" && !strings.Contains(err.Error(), string(refusal)) {
		t.Fatalf("error does not name %s: %v", refusal, err)
	}
}

// Criteria 1-3 in one lifecycle: the command pushes the checkpoint branch,
// opens the round and parks the attempt; the round completes and the steward's
// wake resumes the same thread with the verdict, the blocking count and the
// review files in the workspace; the executor fixes, commits and opens the
// next round; a replay returns the same round; a reused ID with a new head is
// refused.
func TestReviewTaskCurrentParksResumesWithVerdictAndOpensTheNextRound(t *testing.T) {
	ctx := context.Background()
	h := reviewCheckpointFixture(t, true)
	dir, bare := reviewTaskWorkspace(t)
	h.op.refs = bareRemoteRefs{dir: bare}
	client := &adminNodeWaits{admin: h.admin}
	var out bytes.Buffer
	cli := h.taskCLI(dir, client, &out)

	first := commitIn(t, dir, "work.txt", "one")
	if err := cli.run(ctx, reviewTaskArgs{}); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	cp1 := domain.ReviewCheckpointBranch(h.request.WorkflowRunID, h.request.TaskID, "cp-1")
	if got := remoteHead(t, bare, cp1); got != first {
		t.Fatalf("checkpoint branch names %q, want the task's HEAD %s", got, first)
	}
	if strings.Join(client.calls, ",") != backlogadmin.ReviewCheckpointAction+","+backlogadmin.ReviewCheckpointWaitAction {
		t.Fatalf("coordinator calls: %v", client.calls)
	}
	live := h.liveWaits(t)
	if len(live) != 1 || live[0].ReviewRoundID() == "" || live[0].ThreadID != h.parent.ThreadID {
		t.Fatalf("the attempt is not parked on the review round: %+v", live)
	}
	round1 := live[0].ReviewRoundID()
	if parked := h.attempt(t); parked.Progress != domain.ProgressWaitingExternal {
		t.Fatalf("attempt is %s, not parked", parked.Progress)
	}
	for _, want := range []string{"cp-1", round1, first, "End this turn now"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output does not name %q:\n%s", want, out.String())
		}
	}

	// The child run ends before its reviews are collected: the attempt resumes,
	// but the wake waits for the verdict.
	h.endChildRun(t, round1)
	if err := h.db.SettleNodeWaits(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	h.reportWorkspace(t, dir)
	h.commitWake(t)
	control := &reviewWakeControl{threads: map[string]*domain.Thread{h.parent.ThreadID: {ID: h.parent.ThreadID, ProviderInstanceID: "t3-primary"}}}
	runner := wait.New(queriedRoundStore{Store: h.db, query: cli.query}, control, nil)
	runner.DisableQuotaChecks = true
	runner.Tick(ctx, nil, nil)
	if len(control.texts) != 0 {
		t.Fatalf("the wake went out before the round was collected:\n%s", control.texts[0])
	}
	// The round is collected; the steward wakes the same thread with the verdict.
	h.collectRound(t, round1, map[string]int{"a": 1})
	runner.Tick(ctx, nil, nil)
	if len(control.texts) != 1 || control.sentTo[0] != h.parent.ThreadID {
		waits, _ := h.db.ListTaskWaits(ctx)
		t.Fatalf("the parked thread was not woken exactly once: %v; waits %+v", control.sentTo, waits)
	}
	wake := control.texts[0]
	evidence := filepath.ToSlash(filepath.Join(".t3", "reviews", round1))
	for _, want := range []string{"waking this task", "verdict reject", "blocking findings: 1", evidence + "/a/review.md", evidence + "/a/verdict.json", evidence + "/b/verdict.json"} {
		if !strings.Contains(wake, want) {
			t.Fatalf("wake does not carry %q:\n%s", want, wake)
		}
	}
	if raw, err := os.ReadFile(filepath.Join(dir, ".t3", "reviews", round1, "a", "review.md")); err != nil || !strings.Contains(string(raw), "Review by a") {
		t.Fatalf("review.md was not fetched into the workspace: %q %v", raw, err)
	}
	if raw, err := os.ReadFile(filepath.Join(dir, ".t3", "reviews", round1, "a", "verdict.json")); err != nil || !strings.Contains(string(raw), "blocking finding 1") {
		t.Fatalf("verdict.json was not fetched into the workspace: %q %v", raw, err)
	}
	if status := gitIn(t, dir, "status", "--porcelain"); status != "" {
		t.Fatalf("review evidence shows up as workspace changes: %q", status)
	}
	resumed := h.attempt(t)
	if resumed.ID != h.parent.ID || resumed.ThreadID != h.parent.ThreadID || resumed.Progress != domain.ProgressActive {
		t.Fatalf("the attempt did not resume in the same session: %+v", resumed)
	}

	// Replaying round 1 after the resume returns the same round and does not park.
	runs := h.runCount(t)
	out.Reset()
	if err := cli.run(ctx, reviewTaskArgs{}); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if h.runCount(t) != runs || len(h.liveWaits(t)) != 0 || !strings.Contains(out.String(), "not parked") || !strings.Contains(out.String(), "reject") {
		t.Fatalf("replay after resume opened or parked again:\n%s", out.String())
	}

	// The fix is committed and reviewed as the next checkpoint.
	second := commitIn(t, dir, "work.txt", "two")
	out.Reset()
	if err := cli.run(ctx, reviewTaskArgs{}); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	cp2 := domain.ReviewCheckpointBranch(h.request.WorkflowRunID, h.request.TaskID, "cp-2")
	if remoteHead(t, bare, cp2) != second || remoteHead(t, bare, cp1) != first || h.runCount(t) != runs+1 {
		t.Fatalf("second round did not open on cp-2: cp1=%s cp2=%s runs=%d", remoteHead(t, bare, cp1), remoteHead(t, bare, cp2), h.runCount(t))
	}
	live = h.liveWaits(t)
	if len(live) != 1 || live[0].ReviewRoundID() == round1 || !strings.Contains(out.String(), "cp-2") {
		t.Fatalf("second round did not park: %+v\n%s", live, out.String())
	}

	// A lost acknowledgement: the same command again returns the same round.
	out.Reset()
	if err := cli.run(ctx, reviewTaskArgs{}); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if again := h.liveWaits(t); h.runCount(t) != runs+1 || len(again) != 1 || again[0].ID != live[0].ID || !strings.Contains(out.String(), "cp-2") {
		t.Fatalf("replay opened another round or wait: %+v\n%s", again, out.String())
	}

	// Reusing cp-1 for the new head is refused, and cp-1 is left as reviewed.
	out.Reset()
	err := cli.run(ctx, reviewTaskArgs{checkpoint: "cp-1"})
	requireReviewTaskExit(t, err, 2, domain.ReviewCheckpointHeadConflict)
	if remoteHead(t, bare, cp1) != first || h.runCount(t) != runs+1 || !strings.Contains(err.Error(), "new checkpoint") || !strings.Contains(err.Error(), "--checkpoint cp-3") {
		t.Fatalf("head change under cp-1 was not refused cleanly: %v\n%s", err, out.String())
	}
}

// childEndsBeforeParking ends the round's child run as soon as the round is
// open, before the command asks to park on it.
type childEndsBeforeParking struct {
	h *reviewCheckpointHarness
	t *testing.T
}

func (c childEndsBeforeParking) NodeWait(ctx context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
	response, err := c.h.admin.NodeWait(ctx, checkpointPrincipal, op)
	if err == nil && op.Action == backlogadmin.ReviewCheckpointAction && response.Checkpoint != nil && response.Checkpoint.Round != nil {
		c.h.endChildRun(c.t, response.Checkpoint.Round.RoundID)
	}
	return response, err
}

// Review finding 1: the child run can end between opening the round and
// parking on it. Its reviews are not collected yet, so the task still parks,
// and the steward resumes it with the verdict once collection completes rather
// than the command reporting a round with no verdict as complete.
func TestReviewTaskCurrentParksWhenTheChildEndsBeforeParking(t *testing.T) {
	ctx := context.Background()
	h := reviewCheckpointFixture(t, true)
	dir, bare := reviewTaskWorkspace(t)
	h.op.refs = bareRemoteRefs{dir: bare}
	commitIn(t, dir, "work.txt", "one")
	var out bytes.Buffer
	cli := h.taskCLI(dir, childEndsBeforeParking{h: h, t: t}, &out)
	if err := cli.run(ctx, reviewTaskArgs{}); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	live := h.liveWaits(t)
	if len(live) != 1 || live[0].ReviewRoundID() == "" || !strings.Contains(out.String(), "End this turn now") || strings.Contains(out.String(), "not parked") {
		t.Fatalf("a round whose child ended before parking did not park the task: %+v\n%s", live, out.String())
	}
	round := live[0].ReviewRoundID()
	if parked := h.attempt(t); parked.Progress != domain.ProgressWaitingExternal {
		t.Fatalf("attempt is %s, not parked", parked.Progress)
	}

	// The wait settles on the already-ended child; the wake waits for the verdict.
	if err := h.db.SettleNodeWaits(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	h.reportWorkspace(t, dir)
	h.commitWake(t)
	control := &reviewWakeControl{threads: map[string]*domain.Thread{h.parent.ThreadID: {ID: h.parent.ThreadID, ProviderInstanceID: "t3-primary"}}}
	runner := wait.New(queriedRoundStore{Store: h.db, query: cli.query}, control, nil)
	runner.DisableQuotaChecks = true
	runner.Tick(ctx, nil, nil)
	if len(control.texts) != 0 {
		t.Fatalf("the wake went out before the round was collected:\n%s", control.texts[0])
	}
	h.collectRound(t, round, map[string]int{"a": 1})
	runner.Tick(ctx, nil, nil)
	if len(control.texts) != 1 || control.sentTo[0] != h.parent.ThreadID {
		t.Fatalf("the parked thread was not woken exactly once: %v", control.sentTo)
	}
	for _, want := range []string{"verdict reject", "blocking findings: 1", filepath.ToSlash(filepath.Join(".t3", "reviews", round, "a", "verdict.json"))} {
		if !strings.Contains(control.texts[0], want) {
			t.Fatalf("wake does not carry %q:\n%s", want, control.texts[0])
		}
	}
	if resumed := h.attempt(t); resumed.ID != h.parent.ID || resumed.Progress != domain.ProgressActive {
		t.Fatalf("the attempt did not resume in the same session: %+v", resumed)
	}
}

// Self-review P2-1: a checkpoint whose round deadline has passed is answered
// as over, never as a retryable refusal a repeating task would loop on, and
// nothing is parked for it.
func TestReviewTaskCurrentAfterTheRoundDeadlineReportsTheVerdict(t *testing.T) {
	ctx := context.Background()
	h := reviewCheckpointFixture(t, true)
	dir, bare := reviewTaskWorkspace(t)
	h.op.refs = bareRemoteRefs{dir: bare}
	head := commitIn(t, dir, "work.txt", "one")
	gitIn(t, dir, "push", "-q", "origin", head+":"+domain.ReviewCheckpointBranch(h.request.WorkflowRunID, h.request.TaskID, "cp-1"))
	request := h.request
	request.HeadCommit = head
	round, err := h.call(t, request)
	if err != nil {
		t.Fatal(err)
	}
	h.finishRound(t, round.RoundID, nil)
	late := func() time.Time { return round.Deadline.Add(time.Minute) }
	h.op.now = late
	h.db.SetClock(late)
	var out bytes.Buffer
	if err := h.taskCLI(dir, &adminNodeWaits{admin: h.admin}, &out).run(ctx, reviewTaskArgs{}); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if len(h.liveWaits(t)) != 0 || !strings.Contains(out.String(), "not parked") || !strings.Contains(out.String(), "verdict accept") {
		t.Fatalf("a round past its deadline was not reported as over:\n%s", out.String())
	}
	if _, err := os.Stat(filepath.Join(dir, ".t3", "reviews", round.RoundID, "a", "verdict.json")); err != nil {
		t.Fatalf("the verdict was not placed in the workspace: %v", err)
	}
}

// Self-review P3-2: a push the remote took but whose answer was lost is a
// published checkpoint, not a refusal.
func TestReviewTaskCurrentTreatsALostPushAnswerAsPublished(t *testing.T) {
	h := reviewCheckpointFixture(t, true)
	dir, bare := reviewTaskWorkspace(t)
	head := commitIn(t, dir, "work.txt", "one")
	git := filepath.Join(t.TempDir(), "git")
	script := "#!/bin/sh\nif [ \"$1\" = push ]; then git \"$@\" >/dev/null 2>&1; echo 'connection reset' >&2; exit 128; fi\nexec git \"$@\"\n"
	if err := os.WriteFile(git, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	round := domain.ReviewCheckpointRound{RoundID: "round-x", CheckpointID: "cp-1", Number: 1}
	client := &scriptedNodeWaits{answers: map[string]backlogadmin.NodeWaitResponse{
		backlogadmin.ReviewCheckpointAction:     {Checkpoint: &domain.ReviewCheckpointResult{Round: &round}},
		backlogadmin.ReviewCheckpointWaitAction: {CheckpointWait: &domain.ReviewCheckpointWaitResult{Park: &domain.ReviewCheckpointPark{Status: "parked", RoundID: "round-x"}}},
	}}
	var out bytes.Buffer
	cli := h.taskCLI(dir, client, &out)
	cli.git = git
	if err := cli.run(context.Background(), reviewTaskArgs{}); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if remoteHead(t, bare, domain.ReviewCheckpointBranch(h.request.WorkflowRunID, h.request.TaskID, "cp-1")) != head || len(client.calls) != 2 {
		t.Fatalf("lost push answer: calls %d\n%s", len(client.calls), out.String())
	}
}

// The coordinator's own structured refusals are printed in plain language with
// the next action, and the exit code says whether repeating can help.
func TestReviewTaskCurrentPrintsCoordinatorRefusals(t *testing.T) {
	for _, tc := range []struct {
		name      string
		refusal   domain.ReviewCheckpointRefusal
		code      int
		nextStep  string
		waitCalls bool
	}{
		{"head conflict", domain.ReviewCheckpointRefusal{Code: domain.ReviewCheckpointHeadConflict, Reason: "checkpoint \"cp-1\" opened round 1 for head x"}, 2, "new checkpoint", false},
		{"stale attempt", domain.ReviewCheckpointRefusal{Code: domain.ReviewCheckpointAttemptStale, Reason: "attempt superseded"}, 2, "no longer the current attempt", false},
		{"stale coordinator", domain.ReviewCheckpointRefusal{Code: domain.ReviewCheckpointStaleCoordinator, Reason: "epoch moved", Retryable: true}, 75, "repeat", false},
		{"not declared", domain.ReviewCheckpointRefusal{Code: domain.ReviewCheckpointNotDeclared, Reason: "no review:"}, 2, "review:", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := reviewCheckpointFixture(t, true)
			dir, _ := reviewTaskWorkspace(t)
			commitIn(t, dir, "work.txt", "one")
			refusal := tc.refusal
			client := &scriptedNodeWaits{answers: map[string]backlogadmin.NodeWaitResponse{
				backlogadmin.ReviewCheckpointAction: {Checkpoint: &domain.ReviewCheckpointResult{Refusal: &refusal}},
			}}
			var out bytes.Buffer
			err := h.taskCLI(dir, client, &out).run(context.Background(), reviewTaskArgs{})
			requireReviewTaskExit(t, err, tc.code, tc.refusal.Code)
			if !strings.Contains(err.Error(), tc.nextStep) {
				t.Fatalf("refusal does not say what to do next (%q): %v", tc.nextStep, err)
			}
			if len(client.calls) != 1 {
				t.Fatalf("a refused round still parked: %+v", client.calls)
			}
			if client.calls[0].Checkpoint == nil || client.calls[0].Checkpoint.HeadCommit == "" || client.calls[0].Checkpoint.CheckpointID != "cp-1" {
				t.Fatalf("request did not state the pushed head for cross-check: %+v", client.calls[0].Checkpoint)
			}
		})
	}
}

// Criterion 4 against the real coordinator: a task without a declared review
// and an attempt that is not the current one are refused before any round.
func TestReviewTaskCurrentRefusesUndeclaredAndStaleAttempts(t *testing.T) {
	t.Run("no declared review", func(t *testing.T) {
		h := reviewCheckpointFixture(t, false)
		dir, bare := reviewTaskWorkspace(t)
		h.op.refs = bareRemoteRefs{dir: bare}
		commitIn(t, dir, "work.txt", "one")
		var out bytes.Buffer
		err := h.taskCLI(dir, &adminNodeWaits{admin: h.admin}, &out).run(context.Background(), reviewTaskArgs{})
		requireReviewTaskExit(t, err, 2, domain.ReviewCheckpointNotDeclared)
		h.noReviewRows(t)
		if len(h.liveWaits(t)) != 0 {
			t.Fatal("an undeclared review parked the task")
		}
	})
	t.Run("stale attempt", func(t *testing.T) {
		h := reviewCheckpointFixture(t, true)
		dir, bare := reviewTaskWorkspace(t)
		h.op.refs = bareRemoteRefs{dir: bare}
		commitIn(t, dir, "work.txt", "one")
		h.request.AssignmentID = "superseded-assignment"
		var out bytes.Buffer
		err := h.taskCLI(dir, &adminNodeWaits{admin: h.admin}, &out).run(context.Background(), reviewTaskArgs{})
		requireReviewTaskExit(t, err, 2, domain.ReviewCheckpointAttemptStale)
		h.noReviewRows(t)
	})
}

// Criterion 4 on the client side: outside a task, with no push binding, and
// when the remote refuses the push, nothing reaches the coordinator.
func TestReviewTaskCurrentRefusesBeforeTheCoordinator(t *testing.T) {
	ctx := context.Background()
	t.Run("not in a task", func(t *testing.T) {
		h := reviewCheckpointFixture(t, true)
		dir, _ := reviewTaskWorkspace(t)
		client := &scriptedNodeWaits{}
		var out bytes.Buffer
		cli := h.taskCLI(dir, client, &out)
		cli.getenv = func(string) string { return "" }
		err := cli.run(ctx, reviewTaskArgs{})
		if !errors.Is(err, errNotInsideTask) || exitCodeFor(err) != 1 || len(client.calls) != 0 {
			t.Fatalf("outside a task: %v (exit %d), calls %d", err, exitCodeFor(err), len(client.calls))
		}
	})
	t.Run("remote missing", func(t *testing.T) {
		h := reviewCheckpointFixture(t, true)
		dir, _ := reviewTaskWorkspace(t)
		gitIn(t, dir, "config", "--unset", "remote.origin.pushurl")
		client := &scriptedNodeWaits{}
		var out bytes.Buffer
		err := h.taskCLI(dir, client, &out).run(ctx, reviewTaskArgs{})
		requireReviewTaskExit(t, err, 1, domain.ReviewCheckpointRemoteMissing)
		if len(client.calls) != 0 {
			t.Fatal("the coordinator was asked without a pushed branch")
		}
	})
	t.Run("push refused", func(t *testing.T) {
		h := reviewCheckpointFixture(t, true)
		dir, bare := reviewTaskWorkspace(t)
		hook := filepath.Join(bare, "hooks", "pre-receive")
		if err := os.WriteFile(hook, []byte("#!/bin/sh\necho 'protected by policy' >&2\nexit 1\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		client := &scriptedNodeWaits{}
		var out bytes.Buffer
		err := h.taskCLI(dir, client, &out).run(ctx, reviewTaskArgs{})
		requireReviewTaskExit(t, err, 1, domain.ReviewCheckpointPushRefused)
		if !strings.Contains(err.Error(), "protected by policy") || len(client.calls) != 0 {
			t.Fatalf("push refusal was not reported or the coordinator was asked: %v, calls %d", err, len(client.calls))
		}
	})
}

// Criterion 5: requirements come only from the manifest review: declaration.
func TestReviewTaskModeRefusesReviewerRoleAndRiskOverrides(t *testing.T) {
	for _, args := range [][]string{
		{"--task", "current", "--reviewer", "codex/sol"},
		{"--task", "current", "--model", "codex/sol"},
		{"--task", "current", "--independent", "codex/sol"},
		{"--task=current", "--role", "review"},
		{"--task", "current", "--risk", "risky"},
		{"--task", "current", "--risk=routine"},
		{"--task", "current", "--judge", "codex/sol"},
		{"--task", "current", "--swarm"},
		{"--task", "current", "--effort", "high"},
	} {
		_, err := parseReviewTaskArgs(args)
		if err == nil || !strings.Contains(err.Error(), "review:") {
			t.Fatalf("%v was not refused with the manifest rule: %v", args, err)
		}
	}
	for _, args := range [][]string{{"--task", "other"}, {"--task", "current", "extra"}, {"--task", "current", "--checkpoint", "bad/id"}} {
		if _, err := parseReviewTaskArgs(args); err == nil {
			t.Fatalf("%v was accepted", args)
		}
	}
	got, err := parseReviewTaskArgs([]string{"--task", "current", "--checkpoint", "cp-7", "--json"})
	if err != nil || got.checkpoint != "cp-7" || !got.asJSON {
		t.Fatalf("task-mode flags: %+v %v", got, err)
	}
	if !reviewTaskArgsRequested([]string{"--risk", "risky", "--task", "current"}) || reviewTaskArgsRequested([]string{"--diff", "a..b"}) {
		t.Fatal("task mode is not recognized from its --task current flag")
	}
}

func TestReviewHelpDocumentsTaskMode(t *testing.T) {
	for _, want := range []string{"--task current", "--checkpoint ID", "steward/<run>/<task>/<checkpoint>", "review:", "End this turn", "75"} {
		if !strings.Contains(reviewUsage, want) {
			t.Fatalf("review --help does not document %q", want)
		}
	}
}
