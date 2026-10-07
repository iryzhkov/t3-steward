package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// Exit codes of review --task current, beyond the transport classes 3-8.
const (
	// reviewTaskExitRefused is a refusal that repeating the same command
	// cannot fix.
	reviewTaskExitRefused = 2
	// reviewTaskExitRetry is a refusal that the same command can succeed
	// after, EX_TEMPFAIL.
	reviewTaskExitRetry = 75
)

// reviewTaskRefusedFlags are the submission flags of an ordinary review. In
// task mode the round's reviewers, roles, risk and inputs come only from the
// manifest review: declaration, so each of them is refused by name rather
// than ignored.
var reviewTaskRefusedFlags = []string{
	"reviewer", "model", "independent", "judge", "role", "risk", "swarm", "swarm-model", "effort", "policy-file",
	"plan", "diff", "diff-file", "criteria", "project", "deadline", "wait", "no-notify", "notify-thread", "gate",
}

// reviewTaskArgsRequested reports whether review was asked for in task mode.
func reviewTaskArgsRequested(args []string) bool { return currentTaskWaitArgs(args) }

type reviewTaskArgs struct {
	checkpoint string
	asJSON     bool
}

func parseReviewTaskArgs(args []string) (reviewTaskArgs, error) {
	var a reviewTaskArgs
	for _, arg := range args {
		if arg == "--" {
			break
		}
		name, _, _ := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if strings.HasPrefix(arg, "-") && slices.Contains(reviewTaskRefusedFlags, name) {
			return a, fmt.Errorf("--%s is not accepted with --task current: a task's review takes its reviewers, roles, risk and inputs only from the manifest review: declaration", name)
		}
	}
	f := flag.NewFlagSet("review --task current", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	task := f.String("task", "", "")
	f.StringVar(&a.checkpoint, "checkpoint", "", "")
	f.BoolVar(&a.asJSON, "json", false, "")
	if err := f.Parse(args); err != nil {
		return a, err
	}
	if *task != "current" {
		return a, errors.New("review --task takes only current, the task this shell runs inside")
	}
	if f.NArg() != 0 {
		return a, errors.New("review --task current accepts flags only; try review --help")
	}
	if a.checkpoint != "" && !review.IDPattern.MatchString(a.checkpoint) {
		return a, fmt.Errorf("--checkpoint %q must match %s", a.checkpoint, review.IDPattern)
	}
	return a, nil
}

func cmdReviewTask(g globalFlags, args []string) error {
	a, err := parseReviewTaskArgs(args)
	if err != nil {
		return err
	}
	dir, err := os.Getwd()
	if err != nil {
		return err
	}
	// The identity is checked before any configuration is read, so outside a
	// task the refusal says that and nothing else.
	if _, err := resolveTaskIdentityFrom(os.Getenv, dir); err != nil {
		return err
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	cli := reviewTaskCLI{
		dir: dir, getenv: os.Getenv, git: "git", client: transport.client, stdout: os.Stdout, stderr: os.Stderr,
		query: func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
			q.Version, q.Principal = backlogadmin.Version, transport.principal
			return transport.client.Query(ctx, q)
		},
	}
	return cli.run(context.Background(), a)
}

// reviewTaskCLI is review --task current: publish the task's committed work as
// a checkpoint branch, have the coordinator open that checkpoint's review
// round, and park the task on it.
type reviewTaskCLI struct {
	// dir is the directory the command runs in, inside the task workspace.
	dir    string
	getenv func(string) string
	git    string
	client coordinatorNodeWaitClient
	query  func(context.Context, backlogadmin.Query) (backlogadmin.Response, error)
	stdout io.Writer
	stderr io.Writer
}

func (c reviewTaskCLI) gitOut(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, c.git, args...)
	cmd.Dir = c.dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(out)), nil
}

// clientRefusal is a refusal of this command itself: the checkpoint could not
// be published, so the coordinator was never asked.
func clientRefusal(checkpoint string, code domain.ReviewCheckpointCode, format string, args ...any) error {
	return reviewTaskRefusal(checkpoint, domain.ReviewCheckpointRefusal{Code: code, Reason: fmt.Sprintf(format, args...)}, 1, "")
}

// reviewTaskRefusal prints a structured refusal in plain language with the
// next action, and exits with whether repeating can help.
//
// fresh, when set, is an unused checkpoint ID. A refusal that only a new
// checkpoint gets past names it, because the default ID would choose the
// refused checkpoint again while HEAD is unchanged.
func reviewTaskRefusal(checkpoint string, refusal domain.ReviewCheckpointRefusal, code int, fresh string) error {
	if code == 0 {
		code = reviewTaskExitRefused
		if refusal.Retryable {
			code = reviewTaskExitRetry
		}
	}
	retry := "not retryable"
	if refusal.Retryable {
		retry = "retryable"
	}
	if checkpoint == "" {
		checkpoint = "(none)"
	}
	next := reviewTaskNextStep(refusal)
	switch refusal.Code {
	case domain.ReviewCheckpointHeadConflict, domain.ReviewCheckpointHeadMismatch, domain.ReviewCheckpointDeadlineExpired:
		if fresh != "" {
			next += " To review HEAD as a new checkpoint: t3-steward review --task current --checkpoint " + fresh
		}
	}
	return exitCodeError{code: code, error: fmt.Errorf("review checkpoint %s refused (%s, %s): %s\nnext: %s",
		checkpoint, refusal.Code, retry, refusal.Reason, next)}
}

// checkpointCommandError prints a coordinator refusal as a refusal, and passes
// anything else through.
func checkpointCommandError(checkpoint, fresh string, err error) error {
	var refusal *domain.ReviewCheckpointRefusal
	if errors.As(err, &refusal) {
		return reviewTaskRefusal(checkpoint, *refusal, 0, fresh)
	}
	return err
}

func reviewTaskNextStep(refusal domain.ReviewCheckpointRefusal) string {
	switch refusal.Code {
	case domain.ReviewCheckpointStaleCoordinator:
		return "repeat the same command; the current coordinator answers it and replays whatever was already done."
	case domain.ReviewCheckpointAttemptStale:
		return "this shell is no longer the current attempt of its task, or the task has ended; stop work and end the turn without writing further outputs."
	case domain.ReviewCheckpointNotDeclared:
		return "this task's manifest declares no review: requirements, and task mode reviews only against those; continue without an in-task review, or report that the plan needs a review: declaration."
	case domain.ReviewCheckpointAdmission:
		return "the manifest review: declaration could not be admitted for this attempt; report the reason above, an in-task review cannot run until it is fixed."
	case domain.ReviewCheckpointHeadConflict:
		return "this checkpoint ID already reviews a different head; push the new work to a new checkpoint ID."
	case domain.ReviewCheckpointHeadMismatch:
		return "the checkpoint branch moved after it was pushed; make sure nothing else pushes to it, and use a new checkpoint ID."
	case domain.ReviewCheckpointRoundLimit:
		return "this task has used every review round its review: declaration allows; report the open findings instead of opening another round."
	case domain.ReviewCheckpointDeadlineExpired:
		return "this checkpoint's review deadline has passed; use a new checkpoint ID."
	case domain.ReviewCheckpointWorkerSupport, domain.ReviewCheckpointUnavailable:
		return "the coordinator or the task's worker cannot open in-task review rounds; report this, and do not end the turn waiting for a review."
	case domain.ReviewCheckpointPushRefused:
		return "the project remote refused the checkpoint branch; fix what the remote said above, then repeat the same command."
	case domain.ReviewCheckpointRemoteMissing:
		return "this workspace's origin has no push URL, so the project has no push binding and its work cannot be reviewed in-task; report this."
	case domain.ReviewCheckpointInvalidRequest:
		return "fix the command; see t3-steward review --help."
	}
	if refusal.Retryable {
		return "repeat the same command; it resumes from what was already done."
	}
	return "report the reason above; repeating the same command will not change it."
}

// remoteCheckpoints lists the checkpoint branches of this task on the push
// remote: checkpoint ID to the commit it names.
func (c reviewTaskCLI) remoteCheckpoints(ctx context.Context, pushURL string, identity taskIdentity) (map[string]string, error) {
	prefix := strings.TrimSuffix(domain.ReviewCheckpointBranch(identity.WorkflowRunID, identity.TaskID, "x"), "x")
	out, err := c.gitOut(ctx, "ls-remote", "--refs", "--", pushURL, prefix+"*")
	if err != nil {
		return nil, err
	}
	branches := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		object, ref, ok := strings.Cut(line, "\t")
		if id, found := strings.CutPrefix(ref, prefix); ok && found && review.IDPattern.MatchString(id) {
			branches[id] = object
		}
	}
	return branches, nil
}

// nextCheckpointID is the default checkpoint: the one already naming head,
// which makes a repeated command replay its round after a lost answer, or the
// next cp-N after every one already pushed for this task.
func nextCheckpointID(branches map[string]string, head string) string {
	last, replay := 0, 0
	for id, object := range branches {
		n, err := strconv.Atoi(strings.TrimPrefix(id, "cp-"))
		if !strings.HasPrefix(id, "cp-") || err != nil || n < 1 || "cp-"+strconv.Itoa(n) != id {
			continue
		}
		last = max(last, n)
		if object == head {
			replay = max(replay, n)
		}
	}
	if replay > 0 {
		return "cp-" + strconv.Itoa(replay)
	}
	return "cp-" + strconv.Itoa(last+1)
}

// unusedCheckpointID is the next cp-N no branch of this task has used.
func unusedCheckpointID(branches map[string]string) string {
	return nextCheckpointID(branches, "")
}

// publish pushes head to the checkpoint branch without ever moving it: a
// branch that does not exist is created, one that already names head is left
// alone, and one that names anything else is refused.
func (c reviewTaskCLI) publish(ctx context.Context, checkpoint, branch, head, pushURL string, existing map[string]string) error {
	conflict := func(object string) error {
		return reviewTaskRefusal(checkpoint, domain.ReviewCheckpointRefusal{Code: domain.ReviewCheckpointHeadConflict,
			Reason: fmt.Sprintf("%s already names %s on the project remote, not this workspace's HEAD %s; a checkpoint branch is never moved", branch, object, head)}, 0, unusedCheckpointID(existing))
	}
	if object, ok := existing[checkpoint]; ok {
		if object != head {
			return conflict(object)
		}
		return nil
	}
	cmd := exec.CommandContext(ctx, c.git, "push", "--porcelain", "--force-with-lease="+branch+":", "origin", head+":"+branch)
	cmd.Dir = c.dir
	out, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	// The remote may have taken the push and lost the answer, or another push
	// may have won the race; what the branch names now decides which.
	if object, lsErr := c.gitOut(ctx, "ls-remote", "--refs", "--", pushURL, branch); lsErr == nil {
		if fields := strings.Fields(object); len(fields) == 2 && fields[0] == head {
			return nil
		} else if len(fields) == 2 {
			return conflict(fields[0])
		}
	}
	return clientRefusal(checkpoint, domain.ReviewCheckpointPushRefused, "git push of %s to %s failed: %v: %s", head, branch, err, strings.TrimSpace(string(out)))
}

func (c reviewTaskCLI) run(ctx context.Context, a reviewTaskArgs) error {
	identity, err := resolveTaskIdentityFrom(c.getenv, c.dir)
	if err != nil {
		return err
	}
	top, err := c.gitOut(ctx, "rev-parse", "--show-toplevel")
	if err != nil {
		return fmt.Errorf("review --task current runs inside the task's git workspace: %w", err)
	}
	head, err := c.gitOut(ctx, "rev-parse", "--verify", "--end-of-options", "HEAD^{commit}")
	if err != nil {
		return fmt.Errorf("this workspace has no commit to review; commit the work first: %w", err)
	}
	pushURL, _ := c.gitOut(ctx, "config", "--get", "remote.origin.pushurl")
	if pushURL == "" {
		return clientRefusal(a.checkpoint, domain.ReviewCheckpointRemoteMissing, "origin has no push URL in %s, so there is no project remote to publish the checkpoint to", top)
	}
	if dirty, err := c.gitOut(ctx, "status", "--porcelain", "--untracked-files=no"); err == nil && dirty != "" {
		fmt.Fprintf(c.stderr, "warning: uncommitted changes are not part of the checkpoint; only HEAD %s is reviewed\n", head)
	}
	branches, err := c.remoteCheckpoints(ctx, pushURL, identity)
	if err != nil {
		return clientRefusal(a.checkpoint, domain.ReviewCheckpointRemote, "could not list this task's checkpoint branches on the project remote: %v", err)
	}
	checkpoint := a.checkpoint
	if checkpoint == "" {
		checkpoint = nextCheckpointID(branches, head)
	}
	branch := domain.ReviewCheckpointBranch(identity.WorkflowRunID, identity.TaskID, checkpoint)
	if err := c.publish(ctx, checkpoint, branch, head, pushURL, branches); err != nil {
		return err
	}

	request := domain.ReviewCheckpointRequest{
		WorkflowRunID: identity.WorkflowRunID, TaskID: identity.TaskID, AttemptID: identity.AttemptID,
		IssuedRevision: identity.AttemptRevision, AssignmentID: identity.AssignmentID, ThreadID: identity.ThreadID,
		CheckpointID: checkpoint, HeadCommit: head,
	}
	opened, err := c.client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: backlogadmin.ReviewCheckpointAction, Checkpoint: &request})
	if err != nil {
		return err
	}
	if opened.Checkpoint == nil {
		return errors.New("the coordinator returned no review checkpoint answer; it predates in-task review rounds")
	}
	fresh := unusedCheckpointID(branches)
	round, err := opened.Checkpoint.Outcome()
	if err != nil {
		return checkpointCommandError(checkpoint, fresh, err)
	}
	parkRequest := request
	parked, err := c.client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: backlogadmin.ReviewCheckpointWaitAction, Checkpoint: &parkRequest})
	if err != nil {
		return fmt.Errorf("round %s is open but this task could not be parked on it, so do not end the turn for it; repeat the same command: %w", round.RoundID, err)
	}
	if parked.CheckpointWait == nil {
		return fmt.Errorf("round %s is open but the coordinator cannot park a task on it, so do not end the turn for it; collect it with t3-steward review result %s --wait", round.RoundID, round.RoundID)
	}
	park, err := parked.CheckpointWait.Outcome()
	if err != nil {
		return checkpointCommandError(checkpoint, fresh, err)
	}
	if park.Parked() {
		return c.reportParked(round, park, a.asJSON)
	}
	return c.reportComplete(ctx, top, round, park, a.asJSON)
}

// reviewTaskDocument is the --json answer of review --task current.
type reviewTaskDocument struct {
	Schema   string                       `json:"schema"`
	Round    domain.ReviewCheckpointRound `json:"round"`
	Park     domain.ReviewCheckpointPark  `json:"park"`
	Evidence *reviewTaskEvidence          `json:"evidence,omitempty"`
}

type reviewTaskEvidence struct {
	Verdict  string   `json:"verdict"`
	Blocking int      `json:"blocking"`
	Files    []string `json:"files,omitempty"`
	Note     string   `json:"note,omitempty"`
}

func (c reviewTaskCLI) reportParked(round domain.ReviewCheckpointRound, park domain.ReviewCheckpointPark, asJSON bool) error {
	out := c.stdout
	if asJSON {
		if err := encodeCampaignJSON(c.stdout, reviewTaskDocument{Schema: "review-task/v1", Round: round, Park: park}); err != nil {
			return err
		}
		out = c.stderr
	} else {
		c.printRound(round)
		if park.Wait != nil {
			fmt.Fprintf(c.stdout, "task-bound wait %s parks attempt %s on the round until %s\n", park.Wait.ID, park.Wait.AttemptID, park.Wait.Deadline.UTC().Format(time.RFC3339))
		}
	}
	fmt.Fprintln(out, "This task is now parked. End this turn now: nothing is collected and nothing is verified")
	fmt.Fprintf(out, "until the steward resumes this same thread with the review verdict, the blocking finding count\nand the review documents under %s/%s/.\n", wait.ReviewEvidenceDir, round.RoundID)
	return nil
}

func (c reviewTaskCLI) printRound(round domain.ReviewCheckpointRound) {
	replayed := ""
	if round.Replayed {
		replayed = " (replayed: this checkpoint had already opened it)"
	}
	fmt.Fprintf(c.stdout, "review round %s, round %d of checkpoint %s%s\n", round.RoundID, round.Number, round.CheckpointID, replayed)
	fmt.Fprintf(c.stdout, "branch %s\nbase %s\nhead %s\n", round.Branch, round.BaseCommit, round.HeadCommit)
	for _, member := range round.Members {
		fmt.Fprintf(c.stdout, "reviewer %s (%s) task %s\n", member.ID, member.Route, member.TaskID)
	}
	if !round.Deadline.IsZero() {
		fmt.Fprintf(c.stdout, "deadline %s\n", round.Deadline.UTC().Format(time.RFC3339))
	}
}

// reportComplete answers a checkpoint whose round is already over: its wait
// settled, its reviews were collected before anything parked on it, or its
// deadline passed. The task is
// not parked and is told the verdict now, with the documents placed in the
// workspace exactly as a wake places them.
func (c reviewTaskCLI) reportComplete(ctx context.Context, workspace string, round domain.ReviewCheckpointRound, park domain.ReviewCheckpointPark, asJSON bool) error {
	document := reviewTaskDocument{Schema: "review-task/v1", Round: round, Park: park, Evidence: &reviewTaskEvidence{}}
	text := ""
	if c.query == nil {
		document.Evidence.Note = "no coordinator query transport"
	} else if full, err := fetchReviewRound(ctx, c.query, round.RoundID); err != nil {
		document.Evidence.Note = err.Error()
	} else if !full.Terminal() {
		document.Evidence.Note = "the review child has ended but its reviews are not collected yet"
	} else {
		evidence, _ := wait.WriteReviewEvidence(workspace, full)
		text = evidence.Text()
		document.Evidence.Verdict, document.Evidence.Blocking = evidence.Verdict, evidence.Blocking
		for _, member := range evidence.Members {
			for _, path := range []string{member.ReviewPath, member.VerdictPath} {
				if path != "" {
					document.Evidence.Files = append(document.Evidence.Files, path)
				}
			}
		}
	}
	out := c.stdout
	if asJSON {
		if err := encodeCampaignJSON(c.stdout, document); err != nil {
			return err
		}
		out = c.stderr
	} else {
		c.printRound(round)
		fmt.Fprint(c.stdout, text)
	}
	if text == "" {
		fmt.Fprintf(out, "This task is not parked: round %s already ended (%s). Read the verdict with t3-steward review result %s --wait, then continue.\n", round.RoundID, document.Evidence.Note, round.RoundID)
		return nil
	}
	fmt.Fprintf(out, "This task is not parked: round %s is already complete. Act on the verdict above and continue this turn.\n", round.RoundID)
	return nil
}
