package wait

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

// reviewTaskStore is taskMemStore plus the review round evidence a wake of a
// review parent wait reads, and the workspace the attempt's worker reported.
type reviewTaskStore struct {
	*taskMemStore
	workspace string
	round     review.Round
	roundErr  error
}

func (s *reviewTaskStore) GetReviewRound(_ context.Context, id string) (review.Round, error) {
	if s.roundErr != nil {
		return review.Round{}, s.roundErr
	}
	if id != s.round.ID {
		return review.Round{}, errors.New("no such round")
	}
	return s.round, nil
}

func (s *reviewTaskStore) TaskWakesAwaitingDelivery(ctx context.Context, now time.Time) ([]domain.TaskWaitWakeContext, error) {
	wakes, err := s.taskMemStore.TaskWakesAwaitingDelivery(ctx, now)
	for i := range wakes {
		wakes[i].WorkspacePath = s.workspace
	}
	return wakes, err
}

func reviewParentRunner(t *testing.T) (*Runner, *reviewTaskStore, *taskControl, *time.Time) {
	t.Helper()
	runner, mem, control, now := taskWaitRunner(t, domain.ProgressWaitingExternal)
	delete(mem.waits, "w1")
	settled := *now
	mem.taskWaits["tw-1"] = domain.TaskWait{
		ID: "tw-1", RequestID: domain.ReviewParentWaitPrefix + "cp-key", AttemptID: "a1", ThreadID: "t1", Wake: domain.WakeEach,
		Kind: domain.WaitKindNode, Node: &domain.NodeWaitCondition{Target: domain.NodeRef{RunID: "round-1", TaskID: "sink"}, State: domain.NodeStateTerminal},
		Name: "review round", RegisteredAt: *now, Deadline: now.Add(time.Hour), SettledAt: &settled,
		Result: &domain.TaskWaitResult{Outcome: domain.TaskWaitMet, ObservedAt: settled},
	}
	store := &reviewTaskStore{taskMemStore: mem, workspace: t.TempDir(), round: review.Round{ID: "round-1", Reviewers: []review.Reviewer{
		{ID: "a", Route: "codex/sol", Required: true, State: "pending"},
	}}}
	runner.TaskStore = store
	return runner, store, control, now
}

// The review child can end before its reviews are collected. The resumed turn
// is told nothing until the verdict exists, and then it gets the verdict, the
// blocking count and the documents in its workspace.
func TestReviewParentWakeWaitsForCollectionThenCarriesTheVerdict(t *testing.T) {
	runner, store, control, now := reviewParentRunner(t)
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(control.sends) != 0 {
		t.Fatalf("a wake went out before the round was collected: %q", control.texts)
	}
	if store.taskWaits["tw-1"].Delivery != "pending" {
		t.Fatalf("held wake was claimed: %q", store.taskWaits["tw-1"].Delivery)
	}

	verdict := review.Verdict{Schema: review.Schema, Verdict: "reject", Findings: []review.Finding{{ID: "f1", Severity: "high", Blocking: true, Title: "breaks replay"}}}
	store.round.Reviewers[0] = review.Reviewer{ID: "a", Route: "codex/sol", Required: true, State: "succeeded", Verdict: &verdict,
		ReviewMD: "# a review\n", VerdictJSON: []byte(`{"verdict":"reject"}`)}
	store.round.Combined = store.round.CombinedVerdict()
	*now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(control.sends) != 1 || control.resumed[0] != "t1" {
		t.Fatalf("collected round did not wake the thread once: %v", control.sends)
	}
	text := control.texts[0]
	for _, want := range []string{"verdict reject", "blocking findings: 1", "breaks replay", ".t3/reviews/round-1/a/review.md", ".t3/reviews/round-1/a/verdict.json"} {
		if !strings.Contains(text, want) {
			t.Fatalf("wake does not carry %q:\n%s", want, text)
		}
	}
	for name, want := range map[string]string{"review.md": "# a review\n", "verdict.json": `{"verdict":"reject"}`} {
		raw, err := os.ReadFile(filepath.Join(store.workspace, ".t3", "reviews", "round-1", "a", name))
		if err != nil || string(raw) != want {
			t.Fatalf("%s in the workspace: %q %v", name, raw, err)
		}
	}
}

// Review finding 2: a review wait can settle while the turn is already running,
// because another wait of the same attempt resumed it first, so its delivery is
// not the resumption. The verdict, the blocking count and the documents still
// travel with it, and it is still held until the round is collected.
func TestReviewParentWakeArrivingMidTurnCarriesTheVerdict(t *testing.T) {
	runner, store, control, now := reviewParentRunner(t)
	attempt := store.attempts["a1"]
	attempt.Progress, attempt.Control = domain.ProgressActive, domain.ControlRunning
	store.attempts["a1"] = attempt
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(control.sends) != 0 {
		t.Fatalf("a mid-turn review wake went out before the round was collected: %q", control.texts)
	}
	if w := store.taskWaits["tw-1"]; w.Resumption || w.Delivery != "pending" {
		t.Fatalf("the review wake is not a held mid-turn delivery: resumption=%v delivery=%q", w.Resumption, w.Delivery)
	}

	verdict := review.Verdict{Schema: review.Schema, Verdict: "reject", Findings: []review.Finding{{ID: "f1", Blocking: true, Title: "fix me"}}}
	store.round.Reviewers[0] = review.Reviewer{ID: "a", Route: "codex/sol", Required: true, State: "succeeded", Verdict: &verdict,
		ReviewMD: "review\n", VerdictJSON: []byte(`{}`)}
	*now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(control.texts) != 1 {
		t.Fatalf("the collected round did not deliver one mid-turn wake: %q", control.texts)
	}
	for _, want := range []string{"verdict reject", "blocking findings: 1", "fix me", ".t3/reviews/round-1/a/review.md", ".t3/reviews/round-1/a/verdict.json"} {
		if !strings.Contains(control.texts[0], want) {
			t.Fatalf("mid-turn review wake does not carry %q:\n%s", want, control.texts[0])
		}
	}
	if _, err := os.Stat(filepath.Join(store.workspace, ".t3", "reviews", "round-1", "a", "verdict.json")); err != nil {
		t.Fatalf("verdict.json was not placed for a mid-turn wake: %v", err)
	}
}

// Evidence that cannot be placed is named in the wake rather than blocking it,
// and a planted link is never followed.
func TestReviewParentWakeNamesEvidenceItCouldNotWrite(t *testing.T) {
	runner, store, control, _ := reviewParentRunner(t)
	verdict := review.Verdict{Schema: review.Schema, Verdict: "accept"}
	store.round.Reviewers[0] = review.Reviewer{ID: "a", Route: "codex/sol", Required: true, State: "succeeded", Verdict: &verdict, ReviewMD: "ok\n", VerdictJSON: []byte(`{}`)}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(store.workspace, ".t3")); err != nil {
		t.Fatal(err)
	}
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(control.sends) != 1 {
		t.Fatalf("wake was held by an unwritable workspace: %v", control.sends)
	}
	if !strings.Contains(control.texts[0], "verdict accept") || !strings.Contains(control.texts[0], "could not be written") {
		t.Fatalf("wake does not explain the missing files:\n%s", control.texts[0])
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 0 {
		t.Fatalf("evidence was written through a symlink: %v", entries)
	}
}

// Self-review P2-2: the git exclusion is written outside any sandbox into a
// directory the task controls, so a planted link at .git/info or at its
// exclude file must not redirect the write, for review evidence or an answer.
func TestGitExclusionNeverWritesThroughALink(t *testing.T) {
	round := review.Round{ID: "round-1", Reviewers: []review.Reviewer{{ID: "a", State: "succeeded", ReviewMD: "ok\n"}}}
	for _, planted := range []string{"exclude", "info"} {
		t.Run(planted, func(t *testing.T) {
			workspace, outside := t.TempDir(), t.TempDir()
			target := filepath.Join(outside, "victim")
			if err := os.WriteFile(target, []byte("keep\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(workspace, ".git"), 0o700); err != nil {
				t.Fatal(err)
			}
			if planted == "exclude" {
				if err := os.Mkdir(filepath.Join(workspace, ".git", "info"), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(workspace, ".git", "info", "exclude")); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Symlink(outside, filepath.Join(workspace, ".git", "info")); err != nil {
				t.Fatal(err)
			}
			if _, err := WriteReviewEvidence(workspace, round); err != nil {
				t.Fatal(err)
			}
			if err := writeAskAnswerFile(workspace, domain.AskAnswer{}); err != nil {
				t.Fatal(err)
			}
			if raw, _ := os.ReadFile(target); string(raw) != "keep\n" {
				t.Fatalf("the exclusion was written through a link: %q", raw)
			}
			if entries, _ := os.ReadDir(outside); len(entries) != 1 {
				t.Fatalf("files appeared outside the workspace: %v", entries)
			}
		})
	}
}

// A round that is never collected cannot hold a resumed turn forever: past
// the wait's deadline the wake goes out and says where to look.
func TestReviewParentWakeStopsHoldingAfterTheDeadline(t *testing.T) {
	runner, store, control, now := reviewParentRunner(t)
	store.roundErr = errors.New("coordinator unreachable")
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(control.sends) != 0 {
		t.Fatal("wake went out before the deadline without a verdict")
	}
	*now = now.Add(2 * time.Hour)
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(control.sends) != 1 || !strings.Contains(control.texts[0], "t3-steward review result round-1") {
		t.Fatalf("wake after the deadline: %v %q", control.sends, control.texts)
	}
}
