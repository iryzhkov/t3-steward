package wait

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

// ReviewRoundReader is the optional store surface a wake of a review parent
// wait reads the round from, documents included. A store that does not offer
// it still wakes the task, and the wake says where to read the verdict.
type ReviewRoundReader interface {
	GetReviewRound(context.Context, string) (review.Round, error)
}

// ReviewEvidenceDir is where the documents of a review round are placed in the
// task's workspace, one directory per round and reviewer.
const ReviewEvidenceDir = ".t3/reviews"

// reviewCollectionGrace is how long past a review parent wait's deadline the
// wake keeps waiting for the round to be collected. The collector times out
// every unfinished reviewer at the round deadline, which is also the wait's,
// so a round still uncollected after this is not going to be.
const reviewCollectionGrace = 10 * time.Minute

// ReviewMemberEvidence is one reviewer's part of a collected round.
type ReviewMemberEvidence struct {
	ID, Route, State, Verdict string
	Required                  bool
	Blocking                  int
	BlockingTitles            []string
	Failure                   string
	// ReviewPath and VerdictPath are relative to the workspace, empty when the
	// reviewer produced no such document or it could not be written.
	ReviewPath, VerdictPath string
}

// ReviewEvidence is what a resumed task is told about its review round.
type ReviewEvidence struct {
	RoundID  string
	Verdict  string
	Blocking int
	Members  []ReviewMemberEvidence
	// WriteErr is why the documents are not in the workspace, if they are not.
	WriteErr error
}

// SummarizeReviewRound reads the verdict and blocking findings of a round
// without touching any file.
func SummarizeReviewRound(round review.Round) ReviewEvidence {
	evidence := ReviewEvidence{RoundID: round.ID, Verdict: round.CombinedVerdict()}
	for _, v := range round.Reviewers {
		member := ReviewMemberEvidence{ID: v.ID, Route: v.Route, State: v.State, Required: v.Required, Failure: v.Failure}
		if v.Verdict != nil {
			member.Verdict = v.Verdict.Verdict
			for _, finding := range v.Verdict.Findings {
				if finding.Blocking {
					member.Blocking++
					member.BlockingTitles = append(member.BlockingTitles, finding.Title)
				}
			}
		}
		evidence.Blocking += member.Blocking
		evidence.Members = append(evidence.Members, member)
	}
	return evidence
}

// WriteReviewEvidence places every reviewer's review.md and verdict.json of a
// collected round under ReviewEvidenceDir in the workspace and returns the
// summary with the paths it wrote.
//
// The workspace is the path the attempt's worker reported. Each directory on
// the way must be a real directory, never a link, and each file is written
// beside its final name and renamed, the way the ask answer file is, so a
// planted link cannot redirect the write and the task never reads half a
// document. A failure is returned on the evidence as well: the verdict is
// still worth telling the task.
func WriteReviewEvidence(workspace string, round review.Round) (ReviewEvidence, error) {
	evidence := SummarizeReviewRound(round)
	err := writeReviewEvidence(workspace, round, &evidence)
	if err != nil {
		for i := range evidence.Members {
			evidence.Members[i].ReviewPath, evidence.Members[i].VerdictPath = "", ""
		}
		evidence.WriteErr = err
	}
	return evidence, err
}

func writeReviewEvidence(workspace string, round review.Round, evidence *ReviewEvidence) error {
	if workspace == "" || !filepath.IsAbs(workspace) {
		return fmt.Errorf("workspace %q is not an absolute path", workspace)
	}
	if !review.IDPattern.MatchString(round.ID) {
		return fmt.Errorf("review round %q is not a safe directory name", round.ID)
	}
	if err := realDirectory(workspace, false); err != nil {
		return err
	}
	directory := workspace
	for _, part := range append(strings.Split(ReviewEvidenceDir, "/"), round.ID) {
		directory = filepath.Join(directory, part)
		if err := realDirectory(directory, true); err != nil {
			return err
		}
	}
	for i, v := range round.Reviewers {
		if v.ReviewMD == "" && len(v.VerdictJSON) == 0 {
			continue
		}
		if !review.IDPattern.MatchString(v.ID) {
			return fmt.Errorf("reviewer %q is not a safe directory name", v.ID)
		}
		member := filepath.Join(directory, v.ID)
		if err := realDirectory(member, true); err != nil {
			return err
		}
		relative := ReviewEvidenceDir + "/" + round.ID + "/" + v.ID + "/"
		if v.ReviewMD != "" {
			if err := replaceWorkspaceFile(member, "review.md", []byte(v.ReviewMD)); err != nil {
				return err
			}
			evidence.Members[i].ReviewPath = relative + "review.md"
		}
		if len(v.VerdictJSON) != 0 {
			if err := replaceWorkspaceFile(member, "verdict.json", v.VerdictJSON); err != nil {
				return err
			}
			evidence.Members[i].VerdictPath = relative + "verdict.json"
		}
	}
	excludeFromGit(workspace, "/"+ReviewEvidenceDir+"/")
	return nil
}

// realDirectory requires path to be a directory and not a link, creating it
// privately when create is set and it does not exist.
func realDirectory(path string, create bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && create {
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s is not a directory; refusing to write review evidence through it", path)
	}
	return nil
}

func replaceWorkspaceFile(directory, name string, content []byte) error {
	target := filepath.Join(directory, name)
	if existing, err := os.Lstat(target); err == nil && !existing.Mode().IsRegular() {
		return fmt.Errorf("%s exists and is not a regular file; refusing to replace it", target)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := os.CreateTemp(directory, "."+name+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporary.Name(), target)
}

// excludeFromGit adds one pattern to .git/info/exclude of a plain checkout, so
// a task that commits everything does not commit the steward's files. It is
// best effort: a failure leaves a file the task may commit, which is visible.
//
// The steward writes here outside any sandbox, into a directory the task
// controls, so nothing on the way may be a link: .git and .git/info must be
// real directories, exclude a regular file, and the new content is renamed
// into place rather than written through whatever the name points at.
func excludeFromGit(workspace, line string) {
	gitDir := filepath.Join(workspace, ".git")
	if err := realDirectory(gitDir, false); err != nil {
		return
	}
	info := filepath.Join(gitDir, "info")
	if err := realDirectory(info, true); err != nil {
		return
	}
	exclude := filepath.Join(info, "exclude")
	var current []byte
	if existing, err := os.Lstat(exclude); err == nil {
		if !existing.Mode().IsRegular() {
			return
		}
		if current, err = os.ReadFile(exclude); err != nil {
			return
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return
	}
	for _, existing := range strings.Split(string(current), "\n") {
		if strings.TrimSpace(existing) == line {
			return
		}
	}
	updated := string(current)
	if updated != "" && !strings.HasSuffix(updated, "\n") {
		updated += "\n"
	}
	_ = replaceWorkspaceFile(info, "exclude", []byte(updated+line+"\n"))
}

// Text renders the evidence for an agent: the combined verdict and blocking
// count first, then each reviewer with its blocking titles and the workspace
// paths of its documents.
func (e ReviewEvidence) Text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Review round %s: verdict %s; blocking findings: %d.\n", e.RoundID, e.Verdict, e.Blocking)
	for _, m := range e.Members {
		outcome := m.Verdict
		if outcome == "" {
			outcome = m.State
		}
		required := ""
		if !m.Required {
			required = ", optional"
		}
		fmt.Fprintf(&b, "- %s (%s%s): %s; blocking %d\n", m.ID, m.Route, required, outcome, m.Blocking)
		for _, title := range m.BlockingTitles {
			fmt.Fprintf(&b, "  blocking: %s\n", evidenceLine(title))
		}
		if m.Failure != "" {
			fmt.Fprintf(&b, "  failure: %s\n", evidenceLine(m.Failure))
		}
		for _, path := range []string{m.ReviewPath, m.VerdictPath} {
			if path != "" {
				fmt.Fprintf(&b, "  %s\n", path)
			}
		}
	}
	if e.WriteErr != nil {
		fmt.Fprintf(&b, "The review documents could not be written into the workspace (%s); read them with `t3-steward review result %s`.\n", evidenceLine(e.WriteErr.Error()), e.RoundID)
	}
	return b.String()
}

// evidenceLine keeps reviewer-controlled text on one line of the message.
func evidenceLine(value string) string {
	return strings.NewReplacer("\r", " ", "\n", " ", "\t", " ").Replace(value)
}

// reviewWakeEvidenceFor prepares the review evidence of every review parent
// wait in one delivery, and reports hold if any of them is still held.
func (r *Runner) reviewWakeEvidenceFor(ctx context.Context, store TaskWaitStore, wake domain.TaskWaitWakeContext, deliveryID string, now time.Time) (string, bool) {
	notes := ""
	for _, member := range wake.Waits {
		if member.DeliveryID != deliveryID || member.ReviewRoundID() == "" {
			continue
		}
		note, hold := r.reviewWakeEvidence(ctx, store, wake, member, now)
		if hold {
			return "", true
		}
		notes += note
	}
	return notes, false
}

// reviewWakeEvidence prepares what the wake of a review parent wait tells the
// task. The child run can end before its reviews are collected, so a wake that
// resumes the task while its round is not collected yet is held, by reporting
// hold, until the round is, or until the round deadline plus a grace has
// passed; then it goes out saying where the verdict can be read.
//
// A wake arriving mid-turn is never held: it is pinned to the attempt revision
// it was woken at, and the store abandons it once the running turn moves that
// revision on, so holding it could lose it with nothing said. It goes out at
// once and says where the verdict will be read.
func (r *Runner) reviewWakeEvidence(ctx context.Context, store TaskWaitStore, wake domain.TaskWaitWakeContext, w domain.TaskWait, now time.Time) (string, bool) {
	roundID := w.ReviewRoundID()
	reader, ok := store.(ReviewRoundReader)
	if !ok {
		return fmt.Sprintf("\nReview round %s: this steward cannot read review rounds; read the verdict with `t3-steward review result %s --wait`.\n", roundID, roundID), false
	}
	round, err := reader.GetReviewRound(ctx, roundID)
	if err == nil && round.Terminal() {
		evidence, writeErr := WriteReviewEvidence(wake.WorkspacePath, round)
		if writeErr != nil {
			r.log.Warn("review evidence not written; the verdict is still in the wake message", "wait", w.ID, "round", roundID, "err", writeErr)
		}
		return "\n" + evidence.Text(), false
	}
	if err == nil && !w.Resumption {
		return fmt.Sprintf("\nReview round %s: its reviews are not collected yet; read the verdict with `t3-steward review result %s --wait`.\n", roundID, roundID), false
	}
	if w.Resumption && !w.Deadline.IsZero() && now.Before(w.Deadline.Add(reviewCollectionGrace)) {
		// The attempt has already resumed, so this hold keeps a resumed turn
		// from starting; it is reported at info level for that reason.
		r.log.Info("review wake held until the round is collected", "wait", w.ID, "round", roundID, "err", err)
		return "", true
	}
	reason := "its reviews were not collected"
	if err != nil {
		reason = err.Error()
	}
	return fmt.Sprintf("\nReview round %s: no verdict is available (%s); read it with `t3-steward review result %s --wait`.\n", roundID, evidenceLine(reason), roundID), false
}
