package review

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type ReviewerReply struct {
	ID             string   `json:"reviewer"`
	Role           string   `json:"role"`
	Route          string   `json:"route"`
	State          string   `json:"state"`
	Verdict        string   `json:"verdict,omitempty"`
	Failure        string   `json:"failure,omitempty"`
	Blocking       int      `json:"blocking"`
	NonBlocking    int      `json:"nonBlocking"`
	BlockingTitles []string `json:"blockingTitles"`
	ReviewPath     string   `json:"reviewPath,omitempty"`
	VerdictPath    string   `json:"verdictPath,omitempty"`
}
type Reply struct {
	Schema              string          `json:"schema"`
	Round               string          `json:"round"`
	CombinedVerdict     string          `json:"combinedVerdict"`
	InputManifestDigest string          `json:"inputManifestDigest"`
	Reviewers           []ReviewerReply `json:"reviewers"`
	Blocking            int             `json:"blocking"`
	NonBlocking         int             `json:"nonBlocking"`
	SummaryPath         string          `json:"summaryPath"`
}
type AttributedFinding struct {
	Reviewer string `json:"reviewer"`
	Route    string `json:"route"`
	Finding
}
type Summary struct {
	Reply
	BaseCommit string              `json:"baseCommit,omitempty"`
	HeadCommit string              `json:"headCommit,omitempty"`
	Findings   []AttributedFinding `json:"findings"`
}

func severityRank(severity string) int {
	switch severity {
	case "high":
		return 0
	case "medium":
		return 1
	default:
		return 2
	}
}

// WriteOutput puts evidence under the configured steward results root. It uses
// no workspace path and refuses symlinks in the output tree, including old files.
func WriteOutput(base string, r Round) (Reply, error) {
	reply := Reply{Schema: "review-result/v1", Round: r.ID, CombinedVerdict: r.CombinedVerdict(), InputManifestDigest: r.InputManifestDigest, Reviewers: []ReviewerReply{}}
	if !IDPattern.MatchString(r.ID) {
		return reply, fmt.Errorf("unsafe round id %q", r.ID)
	}
	absolute, err := filepath.Abs(base)
	if err != nil {
		return reply, err
	}
	// Resolve the existing ancestor before making directories, so a configured
	// state path or symlink cannot place review evidence in a checkout.
	ancestor := absolute
	var suffix []string
	for {
		if _, err := os.Lstat(ancestor); err == nil {
			break
		} else if !os.IsNotExist(err) {
			return reply, err
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return reply, fmt.Errorf("cannot resolve results root")
		}
		suffix = append(suffix, filepath.Base(ancestor))
		ancestor = parent
	}
	ancestor, err = filepath.EvalSymlinks(ancestor)
	if err != nil {
		return reply, err
	}
	for i := len(suffix) - 1; i >= 0; i-- {
		ancestor = filepath.Join(ancestor, suffix[i])
	}
	for dir := ancestor; ; dir = filepath.Dir(dir) {
		if _, err := os.Lstat(filepath.Join(dir, ".git")); err == nil {
			return reply, fmt.Errorf("review results cannot be written inside a checkout; configure state_path outside it")
		}
		if filepath.Dir(dir) == dir {
			break
		}
	}
	base = ancestor
	if err := os.MkdirAll(base, 0700); err != nil {
		return reply, err
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		return reply, err
	}
	defer root.Close()
	mkdir := func(name string) error {
		if err := root.Mkdir(name, 0700); err != nil && !os.IsExist(err) {
			return err
		}
		info, err := root.Lstat(name)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe result directory %q", name)
		}
		return nil
	}
	write := func(name string, raw []byte) error {
		// Refuse links even when they resolve inside root. O_EXCL protects the
		// first write; subsequent collections replace regular files by unlinking.
		if info, err := root.Lstat(name); err == nil {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("unsafe result file %q", name)
			}
			if err := root.Remove(name); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, writeErr := f.Write(raw)
		closeErr := f.Close()
		if writeErr != nil {
			return writeErr
		}
		return closeErr
	}
	if err := mkdir("reviews"); err != nil {
		return reply, err
	}
	directory := filepath.Join("reviews", r.ID)
	if err := mkdir(directory); err != nil {
		return reply, err
	}
	summary := Summary{Reply: reply, BaseCommit: r.BaseCommit, HeadCommit: r.HeadCommit, Findings: []AttributedFinding{}}
	reviewers := append([]Reviewer(nil), r.Reviewers...)
	sort.Slice(reviewers, func(i, j int) bool { return reviewers[i].ID < reviewers[j].ID })
	seen := map[string]bool{}
	for _, v := range reviewers {
		if !IDPattern.MatchString(v.ID) || seen[v.ID] {
			return reply, fmt.Errorf("unsafe or duplicate reviewer id %q", v.ID)
		}
		seen[v.ID] = true
		dir := filepath.Join(directory, v.ID)
		if err := mkdir(dir); err != nil {
			return reply, err
		}
		row := ReviewerReply{ID: v.ID, Role: v.Role, Route: v.Route, State: v.State, Failure: v.Failure, BlockingTitles: []string{}}
		if v.ReviewMD != "" {
			name := filepath.Join(dir, "review.md")
			if err := write(name, []byte(v.ReviewMD)); err != nil {
				return reply, err
			}
			row.ReviewPath = filepath.Join(base, name)
		}
		if len(v.VerdictJSON) != 0 {
			name := filepath.Join(dir, "verdict.json")
			if err := write(name, v.VerdictJSON); err != nil {
				return reply, err
			}
			row.VerdictPath = filepath.Join(base, name)
		}
		if v.State == "succeeded" && v.Verdict != nil {
			row.Verdict = v.Verdict.Verdict
			for _, f := range v.Verdict.Findings {
				summary.Findings = append(summary.Findings, AttributedFinding{Reviewer: v.ID, Route: v.Route, Finding: f})
				if f.Blocking {
					row.Blocking++
					row.BlockingTitles = append(row.BlockingTitles, f.Title)
				} else {
					row.NonBlocking++
				}
			}
		}
		reply.Blocking += row.Blocking
		reply.NonBlocking += row.NonBlocking
		reply.Reviewers = append(reply.Reviewers, row)
	}
	reply.SummaryPath = filepath.Join(base, directory, "summary.json")
	sort.SliceStable(summary.Findings, func(i, j int) bool {
		a, b := summary.Findings[i], summary.Findings[j]
		if severityRank(a.Severity) != severityRank(b.Severity) {
			return severityRank(a.Severity) < severityRank(b.Severity)
		}
		if a.Reviewer != b.Reviewer {
			return a.Reviewer < b.Reviewer
		}
		return a.ID < b.ID
	})
	summary.Reply = reply
	raw, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return reply, err
	}
	if err := write(filepath.Join(directory, "summary.json"), append(raw, '\n')); err != nil {
		return reply, err
	}
	return reply, nil
}
