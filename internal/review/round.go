package review

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
)

type Round struct {
	ID                  string     `json:"id"`
	WorkflowRunID       string     `json:"workflowRunId,omitempty"`
	InputManifestDigest string     `json:"inputManifestDigest"`
	BaseCommit          string     `json:"baseCommit,omitempty"`
	HeadCommit          string     `json:"headCommit,omitempty"`
	Reviewers           []Reviewer `json:"reviewers"`
	Combined            string     `json:"combinedVerdict"`
	Revision            int64      `json:"revision"`
	CreatedAt           time.Time  `json:"createdAt"`
	UpdatedAt           time.Time  `json:"updatedAt"`
}
type Reviewer struct {
	ID               string   `json:"id"`
	TaskID           string   `json:"taskId,omitempty"`
	Role             string   `json:"role"`
	Route            string   `json:"route"`
	Required         bool     `json:"required"`
	State            string   `json:"state"`
	Verdict          *Verdict `json:"verdict,omitempty"`
	Failure          string   `json:"failure,omitempty"`
	ReviewMD         string   `json:"reviewMarkdown,omitempty"`
	VerdictJSON      []byte   `json:"verdictJSON,omitempty"`
	ReviewAvailable  bool     `json:"reviewAvailable,omitempty"`
	VerdictAvailable bool     `json:"verdictAvailable,omitempty"`
}
type Result struct {
	State       string
	Failure     string
	ReviewMD    string
	VerdictJSON []byte
}

func terminalState(state string) bool {
	switch state {
	case "succeeded", "failed", "timed-out", "invalid":
		return true
	}
	return false
}

// Metadata omits bulk evidence, which callers fetch one document at a time.
func (r Round) Metadata() Round {
	r.Reviewers = append([]Reviewer(nil), r.Reviewers...)
	for i := range r.Reviewers {
		v := &r.Reviewers[i]
		v.ReviewAvailable = v.ReviewMD != ""
		v.VerdictAvailable = len(v.VerdictJSON) > 0
		v.ReviewMD = ""
		v.VerdictJSON = nil
		if v.Verdict != nil {
			copy := *v.Verdict
			copy.Findings = nil
			v.Verdict = &copy
		}
	}
	return r
}
func (r Round) Terminal() bool {
	if len(r.Reviewers) == 0 {
		return false
	}
	for _, v := range r.Reviewers {
		if !terminalState(v.State) {
			return false
		}
	}
	return true
}
func (r Round) CombinedVerdict() string {
	required := 0
	pending := false
	changes := false
	for _, v := range r.Reviewers {
		if !v.Required {
			continue
		}
		required++
		switch v.State {
		case "succeeded":
			if v.Verdict == nil || v.Verdict.Verdict == "reject" {
				return "reject"
			}
			if v.Verdict.Verdict != "accept" && v.Verdict.Verdict != "accept-with-changes" {
				return "reject"
			}
			for _, f := range v.Verdict.Findings {
				if f.Blocking {
					return "reject"
				}
			}
			changes = changes || v.Verdict.Verdict == "accept-with-changes"
		case "failed", "timed-out", "invalid":
			return "reject"
		default:
			pending = true
		}
	}
	if required == 0 {
		return "reject"
	}
	if pending {
		return "pending"
	}
	if changes {
		return "accept-with-changes"
	}
	return "accept"
}
func (r *Round) Initialize(now time.Time) error {
	if len(r.WorkflowRunID) > 128 || len(r.Reviewers) > 32 {
		return errors.New("round metadata exceeds limits: 32 reviewers and 128-byte run id")
	}
	if !IDPattern.MatchString(r.ID) || !pinnedinput.ValidDigest(r.InputManifestDigest) {
		return errors.New("round id and input manifest digest are required")
	}
	for _, commit := range []string{r.BaseCommit, r.HeadCommit} {
		if commit == "" {
			continue
		}
		if len(commit) != 40 && len(commit) != 64 {
			return errors.New("commit must be a full 40 or 64 character object id")
		}
		for _, c := range commit {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return errors.New("invalid commit id")
			}
		}
	}
	seen := map[string]bool{}
	required := 0
	for i := range r.Reviewers {
		v := &r.Reviewers[i]
		if !IDPattern.MatchString(v.ID) || seen[v.ID] || strings.TrimSpace(v.Role) == "" || len(v.Role) > 64 || len(v.TaskID) > 128 || !ValidRoute(v.Route) {
			return errors.New("reviewer needs a unique safe id, role and instance/model route")
		}
		if v.State != "" && v.State != "pending" || v.Verdict != nil || v.ReviewMD != "" || len(v.VerdictJSON) != 0 || v.Failure != "" {
			return errors.New("new round cannot contain reviewer results")
		}
		seen[v.ID] = true
		v.State = "pending"
		if v.Required {
			required++
		}
	}
	if required == 0 {
		return errors.New("round requires at least one required reviewer")
	}
	r.Revision = 1
	r.CreatedAt = now.UTC()
	r.UpdatedAt = r.CreatedAt
	r.Combined = r.CombinedVerdict()
	return nil
}

// ApplyResult validates and binds successful results before they become durable.
// Invalid evidence is retained as a failure, never as an accepted verdict.
func (r *Round) ApplyResult(id string, result Result, now time.Time) error {
	if !terminalState(result.State) || result.State == "invalid" {
		return errors.New("result state must be succeeded, failed or timed-out")
	}
	if result.State != "succeeded" && strings.TrimSpace(result.Failure) == "" {
		return errors.New("failed reviewer requires a reason")
	}
	oversized := len(result.ReviewMD) > MaxDocumentBytes || len(result.VerdictJSON) > MaxDocumentBytes
	if oversized {
		result.ReviewMD = ""
		result.VerdictJSON = nil
	}
	for i := range r.Reviewers {
		v := &r.Reviewers[i]
		if v.ID != id {
			continue
		}
		if terminalState(v.State) {
			return errors.New("reviewer already has a terminal result")
		}
		v.VerdictJSON = append([]byte(nil), result.VerdictJSON...)
		v.State = result.State
		v.ReviewMD = result.ReviewMD
		v.Failure = result.Failure
		if result.State == "succeeded" {
			verdict, err := ValidateVerdict(result.VerdictJSON, r.InputManifestDigest, v.Route)
			if oversized {
				err = errors.New("review result exceeds 1 MiB per document")
			}
			if err == nil && strings.TrimSpace(result.ReviewMD) == "" {
				err = errors.New("review.md is missing or empty")
			}
			if err != nil {
				v.State = "invalid"
				v.Failure = err.Error()
			} else {
				v.Verdict = &verdict
				// Preserve the bounded original JSON bytes; canonicalizing JSON can
				// expand HTML characters and break the document transport limit.
				v.Failure = ""
			}
		}
		if len(v.Failure) > 4096 {
			end := 4096
			for end > 0 && !utf8.RuneStart(v.Failure[end]) {
				end--
			}
			v.Failure = v.Failure[:end]
		}
		r.Revision++
		r.UpdatedAt = now.UTC()
		r.Combined = r.CombinedVerdict()
		return nil
	}
	return fmt.Errorf("reviewer %q is not part of this round", id)
}
