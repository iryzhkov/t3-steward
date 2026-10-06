package domain

import (
	"fmt"
	"strings"
)

// WorkspaceHeadSchema versions the worker's report of a task workspace's
// physical HEAD at collection.
const WorkspaceHeadSchema = "workspace-head/v1"

// WorkspaceHead is what the worker found in a review-declared task's workspace
// when it collected the turn: the commit HEAD names and whether tracked files
// differ from it. Declared outputs and the task's own .t3 and .t3-steward
// directories are not counted as changes, and untracked files are not either.
//
// The worker produces it from the workspace itself; nothing the executor writes
// is read. Error is set instead of Head when the workspace has no usable HEAD.
type WorkspaceHead struct {
	Schema     string   `json:"schema"`
	Head       string   `json:"head,omitempty"`
	Dirty      bool     `json:"dirty"`
	DirtyPaths []string `json:"dirtyPaths,omitempty"`
	Error      string   `json:"error,omitempty"`
}

// MaxWorkspaceHeadDirtyPaths bounds how many changed paths a report names. The
// dirty flag stays exact; the list is for an operator reading the reason.
const MaxWorkspaceHeadDirtyPaths = 20

// ReviewRoundHead is the latest review round a task opened, as the
// coordinator's durable round records hold it. Accepted is true only when the
// round's retained results re-validate as an accepting verdict for exactly
// this head; Verdict is the combined verdict otherwise (pending, reject, or
// invalid when stored results do not re-validate).
type ReviewRoundHead struct {
	RoundID      string `json:"roundId"`
	Number       int    `json:"number"`
	CheckpointID string `json:"checkpointId"`
	BaseCommit   string `json:"baseCommit,omitempty"`
	HeadCommit   string `json:"headCommit"`
	Verdict      string `json:"verdict"`
	Accepted     bool   `json:"accepted"`
	Detail       string `json:"detail,omitempty"`
}

// DeclaredCommitHead is the commit one declared commit output resolved to.
type DeclaredCommitHead struct {
	Name   string `json:"name"`
	Commit string `json:"commit"`
}

// ReviewGateCode is the stable reason of a completion gate decision.
type ReviewGateCode string

const (
	// ReviewGateAccepted: the latest round accepted the workspace's HEAD and the
	// tree is clean.
	ReviewGateAccepted ReviewGateCode = "accepted-head"
	// ReviewGateRequired: the task declares review: and opened no round.
	ReviewGateRequired ReviewGateCode = "review-required"
	// ReviewGateNotAccepted: the latest round is pending, rejected or invalid.
	ReviewGateNotAccepted ReviewGateCode = "review-not-accepted"
	// ReviewGateHeadChanged: HEAD, or a declared commit, is not the head the
	// latest round accepted.
	ReviewGateHeadChanged ReviewGateCode = "head-changed-after-review"
	// ReviewGateDirtyTree: HEAD is the accepted head but tracked files differ.
	ReviewGateDirtyTree ReviewGateCode = "dirty-tree-after-review"
	// ReviewGateHeadUnknown: the worker reported no usable HEAD, so nothing
	// can be compared and the gate fails closed.
	ReviewGateHeadUnknown ReviewGateCode = "workspace-head-unknown"
)

// ReviewCompletionGate is the coordinator's decision on whether a
// review-declared task may complete with the work its workspace holds. It is
// recorded on the attempt so explain and task result show the heads compared.
type ReviewCompletionGate struct {
	Passed         bool                `json:"passed"`
	Code           ReviewGateCode      `json:"code"`
	Detail         string              `json:"detail"`
	RoundID        string              `json:"roundId,omitempty"`
	RoundNumber    int                 `json:"roundNumber,omitempty"`
	CheckpointID   string              `json:"checkpointId,omitempty"`
	RoundVerdict   string              `json:"roundVerdict,omitempty"`
	ReviewedHead   string              `json:"reviewedHead,omitempty"`
	PhysicalHead   string              `json:"physicalHead,omitempty"`
	DirtyPaths     []string            `json:"dirtyPaths,omitempty"`
	DeclaredCommit *DeclaredCommitHead `json:"declaredCommit,omitempty"`
	// NewRoundNeeded says the work can only complete through another review
	// round, as opposed to waiting for the current one to finish.
	NewRoundNeeded bool `json:"newRoundNeeded,omitempty"`
}

// EvaluateReviewCompletionGate decides completion from the task's latest
// round, the worker's report of the workspace, and the commits the task's
// declared commit outputs resolved to. latest is nil when no round exists and
// head is nil when the worker reported nothing.
//
// The latest round decides, not the best one: an accepted round followed by a
// newer one means the work moved on, and only the newer round has seen it.
func EvaluateReviewCompletionGate(latest *ReviewRoundHead, head *WorkspaceHead, commits []DeclaredCommitHead) ReviewCompletionGate {
	gate := ReviewCompletionGate{}
	physical, headErr := usableWorkspaceHead(head)
	if headErr == "" {
		gate.PhysicalHead = physical
	}
	if latest == nil {
		gate.Code, gate.NewRoundNeeded = ReviewGateRequired, true
		gate.Detail = "the task declares review: and no review round was opened"
		if gate.PhysicalHead != "" {
			gate.Detail += "; open a review round on " + gate.PhysicalHead
		}
		return gate
	}
	gate.RoundID, gate.RoundNumber, gate.CheckpointID = latest.RoundID, latest.Number, latest.CheckpointID
	gate.RoundVerdict, gate.ReviewedHead = latest.Verdict, latest.HeadCommit
	round := fmt.Sprintf("review round %d (checkpoint %s) at %s", latest.Number, latest.CheckpointID, latest.HeadCommit)
	if !latest.Accepted {
		gate.Code = ReviewGateNotAccepted
		gate.Detail = fmt.Sprintf("latest %s is %s, not accepted", round, latest.Verdict)
		if latest.Detail != "" {
			gate.Detail += " (" + latest.Detail + ")"
		}
		if latest.Verdict == "pending" {
			gate.Detail += "; the task finished before the round did"
		} else {
			gate.NewRoundNeeded = true
			gate.Detail += "; a new review round is needed"
		}
		return gate
	}
	if headErr != "" {
		gate.Code = ReviewGateHeadUnknown
		gate.Detail = "the worker reported no usable workspace HEAD to compare with " + round + ": " + headErr
		return gate
	}
	if physical != latest.HeadCommit {
		gate.Code, gate.NewRoundNeeded = ReviewGateHeadChanged, true
		gate.Detail = fmt.Sprintf("workspace HEAD %s is not the head %s accepted by review round %d (checkpoint %s); a new review round on %s is needed",
			physical, latest.HeadCommit, latest.Number, latest.CheckpointID, physical)
		return gate
	}
	if head.Dirty {
		gate.Code, gate.DirtyPaths = ReviewGateDirtyTree, append([]string(nil), head.DirtyPaths...)
		gate.Detail = fmt.Sprintf("the workspace at accepted head %s has uncommitted tracked changes (%s) that review round %d never saw; commit them and open a new review round, or discard them",
			latest.HeadCommit, strings.Join(head.DirtyPaths, ", "), latest.Number)
		return gate
	}
	for _, commit := range commits {
		if commit.Commit != latest.HeadCommit {
			declared := commit
			gate.Code, gate.NewRoundNeeded, gate.DeclaredCommit = ReviewGateHeadChanged, true, &declared
			gate.Detail = fmt.Sprintf("declared commit %q resolves to %s, not the head %s accepted by review round %d (checkpoint %s); a new review round on %s is needed",
				commit.Name, commit.Commit, latest.HeadCommit, latest.Number, latest.CheckpointID, commit.Commit)
			return gate
		}
	}
	gate.Passed, gate.Code = true, ReviewGateAccepted
	gate.Detail = fmt.Sprintf("workspace HEAD %s is the head accepted by review round %d (checkpoint %s, verdict %s) and the tree is clean",
		physical, latest.Number, latest.CheckpointID, latest.Verdict)
	return gate
}

// Failure is the attempt failure a failing decision records, or empty.
func (g ReviewCompletionGate) Failure() string {
	if g.Passed {
		return ""
	}
	return "review gate " + string(g.Code) + ": " + g.Detail
}

// Summary is the one line explain and task result print for the decision.
func (g ReviewCompletionGate) Summary() string {
	verdict := "failed"
	if g.Passed {
		verdict = "passed"
	}
	return fmt.Sprintf("review gate %s (%s): reviewed head %s, workspace HEAD %s; %s",
		verdict, g.Code, orNone(g.ReviewedHead), orNone(g.PhysicalHead), g.Detail)
}

func orNone(value string) string {
	if value == "" {
		return "none"
	}
	return value
}

func usableWorkspaceHead(head *WorkspaceHead) (string, string) {
	switch {
	case head == nil:
		return "", "no workspace HEAD evidence was collected"
	case head.Schema != WorkspaceHeadSchema:
		return "", fmt.Sprintf("unsupported workspace HEAD evidence schema %q", head.Schema)
	case head.Error != "":
		return "", head.Error
	case !fullCommitID(head.Head):
		return "", fmt.Sprintf("workspace HEAD %q is not a full commit ID", head.Head)
	}
	return head.Head, ""
}

func fullCommitID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
