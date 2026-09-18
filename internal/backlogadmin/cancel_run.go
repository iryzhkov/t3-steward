package backlogadmin

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// MutationScopeRun is the cancel scope that means the whole run. It travels in
// the command payload, beside the payloads pause and delay already carry, so a
// scoped cancel is one ordinary revision-fenced admin command and not a second
// kind of command with a second execution path.
const MutationScopeRun = "run"

// commandScope is the payload shape a scoped command carries.
type commandScope struct {
	Scope string `json:"scope,omitempty"`
}

// runScopedCommand reports whether a command payload asks for the whole run.
// A payload that is not a scope, such as the pause payload, is not one: the
// field is absent and the answer is no.
func runScopedCommand(kind domain.AdminCommandKind, payload json.RawMessage) (bool, error) {
	if len(payload) == 0 || string(payload) == "null" {
		return false, nil
	}
	var scope commandScope
	if err := json.Unmarshal(payload, &scope); err != nil || scope.Scope == "" {
		return false, nil
	}
	if scope.Scope != MutationScopeRun {
		return false, fmt.Errorf("%w: scope %q is not a command scope; the only scope is %q",
			ErrInvalidQuery, scope.Scope, MutationScopeRun)
	}
	if kind != domain.AdminCommandCancel {
		return false, fmt.Errorf("%w: only cancel takes scope %q (got %q)",
			ErrInvalidQuery, MutationScopeRun, kind)
	}
	return true, nil
}

// RunCancelAnchorID is the attempt a run-scoped cancel is fenced on: the
// non-terminal attempt of the run whose id sorts first. The rule is exported
// because the client has to fence the same attempt it will be applied to, and
// two implementations of "which one" would disagree the first time a run had
// two non-terminal tasks.
//
// The other non-terminal attempts travel as related attempts and each carries
// its own revision fence, so nothing about this choice makes the command less
// safe for them; it only decides whose revision the caller states.
func RunCancelAnchorID(attempts []domain.Attempt, runID string) (string, bool) {
	anchor := ""
	for _, attempt := range domain.DeclaredTaskAttempts(attempts) {
		if attempt.WorkflowRunID != runID || attempt.Progress.Terminal() {
			continue
		}
		if anchor == "" || attempt.ID < anchor {
			anchor = attempt.ID
		}
	}
	return anchor, anchor != ""
}

// resolveRunCancelTarget turns a run-scoped cancel into its anchor attempt.
func resolveRunCancelTarget(records sqlite.CoordinatorRecords, runID string) (domain.AdminTargetType, string, error) {
	found := false
	for _, run := range records.WorkflowRuns {
		if run.ID == runID {
			found = true
			break
		}
	}
	if !found {
		return "", "", notFound("workflow run", runID)
	}
	anchor, ok := RunCancelAnchorID(records.Attempts, runID)
	if !ok {
		return "", "", fmt.Errorf("%w: run %s has no task to cancel; every task of it is already terminal",
			ErrInvalidQuery, runID)
	}
	return domain.AdminTargetAttempt, anchor, nil
}

// runCancelReplayMatches accepts the replay of a run-scoped cancel: the
// command's target must be an attempt of the named run. The task-scoped rule
// cannot be used because the request names no task.
func runCancelReplayMatches(records sqlite.CoordinatorRecords, request Mutation, command domain.AdminCommand) bool {
	if command.TargetType != domain.AdminTargetAttempt {
		return false
	}
	for _, attempt := range records.Attempts {
		if attempt.ID == command.TargetID && attempt.WorkflowRunID == request.WorkflowRunID {
			return true
		}
	}
	return false
}

// collectCancelledAttempts splits a cancellation snapshot into the command's
// target and the attempts it takes with it, each at one revision past the one
// it was read at.
func collectCancelledAttempts(snapshot backlog.DAGState, original map[string]domain.Attempt, anchorID string) (*domain.Attempt, []domain.Attempt) {
	var target *domain.Attempt
	var related []domain.Attempt
	for _, candidate := range snapshot.Attempts {
		previous, found := original[candidate.ID]
		if !found || previous.Progress.Terminal() || candidate.Progress != domain.ProgressCancelled {
			continue
		}
		candidate.Revision = previous.Revision + 1
		candidate.AdminForceStart = false
		candidate.AdminNotBefore = nil
		if candidate.ID == anchorID {
			item := candidate
			target = &item
			continue
		}
		related = append(related, candidate)
	}
	sort.Slice(related, func(i, j int) bool { return related[i].ID < related[j].ID })
	return target, related
}
