package workerruntime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// MaxLiveCommandNudges is how many follow-up turns one attempt is sent because
// its turn ended while commands it started were still running. A later turn
// that ends the same way fails the attempt.
const MaxLiveCommandNudges = 2

// LiveCommandsFailure is the fixed reason, and the prefix of the failure text,
// of an attempt failed because its turns kept ending while its commands ran.
const LiveCommandsFailure = "live-children-at-turn-end"

// maxTurnEndNote bounds the turn-end note a snapshot carries for one attempt.
const maxTurnEndNote = 512

// maxLiveCommandFailure keeps the failure inside the 2048 bytes a journal
// excerpt carries.
const maxLiveCommandFailure = 1800

// TurnEndCheck is what the worker did about background commands at the end of
// the attempt's turns. It is durable so that a worker restart neither sends a
// second nudge for a turn already nudged nor loses the budget.
type TurnEndCheck struct {
	// Nudges is how many nudges have been claimed, sent or not.
	Nudges int `json:"nudges,omitempty"`
	// NudgedTurnID is the ended provider turn the latest nudge answers.
	NudgedTurnID string `json:"nudgedTurnId,omitempty"`
	// NudgeSent records that the nudge for NudgedTurnID reached T3. Until it
	// does, the same text is sent again under the same identity.
	NudgeSent bool   `json:"nudgeSent,omitempty"`
	NudgeText string `json:"nudgeText,omitempty"`
	// Note is the operator-facing state the snapshot reports for explain.
	Note string `json:"note,omitempty"`
}

// turnEndInspector is the optional driver side of the turn-end check: it
// finds the commands an attempt left running, sends the follow-up turn to the
// same session, and snapshots uncommitted work before a failure.
type turnEndInspector interface {
	LiveCommands(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) (LiveCommandReport, error)
	// NudgeLiveCommands starts one follow-up turn. The token identifies the
	// nudge, so a retry of the same nudge is recognised, not repeated.
	NudgeLiveCommands(ctx context.Context, pkg workerproto.ExecutionPackage, token, text string) error
	// SnapshotWorkInProgress retains uncommitted work and describes what was
	// kept. An empty description means the task has no commit to recover.
	SnapshotWorkInProgress(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) (string, error)
}

// holdForLiveCommands decides, for a turn that ended with nothing parking the
// attempt, whether the attempt is held instead of collected because commands
// it started are still running. It reports true when the caller must not
// collect: the session has been nudged and its next turn is awaited, or the
// attempt has been failed and its failure published.
//
// A driver without the check, or a turn without an identity to bind a nudge
// to, is collected as before.
func (r *Runtime) holdForLiveCommands(ctx context.Context, id string, record AttemptRecord, turnID string) (bool, error) {
	inspector, ok := r.driver.(turnEndInspector)
	if !ok || turnID == "" {
		return false, nil
	}
	pkg := record.Package.Package
	var check TurnEndCheck
	if record.TurnEnd != nil {
		check = *record.TurnEnd
	}
	token := pkg.Identity.DispatchToken + "/turn-end/" + turnID
	if check.NudgedTurnID == turnID {
		if check.NudgeSent {
			// The nudge for this ended turn is in T3 and the turn it starts has
			// not been observed yet.
			return true, nil
		}
		return true, r.sendLiveCommandNudge(ctx, id, inspector, pkg, turnID, token, check.NudgeText)
	}
	report, err := inspector.LiveCommands(ctx, pkg, record.WorkspacePath)
	if err != nil {
		report = LiveCommandReport{Unsupported: "background command check failed: " + err.Error()}
	}
	if report.Unsupported != "" {
		note := report.Unsupported + "; the turn end was collected without looking for background commands"
		r.log.Warn("background commands cannot be checked at turn end; collecting as before",
			"assignment", id, "reason", report.Unsupported)
		return false, r.updateTurnEnd(id, func(current *TurnEndCheck) { current.Note = truncateText(note, maxTurnEndNote) })
	}
	if len(report.Commands) == 0 {
		if check.Note != "" {
			return false, r.updateTurnEnd(id, func(current *TurnEndCheck) { current.Note = "" })
		}
		return false, nil
	}
	if check.Nudges >= MaxLiveCommandNudges {
		return true, r.failLiveCommands(ctx, id, record, inspector, report.Commands, check.Nudges)
	}
	nudge := check.Nudges + 1
	text := liveCommandNudgeText(report.Commands, nudge)
	note := fmt.Sprintf("waiting for %s: %s (nudge %d of %d)",
		countCommands(len(report.Commands)), commandNames(report.Commands), nudge, MaxLiveCommandNudges)
	// The claim is durable before the effect: a worker that dies after
	// sending comes back to a claimed nudge and resends it under the same
	// identity, which T3 recognises, rather than sending a second one.
	if err := r.updateTurnEnd(id, func(current *TurnEndCheck) {
		current.Nudges, current.NudgedTurnID, current.NudgeSent = nudge, turnID, false
		current.NudgeText, current.Note = text, truncateText(note, maxTurnEndNote)
	}); err != nil {
		return true, err
	}
	r.log.Info("turn ended while commands it started are still running; the session is told to wait for them",
		"assignment", id, "thread", pkg.Identity.ThreadID, "turn", turnID, "commands", len(report.Commands),
		"nudge", nudge, "of", MaxLiveCommandNudges)
	return true, r.sendLiveCommandNudge(ctx, id, inspector, pkg, turnID, token, text)
}

func (r *Runtime) sendLiveCommandNudge(ctx context.Context, id string, inspector turnEndInspector, pkg workerproto.ExecutionPackage, turnID, token, text string) error {
	if err := inspector.NudgeLiveCommands(ctx, pkg, token, text); err != nil {
		r.log.Warn("turn-end nudge is not delivered yet; it is sent again on a later pass", "assignment", id, "error", err)
		return nil
	}
	return r.updateTurnEnd(id, func(current *TurnEndCheck) {
		if current.NudgedTurnID == turnID {
			current.NudgeSent = true
		}
	})
}

// failLiveCommands fails an attempt whose turn ended with commands running
// after its nudges were spent. Uncommitted work is snapshotted first, and the
// failure names the commands, the declared outputs and what was retained.
func (r *Runtime) failLiveCommands(ctx context.Context, id string, record AttemptRecord, inspector turnEndInspector, commands []LiveCommand, nudges int) error {
	pkg := record.Package.Package
	retained, err := inspector.SnapshotWorkInProgress(ctx, pkg, record.WorkspacePath)
	if err != nil {
		retained = "wip.bundle not retained: " + err.Error()
	}
	listed := make([]string, 0, len(commands))
	for index, command := range commands {
		if index == 5 {
			listed = append(listed, fmt.Sprintf("and %d more", len(commands)-index))
			break
		}
		listed = append(listed, fmt.Sprintf("pid %d %s", command.PID, command.Command))
	}
	evidence := []string{declaredOutputEvidence(pkg, record.WorkspacePath)}
	if retained != "" {
		evidence = append(evidence, retained)
	}
	suffix := "; " + strings.Join(evidence, "; ")
	head := fmt.Sprintf("%s: %s still running after %d nudges: ", LiveCommandsFailure, countCommands(len(commands)), nudges)
	processes := truncateText(strings.Join(listed, "; "), max(maxLiveCommandFailure-len(head)-len(suffix), 64))
	reason := truncateText(head+processes+suffix, 2048)
	r.log.Warn("turn ended with commands still running after every nudge; the attempt fails",
		"assignment", id, "thread", pkg.Identity.ThreadID, "commands", len(commands), "retained", retained)
	if err := r.updateTurnEnd(id, func(current *TurnEndCheck) { current.Note = "" }); err != nil {
		return err
	}
	if err := r.markFailed(ctx, id, reason); err != nil {
		return err
	}
	return r.collect(ctx, id)
}

func (r *Runtime) updateTurnEnd(id string, change func(*TurnEndCheck)) error {
	return r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok {
			return nil
		}
		var check TurnEndCheck
		if current.TurnEnd != nil {
			check = *current.TurnEnd
		}
		change(&check)
		current.TurnEnd = &check
		current.UpdatedAt = r.now()
		state.Attempts[id] = current
		state.Sequence++
		return nil
	})
}

func countCommands(n int) string {
	if n == 1 {
		return "1 background command"
	}
	return fmt.Sprintf("%d background commands", n)
}

// commandNames lists the first few commands' text for a one-line note.
func commandNames(commands []LiveCommand) string {
	names := make([]string, 0, 3)
	for index, command := range commands {
		if index == 3 {
			names = append(names, fmt.Sprintf("and %d more", len(commands)-index))
			break
		}
		names = append(names, truncateText(command.Command, 80))
	}
	return strings.Join(names, ", ")
}

func liveCommandNudgeText(commands []LiveCommand, nudge int) string {
	var text strings.Builder
	verb := "are"
	if len(commands) == 1 {
		verb = "is"
	}
	fmt.Fprintf(&text, "Your turn ended while %s you started in this workspace %s still running:\n", countCommands(len(commands)), verb)
	for index, command := range commands {
		if index == 10 {
			fmt.Fprintf(&text, "- and %d more\n", len(commands)-index)
			break
		}
		fmt.Fprintf(&text, "- pid %d: %s\n", command.PID, command.Command)
	}
	fmt.Fprintf(&text, "Ending a turn completes the task, and the task cannot complete while these run. "+
		"Wait for each of them to exit in the foreground: poll until it has exited, and do not start another background command. "+
		"Then read their results, finish the work, write every declared output, commit what the task declares, and only then end your turn. "+
		"This is reminder %d of %d. If commands you started are still running when a turn ends after the last reminder, the task fails as %s.",
		nudge, MaxLiveCommandNudges, LiveCommandsFailure)
	return text.String()
}

// declaredOutputEvidence names each declared file output as present or
// missing in the workspace, and the declared commits, which are not files.
func declaredOutputEvidence(pkg workerproto.ExecutionPackage, workspace string) string {
	var present, missing, commits []string
	for _, output := range pkg.Outputs {
		if output.Commit != nil {
			commits = append(commits, output.Name)
			continue
		}
		if _, err := os.Lstat(filepath.Join(workspace, filepath.FromSlash(output.Name))); err == nil {
			present = append(present, output.Name)
		} else {
			missing = append(missing, output.Name)
		}
	}
	if len(present)+len(missing)+len(commits) == 0 {
		return "no declared outputs"
	}
	parts := make([]string, 0, 3)
	for _, group := range []struct {
		label string
		names []string
	}{{"present", present}, {"missing", missing}, {"commit", commits}} {
		if len(group.names) != 0 {
			parts = append(parts, group.label+": "+strings.Join(group.names, ", "))
		}
	}
	return "declared outputs " + strings.Join(parts, "; ")
}
