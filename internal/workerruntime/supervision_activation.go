package workerruntime

// Running one overseer activation on a worker.
//
// An activation reaches this host the way a task does -- offered, claimed,
// prepared, dispatched, collected -- and runs as a T3 thread on the run's own
// overseer route through the same control verbs. What differs is only what an
// activation is not:
//
//   - It prepares no repository. There is no checkout, no setup profile to run
//     and no credential to require, so it takes no workspace lock. An overseer
//     waiting on a lock held by the task it is reviewing is a deadlock.
//   - It runs no preflight and declares no outputs or verification, so nothing
//     it leaves behind is published as a task result.
//   - Its turn ending successfully establishes only that the provider finished.
//     Whether anything was decided is the coordinator's own record, never this
//     transcript, so nothing here maps a clean exit to acceptance.
//
// Native subagent spawning: the T3 thread verbs this worker uses expose no
// switch for it (internal/control/t3/control.go, NewThreadInput), so it is not
// disabled here and SupervisionActivation.SubagentsDisabled stays false. The
// prompt states the rule instead. That is a declaration, not an enforcement,
// and it is recorded as false precisely so no later reader mistakes it for one.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// ActivationPrompt renders what the overseer thread is started with: the
// bounded snapshot the coordinator assembled, then the exact commands this
// activation is scoped to run.
//
// The commands are appended verbatim and last, after the evidence, because they
// are the only authority the overseer has and a truncated read must still end
// with them. Nothing here grants anything: the coordinator authorizes every
// invocation against the activation's live lease, epoch and record revision.
func ActivationPrompt(activation workerproto.SupervisionActivation) string {
	var prompt strings.Builder
	prompt.WriteString(activation.Prompt)
	prompt.WriteString("\n\nScoped commands\n")
	prompt.WriteString(fmt.Sprintf(
		"You are supervisor %q on run %s at activation epoch %d. These commands are your only authority.\n"+
			"Read the current revisions with show before every decision, and give every mutating command a\n"+
			"--request-id you have not used: repeating a key with the same payload returns the first answer,\n"+
			"and the same key with a different payload is refused.\n",
		activation.Principal, activation.RunID, activation.Epoch))
	if activation.CredentialReference != "" {
		prompt.WriteString("Admin credential reference: " + activation.CredentialReference + "\n")
	}
	if !activation.Deadline.IsZero() {
		prompt.WriteString("This activation ends at " + activation.Deadline.UTC().Format(time.RFC3339) +
			", after which your decisions are refused.\n")
	}
	prompt.WriteString(fmt.Sprintf("Turns available to this activation: %d.\n", activation.MaxTurns))
	prompt.WriteString("Do not delegate this review to a native subagent. Every separately scheduled\n" +
		"session is a campaign task the manifest declared.\n")
	for _, action := range activation.Actions {
		prompt.WriteString("\n  " + strings.Join(action.Command, " ") + "\n")
		for _, constraint := range action.Constraints {
			prompt.WriteString("    - " + constraint + "\n")
		}
	}
	prompt.WriteString("\nEnding this turn is not a decision. If the evidence does not support one you are\n" +
		"scoped to make, escalate and stop.\n")
	return prompt.String()
}

// prepareActivation gives the activation an empty workspace of its own.
//
// Empty is the requirement, not a simplification. The activation reads evidence
// through its scoped commands and through artifact reads, never from a checkout,
// so a repository here would buy nothing and would take the workflow checkout
// lock its reviewed tasks need.
func (d *LocalDriver) prepareActivation(pkg workerproto.ExecutionPackage) (string, error) {
	workspace := filepath.Join(d.workspacePath(pkg), "activation")
	if err := ensureRealDirectory(workspace); err != nil {
		return "", fmt.Errorf("prepare supervision activation workspace: %w", err)
	}
	if err := d.writeSupervisionIdentity(pkg, workspace); err != nil {
		return "", err
	}
	return workspace, nil
}

// writeSupervisionIdentity records which admin client the overseer's CLI must
// present, inside the activation workspace, before any thread is dispatched.
//
// This is how the identity actually reaches the CLI. The thread environment
// carries the same two selectors, but only when the deployment turned
// t3.send_thread_environment on, and a fleet that has not verified that field
// against its T3 release leaves it off; the overseer then authenticated as its
// host's own coordinator client and decided gates as an operator. The record is
// written 0600 and carries no secret, so a host without the named credential
// fails to resolve it rather than gaining authority.
func (d *LocalDriver) writeSupervisionIdentity(pkg workerproto.ExecutionPackage, workspace string) error {
	if pkg.Supervision == nil {
		return nil
	}
	if workspace == "" {
		return errors.New("supervision identity needs a prepared activation workspace")
	}
	content, declared, err := pkg.Supervision.ActivationIdentityRecord()
	if err != nil {
		return err
	}
	if !declared {
		// The deployment configured no supervisor credential. Writing an empty
		// record would say the CLI has an identity to present when it has none.
		return nil
	}
	directory := filepath.Join(workspace, workerproto.SupervisorIdentityDir)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create supervision identity directory: %w", err)
	}
	path := filepath.Join(workspace, filepath.FromSlash(workerproto.SupervisorIdentityFile))
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return fmt.Errorf("write supervision identity: %w", err)
	}
	// WriteFile leaves an existing file's mode alone, and a re-prepared
	// activation rewrites this one, so the mode is asserted rather than assumed.
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("restrict supervision identity: %w", err)
	}
	return nil
}

// removeSupervisionIdentity takes the record out of the workspace before
// anything is finalized from it.
func (d *LocalDriver) removeSupervisionIdentity(workspace string) error {
	if workspace == "" {
		return nil
	}
	if err := os.RemoveAll(filepath.Join(workspace, workerproto.SupervisorIdentityDir)); err != nil {
		return fmt.Errorf("remove supervision identity: %w", err)
	}
	return nil
}

// createActivationThread starts the overseer's T3 session.
func (d *LocalDriver) createActivationThread(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) error {
	activation := pkg.Supervision
	if activation == nil {
		return errors.New("supervision activation: the package carries no activation")
	}
	if d.Config.Authorization != nil && !d.Config.Authorization.AuthorizesRoute(pkg.Route) {
		return errors.New("execution package route is no longer authorized")
	}
	if d.Config.DryRun {
		return os.WriteFile(d.noEffectsThreadPath(pkg), []byte("active\n"), 0o600)
	}
	// The overseer's sessions get a project of their own, so one never lands in
	// a project a reviewed task is using. It is keyed by the run rather than by
	// the thread: every activation of one run is the same supervision of the
	// same work, and a project per activation put five projects in the T3 picker
	// for one run -- where a person choosing a project for their own session has
	// to read past every one of them.
	//
	// Its workspace root is an owned metadata directory, as a task project's is,
	// because a project is identified by that root and every activation of a run
	// has a different prepared workspace. The thread still opens in its own
	// prepared workspace; that is its worktree path below, not the project's.
	encodedKey, err := json.Marshal([]string{pkg.CoordinatorID, pkg.WorkerID, "supervision", activation.RunID})
	if err != nil {
		return err
	}
	sum := sha256.Sum256(encodedKey)
	projectID, err := d.T3.EnsureProject(ctx, t3control.ManagedProject{
		Key:           string(encodedKey),
		Title:         "Steward supervision: " + activation.RunID,
		WorkspaceRoot: filepath.Join(d.Config.RunsRoot, ".projects", hex.EncodeToString(sum[:])),
	})
	if err != nil {
		return err
	}
	selection := map[string]any{"instanceId": pkg.Route.ProviderInstanceID, "model": pkg.Route.Model}
	// The overseer's CLI has to authenticate as the supervisor client rather
	// than as this host's own coordinator client, and the activation is the only
	// place that says which one that is. Naming it in the prompt alone left the
	// CLI with no way to act on it; see
	// workerproto.SupervisorCredentialEnvironment.
	environment := pkg.Identity.TaskEnvironment()
	for name, value := range activation.ActivationEnvironment() {
		environment[name] = value
	}
	threadID, err := d.T3.CreateAndStartThread(ctx, t3control.NewThreadInput{
		ThreadID: pkg.Identity.ThreadID, DispatchToken: pkg.Identity.DispatchToken,
		ProjectID: projectID, Title: activation.ActivationID,
		ModelSelection: selection, RuntimeMode: "full-access", InteractionMode: "default",
		WorktreePath: workspace, Prompt: ActivationPrompt(*activation),
		Environment: environment,
	})
	if threadID != "" && threadID != pkg.Identity.ThreadID {
		return errors.New("T3 returned a different deterministic thread identity")
	}
	return err
}

// collectActivation ends the activation's turn.
//
// It publishes the turn the way a task result is published, because that is how
// a worker tells the coordinator anything, and it publishes nothing a task
// result carries: no declared output, no verification evidence, no commit. The
// coordinator maps this turn onto the activation's outcome from its own
// decision records; see backlog.ActivationTurnOutcome, whose decision count is
// deliberately an argument rather than something read from the transcript.
func (d *LocalDriver) collectActivation(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) error {
	if d.Config.DryRun {
		return nil
	}
	thread, err := d.T3.GetThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return fmt.Errorf("collect supervision activation thread state: %w", err)
	}
	message, archive := "", []byte("{}")
	if thread != nil && !workerThreadTerminal(*thread) {
		return errors.New("T3 turn is not yet terminal; activation collection deferred")
	}
	if thread != nil {
		if message, err = d.T3.LastAssistantMessage(ctx, pkg.Identity.ThreadID); err != nil {
			return fmt.Errorf("collect supervision activation message: %w", err)
		}
		if archive, err = d.T3.ExportThread(ctx, pkg.Identity.ThreadID); err != nil {
			return fmt.Errorf("collect supervision activation archive: %w", err)
		}
	}
	// Zero recorded decisions here: this worker cannot know what the coordinator
	// recorded, and guessing would be the one mistake this whole lane exists to
	// prevent. The outcome computed here is therefore never better than
	// no-decision, and the coordinator upgrades it to decided from its own rows.
	outcome, failure, err := backlog.ActivationTurnOutcome(archive, pkg.Identity.ThreadID, message, 0)
	if err != nil {
		return err
	}
	d.logger().Info("supervision activation turn ended",
		"activation", pkg.Supervision.ActivationID, "run", pkg.Supervision.RunID,
		"epoch", pkg.Supervision.Epoch, "outcome", outcome, "reason", failure)
	// The identity record leaves the workspace before anything is finalized
	// from it, for the same reason a task's does.
	if err := d.removeSupervisionIdentity(workspace); err != nil {
		return err
	}
	task, attempt := packageRecords(pkg, d.Now().UTC())
	finalized, err := d.Finalizer.Finalize(ctx, backlog.AttemptFinalization{
		Task: task, Attempt: attempt, WorkspaceDir: workspace, ExplicitSuccess: failure == "",
	})
	if err != nil {
		return err
	}
	if err := d.Publisher.PublishResult(ctx, pkg, PublishedResult{
		Finalized: finalized, FinalMessage: message, ThreadArchive: archive,
	}); err != nil {
		return fmt.Errorf("publish supervision activation custody: %w", err)
	}
	if thread == nil {
		return nil
	}
	if err := d.T3.SettleThread(ctx, pkg.Identity.ThreadID, pkg.Identity.DispatchToken); err != nil {
		return fmt.Errorf("%w: %v", ErrSettleUnproven, err)
	}
	return nil
}
