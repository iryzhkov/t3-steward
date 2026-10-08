package backlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// AttemptFinalization describes a completed agent turn whose declared outputs and
// verification commands must be reconciled before DAG completion.
type AttemptFinalization struct {
	Task            domain.Task
	Attempt         domain.Attempt
	WorkspaceDir    string
	ExplicitSuccess bool
	// Extra carries evidence produced before the agent session, such as
	// preflight logs, that must be captured with the attempt's own outputs.
	// The capture tree is made immutable and then renamed into place, so an
	// artifact added afterwards cannot be written at all; it has to take part
	// in the same pass.
	Extra []FinalizationArtifact
	// Repository and BaseCommit describe where the workspace came from. A
	// declared commit output records both in its provenance, so that a
	// downstream task knows which repository the commit belongs to and which
	// commit it was built on.
	Repository string
	BaseCommit string
	WorkerID   string
	// CommitBundles retains a bundle of each declared commit so that a
	// consumer on another worker can import it. It is set only when the
	// coordinator declared the commit bundle capability on the package, which
	// is its statement that it accepts the bundle artifact.
	CommitBundles bool
	// FailedCommits permits quarantine records only when the coordinator offers
	// campaign-failed-commit-v1; older coordinators reject those objects.
	FailedCommits bool
	// CommitBundleLimit is the largest bundle the artifact transport accepts.
	// Zero leaves only the campaign ref store's own limit.
	CommitBundleLimit int64
	// AdmitResult, when set, is the check the upload carrying the attempt's
	// result applies to it, given every artifact the finalizer would capture:
	// the publisher's own validation, with every limit it enforces, of the
	// result together with what collection adds after finalization. Bundle
	// metadata is optional, so it is kept only in a result this admits, and a
	// result it admits with none is never given more. Nil leaves bundles bounded
	// only one by one.
	AdmitResult func([]domain.Artifact) error
	// ReviewGated marks a task whose completion the coordinator decides by
	// comparing its work with the head its latest review round accepted. Its
	// workspace HEAD is reported as it stands after verification, and its
	// declared commits are staged rather than published, because only the
	// coordinator can say whether they are the reviewed work. A task that
	// declares review requirements is gated whether or not this is set.
	ReviewGated bool
}

// FinalizationArtifact is evidence captured alongside an attempt's declared
// outputs. It carries its own bytes because its producer is not the workspace.
type FinalizationArtifact struct {
	// ID preserves an identity the producer already established. Preflight
	// evidence is addressed by its own identity elsewhere, so minting a fresh
	// one here would break the reference a receipt hands out. Empty means the
	// finalizer assigns one, as it does for outputs and verification reports.
	ID        string
	Name      string
	MediaType string
	Kind      domain.ArtifactKind
	Producer  string
	Content   []byte
}

// VerificationReport is the immutable result of one declared verification command.
type VerificationReport struct {
	Command     string    `json:"command"`
	ExitCode    int       `json:"exitCode"`
	Output      string    `json:"output"`
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`
}

// FinalizedAttempt contains artifacts ready for coordinator persistence and the
// strict completion result to apply to the DAG transition engine.
type FinalizedAttempt struct {
	Completion CompletionResult
	Artifacts  []domain.Artifact
	StorageDir string
}

// AttemptFinalizer captures declared outputs and verification reports in
// coordinator-owned storage. It does not persist metadata or mutate DAG state.
type AttemptFinalizer struct {
	StorageRoot string
	Now         func() time.Time
	NewID       func(kind string) string
	Processes   ProcessRunner
	// CampaignRefs keeps a declared commit reachable for the campaign's
	// lifetime. It is required only by a task that declares one.
	CampaignRefs          CampaignRefStore
	GateTimeoutMax        time.Duration
	GateToolchainIdentity string
	// GateContained marks a gate run through the contained supervisor, which
	// reports exit status only; the report records that limitation.
	GateContained bool
	// afterGate, when set by a test, runs once the gate has finished and
	// before outputs are captured: the window a process left behind by a gate
	// command could use.
	afterGate func()
}

// Finalize runs verification, captures immutable artifacts, and returns a strict
// completion result. Declared-output or command failures are task failures, while
// unsafe paths and storage failures are operational errors.
func (f AttemptFinalizer) Finalize(ctx context.Context, request AttemptFinalization) (FinalizedAttempt, error) {
	if err := f.validateRequest(request); err != nil {
		return FinalizedAttempt{}, fmt.Errorf("finalize attempt: %w", err)
	}
	result := FinalizedAttempt{Completion: CompletionResult{ExplicitSuccess: request.ExplicitSuccess}}
	if !request.ExplicitSuccess {
		result.Completion.Failure = "missing explicit success"
		return result, nil
	}

	reports := make([]VerificationReport, 0, len(request.Task.Verification))
	failures := make([]string, 0)
	for index, command := range request.Task.Verification {
		processID := fmt.Sprintf("verify-%s-%d", request.Attempt.ID, index)
		report, err := f.runVerification(ctx, processID, request.WorkspaceDir, command)
		if err != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt verification %q: %w", command, err)
		}
		reports = append(reports, report)
		if report.ExitCode != 0 {
			failures = append(failures, fmt.Sprintf("verification command failed (%d): %s", report.ExitCode, command))
			break
		}
	}
	gated := request.ReviewGated || request.Task.ReviewRequirements != nil

	// Only failures of the task's own verification commands make a declared
	// commit retainable as failed evidence. The worker-owned gate runs only
	// after verification passed, and its failure withholds the commit as any
	// other non-verification failure does.
	verificationFailures := slices.Clone(failures)
	// gatedCommit pins publication to the commit the gate attested, so a
	// declared revision that moves afterwards cannot publish an ungated tree.
	gatedCommit := ""
	// gateReport and gateExtra let a passing report be amended when a declared
	// output no longer matches the gated commit at capture.
	var gateReport GateReport
	gateExtra := -1
	if request.Task.Gate != nil && len(failures) == 0 {
		report, log, gateErr := f.runGate(ctx, request)
		if gateErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize gate: %w", gateErr)
		}
		if f.afterGate != nil {
			f.afterGate()
		}
		raw, marshalErr := json.MarshalIndent(report, "", "  ")
		if marshalErr != nil {
			return FinalizedAttempt{}, marshalErr
		}
		gateReport, gateExtra = report, len(request.Extra)
		request.Extra = append(slices.Clone(request.Extra),
			FinalizationArtifact{ID: "gate-" + request.Attempt.ID, Name: "gate", Kind: domain.ArtifactGate, MediaType: "application/json", Producer: "gate", Content: append(raw, '\n')},
			FinalizationArtifact{ID: "gate-log-" + request.Attempt.ID, Name: "gate/log.txt", Kind: domain.ArtifactGate, MediaType: "text/plain", Producer: "gate", Content: log})
		if report.Passed {
			gatedCommit = report.attestedCommit
			if gatedCommit == "" {
				return FinalizedAttempt{}, errors.New("finalize gate: passing report names no attested commit")
			}
		} else {
			failures = append(failures, fmt.Sprintf("gate command failed (%d): %s: %s", report.Failure.ExitCode, report.Failure.Command, report.Failure.Reason))
		}
	}
	workspaceRoot, err := os.OpenRoot(request.WorkspaceDir)
	if err != nil {
		return FinalizedAttempt{}, fmt.Errorf("finalize attempt: open workspace: %w", err)
	}
	defer workspaceRoot.Close()

	type outputSource struct {
		declaration domain.ArtifactDeclaration
		relative    string
	}
	outputs := make([]outputSource, 0, len(request.Task.Outputs))
	var commits []domain.ArtifactDeclaration
	var missing []string
	for _, declaration := range request.Task.Outputs {
		if declaration.Commit != nil {
			// A declared commit is not a file in the workspace. It is published
			// under its campaign ref and retained as its provenance record.
			commits = append(commits, declaration)
			continue
		}
		resolved, resolveErr := safeBundleFile(request.WorkspaceDir, declaration.Name)
		if resolveErr != nil {
			if errors.Is(resolveErr, os.ErrNotExist) {
				missing = append(missing, declaration.Name)
				continue
			}
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt output %q: %w", declaration.Name, resolveErr)
		}
		relative, relativeErr := filepath.Rel(request.WorkspaceDir, resolved)
		if relativeErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt output %q: %w", declaration.Name, relativeErr)
		}
		outputs = append(outputs, outputSource{declaration: declaration, relative: relative})
	}
	if len(missing) != 0 {
		failures = append(failures, MissingOutputFailure(missing))
	}

	runRoot := filepath.Join(f.StorageRoot, "runs", request.Attempt.WorkflowRunID, request.Task.ID)
	if err := os.MkdirAll(runRoot, 0o700); err != nil {
		return FinalizedAttempt{}, fmt.Errorf("finalize attempt: create storage: %w", err)
	}
	stageDir, err := os.MkdirTemp(runRoot, ".finalize-")
	if err != nil {
		return FinalizedAttempt{}, fmt.Errorf("finalize attempt: create staging directory: %w", err)
	}
	keepStage := false
	defer func() {
		if !keepStage {
			removeIngestedTree(stageDir)
		}
	}()

	now := f.now()
	artifacts := make([]domain.Artifact, 0, len(outputs)+len(reports))
	var changedOutputs []string
	for _, output := range outputs {
		artifactName := filepath.ToSlash(output.declaration.Name)
		storagePath := filepath.ToSlash(filepath.Join(
			"runs", request.Attempt.WorkflowRunID, request.Task.ID, request.Attempt.ID,
			"artifacts", "outputs", output.declaration.Name,
		))
		file, copyErr := copyIngestedFile(
			workspaceRoot,
			output.relative,
			filepath.Join(stageDir, "artifacts", "outputs", output.declaration.Name),
			output.declaration.Name,
			storagePath,
		)
		if copyErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt output %q: %w", output.declaration.Name, copyErr)
		}
		if gatedCommit != "" {
			// A process left behind by a gate command can rewrite an output
			// after the gate's checks, so what was captured must be what the
			// gate saw and, for a tracked file, the gated commit's content.
			if reason, checkErr := gatedOutputMatches(ctx, request.WorkspaceDir, gatedCommit, gateReport.outputDigests,
				output.declaration.Name, output.relative,
				filepath.Join(stageDir, "artifacts", "outputs", output.declaration.Name), file.sha256); checkErr != nil {
				return FinalizedAttempt{}, fmt.Errorf("finalize attempt output %q: %w", output.declaration.Name, checkErr)
			} else if reason != "" {
				changed := fmt.Sprintf("declared output %q %s", output.declaration.Name, reason)
				failures = append(failures, changed)
				changedOutputs = append(changedOutputs, changed)
			}
		}
		media := output.declaration.MediaType
		if media == "" {
			media = mediaType(output.declaration.Name)
		}
		artifacts = append(artifacts, domain.Artifact{
			ID: f.newID("artifact"), WorkflowRunID: request.Attempt.WorkflowRunID,
			TaskID: request.Task.ID, AttemptID: request.Attempt.ID,
			Kind: domain.ArtifactOutput, Name: artifactName, MediaType: media,
			Size: file.size, SHA256: file.sha256, StoragePath: file.storagePath,
			Producer: request.Task.Name, CreatedAt: now,
		})
	}
	if len(changedOutputs) != 0 {
		// The coordinator decides the attempt from the uploaded evidence, not
		// from this completion, so the gate report itself must fail. The log is
		// kept as the gate wrote it.
		raw, amendErr := amendGateForChangedOutputs(gateReport, changedOutputs)
		if amendErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize gate: %w", amendErr)
		}
		request.Extra[gateExtra].Content = raw
	}
	for index, report := range reports {
		name := fmt.Sprintf("verification/%03d.json", index+1)
		raw, marshalErr := json.MarshalIndent(report, "", "  ")
		if marshalErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt report %d: %w", index+1, marshalErr)
		}
		raw = append(raw, '\n')
		storagePath := filepath.ToSlash(filepath.Join(
			"runs", request.Attempt.WorkflowRunID, request.Task.ID, request.Attempt.ID,
			"artifacts", name,
		))
		file, writeErr := writeIngestedFile(
			bytes.NewReader(raw),
			filepath.Join(stageDir, "artifacts", filepath.FromSlash(name)),
			name,
			storagePath,
		)
		if writeErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt report %d: %w", index+1, writeErr)
		}
		artifacts = append(artifacts, domain.Artifact{
			ID: f.newID("artifact"), WorkflowRunID: request.Attempt.WorkflowRunID,
			TaskID: request.Task.ID, AttemptID: request.Attempt.ID,
			Kind: domain.ArtifactVerification, Name: name, MediaType: "application/json",
			Size: file.size, SHA256: file.sha256, StoragePath: file.storagePath,
			Producer: "verification", CreatedAt: now,
		})
	}

	// Failed verification retains a candidate under its attempt's quarantine
	// ref, while ordinary publication still requires success. Other failures
	// withhold the commit entirely; a retry can always publish its normal ref.
	retainFailed := request.FailedCommits && len(verificationFailures) != 0 && len(failures) == len(verificationFailures)
	if len(commits) != 0 && len(failures) != 0 && !retainFailed {
		names := make([]string, 0, len(commits))
		for _, declaration := range commits {
			names = append(names, fmt.Sprintf("%q", declaration.Name))
		}
		failures = append(failures, fmt.Sprintf(
			"declared commit %s not published because the attempt had already failed; the retry publishes it",
			strings.Join(names, ", ")))
		commits = nil
	}
	var published []publishedCommit
	defer func() {
		for _, commit := range published {
			commit.bundle.discard()
		}
	}()
	for _, declaration := range commits {
		if f.CampaignRefs.Root == "" {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt commit %q: campaign ref store is required", declaration.Name)
		}
		var failedAttempt *FailedCommitAttempt
		if retainFailed {
			failedAttempt = &FailedCommitAttempt{ID: request.Attempt.ID, VerificationFailures: slices.Clone(verificationFailures)}
		}
		publication := PublishCommitRequest{
			WorkflowRunID: request.Attempt.WorkflowRunID, TaskID: request.Task.ID,
			Name: declaration.Name, Repository: request.Repository,
			WorkspaceDir: request.WorkspaceDir, Revision: declaration.Commit.Revision,
			ExpectedCommit: gatedCommit, Base: request.BaseCommit, CreatedAt: now, FailedAttempt: failedAttempt,
		}
		var provenance CommitProvenance
		var publishErr error
		// A verification-failed candidate goes to its attempt's quarantine ref
		// whether or not the task is review-gated: the review gate never
		// accepts a failed attempt, so staging it for promotion is pointless.
		if gated && failedAttempt == nil {
			// Only the coordinator's review gate can say whether this commit
			// is the reviewed work, so it is staged under this attempt and
			// becomes the task's campaign output only when a dependent task
			// consumes the accepted result.
			provenance, publishErr = f.CampaignRefs.Stage(ctx, publication, request.Attempt.ID, nil)
		} else {
			provenance, publishErr = f.CampaignRefs.Publish(ctx, publication, nil)
		}
		if publishErr != nil {
			// The task promised a commit and the promise could not be kept.
			// That is the task's failure, reported with its cause, exactly as a
			// missing declared output is.
			failures = append(failures, fmt.Sprintf("declared commit %q: %v", declaration.Name, publishErr))
			continue
		}
		entry := publishedCommit{declaration: declaration, provenance: provenance}
		// A staged commit's bundle names its staging, so a worker that
		// imports it holds it as staged work and publishes it only for a
		// consumer the coordinator accepted the result for; an operator
		// exports it once the coordinator accepted this attempt.
		if request.CommitBundles {
			bound, bundle, bundleErr := f.makeCommitBundle(ctx, request, provenance)
			if bundleErr != nil {
				failures = append(failures, fmt.Sprintf("declared commit %q: %v", declaration.Name, bundleErr))
				continue
			}
			entry.provenance, entry.bundle = bound, bundle
		}
		published = append(published, entry)
	}

	// A failed candidate is reusable only when every declared promise apart
	// from verification was kept. A resolution or bundle failure must not
	// expose the other candidates as verification-only evidence.
	if retainFailed && len(failures) != len(verificationFailures) {
		for _, commit := range published {
			commit.bundle.discard()
		}
		if err := f.CampaignRefs.discardFailedAttempt(ctx, request.Attempt.WorkflowRunID, request.Task.ID, request.Attempt.ID); err != nil {
			return FinalizedAttempt{}, fmt.Errorf("discard invalid failed candidates: %w", err)
		}
		published = nil
	}
	// Every artifact still to be captured has its identity now, so that the
	// result admitted below is exactly the result captured.
	for index := range published {
		if published[index].bundle != nil {
			published[index].bundleID = f.newID("artifact")
		}
		published[index].recordID = f.newID("artifact")
	}
	extras := request.Extra
	if gated {
		// The review completion gate compares this HEAD with the head the
		// task's latest review round accepted. It is read last, after
		// verification and after every declared commit is staged, because
		// work done by anything collection runs in the workspace before this
		// point, such as a verification command that rewrites tracked source,
		// is work the review never saw. It is part of the result upload, so
		// it is read before that upload is admitted below.
		head, err := MarshalWorkspaceHead(CaptureWorkspaceHead(ctx, "", request.WorkspaceDir, request.Task.Outputs))
		if err != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt: %w", err)
		}
		extras = append(slices.Clip(extras), FinalizationArtifact{
			ID: WorkspaceHeadArtifactID(request.Attempt.ID), Name: WorkspaceHeadArtifactName,
			MediaType: "application/json", Kind: domain.ArtifactGitState, Producer: "worker", Content: head,
		})
	}
	extraIDs := make([]string, len(extras))
	for index, extra := range extras {
		extraIDs[index] = extra.ID
		if extraIDs[index] == "" {
			extraIDs[index] = f.newID("artifact")
		}
	}
	// The attempt's result travels as one upload, and bundle metadata is the
	// only optional part of it, so it is what gives way when the upload's own
	// validation would refuse it.
	if request.AdmitResult != nil && len(published) != 0 {
		// candidate is every artifact the result would capture with the
		// records as they currently read, in capture order.
		candidate := func() ([]domain.Artifact, error) {
			result := slices.Clone(artifacts)
			for _, commit := range published {
				if commit.provenance.Bundle != nil {
					result = append(result, domain.Artifact{
						ID: commit.bundleID, Kind: domain.ArtifactGitState, Name: CommitBundleArtifactName(commit.provenance.Name),
						MediaType: CommitBundleMediaType, Size: commit.bundle.size, SHA256: commit.bundle.sha256,
					})
				}
				record, err := MarshalCommitProvenance(commit.provenance)
				if err != nil {
					return nil, err
				}
				kind, name := failedCommitRecordName(commit.provenance)
				result = append(result, domain.Artifact{
					ID: commit.recordID, Kind: kind, Name: filepath.ToSlash(name),
					MediaType: "application/json", Size: int64(len(record)), SHA256: fmt.Sprintf("%x", sha256.Sum256(record)),
				})
			}
			for index, extra := range extras {
				result = append(result, domain.Artifact{
					ID: extraIDs[index], Kind: extra.Kind, Name: extra.Name, MediaType: extra.MediaType,
					Size: int64(len(extra.Content)), SHA256: fmt.Sprintf("%x", sha256.Sum256(extra.Content)),
				})
			}
			return result, nil
		}
		if err := admitCommitBundles(published, candidate, request.AdmitResult); err != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt commits: %w", err)
		}
	}
	for _, commit := range published {
		declaration, provenance := commit.declaration, commit.provenance
		if commit.bundle != nil && provenance.Bundle != nil {
			bundle, captureErr := f.captureCommitBundle(request, stageDir, provenance, *commit.bundle, commit.bundleID, now)
			if captureErr != nil {
				return FinalizedAttempt{}, fmt.Errorf("finalize attempt commit %q: %w", declaration.Name, captureErr)
			}
			artifacts = append(artifacts, bundle)
		}
		record, marshalErr := MarshalCommitProvenance(provenance)
		if marshalErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt commit %q: %w", declaration.Name, marshalErr)
		}
		kind, name := failedCommitRecordName(provenance)
		storagePath := filepath.ToSlash(filepath.Join(
			"runs", request.Attempt.WorkflowRunID, request.Task.ID, request.Attempt.ID,
			"artifacts", "outputs", name,
		))
		file, writeErr := writeIngestedFile(
			bytes.NewReader(record),
			filepath.Join(stageDir, "artifacts", "outputs", name),
			name,
			storagePath,
		)
		if writeErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt commit %q: %w", declaration.Name, writeErr)
		}
		artifacts = append(artifacts, domain.Artifact{
			ID: commit.recordID, WorkflowRunID: request.Attempt.WorkflowRunID,
			TaskID: request.Task.ID, AttemptID: request.Attempt.ID,
			Kind: kind, Name: filepath.ToSlash(name),
			MediaType: "application/json",
			Size:      file.size, SHA256: file.sha256, StoragePath: file.storagePath,
			Producer: request.Task.Name, CreatedAt: now,
		})
	}

	for index, extra := range extras {
		storageName := extra.Name
		if extra.Kind == domain.ArtifactGate && extra.Name == "gate" {
			storageName = "gate/report.json"
		}
		storagePath := filepath.ToSlash(filepath.Join(
			"runs", request.Attempt.WorkflowRunID, request.Task.ID, request.Attempt.ID,
			"artifacts", storageName,
		))
		file, writeErr := writeIngestedFile(
			bytes.NewReader(extra.Content),
			filepath.Join(stageDir, "artifacts", filepath.FromSlash(storageName)),
			extra.Name,
			storagePath,
		)
		if writeErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt extra artifact %d: %w", index+1, writeErr)
		}
		artifacts = append(artifacts, domain.Artifact{
			ID: extraIDs[index], WorkflowRunID: request.Attempt.WorkflowRunID,
			TaskID: request.Task.ID, AttemptID: request.Attempt.ID,
			Kind: extra.Kind, Name: extra.Name, MediaType: extra.MediaType,
			Size: file.size, SHA256: file.sha256, StoragePath: file.storagePath,
			Producer: extra.Producer, CreatedAt: now,
		})
	}

	finalDir := filepath.Join(runRoot, request.Attempt.ID)
	if len(artifacts) != 0 {
		if err := makeIngestedTreeImmutable(stageDir); err != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt: protect staged artifacts: %w", err)
		}
		// Finalization is retried after a lost settlement or publication; the
		// previous capture of the same attempt is replaced, not a failure.
		if _, err := os.Lstat(finalDir); err == nil {
			if err := removeIngestedTree(finalDir); err != nil {
				return FinalizedAttempt{}, fmt.Errorf("finalize attempt: replace previous capture: %w", err)
			}
		}
		if err := os.Rename(stageDir, finalDir); err != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt: publish artifacts: %w", err)
		}
		keepStage = true
		result.StorageDir = finalDir
	}
	result.Artifacts = artifacts
	result.Completion.VerificationPassed = len(failures) == 0
	if len(failures) != 0 {
		result.Completion.Failure = strings.Join(failures, "; ")
	}
	return result, nil
}

func (f AttemptFinalizer) validateRequest(request AttemptFinalization) error {
	if f.StorageRoot == "" {
		return errors.New("storage root is required")
	}
	if request.WorkspaceDir == "" {
		return errors.New("workspace directory is required")
	}
	if request.Task.ID == "" || request.Task.Name == "" {
		return errors.New("task ID and name are required")
	}
	if request.Attempt.ID == "" || request.Attempt.WorkflowRunID == "" {
		return errors.New("attempt ID and workflow run ID are required")
	}
	if request.Attempt.TaskID != request.Task.ID {
		return fmt.Errorf("attempt task %q does not match task %q", request.Attempt.TaskID, request.Task.ID)
	}
	for label, value := range map[string]string{
		"workflow run ID": request.Attempt.WorkflowRunID,
		"task ID":         request.Task.ID,
		"attempt ID":      request.Attempt.ID,
	} {
		if !safePathComponent(value) {
			return fmt.Errorf("%s %q is not a safe storage component", label, value)
		}
	}
	for _, output := range request.Task.Outputs {
		if err := validateRelativePath(output.Name, false); err != nil {
			return fmt.Errorf("output %q: %w", output.Name, err)
		}
	}
	for _, command := range request.Task.Verification {
		if strings.TrimSpace(command) == "" {
			return errors.New("verification command must not be empty")
		}
	}
	return nil
}

func (f AttemptFinalizer) runVerification(ctx context.Context, processID, workspace, command string) (VerificationReport, error) {
	started := f.now()
	result, err := f.processRunner().Run(ctx, ProcessRequest{
		ID: processID, Dir: workspace, Program: "/bin/sh",
		// Set the umask only in the verification child, matching an agent's
		// conventional shell without changing the worker's private writes.
		// Pass the command as an argument so the wrapper never interpolates it.
		Args:   []string{"-c", `umask 022 && exec "$0" "$@"`, "/bin/sh", "-c", command},
		Limits: ProcessLimitsFromContext(ctx),
	})
	completed := f.now()
	report := VerificationReport{
		Command: command, ExitCode: result.ExitCode, Output: result.Output,
		StartedAt: started, CompletedAt: completed,
	}
	if err == nil {
		return report, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return VerificationReport{}, ctxErr
	}
	var exitError *ProcessExitError
	if errors.As(err, &exitError) {
		report.ExitCode = exitError.ExitCode
		return report, nil
	}
	return VerificationReport{}, err
}

func (f AttemptFinalizer) processRunner() ProcessRunner {
	if f.Processes != nil {
		return f.Processes
	}
	return SystemdScopeRunner{}
}

func (f AttemptFinalizer) now() time.Time {
	if f.Now != nil {
		return f.Now().UTC()
	}
	return time.Now().UTC()
}

func (f AttemptFinalizer) newID(kind string) string {
	if f.NewID != nil {
		return f.NewID(kind)
	}
	return kind + "-" + uuid.NewString()
}

// MissingOutputFailure is the failure of an attempt whose turn ended without
// the declared outputs. The commonest cause in the field was a task that
// started its work in the background and ended its turn to wait for it, which
// completes the task (S12), so the failure says that and names the supported
// way to wait.
func MissingOutputFailure(names []string) string {
	return "missing declared output: " + strings.Join(names, ", ") +
		" (the turn ended before it was written; ending the turn completes the task, so wait for background work first, or park with t3-steward wait add --task current)"
}

// MaterializeDependencies copies the selected immutable output artifacts into
// .t3/dependencies/<producer>/ and verifies their size and checksum before publish.
func MaterializeDependencies(workspaceDir, storageRoot, workflowRunID string, task domain.Task, tasks []domain.Task, artifacts []domain.Artifact) ([]string, error) {
	if len(task.DependencyInputs) == 0 && len(task.CarriedInputs) == 0 {
		return nil, nil
	}
	if workspaceDir == "" || storageRoot == "" {
		return nil, errors.New("materialize dependencies: workspace and storage roots are required")
	}
	if !safePathComponent(workflowRunID) {
		return nil, fmt.Errorf("materialize dependencies: workflow run ID %q is not a safe storage component", workflowRunID)
	}
	taskByName := make(map[string]domain.Task, len(tasks))
	for _, candidate := range tasks {
		if _, exists := taskByName[candidate.Name]; exists {
			return nil, fmt.Errorf("materialize dependencies: duplicate task name %q", candidate.Name)
		}
		taskByName[candidate.Name] = candidate
	}
	type artifactKey struct {
		taskID string
		name   string
	}
	artifactByKey := make(map[artifactKey]domain.Artifact, len(artifacts))
	for _, artifact := range artifacts {
		if artifact.Kind != domain.ArtifactOutput && !(artifact.Kind == domain.ArtifactGate && (artifact.Name == "gate" || artifact.Name == "gate/log.txt")) {
			continue
		}
		key := artifactKey{taskID: artifact.TaskID, name: artifact.Name}
		if _, exists := artifactByKey[key]; exists {
			return nil, fmt.Errorf("materialize dependencies: duplicate output %q for task %q", artifact.Name, artifact.TaskID)
		}
		artifactByKey[key] = artifact
	}

	type selectedArtifact struct {
		producer         string
		artifact         domain.Artifact
		source           string
		materializedPath string
	}
	var selected []selectedArtifact
	directDependencies := make(map[string]struct{}, len(task.Needs))
	for _, dependency := range task.Needs {
		directDependencies[dependency] = struct{}{}
	}
	producers := make([]string, 0, len(task.DependencyInputs))
	for producer := range task.DependencyInputs {
		if _, direct := directDependencies[producer]; !direct {
			return nil, fmt.Errorf("materialize dependencies: producer %q is not a direct dependency", producer)
		}
		producers = append(producers, producer)
	}
	sort.Strings(producers)
	for _, producer := range producers {
		producerTask, exists := taskByName[producer]
		if !exists {
			return nil, fmt.Errorf("materialize dependencies: missing producer task %q", producer)
		}
		if !safePathComponent(producer) {
			return nil, fmt.Errorf("materialize dependencies: producer %q is not a safe path component", producer)
		}
		names := append([]string(nil), task.DependencyInputs[producer]...)
		sort.Strings(names)
		for _, name := range names {
			if err := validateRelativePath(name, false); err != nil {
				return nil, fmt.Errorf("materialize dependencies: artifact %q: %w", name, err)
			}
			artifact, exists := artifactByKey[artifactKey{taskID: producerTask.ID, name: filepath.ToSlash(name)}]
			if !exists {
				if task.ReviewJudge {
					continue
				}
				return nil, fmt.Errorf("materialize dependencies: missing output %q from %q", name, producer)
			}
			if artifact.WorkflowRunID != workflowRunID {
				return nil, fmt.Errorf("materialize dependencies: output %q from %q belongs to run %q, want %q", name, producer, artifact.WorkflowRunID, workflowRunID)
			}
			resolved, err := safeBundleFile(storageRoot, filepath.FromSlash(artifact.StoragePath))
			if err != nil {
				return nil, fmt.Errorf("materialize dependencies: open output %q from %q: %w", name, producer, err)
			}
			relative, err := filepath.Rel(storageRoot, resolved)
			if err != nil {
				return nil, fmt.Errorf("materialize dependencies: output %q from %q: %w", name, producer, err)
			}
			selected = append(selected, selectedArtifact{producer: producer, artifact: artifact, source: relative})
		}
	}
	// Carried inputs come from the source run of a rerun. Their producer is
	// not a task here, so they are resolved by artifact ID; everything after
	// this point treats them exactly like any other dependency file.
	carried := append([]domain.CarriedInput(nil), task.CarriedInputs...)
	sort.Slice(carried, func(i, j int) bool {
		if carried[i].Producer != carried[j].Producer {
			return carried[i].Producer < carried[j].Producer
		}
		return carried[i].Name < carried[j].Name
	})
	artifactByID := make(map[string]domain.Artifact, len(artifacts))
	for _, artifact := range artifacts {
		artifactByID[artifact.ID] = artifact
	}
	for _, item := range carried {
		if !safePathComponent(item.Producer) {
			return nil, fmt.Errorf("materialize dependencies: carried producer %q is not a safe path component", item.Producer)
		}
		if err := validateRelativePath(item.Name, false); err != nil {
			return nil, fmt.Errorf("materialize dependencies: carried artifact %q: %w", item.Name, err)
		}
		artifact, exists := artifactByID[item.ArtifactID]
		if !exists {
			return nil, fmt.Errorf("materialize dependencies: missing carried output %q from %q", item.Name, item.Producer)
		}
		if artifact.WorkflowRunID != workflowRunID {
			return nil, fmt.Errorf("materialize dependencies: carried output %q from %q belongs to run %q, want %q", item.Name, item.Producer, artifact.WorkflowRunID, workflowRunID)
		}
		resolved, err := safeBundleFile(storageRoot, filepath.FromSlash(artifact.StoragePath))
		if err != nil {
			return nil, fmt.Errorf("materialize dependencies: open carried output %q from %q: %w", item.Name, item.Producer, err)
		}
		relative, err := filepath.Rel(storageRoot, resolved)
		if err != nil {
			return nil, fmt.Errorf("materialize dependencies: carried output %q from %q: %w", item.Name, item.Producer, err)
		}
		selected = append(selected, selectedArtifact{producer: item.Producer, artifact: artifact, source: relative, materializedPath: gateCarriedDependencyPath(item)})
	}
	if len(selected) == 0 {
		return nil, nil
	}

	storage, err := os.OpenRoot(storageRoot)
	if err != nil {
		return nil, fmt.Errorf("materialize dependencies: open storage: %w", err)
	}
	defer storage.Close()
	t3Dir := filepath.Join(workspaceDir, ".t3")
	if info, statErr := os.Lstat(t3Dir); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return nil, errors.New("materialize dependencies: .t3 is not a real directory")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("materialize dependencies: inspect task metadata directory: %w", statErr)
	} else if err := os.Mkdir(t3Dir, 0o700); err != nil {
		return nil, fmt.Errorf("materialize dependencies: create task metadata directory: %w", err)
	}
	stageDir, err := os.MkdirTemp(t3Dir, ".dependencies-")
	if err != nil {
		return nil, fmt.Errorf("materialize dependencies: create staging directory: %w", err)
	}
	keepStage := false
	defer func() {
		if !keepStage {
			removeIngestedTree(stageDir)
		}
	}()

	materialized := make([]string, 0, len(selected))
	for _, item := range selected {
		materializedName := item.artifact.Name
		if item.materializedPath != "" {
			materializedName = item.materializedPath
		}
		if item.artifact.Kind == domain.ArtifactGate && materializedName == "gate" {
			materializedName = "gate/report.json"
		}
		destination := filepath.Join(stageDir, item.producer, filepath.FromSlash(materializedName))
		file, err := copyIngestedFile(storage, item.source, destination, item.artifact.Name, "")
		if err != nil {
			return nil, fmt.Errorf("materialize dependencies: copy %q from %q: %w", item.artifact.Name, item.producer, err)
		}
		if file.size != item.artifact.Size || !strings.EqualFold(file.sha256, item.artifact.SHA256) {
			return nil, fmt.Errorf("materialize dependencies: checksum mismatch for %q from %q", item.artifact.Name, item.producer)
		}
		materialized = append(materialized, filepath.ToSlash(filepath.Join(".t3", "dependencies", item.producer, materializedName)))
	}
	if err := makeIngestedTreeImmutable(stageDir); err != nil {
		return nil, fmt.Errorf("materialize dependencies: protect staged files: %w", err)
	}
	finalDir := filepath.Join(t3Dir, "dependencies")
	if err := os.Rename(stageDir, finalDir); err != nil {
		return nil, fmt.Errorf("materialize dependencies: publish files: %w", err)
	}
	keepStage = true
	return materialized, nil
}

func safePathComponent(value string) bool {
	return value != "" && value != "." && value != ".." &&
		filepath.Base(value) == value && !strings.ContainsAny(value, `/\\`)
}
