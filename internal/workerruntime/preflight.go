package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// DefaultPreflightFreshness bounds how long a receipt may stand in for a rerun
// of the same identity. A retry minutes later reuses an unchanged baseline; a
// day-old one is re-established.
const DefaultPreflightFreshness = 24 * time.Hour

// preflightArtifactRecord binds one step's full output to the reference its
// receipt names, so a reference fact points at bytes that exist.
type preflightArtifactRecord struct {
	Reference string `json:"reference"`
	StepID    string `json:"stepId"`
	Name      string `json:"name"`
	SHA256    string `json:"sha256"`
	File      string `json:"file"`
	Size      int64  `json:"size"`
}

// preflightState is the durable, task-scoped preflight record. It outlives one
// attempt on purpose: a retry of the same task reuses evidence whose identity
// still matches instead of re-establishing an unchanged baseline.
type preflightState struct {
	UpdatedAt time.Time                 `json:"updatedAt"`
	Receipts  []domain.PreflightReceipt `json:"receipts"`
	Artifacts []preflightArtifactRecord `json:"artifacts"`
}

// capturingPreflightRunner keeps the unbounded output of every process the
// engine ran. The receipt carries a bounded excerpt; custody needs the whole
// thing, and capturing it at the runner boundary is the only place it exists.
type capturingPreflightRunner struct {
	inner   backlog.PreflightRunner
	outputs map[string]string
}

func (r *capturingPreflightRunner) Run(ctx context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	result, err := r.inner.Run(ctx, request)
	if r.outputs == nil {
		r.outputs = make(map[string]string)
	}
	r.outputs[request.ID] += result.Output
	return result, err
}

// capturedFor joins every capture belonging to one step. A command step runs
// one process; a probe may run several, and their order is made deterministic
// by sorting the process identities.
func (r *capturingPreflightRunner) capturedFor(attemptID, stepID string) string {
	commandKey := fmt.Sprintf("preflight-%s-%s", attemptID, stepID)
	probePrefix := fmt.Sprintf("probe-%s-", stepID)
	keys := make([]string, 0, len(r.outputs))
	for key := range r.outputs {
		if key == commandKey || strings.HasPrefix(key, probePrefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var builder strings.Builder
	for _, key := range keys {
		builder.WriteString(r.outputs[key])
	}
	return builder.String()
}

func (d *LocalDriver) preflightDir(pkg workerproto.ExecutionPackage) string {
	return filepath.Join(d.Config.RunsRoot, pkg.Identity.WorkflowRunID, pkg.Identity.TaskID, "preflight")
}

func (d *LocalDriver) preflightFreshness() time.Duration {
	if d.Config.PreflightFreshness > 0 {
		return d.Config.PreflightFreshness
	}
	return DefaultPreflightFreshness
}

func (d *LocalDriver) preflightRunner() backlog.PreflightRunner {
	if d.Preflight != nil {
		return d.Preflight
	}
	return nil
}

func (d *LocalDriver) loadPreflightState(pkg workerproto.ExecutionPackage) (preflightState, error) {
	var state preflightState
	raw, err := readBoundedRegularFile(filepath.Join(d.preflightDir(pkg), "state.json"), 8<<20)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return preflightState{}, fmt.Errorf("read preflight state: %w", err)
	}
	return state, nil
}

func (d *LocalDriver) savePreflightState(pkg workerproto.ExecutionPackage, state preflightState) error {
	directory := d.preflightDir(pkg)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	state.UpdatedAt = d.Now().UTC()
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	stage, err := os.CreateTemp(directory, ".state-*")
	if err != nil {
		return err
	}
	stagePath := stage.Name()
	defer os.Remove(stagePath)
	if _, err = stage.Write(raw); err != nil {
		stage.Close()
		return err
	}
	if err = stage.Close(); err != nil {
		return err
	}
	return os.Rename(stagePath, filepath.Join(directory, "state.json"))
}

// preflightEnvironmentDigest addresses the environment an attempt executes in.
// The source revision is deliberately excluded: it is a separate axis of the
// receipt identity, and folding it in here would hide which one changed.
func preflightEnvironmentDigest(pkg workerproto.ExecutionPackage) string {
	fields := []string{
		pkg.Environment.CatalogRevision, pkg.Environment.Type, pkg.Environment.Project,
		pkg.Environment.Repository, pkg.Environment.Scope, pkg.Environment.SetupProfile,
	}
	var builder strings.Builder
	for _, field := range fields {
		fmt.Fprintf(&builder, "%d:%s\n", len(field), field)
	}
	sum := sha256.Sum256([]byte(builder.String()))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// runPreflight executes the declared steps and returns the initial prompt.
//
// It is called after the workspace is prepared and verified inputs are
// materialized, and strictly before the provider session is created. A package
// that declares no steps returns the prompt artifact unchanged, so a task that
// asks for no preflight launches exactly as it did before.
//
// A blocking outcome returns an error, and the caller returns before creating
// the session: the session is never created rather than created and stopped.
func (d *LocalDriver) runPreflight(ctx context.Context, pkg workerproto.ExecutionPackage, workspace, prompt string) (string, error) {
	steps := backlog.PreflightStepsFromPackage(pkg.Preflight)
	if len(steps) == 0 {
		return prompt, nil
	}
	runner := &capturingPreflightRunner{inner: d.preflightRunner()}
	state, err := d.loadPreflightState(pkg)
	if err != nil {
		return "", err
	}
	engine := backlog.PreflightEngine{Runner: runner, Now: func() time.Time { return d.Now().UTC() }}
	report, err := engine.Run(ctx, backlog.PreflightRequest{
		TaskID:            pkg.Identity.TaskID,
		AttemptID:         pkg.Identity.AttemptID,
		WorkspaceDir:      workspace,
		EnvironmentDigest: preflightEnvironmentDigest(pkg),
		SourceRevision:    pkg.Environment.Ref,
		WorkerID:          pkg.WorkerID,
		InputDigests:      preflightInputDigests(pkg),
		Steps:             steps,
		Cached:            state.Receipts,
		Freshness:         d.preflightFreshness(),
	})
	if err != nil {
		return "", fmt.Errorf("preflight: %w", err)
	}
	// Custody first: evidence that cannot be preserved must not become a launch.
	if err := d.custodyPreflight(pkg, report, runner, &state); err != nil {
		return "", fmt.Errorf("preflight custody: %w", err)
	}
	if !report.MayLaunch() {
		return "", fmt.Errorf("preflight blocked the provider session (%s): %s", report.Block, report.BlockReason)
	}
	envelope, err := backlog.BuildPromptEnvelope(backlog.PromptRequest{
		TaskID:          pkg.Identity.TaskID,
		Objective:       prompt,
		Inputs:          promptInputsFor(pkg),
		RequiredOutputs: promptOutputsFor(pkg),
		Report:          report,
	})
	if err != nil {
		return "", fmt.Errorf("preflight prompt: %w", err)
	}
	return envelope.Render(), nil
}

// custodyPreflight stores every step's full output and persists the receipts
// that a later attempt may reuse.
func (d *LocalDriver) custodyPreflight(
	pkg workerproto.ExecutionPackage,
	report backlog.PreflightReport,
	runner *capturingPreflightRunner,
	state *preflightState,
) error {
	directory := d.preflightDir(pkg)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	for _, receipt := range report.Receipts {
		stepID := receipt.Identity.StepID
		reference := backlog.PreflightReference(stepID, receipt.IdentityDigest)
		output := runner.capturedFor(pkg.Identity.AttemptID, stepID)
		if output == "" {
			// A native probe runs no process, and reused evidence ran none this
			// time; the receipt excerpt is then the whole of what exists.
			output = receipt.Stdout
		}
		name := "preflight/" + stepID + ".log"
		file := receipt.IdentityDigest + ".log"
		sum := sha256.Sum256([]byte(output))
		if err := writePreflightFile(filepath.Join(directory, file), []byte(output)); err != nil {
			return err
		}
		record := preflightArtifactRecord{
			Reference: reference, StepID: stepID, Name: name,
			SHA256: hex.EncodeToString(sum[:]), File: file, Size: int64(len(output)),
		}
		state.Artifacts = replacePreflightArtifact(state.Artifacts, record)
		state.Receipts = replacePreflightReceipt(state.Receipts, receipt)
	}
	return d.savePreflightState(pkg, *state)
}

func replacePreflightReceipt(receipts []domain.PreflightReceipt, receipt domain.PreflightReceipt) []domain.PreflightReceipt {
	for index, existing := range receipts {
		if existing.IdentityDigest == receipt.IdentityDigest {
			receipts[index] = receipt
			return receipts
		}
	}
	return append(receipts, receipt)
}

func replacePreflightArtifact(records []preflightArtifactRecord, record preflightArtifactRecord) []preflightArtifactRecord {
	for index, existing := range records {
		if existing.Reference == record.Reference {
			records[index] = record
			return records
		}
	}
	return append(records, record)
}

// PreflightArtifact resolves the reference a receipt or a reference fact names
// into its stored artifact and the file that holds its bytes.
func (d *LocalDriver) PreflightArtifact(pkg workerproto.ExecutionPackage, reference string) (domain.Artifact, string, error) {
	state, err := d.loadPreflightState(pkg)
	if err != nil {
		return domain.Artifact{}, "", err
	}
	for _, record := range state.Artifacts {
		if record.Reference != reference {
			continue
		}
		return domain.Artifact{
			ID:            "preflight-" + record.SHA256[:16],
			WorkflowRunID: pkg.Identity.WorkflowRunID,
			TaskID:        pkg.Identity.TaskID,
			AttemptID:     pkg.Identity.AttemptID,
			Kind:          domain.ArtifactLog,
			Name:          record.Name,
			MediaType:     "text/plain; charset=utf-8",
			Size:          record.Size,
			SHA256:        record.SHA256,
			StoragePath:   filepath.ToSlash(filepath.Join("runs", pkg.Identity.WorkflowRunID, pkg.Identity.TaskID, "preflight", record.File)),
			Producer:      "preflight",
			CreatedAt:     state.UpdatedAt,
		}, filepath.Join(d.preflightDir(pkg), record.File), nil
	}
	return domain.Artifact{}, "", fmt.Errorf("preflight reference %q has no stored output", reference)
}

// publishPreflightArtifacts copies the retained preflight logs into the
// finalized attempt capture so that they travel to the coordinator through the
// existing result publication path rather than a second one.
func (d *LocalDriver) publishPreflightArtifacts(pkg workerproto.ExecutionPackage, finalized *backlog.FinalizedAttempt) error {
	state, err := d.loadPreflightState(pkg)
	if err != nil || len(state.Artifacts) == 0 {
		return err
	}
	if finalized.StorageDir == "" {
		directory := filepath.Join(d.Config.ArtifactRoot, "runs", pkg.Identity.WorkflowRunID, pkg.Identity.TaskID, pkg.Identity.AttemptID)
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return err
		}
		finalized.StorageDir = directory
	}
	for _, record := range state.Artifacts {
		raw, err := readBoundedRegularFile(filepath.Join(d.preflightDir(pkg), record.File), pkg.Limits.MaxArtifactBytes)
		if err != nil {
			return err
		}
		destination := filepath.Join(finalized.StorageDir, "artifacts", filepath.FromSlash(record.Name))
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			return err
		}
		if err := writePreflightFile(destination, raw); err != nil {
			return err
		}
		artifact, _, err := d.PreflightArtifact(pkg, record.Reference)
		if err != nil {
			return err
		}
		artifact.StoragePath = filepath.ToSlash(filepath.Join("runs", pkg.Identity.WorkflowRunID, pkg.Identity.TaskID, pkg.Identity.AttemptID, "artifacts", record.Name))
		finalized.Artifacts = append(finalized.Artifacts, artifact)
	}
	return nil
}

// writePreflightFile publishes bytes through a staged rename with private file
// permissions, so a partially written log never looks like custody.
func writePreflightFile(path string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	stage, err := os.CreateTemp(filepath.Dir(path), ".preflight-*")
	if err != nil {
		return err
	}
	stagePath := stage.Name()
	defer os.Remove(stagePath)
	if err = stage.Chmod(0o600); err != nil {
		stage.Close()
		return err
	}
	if _, err = stage.Write(raw); err != nil {
		stage.Close()
		return err
	}
	if err = stage.Close(); err != nil {
		return err
	}
	return os.Rename(stagePath, path)
}

func promptInputsFor(pkg workerproto.ExecutionPackage) []domain.PromptInput {
	inputs := make([]domain.PromptInput, 0, len(pkg.StaticInputs))
	for _, object := range pkg.StaticInputs {
		inputs = append(inputs, domain.PromptInput{Name: object.Path, Location: object.Path, Digest: object.SHA256})
	}
	for _, dependency := range pkg.Dependencies {
		for _, object := range dependency.Artifacts {
			inputs = append(inputs, domain.PromptInput{Name: object.Path, Location: object.Path, Digest: object.SHA256})
		}
	}
	return inputs
}

func promptOutputsFor(pkg workerproto.ExecutionPackage) []string {
	outputs := make([]string, 0, len(pkg.Outputs))
	for _, output := range pkg.Outputs {
		outputs = append(outputs, output.Name)
	}
	return outputs
}

func preflightInputDigests(pkg workerproto.ExecutionPackage) map[string]string {
	digests := map[string]string{"prompt": pkg.Prompt.SHA256}
	for _, object := range pkg.StaticInputs {
		digests[object.Path] = object.SHA256
	}
	for _, dependency := range pkg.Dependencies {
		for _, object := range dependency.Artifacts {
			digests[object.Path] = object.SHA256
		}
	}
	return digests
}
