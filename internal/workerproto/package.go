package workerproto

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

const ExecutionPackageVersion = 1

var identityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$`)

type EnvironmentReference struct {
	DirectoryBindings   []directoryresource.Binding `json:"directoryBindings,omitempty"`
	Type                string                      `json:"type,omitempty"`
	CatalogRevision     string                      `json:"catalogRevision"`
	Project             string                      `json:"project"`
	Repository          string                      `json:"repository"`
	Ref                 string                      `json:"ref"`
	Scope               string                      `json:"scope"`
	SetupProfile        string                      `json:"setupProfile"`
	T3Project           string                      `json:"t3Project"`
	ResourceLocks       []string                    `json:"resourceLocks,omitempty"`
	RequiredCredentials []string                    `json:"requiredCredentials,omitempty"`
}

type ExecutionLimits struct {
	MaxTurns            int           `json:"maxTurns"`
	PrepareTimeout      time.Duration `json:"prepareTimeout"`
	VerificationTimeout time.Duration `json:"verificationTimeout"`
	MaxArtifactBytes    int64         `json:"maxArtifactBytes"`
	MaxTotalBytes       int64         `json:"maxTotalBytes"`
}

type ExecutionIdentity struct {
	WorkflowID    string `json:"workflowId"`
	WorkflowRunID string `json:"workflowRunId"`
	TaskID        string `json:"taskId"`
	AttemptID     string `json:"attemptId"`
	// AttemptRevision is the revision the attempt held when this package was
	// built. It travels with the rest of the identity so a task can report what
	// it was told, and it is recorded on a task-bound wait as evidence.
	//
	// It is not a fence and cannot be one: the coordinator advances the attempt
	// after building this package, at the assignment claim and again when the
	// worker reports the thread running, so this number is already behind by the
	// time the turn starts. The registration resolves the live revision itself;
	// see sqlite.RegisterTaskWait.
	AttemptRevision int64  `json:"attemptRevision,omitempty"`
	AssignmentID    string `json:"assignmentId"`
	AssignmentEpoch int64  `json:"assignmentEpoch"`
	DispatchToken   string `json:"dispatchToken"`
	ThreadID        string `json:"threadId"`
}

// TaskEnvironment is the execution identity as the six environment variables a
// task process is given.
//
// The dispatch token is deliberately absent. It authorizes task effects, and
// nothing inside the agent process needs it: the agent names itself, and the
// coordinator decides what that name is allowed to do.
func (i ExecutionIdentity) TaskEnvironment() map[string]string {
	return map[string]string{
		domain.TaskWaitEnvWorkflowRunID:   i.WorkflowRunID,
		domain.TaskWaitEnvTaskID:          i.TaskID,
		domain.TaskWaitEnvAttemptID:       i.AttemptID,
		domain.TaskWaitEnvAttemptRevision: strconv.FormatInt(i.AttemptRevision, 10),
		domain.TaskWaitEnvAssignmentID:    i.AssignmentID,
		domain.TaskWaitEnvThreadID:        i.ThreadID,
	}
}

type DependencyProvenance struct {
	RunID           string            `json:"runId"`
	TaskID          string            `json:"taskId"`
	AttemptID       string            `json:"attemptId"`
	SourceArtifacts map[string]string `json:"sourceArtifacts"`
}

type DependencyInput struct {
	TaskID     string                `json:"taskId"`
	Provenance *DependencyProvenance `json:"provenance,omitempty"`
	Artifacts  []ArtifactObject      `json:"artifacts"`
	// AcceptedCommits names the producer's declared commit outputs, among
	// these artifacts, whose result the coordinator's review gate accepted.
	// Only such a commit reference may make a commit the producer staged its
	// campaign output: a review judge may also be given a rejected result to
	// inspect, and any other file in the producer's outputs is the executor's
	// content. It is set only for review-declared producers and requires the
	// accepted-dependencies capability.
	AcceptedCommits []string `json:"acceptedCommits,omitempty"`
	// CommitOutputs names the producer's declared commit outputs among these
	// artifacts, by their name in the producer's directory. In a package that
	// declares PackageCapabilityCommitOutputs, these files, and only these,
	// are commit references the worker resolves; every other file is the
	// executor's content, even when it parses as a commit record. A package
	// without that capability comes from a coordinator that does not mark
	// them, and the worker then takes any artifact as a candidate.
	CommitOutputs []string `json:"commitOutputs,omitempty"`
}

// CommitBundleInput is the retained bundle of one declared commit a dependency
// produced on another worker. It is delivered with the other inputs and is
// read only when the consuming worker's own store lacks the commit.
//
// Exactly one of Bundle and Omitted is set. Omitted is the coordinator's reason
// for not delivering a bundle it retains, such as the package's total byte
// limit; the consuming worker refuses the commit with it, and only when its own
// store does not already hold the commit.
type CommitBundleInput struct {
	// WorkflowRunID and TaskID name the run and task that produced the commit,
	// which for an input carried from another run are that source run's, and
	// Name is the declared commit.
	WorkflowRunID string          `json:"workflowRunId"`
	TaskID        string          `json:"taskId"`
	Name          string          `json:"name"`
	Bundle        *ArtifactObject `json:"bundle,omitempty"`
	Omitted       string          `json:"omitted,omitempty"`
}

// maxCommitBundleOmission bounds the reason a package gives for a bundle it
// does not carry.
const maxCommitBundleOmission = 1024

// CommitBundlePath is where a package places the bundle of one declared
// commit. It is outside dependencies/, so it never reaches the task's view.
func CommitBundlePath(runID, taskID, name string) string {
	return "commit-bundles/" + runID + "/" + taskID + "/" + name + ".bundle"
}

// Package capabilities name behaviour a worker must implement to execute a
// package faithfully. A package that declares one is only understood by a build
// that supports it; see ValidateExecutionPackage.
const (
	PackageCapabilityWorkerOwnedGate     = "worker-owned-gate-v1"
	PackageCapabilityPreflight           = "preflight"
	PackageCapabilitySupervisionEvidence = "supervision-evidence-v1"
	PackageCapabilityRecoveryRetry       = "recovery-retry-v1"
	PackageCapabilityRecoverySupplement  = "recovery-supplement-v1"
	PackageCapabilityProjectContext      = "project-context-v1"
	// PackageCapabilityCommitBundle is the transport of a declared campaign
	// commit between workers. A producing worker that has it retains a bundle
	// of each declared commit, and a consuming worker that has it imports one
	// delivered in CommitBundles into its own campaign ref store.
	PackageCapabilityCommitBundle = "campaign-commit-bundle-v1"
	// PackageCapabilityWorkspaceHead asks the worker to report the workspace's
	// physical HEAD and tracked changes when it collects the turn. A
	// review-declared task requires it, because the coordinator's completion
	// gate compares that HEAD with the head its latest review round accepted.
	PackageCapabilityWorkspaceHead = "workspace-head-v1"
	// PackageCapabilityAcceptedDependencies asks the worker to publish a
	// review-declared producer's staged commit only for a dependency the
	// package marks as accepted.
	PackageCapabilityAcceptedDependencies = "accepted-dependencies-v1"
	// PackageCapabilityContinuationCheckpoint is the continuation.md
	// checkpoint contract. A package that declares it tells the worker that
	// this coordinator accepts the attempt's snapshots as checkpoint uploads
	// while it runs and the latest one in its result, and it may carry the
	// snapshot an earlier attempt of the task left.
	PackageCapabilityContinuationCheckpoint = "continuation-checkpoint-v1"
	// PackageCapabilityCommitOutputs says the package names every
	// dependency's declared commit outputs in DependencyInput.CommitOutputs,
	// so the worker resolves a commit record only from one of those files.
	// The coordinator declares it for every package with dependencies, even
	// when none of them is a commit, to a worker it froze the continuation
	// checkpoint decision for; both arrived in the same release.
	PackageCapabilityCommitOutputs = "dependency-commit-outputs-v1"
)

// ContinuationInputPath is where a package places the previous attempt's
// continuation snapshot. Static inputs are materialized under the workspace's
// .t3/inputs/, outside the task's tree.
const ContinuationInputPath = "inputs/continuation/previous.md"

// ContinuationInput describes the snapshot a package carries at
// ContinuationInputPath: which attempt left it, when, and how large it is.
type ContinuationInput struct {
	Path       string    `json:"path"`
	AttemptID  string    `json:"attemptId"`
	Size       int64     `json:"size"`
	CapturedAt time.Time `json:"capturedAt"`
}

// SupportedPackageCapabilities is what this build implements. A package that
// requires anything else is refused by name instead of being run without the
// evidence it promised to produce.
func SupportedPackageCapabilities() []string {
	return []string{PackageCapabilityWorkerOwnedGate, PackageCapabilityPreflight, PackageCapabilitySupervisionEvidence, PackageCapabilityRecoveryRetry, PackageCapabilityRecoverySupplement, PackageCapabilityProjectContext, PackageCapabilitySessionDisplay, PackageCapabilityCommitBundle, PackageCapabilityWorkspaceHead, PackageCapabilityAcceptedDependencies, PackageCapabilityContinuationCheckpoint, PackageCapabilityCommitOutputs}
}

// MarksCommitOutputs reports whether the package names its dependencies'
// declared commit outputs, which makes every other dependency file ordinary.
func (pkg ExecutionPackage) MarksCommitOutputs() bool {
	return slices.Contains(pkg.RequiredCapabilities, PackageCapabilityCommitOutputs)
}

// DependencyCommitOutputs names the files of a dependency the worker may
// resolve as commit references: the declared commit outputs the package
// marks, or, from a coordinator that does not mark them, every artifact.
func (pkg ExecutionPackage) DependencyCommitOutputs(dependency DependencyInput) []string {
	if pkg.MarksCommitOutputs() {
		return append([]string(nil), dependency.CommitOutputs...)
	}
	names := make([]string, 0, len(dependency.Artifacts))
	for _, artifact := range dependency.Artifacts {
		if parts := strings.SplitN(artifact.Path, "/", 3); len(parts) == 3 && parts[0] == "dependencies" {
			names = append(names, parts[2])
		}
	}
	return names
}

// RequiresWorkspaceHead reports whether the worker must report the workspace's
// physical HEAD with this package's result.
func (pkg ExecutionPackage) RequiresWorkspaceHead() bool {
	return slices.Contains(pkg.RequiredCapabilities, PackageCapabilityWorkspaceHead)
}

// HasAcceptedDependencies reports whether any dependency names an accepted
// commit output of a review-declared producer.
func (pkg ExecutionPackage) HasAcceptedDependencies() bool {
	return slices.ContainsFunc(pkg.Dependencies, func(dependency DependencyInput) bool { return len(dependency.AcceptedCommits) != 0 })
}

// PreflightStep is one declared step the worker runs after the workspace is
// prepared and strictly before the provider session is created.
//
// The declaration is durable task state, so the type belongs to the domain and
// this is an alias rather than a copy. A task carries its steps, the execution
// package carries them to the worker, and both name the same struct: a third
// spelling of nine fields would only create somewhere for them to drift.
type PreflightStep = domain.PreflightStep

type RecoveryExecutionContext struct {
	IncidentID      string   `json:"incidentId"`
	InstructionPath string   `json:"instructionPath"`
	CheckpointPaths []string `json:"checkpointPaths,omitempty"`
}

type ExecutionPackage struct {
	Display          *SessionDisplay   `json:"display,omitempty"`
	Timeout          time.Duration     `json:"timeout,omitempty"`
	GraphRevision    int64             `json:"graphRevision,omitempty"`
	TaskRevision     int64             `json:"taskRevision,omitempty"`
	TaskDigest       string            `json:"taskDigest,omitempty"`
	Version          int               `json:"version"`
	ID               string            `json:"id"`
	CoordinatorID    string            `json:"coordinatorId"`
	CoordinatorEpoch int64             `json:"coordinatorEpoch"`
	WorkerID         string            `json:"workerId"`
	WorkerEpoch      string            `json:"workerEpoch"`
	Identity         ExecutionIdentity `json:"identity"`
	Class            domain.TaskClass  `json:"class"`
	Prompt           ArtifactObject    `json:"prompt"`
	StaticInputs     []ArtifactObject  `json:"staticInputs,omitempty"`
	Dependencies     []DependencyInput `json:"dependencies,omitempty"`
	// CommitBundles requires PackageCapabilityCommitBundle.
	CommitBundles []CommitBundleInput    `json:"commitBundles,omitempty"`
	Context       *domain.ProjectContext `json:"context,omitempty"`
	Route         domain.ProviderRoute   `json:"route"`
	Environment   EnvironmentReference   `json:"environment"`
	Verification  []string               `json:"verification,omitempty"`
	Gate          *domain.TaskGate       `json:"gate,omitempty"`
	Preflight     []PreflightStep        `json:"preflight,omitempty"`
	// RequiredCapabilities names what a worker must implement to run this
	// package. The manifest content address already stops an older build from
	// silently dropping a field it cannot decode; this list makes the refusal
	// explicit and nameable.
	RequiredCapabilities []string `json:"requiredCapabilities,omitempty"`
	// Supervision makes this package an overseer activation rather than a
	// declared task. It is nil for every task package, which is every package
	// an unsupervised run produces. See SupervisionActivation.
	Supervision *SupervisionActivation    `json:"supervision,omitempty"`
	Recovery    *RecoveryExecutionContext `json:"recovery,omitempty"`
	// Continuation requires PackageCapabilityContinuationCheckpoint.
	Continuation *ContinuationInput           `json:"continuation,omitempty"`
	Outputs      []domain.ArtifactDeclaration `json:"outputs,omitempty"`
	NotBefore    *time.Time                   `json:"notBefore,omitempty"`
	Deadline     *time.Time                   `json:"deadline,omitempty"`
	ExpiresAt    *time.Time                   `json:"expiresAt,omitempty"`
	Limits       ExecutionLimits              `json:"limits"`
	CreatedAt    time.Time                    `json:"createdAt"`
}

type ExecutionPackageManifest struct {
	MediaType string           `json:"mediaType"`
	Size      int64            `json:"size"`
	SHA256    string           `json:"sha256"`
	Package   ExecutionPackage `json:"package"`
}

func BuildExecutionPackageManifest(pkg ExecutionPackage) (ExecutionPackageManifest, error) {
	if err := ValidateExecutionPackage(pkg); err != nil {
		return ExecutionPackageManifest{}, err
	}
	data, err := json.Marshal(pkg)
	if err != nil {
		return ExecutionPackageManifest{}, fmt.Errorf("execution package: encode: %w", err)
	}
	sum := sha256.Sum256(data)
	return ExecutionPackageManifest{
		MediaType: "application/vnd.t3-steward.execution-package.v1+json",
		Size:      int64(len(data)), SHA256: hex.EncodeToString(sum[:]), Package: pkg,
	}, nil
}

func ValidateExecutionPackageManifest(manifest ExecutionPackageManifest, maxBytes int64) error {
	if manifest.MediaType != "application/vnd.t3-steward.execution-package.v1+json" {
		return errors.New("execution package manifest: unsupported media type")
	}
	if maxBytes <= 0 || manifest.Size <= 0 || manifest.Size > maxBytes {
		return errors.New("execution package manifest: invalid or excessive size")
	}
	if !validSHA256(manifest.SHA256) {
		return errors.New("execution package manifest: invalid checksum")
	}
	if err := ValidateExecutionPackage(manifest.Package); err != nil {
		return err
	}
	data, err := json.Marshal(manifest.Package)
	if err != nil {
		return fmt.Errorf("execution package manifest: encode: %w", err)
	}
	sum := sha256.Sum256(data)
	if int64(len(data)) != manifest.Size || !strings.EqualFold(hex.EncodeToString(sum[:]), manifest.SHA256) {
		return errors.New("execution package manifest: content address mismatch")
	}
	return nil
}

func ValidateExecutionPackage(pkg ExecutionPackage) error {
	seenDirectories := map[string]bool{}
	for _, binding := range pkg.Environment.DirectoryBindings {
		if err := directoryresource.ValidateBinding(binding); err != nil {
			return fmt.Errorf("execution package directory: %w", err)
		}
		r := binding.Identity.Registration
		if r.WorkerID != pkg.WorkerID || seenDirectories[r.ResourceID] {
			return errors.New("execution package directory worker mismatch or duplicate resource")
		}
		seenDirectories[r.ResourceID] = true
	}
	if pkg.Timeout < 0 || pkg.Timeout > 7*24*time.Hour {
		return errors.New("execution timeout is out of bounds")
	}
	if pkg.Version != ExecutionPackageVersion {
		return fmt.Errorf("execution package: unsupported version %d", pkg.Version)
	}
	required := map[string]string{
		"id": pkg.ID, "coordinatorId": pkg.CoordinatorID, "workerId": pkg.WorkerID,
		"workerEpoch": pkg.WorkerEpoch, "workflowId": pkg.Identity.WorkflowID,
		"workflowRunId": pkg.Identity.WorkflowRunID, "taskId": pkg.Identity.TaskID,
		"attemptId": pkg.Identity.AttemptID, "assignmentId": pkg.Identity.AssignmentID,
		"dispatchToken": pkg.Identity.DispatchToken, "threadId": pkg.Identity.ThreadID,
		"catalogRevision": pkg.Environment.CatalogRevision, "project": pkg.Environment.Project,
		"scope": pkg.Environment.Scope, "setupProfile": pkg.Environment.SetupProfile,
	}
	for name, value := range required {
		if !identityPattern.MatchString(value) {
			return fmt.Errorf("execution package: invalid %s", name)
		}
	}
	if pkg.Environment.Type != "" && pkg.Environment.Type != "git" && pkg.Environment.Type != "fresh" {
		return errors.New("execution package: unsupported workspace type")
	}
	if pkg.Environment.Type == "fresh" && (pkg.Environment.Repository != "" || pkg.Environment.Ref != "" || pkg.Environment.Scope != "task") {
		return errors.New("execution package: fresh workspace requires task scope and no repository/ref")
	}
	for name, value := range map[string]string{
		"repository": pkg.Environment.Repository,
		"ref":        pkg.Environment.Ref,
	} {
		if pkg.Environment.Type != "fresh" && !safeOpaqueValue(value, 1024) {
			return fmt.Errorf("execution package: invalid %s", name)
		}
	}
	if pkg.Environment.T3Project != "" && !safeOpaqueValue(pkg.Environment.T3Project, 1024) {
		return errors.New("execution package: invalid t3Project")
	}
	if pkg.CoordinatorEpoch < 1 || pkg.Identity.AssignmentEpoch < 1 {
		return errors.New("execution package: coordinator and assignment epochs must be positive")
	}
	if pkg.Class != domain.TaskClassRequired && pkg.Class != domain.TaskClassSurplus {
		return errors.New("execution package: invalid task class")
	}
	if pkg.Route.ProviderInstanceID == "" || pkg.Route.Model == "" || pkg.Route.WorkerID != pkg.WorkerID || pkg.Route.QuotaPoolID == "" {
		return errors.New("execution package: route must bind worker, provider, model, and quota pool")
	}
	if pkg.CreatedAt.IsZero() || pkg.Limits.MaxTurns < 1 || pkg.Limits.PrepareTimeout <= 0 ||
		pkg.Limits.VerificationTimeout <= 0 || pkg.Limits.MaxArtifactBytes <= 0 ||
		pkg.Limits.MaxTotalBytes < pkg.Limits.MaxArtifactBytes {
		return errors.New("execution package: invalid creation time or limits")
	}
	if pkg.NotBefore != nil && pkg.Deadline != nil && pkg.NotBefore.After(*pkg.Deadline) {
		return errors.New("execution package: not-before is after deadline")
	}
	if pkg.Deadline != nil && pkg.ExpiresAt != nil && pkg.Deadline.After(*pkg.ExpiresAt) {
		return errors.New("execution package: deadline is after expiry")
	}
	paths := make(map[string]struct{})
	if err := validatePackageArtifact(pkg.Prompt, pkg.Limits.MaxArtifactBytes, paths); err != nil {
		return fmt.Errorf("execution package: prompt: %w", err)
	}
	// Every addition is guarded before it is made, as the transfer manifest
	// does: a total that wraps past MaxInt64 would pass a check made after.
	var total int64 = pkg.Prompt.Size
	add := func(size int64) error {
		if size > pkg.Limits.MaxTotalBytes-total {
			return errors.New("execution package: inputs exceed total byte limit")
		}
		total += size
		return nil
	}
	for _, artifact := range pkg.StaticInputs {
		if err := validatePackageArtifact(artifact, pkg.Limits.MaxArtifactBytes, paths); err != nil {
			return fmt.Errorf("execution package: static input: %w", err)
		}
		if err := add(artifact.Size); err != nil {
			return err
		}
	}
	dependencies := make(map[string]struct{})
	for _, dependency := range pkg.Dependencies {
		if !identityPattern.MatchString(dependency.TaskID) {
			return errors.New("execution package: invalid dependency task id")
		}
		if _, exists := dependencies[dependency.TaskID]; exists {
			return errors.New("execution package: duplicate dependency task")
		}
		dependencies[dependency.TaskID] = struct{}{}
		if err := validateAcceptedCommits(dependency); err != nil {
			return err
		}
		if err := validateCommitOutputs(dependency); err != nil {
			return err
		}
		if provenance := dependency.Provenance; provenance != nil {
			if strings.TrimSpace(provenance.RunID) == "" || strings.TrimSpace(provenance.TaskID) == "" ||
				strings.TrimSpace(provenance.AttemptID) == "" ||
				len(provenance.SourceArtifacts) != len(dependency.Artifacts) {
				return errors.New("execution package: invalid dependency provenance")
			}
			for _, artifact := range dependency.Artifacts {
				if strings.TrimSpace(provenance.SourceArtifacts[artifact.ID]) == "" {
					return errors.New("execution package: dependency provenance is missing a source artifact")
				}
			}
		}
		for _, artifact := range dependency.Artifacts {
			if err := validatePackageArtifact(artifact, pkg.Limits.MaxArtifactBytes, paths); err != nil {
				return fmt.Errorf("execution package: dependency input: %w", err)
			}
			if err := add(artifact.Size); err != nil {
				return err
			}
		}
	}
	bundles := make(map[string]struct{}, len(pkg.CommitBundles))
	for _, input := range pkg.CommitBundles {
		if !identityPattern.MatchString(input.WorkflowRunID) || !identityPattern.MatchString(input.TaskID) ||
			!safeRelativePath(input.Name) || strings.Contains(input.Name, "/") {
			return errors.New("execution package: invalid commit bundle reference")
		}
		key := input.WorkflowRunID + "\x00" + input.TaskID + "\x00" + input.Name
		if _, duplicate := bundles[key]; duplicate {
			return errors.New("execution package: duplicate commit bundle")
		}
		bundles[key] = struct{}{}
		if (input.Bundle == nil) == (input.Omitted == "") || len(input.Omitted) > maxCommitBundleOmission {
			return errors.New("execution package: a commit bundle must be either delivered or omitted with a reason")
		}
		if input.Bundle == nil {
			continue
		}
		if input.Bundle.Path != CommitBundlePath(input.WorkflowRunID, input.TaskID, input.Name) {
			return errors.New("execution package: commit bundle path does not name its commit")
		}
		if err := validatePackageArtifact(*input.Bundle, pkg.Limits.MaxArtifactBytes, paths); err != nil {
			return fmt.Errorf("execution package: commit bundle: %w", err)
		}
		if err := add(input.Bundle.Size); err != nil {
			return err
		}
	}
	// Verification is evidence, not the gate. Success is an output-contract
	// decision, so a task that declares outputs and no verification command is
	// legitimate: its outputs must still materialize for it to succeed.
	//
	// Requiring a command here made such a package unbuildable, and the failure
	// surfaced only as an offer withheld and retried forever, with the reason
	// visible nowhere but a coordinator warning. The manifest already validated,
	// so the campaign looked accepted and simply never ran.
	for _, command := range pkg.Verification {
		if strings.TrimSpace(command) == "" || strings.ContainsRune(command, 0) {
			return errors.New("execution package: invalid verification command")
		}
	}
	if pkg.Gate != nil {
		if err := pkg.Gate.Validate(); err != nil {
			return fmt.Errorf("execution package: %w", err)
		}
		if pkg.Gate.Timeout > pkg.Limits.VerificationTimeout && pkg.Gate.Timeout <= 6*time.Hour {
			return fmt.Errorf("execution package: gate timeout %s exceeds verification timeout %s", pkg.Gate.Timeout, pkg.Limits.VerificationTimeout)
		}
		if len(pkg.Gate.Commands) == 0 || pkg.Gate.Timeout <= 0 || pkg.Gate.Timeout > 6*time.Hour {
			return errors.New("execution package: invalid gate limits")
		}
		for _, command := range pkg.Gate.Commands {
			if strings.TrimSpace(command) == "" || strings.ContainsRune(command, 0) {
				return errors.New("execution package: invalid gate command")
			}
		}
	}
	outputs := make(map[string]struct{})
	for _, output := range pkg.Outputs {
		if !safeRelativePath(output.Name) || output.MediaType == "" {
			return errors.New("execution package: invalid output declaration")
		}
		if _, exists := outputs[output.Name]; exists {
			return errors.New("execution package: duplicate output declaration")
		}
		outputs[output.Name] = struct{}{}
	}
	if !slices.Contains([]string{"task", "workflow"}, pkg.Environment.Scope) {
		return errors.New("execution package: invalid environment scope")
	}
	for _, value := range append(append([]string(nil), pkg.Environment.ResourceLocks...), pkg.Environment.RequiredCredentials...) {
		if !identityPattern.MatchString(value) {
			return errors.New("execution package: invalid catalog reference")
		}
	}
	if err := validatePackageCapabilities(pkg); err != nil {
		return err
	}
	if err := validatePackageProjectContext(pkg); err != nil {
		return err
	}
	if err := validateSupervisionActivation(pkg); err != nil {
		return err
	}
	if err := validateSessionDisplay(pkg); err != nil {
		return err
	}
	return validatePackagePreflight(pkg.Preflight)
}

// validateAcceptedCommits requires each accepted commit to name one of the
// dependency's own artifacts, once, for a producer of this run.
func validateAcceptedCommits(dependency DependencyInput) error {
	if len(dependency.AcceptedCommits) == 0 {
		return nil
	}
	if dependency.Provenance != nil {
		return errors.New("execution package: a carried input cannot be an accepted commit")
	}
	for index, name := range dependency.AcceptedCommits {
		if slices.Contains(dependency.AcceptedCommits[:index], name) {
			return errors.New("execution package: duplicate accepted commit")
		}
		if !slices.ContainsFunc(dependency.Artifacts, func(artifact ArtifactObject) bool {
			parts := strings.SplitN(artifact.Path, "/", 3)
			return len(parts) == 3 && parts[0] == "dependencies" && parts[2] == name
		}) {
			return errors.New("execution package: accepted commit is not one of the dependency's artifacts")
		}
	}
	return nil
}

// validateCommitOutputs requires each declared commit output to name one of
// the dependency's own artifacts, once.
func validateCommitOutputs(dependency DependencyInput) error {
	if len(dependency.CommitOutputs) == 0 {
		return nil
	}
	artifacts := make(map[string]struct{}, len(dependency.Artifacts))
	for _, artifact := range dependency.Artifacts {
		if parts := strings.SplitN(artifact.Path, "/", 3); len(parts) == 3 && parts[0] == "dependencies" {
			artifacts[parts[2]] = struct{}{}
		}
	}
	seen := make(map[string]struct{}, len(dependency.CommitOutputs))
	for _, name := range dependency.CommitOutputs {
		if _, duplicate := seen[name]; duplicate {
			return errors.New("execution package: duplicate dependency commit output")
		}
		seen[name] = struct{}{}
		if _, exists := artifacts[name]; !exists {
			return errors.New("execution package: dependency commit output is not one of the dependency's artifacts")
		}
	}
	return nil
}

func validatePackageCapabilities(pkg ExecutionPackage) error {
	// The campaign-supervision capability is a worker inventory capability, not
	// a package capability: it says which build is running on the host rather
	// than which behaviour this package needs. An activation package names it
	// anyway, so that a worker validating a package it should never have been
	// offered refuses it by name instead of running a review as a task.
	if pkg.Gate != nil && !slices.Contains(pkg.RequiredCapabilities, PackageCapabilityWorkerOwnedGate) {
		return errors.New("execution package: gate requires worker-owned-gate-v1 capability")
	}
	if pkg.Gate == nil && slices.Contains(pkg.RequiredCapabilities, PackageCapabilityWorkerOwnedGate) {
		return errors.New("execution package: worker-owned-gate-v1 capability requires a gate")
	}
	supported := append(SupportedPackageCapabilities(), CapabilityCampaignSupervision)
	declared := make(map[string]struct{}, len(pkg.RequiredCapabilities))
	for _, capability := range pkg.RequiredCapabilities {
		if capability == CapabilityCampaignSupervision && pkg.Supervision == nil {
			return errors.New("execution package: only an activation may require the campaign supervision capability")
		}
		if capability == PackageCapabilitySupervisionEvidence && pkg.Supervision == nil {
			return errors.New("execution package: only an activation may require supervision evidence materialization")
		}
		if capability == PackageCapabilityRecoveryRetry && (pkg.Supervision == nil || pkg.Supervision.Purpose != "repair") {
			return errors.New("execution package: only a repair activation may require recovery retry")
		}
		if capability == PackageCapabilityWorkspaceHead && pkg.Supervision != nil {
			return errors.New("execution package: an activation has no reviewed workspace HEAD to report")
		}
		if capability == PackageCapabilityProjectContext && pkg.Context == nil {
			return errors.New("execution package: project context capability requires a context index")
		}
		if !slices.Contains(supported, capability) {
			return fmt.Errorf("execution package: unsupported required capability %q", capability)
		}
		if _, duplicate := declared[capability]; duplicate {
			return fmt.Errorf("execution package: duplicate required capability %q", capability)
		}
		declared[capability] = struct{}{}
	}
	if _, ok := declared[PackageCapabilityCommitBundle]; len(pkg.CommitBundles) != 0 && !ok {
		return errors.New("execution package: commit bundles require the commit bundle capability")
	}
	if _, ok := declared[PackageCapabilityPreflight]; len(pkg.Preflight) != 0 && !ok {
		return errors.New("execution package: preflight steps require the preflight capability")
	}
	if _, ok := declared[PackageCapabilityRecoverySupplement]; pkg.Recovery != nil && !ok {
		return errors.New("execution package: recovery context requires the recovery supplement capability")
	}
	if _, ok := declared[PackageCapabilityProjectContext]; pkg.Context != nil && !ok {
		return errors.New("execution package: project context requires the project context capability")
	}
	if _, ok := declared[PackageCapabilityAcceptedDependencies]; ok != pkg.HasAcceptedDependencies() {
		return errors.New("execution package: accepted dependencies and the accepted dependencies capability must be declared together")
	}
	if _, ok := declared[PackageCapabilityCommitOutputs]; !ok && slices.ContainsFunc(pkg.Dependencies, func(dependency DependencyInput) bool { return len(dependency.CommitOutputs) != 0 }) {
		return errors.New("execution package: dependency commit outputs require the commit outputs capability")
	}
	if _, ok := declared[PackageCapabilityCommitOutputs]; ok {
		for _, dependency := range pkg.Dependencies {
			for _, name := range dependency.AcceptedCommits {
				if !slices.Contains(dependency.CommitOutputs, name) {
					return errors.New("execution package: an accepted commit is not a declared commit output of its dependency")
				}
			}
		}
	}
	if _, ok := declared[PackageCapabilitySessionDisplay]; pkg.Display != nil && !ok {
		return errors.New("execution package: display requires session display capability")
	}
	if _, ok := declared[PackageCapabilitySessionDisplay]; ok && pkg.Display == nil {
		return errors.New("execution package: session display capability requires display metadata")
	}
	if _, ok := declared[PackageCapabilityContinuationCheckpoint]; pkg.Continuation != nil && !ok {
		return errors.New("execution package: a continuation input requires the continuation checkpoint capability")
	}
	if pkg.Continuation != nil {
		delivered := false
		for _, input := range pkg.StaticInputs {
			delivered = delivered || (input.Path == ContinuationInputPath && input.Size == pkg.Continuation.Size)
		}
		if pkg.Supervision != nil || pkg.Continuation.Path != ContinuationInputPath || pkg.Continuation.AttemptID == "" || !delivered {
			return errors.New("execution package: continuation input is incomplete or attached to an activation")
		}
	}
	if pkg.Recovery != nil {
		if pkg.Supervision != nil || pkg.Recovery.IncidentID == "" || pkg.Recovery.InstructionPath != "inputs/recovery/instructions.md" {
			return errors.New("execution package: recovery context is incomplete or attached to an activation")
		}
		for _, checkpoint := range pkg.Recovery.CheckpointPaths {
			if !strings.HasPrefix(checkpoint, "inputs/recovery/checkpoint-") {
				return errors.New("execution package: recovery checkpoint path is invalid")
			}
		}
	}
	return nil
}

func validatePackagePreflight(steps []PreflightStep) error {
	if len(steps) > 32 {
		return errors.New("execution package: too many preflight steps")
	}
	seen := make(map[string]struct{}, len(steps))
	for index, step := range steps {
		if !identityPattern.MatchString(step.ID) {
			return fmt.Errorf("execution package: preflight step[%d] has an invalid id", index)
		}
		if _, duplicate := seen[step.ID]; duplicate {
			return fmt.Errorf("execution package: duplicate preflight step %q", step.ID)
		}
		seen[step.ID] = struct{}{}
		if !slices.Contains([]string{"check", "context"}, step.Kind) {
			return fmt.Errorf("execution package: preflight step %q has an invalid kind", step.ID)
		}
		if !slices.Contains([]string{"record", "require-pass"}, step.FailurePolicy) {
			return fmt.Errorf("execution package: preflight step %q has an invalid failure policy", step.ID)
		}
		if !slices.Contains([]string{"summary", "reference", "omit"}, step.Include) {
			return fmt.Errorf("execution package: preflight step %q has an invalid include mode", step.ID)
		}
		switch {
		case len(step.Command) == 0 && step.Probe == "":
			return fmt.Errorf("execution package: preflight step %q sets neither command nor probe", step.ID)
		case len(step.Command) != 0 && step.Probe != "":
			return fmt.Errorf("execution package: preflight step %q sets both command and probe", step.ID)
		}
		for _, argument := range step.Command {
			if strings.TrimSpace(argument) == "" || strings.ContainsRune(argument, 0) {
				return fmt.Errorf("execution package: preflight step %q has an empty argument", step.ID)
			}
		}
		if step.Probe != "" && !identityPattern.MatchString(step.Probe) {
			return fmt.Errorf("execution package: preflight step %q has an invalid probe", step.ID)
		}
		if step.MaxOutputBytes <= 0 || step.Timeout <= 0 || step.Timeout > time.Hour {
			return fmt.Errorf("execution package: preflight step %q has invalid bounds", step.ID)
		}
		if step.Kind == "context" && step.FailurePolicy == "require-pass" && !step.Required {
			return fmt.Errorf("execution package: preflight step %q blocks without being required", step.ID)
		}
	}
	return nil
}

func validatePackageArtifact(artifact ArtifactObject, maxBytes int64, paths map[string]struct{}) error {
	if err := ValidateArtifactObject(artifact, maxBytes); err != nil {
		return err
	}
	if _, exists := paths[artifact.Path]; exists {
		return errors.New("duplicate materialization path")
	}
	paths[artifact.Path] = struct{}{}
	return nil
}
