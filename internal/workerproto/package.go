package workerproto

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const ExecutionPackageVersion = 1

var identityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$`)

type EnvironmentReference struct {
	CatalogRevision     string   `json:"catalogRevision"`
	Project             string   `json:"project"`
	Repository          string   `json:"repository"`
	Ref                 string   `json:"ref"`
	Scope               string   `json:"scope"`
	SetupProfile        string   `json:"setupProfile"`
	T3Project           string   `json:"t3Project"`
	ResourceLocks       []string `json:"resourceLocks,omitempty"`
	RequiredCredentials []string `json:"requiredCredentials,omitempty"`
}

type ExecutionLimits struct {
	MaxTurns            int           `json:"maxTurns"`
	PrepareTimeout      time.Duration `json:"prepareTimeout"`
	VerificationTimeout time.Duration `json:"verificationTimeout"`
	MaxArtifactBytes    int64         `json:"maxArtifactBytes"`
	MaxTotalBytes       int64         `json:"maxTotalBytes"`
}

type ExecutionIdentity struct {
	WorkflowID      string `json:"workflowId"`
	WorkflowRunID   string `json:"workflowRunId"`
	TaskID          string `json:"taskId"`
	AttemptID       string `json:"attemptId"`
	AssignmentID    string `json:"assignmentId"`
	AssignmentEpoch int64  `json:"assignmentEpoch"`
	DispatchToken   string `json:"dispatchToken"`
	ThreadID        string `json:"threadId"`
}

type DependencyInput struct {
	TaskID    string           `json:"taskId"`
	Artifacts []ArtifactObject `json:"artifacts"`
}

type ExecutionPackage struct {
	Timeout          time.Duration                `json:"timeout,omitempty"`
	GraphRevision    int64                        `json:"graphRevision,omitempty"`
	TaskRevision     int64                        `json:"taskRevision,omitempty"`
	TaskDigest       string                       `json:"taskDigest,omitempty"`
	Version          int                          `json:"version"`
	ID               string                       `json:"id"`
	CoordinatorID    string                       `json:"coordinatorId"`
	CoordinatorEpoch int64                        `json:"coordinatorEpoch"`
	WorkerID         string                       `json:"workerId"`
	WorkerEpoch      string                       `json:"workerEpoch"`
	Identity         ExecutionIdentity            `json:"identity"`
	Class            domain.TaskClass             `json:"class"`
	Prompt           ArtifactObject               `json:"prompt"`
	StaticInputs     []ArtifactObject             `json:"staticInputs,omitempty"`
	Dependencies     []DependencyInput            `json:"dependencies,omitempty"`
	Route            domain.ProviderRoute         `json:"route"`
	Environment      EnvironmentReference         `json:"environment"`
	Verification     []string                     `json:"verification,omitempty"`
	Outputs          []domain.ArtifactDeclaration `json:"outputs,omitempty"`
	NotBefore        *time.Time                   `json:"notBefore,omitempty"`
	Deadline         *time.Time                   `json:"deadline,omitempty"`
	ExpiresAt        *time.Time                   `json:"expiresAt,omitempty"`
	Limits           ExecutionLimits              `json:"limits"`
	CreatedAt        time.Time                    `json:"createdAt"`
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
	for name, value := range map[string]string{
		"repository": pkg.Environment.Repository,
		"ref":        pkg.Environment.Ref,
		"t3Project":  pkg.Environment.T3Project,
	} {
		if !safeOpaqueValue(value, 1024) {
			return fmt.Errorf("execution package: invalid %s", name)
		}
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
	var total int64 = pkg.Prompt.Size
	for _, artifact := range pkg.StaticInputs {
		if err := validatePackageArtifact(artifact, pkg.Limits.MaxArtifactBytes, paths); err != nil {
			return fmt.Errorf("execution package: static input: %w", err)
		}
		total += artifact.Size
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
		for _, artifact := range dependency.Artifacts {
			if err := validatePackageArtifact(artifact, pkg.Limits.MaxArtifactBytes, paths); err != nil {
				return fmt.Errorf("execution package: dependency input: %w", err)
			}
			total += artifact.Size
		}
	}
	if total > pkg.Limits.MaxTotalBytes {
		return errors.New("execution package: inputs exceed total byte limit")
	}
	if len(pkg.Verification) == 0 {
		return errors.New("execution package: verification command is required")
	}
	for _, command := range pkg.Verification {
		if strings.TrimSpace(command) == "" || strings.ContainsRune(command, 0) {
			return errors.New("execution package: invalid verification command")
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
