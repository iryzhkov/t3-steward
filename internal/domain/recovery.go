package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	RecoveryContractV1 RecoveryContractVersion = "recovery-v1"

	MaxRecoveryAttemptsPerIncident = 20
	MaxRecoveryIncidentDeadline    = 7 * 24 * time.Hour
	MaxRecoveryStalledAfter        = 24 * time.Hour
)

type RecoveryContractVersion string

type RecoveryRole string

const RecoveryRoleRepairExecutor RecoveryRole = "repair-executor"

type RecoveryActivationPurpose string

const RecoveryActivationRepair RecoveryActivationPurpose = "repair"

type RecoveryState string

const (
	RecoveryPendingDispatch RecoveryState = "pending-dispatch"
	RecoveryRecovering      RecoveryState = "recovering"
	RecoveryExpectedWait    RecoveryState = "expected-wait"
	RecoveryNeedsHuman      RecoveryState = "needs-human"
	RecoveryResolved        RecoveryState = "resolved"
)

type RecoveryNextAction string

const (
	RecoveryDispatchRepair RecoveryNextAction = "dispatch-repair"
	RecoveryAwaitCapacity  RecoveryNextAction = "await-capacity"
	RecoveryEscalateHuman  RecoveryNextAction = "escalate-human"
	RecoveryNoAction       RecoveryNextAction = "none"
)

type RecoveryConfig struct {
	Version                RecoveryContractVersion `json:"version"`
	Route                  ProviderRoute           `json:"route"`
	PromptArtifactID       string                  `json:"promptArtifactId"`
	MaxAttemptsPerIncident int                     `json:"maxAttemptsPerIncident"`
	IncidentDeadline       time.Duration           `json:"incidentDeadline"`
	StalledAfter           time.Duration           `json:"stalledAfter"`
}

func (c RecoveryConfig) Validate() error {
	switch {
	case c.Version != RecoveryContractV1:
		return fmt.Errorf("recovery version must be %q", RecoveryContractV1)
	case strings.TrimSpace(c.Route.ProviderInstanceID) == "" || strings.TrimSpace(c.Route.Model) == "":
		return errors.New("recovery needs one route with a provider instance and a model")
	case strings.TrimSpace(c.PromptArtifactID) == "":
		return errors.New("recovery needs a repair prompt artifact")
	case c.MaxAttemptsPerIncident <= 0 || c.MaxAttemptsPerIncident > MaxRecoveryAttemptsPerIncident:
		return fmt.Errorf("recovery needs max attempts per incident above zero and at most %d", MaxRecoveryAttemptsPerIncident)
	case c.IncidentDeadline <= 0 || c.IncidentDeadline > MaxRecoveryIncidentDeadline:
		return fmt.Errorf("recovery needs an incident deadline above zero and at most %s", MaxRecoveryIncidentDeadline)
	case c.StalledAfter <= 0 || c.StalledAfter > MaxRecoveryStalledAfter:
		return fmt.Errorf("recovery needs stalled after above zero and at most %s", MaxRecoveryStalledAfter)
	case c.StalledAfter > c.IncidentDeadline:
		return errors.New("recovery stalled after cannot exceed its incident deadline")
	}
	return nil
}

type RecoveryOwner struct {
	Role             RecoveryRole  `json:"role"`
	Route            ProviderRoute `json:"route"`
	PromptArtifactID string        `json:"promptArtifactId"`
}

type RecoveryDiagnosticIdentity struct {
	FailureFingerprint  string `json:"failureFingerprint"`
	EvidenceFingerprint string `json:"evidenceFingerprint"`
	StrategyFingerprint string `json:"strategyFingerprint"`
}

// RecoveryStrategyFingerprint binds a strategy to retained content. Artifact
// identities are provenance only; changing an ID cannot make identical bytes a
// new recovery approach.
func RecoveryStrategyFingerprint(instruction ArtifactDigest, checkpoints []ArtifactDigest) string {
	parts := []string{strings.TrimSpace(instruction.Digest)}
	for _, checkpoint := range checkpoints {
		parts = append(parts, strings.TrimSpace(checkpoint.Digest))
	}
	sort.Strings(parts[1:])
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

type RepairAttemptSupplement struct {
	OperationID         string                     `json:"operationId"`
	IncidentID          string                     `json:"incidentId"`
	SourceAttemptID     string                     `json:"sourceAttemptId"`
	AttemptID           string                     `json:"attemptId"`
	InstructionArtifact ArtifactDigest             `json:"instructionArtifact"`
	CheckpointArtifacts []ArtifactDigest           `json:"checkpointArtifacts,omitempty"`
	Diagnostic          RecoveryDiagnosticIdentity `json:"diagnostic"`
	CreatedAt           time.Time                  `json:"createdAt"`
}

type RecoveryRetryRequest struct {
	OperationID              string                     `json:"operationId"`
	RunID                    string                     `json:"runId"`
	IncidentID               string                     `json:"incidentId"`
	ExpectedIncidentRevision int64                      `json:"expectedIncidentRevision"`
	ActivationID             string                     `json:"activationId"`
	ActivationEpoch          int64                      `json:"activationEpoch"`
	Principal                string                     `json:"principal"`
	SourceAttemptID          string                     `json:"sourceAttemptId"`
	SourceAttemptRevision    int64                      `json:"sourceAttemptRevision"`
	InstructionArtifact      ArtifactDigest             `json:"instructionArtifact"`
	CheckpointArtifacts      []ArtifactDigest           `json:"checkpointArtifacts,omitempty"`
	Diagnostic               RecoveryDiagnosticIdentity `json:"diagnostic"`
	RequestedAt              time.Time                  `json:"requestedAt"`
}

type RecoveryRetryReceipt struct {
	OperationID   string    `json:"operationId"`
	IncidentID    string    `json:"incidentId"`
	AttemptID     string    `json:"attemptId"`
	AttemptNumber int       `json:"attemptNumber"`
	CommittedAt   time.Time `json:"committedAt"`
}

type RecoveryIncident struct {
	Contract       RecoveryContractVersion   `json:"contract"`
	Purpose        RecoveryActivationPurpose `json:"purpose"`
	Owner          RecoveryOwner             `json:"owner"`
	State          RecoveryState             `json:"state"`
	NextAction     RecoveryNextAction        `json:"nextAction"`
	AttemptBudget  int                       `json:"attemptBudget"`
	AttemptsUsed   int                       `json:"attemptsUsed"`
	Deadline       time.Time                 `json:"deadline"`
	LastProgressAt time.Time                 `json:"lastProgressAt"`
	// RootDiagnostic is immutable evidence from the failure that opened the episode.
	RootDiagnostic   RecoveryDiagnosticIdentity `json:"rootDiagnostic"`
	Diagnostic       RecoveryDiagnosticIdentity `json:"diagnostic"`
	CurrentAttemptID string                     `json:"currentAttemptId,omitempty"`
	ExhaustionReason string                     `json:"exhaustionReason,omitempty"`
	WaitReason       string                     `json:"waitReason,omitempty"`
	NextAttemptAt    *time.Time                 `json:"nextAttemptAt,omitempty"`
}

func NewRecoveryIncident(config RecoveryConfig, diagnostic RecoveryDiagnosticIdentity, now time.Time) *RecoveryIncident {
	return &RecoveryIncident{
		Contract:       RecoveryContractV1,
		Purpose:        RecoveryActivationRepair,
		Owner:          RecoveryOwner{Role: RecoveryRoleRepairExecutor, Route: config.Route, PromptArtifactID: config.PromptArtifactID},
		State:          RecoveryPendingDispatch,
		NextAction:     RecoveryDispatchRepair,
		AttemptBudget:  config.MaxAttemptsPerIncident,
		Deadline:       now.UTC().Add(config.IncidentDeadline),
		LastProgressAt: now.UTC(),
		RootDiagnostic: diagnostic,
		Diagnostic:     diagnostic,
	}
}
