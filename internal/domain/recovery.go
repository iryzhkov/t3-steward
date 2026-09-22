package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	GraphRevision            int64                      `json:"graphRevision"`
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

const RecoveryProposalVersion = 1

// RecoveryProposal is the versioned result-import contract emitted by a repair
// activation. Its retained object digests are part of PayloadDigest; the
// coordinator still rechecks live incident, graph, activation, assignment,
// principal and source-attempt authority before creating a retry.
type RecoveryProposal struct {
	Version                  int                        `json:"version"`
	OperationID              string                     `json:"operationId"`
	RunID                    string                     `json:"runId"`
	IncidentID               string                     `json:"incidentId"`
	ExpectedIncidentRevision int64                      `json:"expectedIncidentRevision"`
	GraphRevision            int64                      `json:"graphRevision"`
	ActivationID             string                     `json:"activationId"`
	ActivationEpoch          int64                      `json:"activationEpoch"`
	AssignmentID             string                     `json:"assignmentId"`
	AssignmentEpoch          int64                      `json:"assignmentEpoch"`
	Principal                string                     `json:"principal"`
	SourceAttemptID          string                     `json:"sourceAttemptId"`
	SourceAttemptRevision    int64                      `json:"sourceAttemptRevision"`
	InstructionArtifact      ArtifactDigest             `json:"instructionArtifact"`
	CheckpointArtifacts      []ArtifactDigest           `json:"checkpointArtifacts,omitempty"`
	Diagnostic               RecoveryDiagnosticIdentity `json:"diagnostic"`
	ProposedAt               time.Time                  `json:"proposedAt"`
	PayloadDigest            string                     `json:"payloadDigest"`
}

func (p RecoveryProposal) canonicalDigest() (string, error) {
	p.PayloadDigest = ""
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func SealRecoveryProposal(proposal RecoveryProposal) (RecoveryProposal, error) {
	digest, err := proposal.canonicalDigest()
	if err != nil {
		return RecoveryProposal{}, err
	}
	proposal.PayloadDigest = digest
	return proposal, nil
}

func (p RecoveryProposal) RetryRequest() (RecoveryRetryRequest, error) {
	switch {
	case p.Version != RecoveryProposalVersion:
		return RecoveryRetryRequest{}, errors.New("recovery proposal version is unsupported")
	case strings.TrimSpace(p.PayloadDigest) == "":
		return RecoveryRetryRequest{}, errors.New("recovery proposal payload digest is required")
	}
	digest, err := p.canonicalDigest()
	if err != nil {
		return RecoveryRetryRequest{}, err
	}
	if !strings.EqualFold(digest, p.PayloadDigest) {
		return RecoveryRetryRequest{}, errors.New("recovery proposal payload digest mismatch")
	}
	return RecoveryRetryRequest{
		OperationID: p.OperationID, RunID: p.RunID, IncidentID: p.IncidentID,
		ExpectedIncidentRevision: p.ExpectedIncidentRevision, GraphRevision: p.GraphRevision,
		ActivationID: p.ActivationID, ActivationEpoch: p.ActivationEpoch, Principal: p.Principal,
		SourceAttemptID: p.SourceAttemptID, SourceAttemptRevision: p.SourceAttemptRevision,
		InstructionArtifact: p.InstructionArtifact, CheckpointArtifacts: append([]ArtifactDigest(nil), p.CheckpointArtifacts...),
		Diagnostic: p.Diagnostic, RequestedAt: p.ProposedAt,
	}, nil
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
