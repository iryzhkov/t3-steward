package backlog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const (
	ActivationEvidenceSnapshotVersion   = 1
	ActivationEvidenceSnapshotMediaType = "application/vnd.t3-steward.supervision-evidence.v1+json"

	ActivationPackageErrorEvidenceMissing  = "activation_evidence_missing"
	ActivationPackageErrorEvidenceMismatch = "activation_evidence_mismatch"
	ActivationPackageErrorPromptTooLarge   = "activation_prompt_too_large"
)

// ActivationPackageError classifies a deterministic package construction
// failure so dispatch can stop offering identical work and route it to
// recovery.
type ActivationPackageError struct {
	Code  string
	Cause error
}

func (e *ActivationPackageError) Error() string {
	return fmt.Sprintf("supervision activation package [%s]: %v", e.Code, e.Cause)
}

func (e *ActivationPackageError) Unwrap() error {
	return e.Cause
}

// ActivationGateEvidence preserves the full gate definition, revision and
// producer evidence that a reviewer is obliged to inspect.
type ActivationGateEvidence struct {
	Gate     domain.Gate              `json:"gate"`
	Evidence *domain.EvidenceSnapshot `json:"evidence,omitempty"`
}

// ActivationEvidenceSnapshot is the complete immutable evidence inventory
// behind a compact activation brief.
type ActivationEvidenceSnapshot struct {
	Version         int                      `json:"version"`
	ActivationID    string                   `json:"activationId"`
	RunID           string                   `json:"runId"`
	Epoch           int64                    `json:"epoch"`
	GraphRevision   int64                    `json:"graphRevision"`
	RecordRevision  int64                    `json:"recordRevision"`
	ConsumedThrough int64                    `json:"consumedThrough"`
	Tasks           []ActivationTaskView     `json:"tasks,omitempty"`
	Gates           []ActivationGateView     `json:"gates,omitempty"`
	Incidents       []ActivationIncidentView `json:"incidents,omitempty"`
	Triggers        []CoalescedTrigger       `json:"triggers,omitempty"`
	Artifacts       []domain.ArtifactDigest  `json:"artifacts,omitempty"`
	TaskContracts   []domain.Task            `json:"taskContracts,omitempty"`
	Attempts        []domain.Attempt         `json:"attempts,omitempty"`
	GateEvidence    []ActivationGateEvidence `json:"gateEvidence,omitempty"`
	OverseerPrompt  domain.ArtifactDigest    `json:"overseerPrompt"`
	Actions         []ActivationAction       `json:"actions"`
	Constraints     []string                 `json:"constraints"`
	SafetyRules     []string                 `json:"safetyRules"`
}

// DecodeActivationEvidenceSnapshot decodes retained bytes and rejects unknown
// versions before they can drive a brief.
func DecodeActivationEvidenceSnapshot(data []byte) (ActivationEvidenceSnapshot, error) {
	var evidence ActivationEvidenceSnapshot
	if err := json.Unmarshal(data, &evidence); err != nil {
		return ActivationEvidenceSnapshot{}, fmt.Errorf("decode activation evidence snapshot: %w", err)
	}
	if evidence.Version != ActivationEvidenceSnapshotVersion || evidence.ActivationID == "" ||
		evidence.RunID == "" || evidence.Epoch <= 0 {
		return ActivationEvidenceSnapshot{}, errors.New("activation evidence snapshot has incomplete or unsupported identity")
	}
	return evidence, nil
}

// ActivationEvidenceObject is canonical snapshot content plus immutable
// metadata suitable for the existing artifact custody and transfer path.
type ActivationEvidenceObject struct {
	ID        string
	MediaType string
	Data      []byte
	Size      int64
	SHA256    string
}

// BuildActivationEvidenceSnapshot builds canonical, redacted bytes. Ordering is
// normalized so replaying the same activation produces the same ID and digest.
func BuildActivationEvidenceSnapshot(snapshot ActivationSnapshot) (ActivationEvidenceObject, error) {
	if err := snapshot.Validate(); err != nil {
		return ActivationEvidenceObject{}, err
	}
	redactor := snapshot.Redactor
	if len(redactor.Patterns) == 0 && len(redactor.Literals) == 0 {
		redactor = DefaultRedactor()
	}
	evidence := ActivationEvidenceSnapshot{
		Version:      ActivationEvidenceSnapshotVersion,
		ActivationID: snapshot.ActivationID, RunID: snapshot.RunID,
		Epoch: snapshot.Epoch, GraphRevision: snapshot.GraphRevision,
		RecordRevision: snapshot.RecordRevision, ConsumedThrough: snapshot.ConsumedThrough,
		Tasks:          append([]ActivationTaskView(nil), snapshot.Tasks...),
		Gates:          append([]ActivationGateView(nil), snapshot.Gates...),
		Incidents:      append([]ActivationIncidentView(nil), snapshot.Incidents...),
		Triggers:       append([]CoalescedTrigger(nil), snapshot.Triggers...),
		Artifacts:      append([]domain.ArtifactDigest(nil), snapshot.Artifacts...),
		TaskContracts:  append([]domain.Task(nil), snapshot.TaskContracts...),
		Attempts:       append([]domain.Attempt(nil), snapshot.Attempts...),
		GateEvidence:   append([]ActivationGateEvidence(nil), snapshot.GateEvidence...),
		OverseerPrompt: snapshot.OverseerPrompt,
		Actions:        append([]ActivationAction(nil), snapshot.Actions...),
		Constraints:    redactAll(redactor, snapshot.Constraints),
		SafetyRules:    append([]string(nil), activationSafetyRules...),
	}
	// Detach nested slices and maps before canonical sorting so snapshot
	// construction never mutates the live coordinator records it observed.
	detached, err := json.Marshal(evidence)
	if err != nil {
		return ActivationEvidenceObject{}, fmt.Errorf("copy activation evidence snapshot: %w", err)
	}
	if err := json.Unmarshal(detached, &evidence); err != nil {
		return ActivationEvidenceObject{}, fmt.Errorf("copy activation evidence snapshot: %w", err)
	}
	sort.Slice(evidence.Tasks, func(i, j int) bool { return evidence.Tasks[i].TaskID < evidence.Tasks[j].TaskID })
	for index := range evidence.Tasks {
		evidence.Tasks[index].Verification, _ = redactor.Redact(evidence.Tasks[index].Verification)
	}
	sort.Slice(evidence.Gates, func(i, j int) bool { return evidence.Gates[i].GateID < evidence.Gates[j].GateID })
	for index := range evidence.Gates {
		sort.Strings(evidence.Gates[index].ObservedTaskIDs)
		sort.Strings(evidence.Gates[index].ProtectedTaskIDs)
	}
	sort.Slice(evidence.Incidents, func(i, j int) bool { return evidence.Incidents[i].IncidentID < evidence.Incidents[j].IncidentID })
	for index := range evidence.Incidents {
		evidence.Incidents[index].Reason, _ = redactor.Redact(evidence.Incidents[index].Reason)
	}
	sort.Slice(evidence.Triggers, func(i, j int) bool {
		if evidence.Triggers[i].Kind != evidence.Triggers[j].Kind {
			return evidence.Triggers[i].Kind < evidence.Triggers[j].Kind
		}
		return evidence.Triggers[i].Subject < evidence.Triggers[j].Subject
	})
	for index := range evidence.Triggers {
		evidence.Triggers[index].Reasons = redactAll(redactor, evidence.Triggers[index].Reasons)
		sort.Strings(evidence.Triggers[index].Reasons)
		sort.Strings(evidence.Triggers[index].EventIDs)
	}
	sort.Slice(evidence.Artifacts, func(i, j int) bool { return evidence.Artifacts[i].ArtifactID < evidence.Artifacts[j].ArtifactID })
	sort.Slice(evidence.TaskContracts, func(i, j int) bool {
		return evidence.TaskContracts[i].ID < evidence.TaskContracts[j].ID
	})
	sort.Slice(evidence.Attempts, func(i, j int) bool {
		if evidence.Attempts[i].TaskID != evidence.Attempts[j].TaskID {
			return evidence.Attempts[i].TaskID < evidence.Attempts[j].TaskID
		}
		return evidence.Attempts[i].Number < evidence.Attempts[j].Number
	})
	for index := range evidence.Attempts {
		evidence.Attempts[index].Failure, _ = redactor.Redact(evidence.Attempts[index].Failure)
	}
	sort.Slice(evidence.GateEvidence, func(i, j int) bool {
		return evidence.GateEvidence[i].Gate.Definition.ID < evidence.GateEvidence[j].Gate.Definition.ID
	})
	for index := range evidence.GateEvidence {
		sort.Strings(evidence.GateEvidence[index].Gate.Definition.ObservedTaskIDs)
		sort.Strings(evidence.GateEvidence[index].Gate.Definition.ProtectedTaskIDs)
		if evidence.GateEvidence[index].Evidence != nil {
			sort.Slice(evidence.GateEvidence[index].Evidence.Producers, func(i, j int) bool {
				return evidence.GateEvidence[index].Evidence.Producers[i].TaskID <
					evidence.GateEvidence[index].Evidence.Producers[j].TaskID
			})
		}
	}
	sort.Slice(evidence.Actions, func(i, j int) bool { return evidence.Actions[i].Name < evidence.Actions[j].Name })
	for index := range evidence.Actions {
		evidence.Actions[index].Constraints = redactAll(redactor, evidence.Actions[index].Constraints)
	}
	return BuildActivationEvidenceObject(evidence)
}

// BuildActivationEvidenceObject canonicalizes a previously frozen snapshot.
// Dispatch replay uses this path after retrieving and decoding retained bytes;
// it does not rebuild evidence from mutable current state.
func BuildActivationEvidenceObject(evidence ActivationEvidenceSnapshot) (ActivationEvidenceObject, error) {
	if evidence.Version != ActivationEvidenceSnapshotVersion || evidence.ActivationID == "" ||
		evidence.RunID == "" || evidence.Epoch <= 0 {
		return ActivationEvidenceObject{}, errors.New("activation evidence snapshot has incomplete or unsupported identity")
	}
	data, err := json.Marshal(evidence)
	if err != nil {
		return ActivationEvidenceObject{}, fmt.Errorf("encode activation evidence snapshot: %w", err)
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	identity := fmt.Sprintf("%s@%d/graph/%d/record/%d/%s",
		evidence.ActivationID, evidence.Epoch, evidence.GraphRevision, evidence.RecordRevision, digest)
	return ActivationEvidenceObject{
		ID:        stableCoordinatorID("activation-evidence", identity),
		MediaType: ActivationEvidenceSnapshotMediaType,
		Data:      data, Size: int64(len(data)), SHA256: digest,
	}, nil
}

// ActivationEvidenceArtifact constructs metadata for bytes already placed in
// immutable coordinator artifact custody by the integration layer.
func ActivationEvidenceArtifact(runID string, object ActivationEvidenceObject, storagePath string, createdAt time.Time) domain.Artifact {
	return domain.Artifact{
		ID: object.ID, WorkflowRunID: runID, Kind: domain.ArtifactInput,
		Name: "supervision-evidence.json", MediaType: object.MediaType,
		Size: object.Size, SHA256: object.SHA256, StoragePath: storagePath,
		Producer: "coordinator/supervision", CreatedAt: createdAt.UTC(),
	}
}

func validateActivationEvidenceArtifact(evidence ActivationEvidenceSnapshot, artifact domain.Artifact) error {
	if artifact.ID == "" {
		return &ActivationPackageError{Code: ActivationPackageErrorEvidenceMissing, Cause: errors.New("immutable activation evidence artifact is required")}
	}
	object, err := BuildActivationEvidenceObject(evidence)
	if err != nil {
		return err
	}
	if artifact.ID != object.ID || artifact.WorkflowRunID != evidence.RunID ||
		artifact.Kind != domain.ArtifactInput || artifact.Name != "supervision-evidence.json" ||
		artifact.MediaType != object.MediaType || artifact.Size != object.Size ||
		artifact.SHA256 != object.SHA256 || artifact.StoragePath == "" {
		return &ActivationPackageError{
			Code: ActivationPackageErrorEvidenceMismatch,
			Cause: fmt.Errorf("artifact %q does not match activation %s epoch %d graph %d record %d digest %s",
				artifact.ID, evidence.ActivationID, evidence.Epoch, evidence.GraphRevision,
				evidence.RecordRevision, object.SHA256),
		}
	}
	return nil
}
