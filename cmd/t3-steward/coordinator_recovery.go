package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func (c coordinatorSupervision) observeRecoveryFailures(ctx context.Context, run domain.WorkflowRun, now time.Time) error {
	if run.Supervision == nil || run.Supervision.Config.Recovery == nil {
		return nil
	}
	config := *run.Supervision.Config.Recovery
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid durable recovery contract: %w", err)
	}
	records, err := c.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return err
	}
	state, err := c.store.LoadSupervisionAdminState(ctx, run.ID)
	if err != nil {
		return err
	}
	ownedAttempts := make(map[string]bool)
	for _, facts := range state.Incidents {
		if facts.Incident.Recovery != nil {
			ownedAttempts[facts.Incident.SourceAttemptID] = true
		}
	}
	latest := make(map[string]domain.Attempt)
	for _, attempt := range domain.DeclaredTaskAttempts(records.Attempts) {
		if attempt.WorkflowRunID != run.ID {
			continue
		}
		current, ok := latest[attempt.TaskID]
		if !ok || attempt.Number > current.Number {
			latest[attempt.TaskID] = attempt
		}
	}
	for _, attempt := range latest {
		if attempt.Progress != domain.ProgressFailed || ownedAttempts[attempt.ID] {
			continue
		}
		artifacts := recoveryArtifactDigests(records.Artifacts, run.ID, attempt)
		failure := recoveryDigest(string(attempt.Progress), strings.TrimSpace(attempt.Failure))
		evidenceParts := []string{attempt.ID, fmt.Sprint(attempt.Number)}
		for _, artifact := range artifacts {
			evidenceParts = append(evidenceParts, artifact.ArtifactID, artifact.Digest)
		}
		evidence := recoveryDigest(evidenceParts...)
		incidentID := "incident:recovery:" + recoveryDigest(run.ID, attempt.TaskID, attempt.ID)[:24]
		eventID := "supervision-event:" + incidentID
		reason := fmt.Sprintf("task %s attempt %s failed: %s", attempt.TaskID, attempt.ID, strings.TrimSpace(attempt.Failure))
		diagnostic := domain.RecoveryDiagnosticIdentity{
			FailureFingerprint: failure, EvidenceFingerprint: evidence,
			StrategyFingerprint: recoveryDigest("initial-diagnosis"),
		}
		recovery := domain.NewRecoveryIncident(config, diagnostic, now)
		event := backlog.SupervisionEvent{
			ID: eventID, RunID: run.ID, Kind: backlog.TriggerTaskJudgmentRequired,
			Reason: reason, TaskID: attempt.TaskID, AttemptID: attempt.ID,
			IncidentID: incidentID, GraphRevision: run.GraphRevision,
			Artifacts: artifacts, OccurredAt: now.UTC(),
		}
		raw, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if _, _, err := c.store.OpenRecoveryIncident(ctx, sqlite.RecoveryIncidentRequest{
			RunID: run.ID, IncidentID: incidentID, EventID: eventID,
			SourceTaskID: attempt.TaskID, SourceAttemptID: attempt.ID,
			Reason: reason, Recovery: *recovery, EventRecord: raw, OpenedAt: now,
		}); err != nil {
			return err
		}
	}
	return nil
}

func recoveryArtifactDigests(all []domain.Artifact, runID string, attempt domain.Attempt) []domain.ArtifactDigest {
	var result []domain.ArtifactDigest
	for _, artifact := range all {
		if artifact.WorkflowRunID != runID || (artifact.AttemptID != attempt.ID && artifact.TaskID != attempt.TaskID) {
			continue
		}
		result = append(result, domain.ArtifactDigest{ArtifactID: artifact.ID, Digest: artifact.SHA256})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ArtifactID != result[j].ArtifactID {
			return result[i].ArtifactID < result[j].ArtifactID
		}
		return result[i].Digest < result[j].Digest
	})
	return result
}

func recoveryDigest(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}
