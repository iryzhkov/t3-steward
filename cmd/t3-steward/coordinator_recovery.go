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
		incident, owned, err := c.store.RecoveryEpisodeForAttempt(ctx, run.ID, attempt.ID)
		if err != nil {
			return err
		}
		if owned {
			if incident.Recovery == nil || incident.Recovery.CurrentAttemptID != attempt.ID ||
				incident.Recovery.State != domain.RecoveryRecovering {
				continue
			}
			switch attempt.Progress {
			case domain.ProgressSucceeded:
				if _, err := c.store.ResolveRecoveryEpisode(ctx, sqlite.RecoveryAttemptSuccessRequest{
					RunID: run.ID, IncidentID: incident.ID, AttemptID: attempt.ID,
					ExpectedIncidentRevision: incident.Revision, ExpectedGraphRevision: run.GraphRevision,
					AttemptRevision: attempt.Revision,
				}); err != nil {
					return err
				}
			case domain.ProgressFailed:
				artifacts := recoveryArtifactDigests(records.Artifacts, run.ID, attempt)
				failure, evidence := recoveryFailureEvidence(attempt, artifacts)
				eventID := "supervision-event:recovery:" + recoveryDigest(incident.ID, attempt.ID)[:24]
				reason := fmt.Sprintf("task %s attempt %s failed: %s", attempt.TaskID, attempt.ID, strings.TrimSpace(attempt.Failure))
				event := backlog.SupervisionEvent{
					ID: eventID, RunID: run.ID, Kind: backlog.TriggerTaskJudgmentRequired,
					Reason: reason, TaskID: attempt.TaskID, AttemptID: attempt.ID,
					IncidentID: incident.ID, GraphRevision: run.GraphRevision,
					Artifacts: artifacts, OccurredAt: now.UTC(),
				}
				raw, err := json.Marshal(event)
				if err != nil {
					return err
				}
				if _, err := c.store.RecordRecoveryAttemptFailure(ctx, sqlite.RecoveryAttemptFailureRequest{
					RunID: run.ID, IncidentID: incident.ID, EventID: eventID, AttemptID: attempt.ID, Reason: reason,
					ExpectedIncidentRevision: incident.Revision, ExpectedGraphRevision: run.GraphRevision,
					AttemptRevision: attempt.Revision, FailureFingerprint: failure, EvidenceFingerprint: evidence,
					EventRecord: raw,
				}); err != nil {
					return err
				}
			}
			continue
		}
		if attempt.Progress != domain.ProgressFailed {
			continue
		}
		artifacts := recoveryArtifactDigests(records.Artifacts, run.ID, attempt)
		failure, evidence := recoveryFailureEvidence(attempt, artifacts)
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
			ExpectedGraphRevision: run.GraphRevision, SourceAttemptRevision: attempt.Revision,
			RecoveryConfig: config, Reason: reason, Recovery: *recovery, EventRecord: raw, OpenedAt: now,
		}); err != nil {
			return err
		}
	}
	return nil
}

func recoveryFailureEvidence(attempt domain.Attempt, artifacts []domain.ArtifactDigest) (string, string) {
	failure := recoveryDigest(string(attempt.Progress), strings.TrimSpace(attempt.Failure))
	digests := make([]string, 0, len(artifacts))
	for _, artifact := range artifacts {
		digests = append(digests, artifact.Digest)
	}
	sort.Strings(digests)
	return failure, recoveryDigest(digests...)
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
