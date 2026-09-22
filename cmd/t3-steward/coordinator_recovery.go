package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
			incident, err = c.reconcileRecoveryLiveness(ctx, run, incident, attempt, records, config, now)
			if err != nil {
				return err
			}
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

func (c coordinatorSupervision) reconcileRecoveryLiveness(
	ctx context.Context,
	run domain.WorkflowRun,
	incident domain.ReviewIncident,
	attempt domain.Attempt,
	records sqlite.CoordinatorRecords,
	config domain.RecoveryConfig,
	now time.Time,
) (domain.ReviewIncident, error) {
	if incident.Recovery == nil || incident.State != domain.IncidentOpen ||
		incident.Recovery.State == domain.RecoveryNeedsHuman || incident.Recovery.State == domain.RecoveryResolved {
		return incident, nil
	}
	waitReason, err := c.recoverySupportedWait(ctx, run.ID, incident, attempt, records)
	if err != nil {
		return domain.ReviewIncident{}, err
	}
	eventID := "supervision-event:recovery-watchdog:" + recoveryDigest(incident.ID)[:24]
	event := backlog.SupervisionEvent{
		ID: eventID, RunID: run.ID, Kind: backlog.TriggerTaskJudgmentRequired,
		Reason: "the recovery watchdog found an open episode without durable progress",
		TaskID: attempt.TaskID, AttemptID: attempt.ID, IncidentID: incident.ID,
		GraphRevision: run.GraphRevision, OccurredAt: now.UTC(),
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return domain.ReviewIncident{}, err
	}
	result, err := c.store.ReconcileRecoveryWatchdog(ctx, sqlite.RecoveryWatchdogRequest{
		RunID: run.ID, IncidentID: incident.ID, EventID: eventID, EscalationEventID: incident.SourceEventID,
		ExpectedIncidentRevision: incident.Revision, StalledAfter: config.StalledAfter,
		ProgressObserved: attempt.Progress.Terminal() && incident.Recovery.CurrentAttemptID != incident.SourceAttemptID,
		WaitReason:       waitReason, EventRecord: raw, ObservedAt: now,
	})
	if err != nil {
		return domain.ReviewIncident{}, err
	}
	return result.Incident, nil
}

func (c coordinatorSupervision) recoverySupportedWait(
	ctx context.Context,
	runID string,
	incident domain.ReviewIncident,
	attempt domain.Attempt,
	records sqlite.CoordinatorRecords,
) (string, error) {
	waits, err := c.store.ListTaskWaits(ctx)
	if err != nil {
		return "", err
	}
	for _, wait := range waits {
		if wait.WorkflowRunID != runID || wait.AttemptID != attempt.ID {
			continue
		}
		if wait.Live() {
			return "repair attempt is deliberately parked on a task-bound external wait", nil
		}

	}
	for _, assignment := range records.Assignments {
		if assignment.AttemptID != attempt.ID {
			continue
		}
		if assignment.State == domain.AssignmentOffered || assignment.State == domain.AssignmentClaimed {
			return "repair attempt has an active executor assignment", nil
		}
	}
	if c.activations.Store != nil {
		state, err := c.activations.Store.LoadSupervisionActivationState(ctx, runID)
		if err == nil && (state.Activation.IncidentID == "" || state.Activation.IncidentID == incident.ID) {
			if assignment, ok := activationAssignmentOf(records, state.Activation); ok &&
				(assignment.State == domain.AssignmentOffered || assignment.State == domain.AssignmentClaimed) {
				return "repair diagnosis has an active supervision assignment", nil
			}
			if state.Activation.State == "" || state.Activation.State == domain.ActivationIdle ||
				state.Activation.State == domain.ActivationPendingDispatch {
				blocked, reason, blockErr := c.recoveryDispatchBlocked(ctx, runID, incident.ID, state, records)
				if blockErr != nil {
					return "", blockErr
				}
				if blocked {
					return reason, nil
				}
			}
		}
		if err != nil && !errors.Is(err, backlog.ErrSupervisionNotConfigured) {
			return "", err
		}
	}
	return "", nil
}

func (c coordinatorSupervision) recoveryDispatchBlocked(
	ctx context.Context,
	runID string,
	incidentID string,
	state backlog.SupervisionActivationState,
	records sqlite.CoordinatorRecords,
) (bool, string, error) {
	pendingRepair := false
	for _, event := range repairSupervisionEvents(state.Pending) {
		if event.IncidentID == incidentID {
			pendingRepair = true
			break
		}
	}
	if !pendingRepair {
		return false, "", nil
	}
	var route domain.ProviderRoute
	for _, run := range records.WorkflowRuns {
		if run.ID == runID && run.Supervision != nil && run.Supervision.Config.Recovery != nil {
			route = run.Supervision.Config.Recovery.Route
			break
		}
	}
	if route.ProviderInstanceID == "" || route.QuotaPoolID == "" {
		return false, "", nil
	}
	quotaOpen := false
	for _, pool := range records.QuotaPools {
		if pool.ID != route.QuotaPoolID {
			continue
		}
		if pool.Admission != domain.AdmissionOpen ||
			(pool.MaxConcurrent > 0 && pool.ActiveAssignments >= pool.MaxConcurrent) {
			return true, "repair trigger is durably queued behind a closed or exhausted quota pool", nil
		}
		quotaOpen = true
		break
	}
	if !quotaOpen || c.workers == nil {
		return false, "", nil
	}
	workers, err := c.workers(ctx)
	if err != nil {
		return false, "", err
	}
	now := time.Now().UTC()
	if c.now != nil {
		now = c.now().UTC()
	}
	capacity := make([]domain.WorkerSnapshot, 0, len(workers))
	for _, worker := range workers {
		available, capacityErr := c.store.ExecutorSlotAvailable(ctx, worker.WorkerID, now)
		if capacityErr != nil {
			if errors.Is(capacityErr, sqlite.ErrExecutorCapacityEvidence) {
				continue
			}
			return false, "", capacityErr
		}
		if available {
			capacity = append(capacity, worker)
		}
	}
	_, err = backlog.PlaceActivation(backlog.ActivationPlacementRequest{
		Route: route, Workers: capacity, Epoch: c.settings.CoordinatorEpoch, Now: now,
		Admission: backlog.WorkerAdmissionPolicy{OpenQuotaPools: map[string]struct{}{route.QuotaPoolID: {}}},
	})
	if errors.Is(err, backlog.ErrActivationUnplaceable) {
		return true, "repair trigger is durably queued without a capable worker executor slot", nil
	}
	if err != nil {
		return false, "", err
	}
	return false, "", nil
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
