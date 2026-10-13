package main

import (
	"context"
	"reflect"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/daemon"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// newCoordinatorQuotaRecoveryGrant supplies source authority after exact live
// assignment and stop acknowledgement checks. Missing source evidence denies
// this resume while preserving its existing pause projection.
func newCoordinatorQuotaRecoveryGrant(cfg config.Config, store *sqlite.Store) backlog.CoordinatorQuotaRecoveryGrant {
	return func(ctx context.Context, pool domain.QuotaPool, record domain.ThrottleAttemptRecord, snapshot domain.WorkerSnapshot, now time.Time) (*domain.QuotaRecoveryGrant, error) {
		current, covered := cfg.CollectorCoverage(record.Command.Route)
		if !covered || !daemon.CollectorPoolCovered(current) ||
			!snapshot.Connected || snapshot.ObservedAt.IsZero() || snapshot.ObservedAt.After(now) || !snapshot.ValidUntil.After(now) ||
			snapshot.WorkerID != record.Command.WorkerID {
			return nil, nil
		}
		epoch, err := store.CoordinatorEpoch(ctx)
		if err != nil {
			return nil, err
		}
		if snapshot.CoordinatorEpoch != epoch {
			return nil, nil
		}
		// Coverage is taken from loaded config; the active pool adds only capacity.
		if domain.QuotaRecoveryCoverageDigest(pool, record.Command.Route) != domain.QuotaRecoveryCoverageDigest(current, record.Command.Route) {
			return nil, nil
		}
		records, err := store.LoadWorkerExchangeRecords(ctx, snapshot.WorkerID, false)
		if err != nil {
			return nil, err
		}
		live := false
		for _, assignment := range records.Assignments {
			if assignment.ID == record.Command.AssignmentID && assignment.AttemptID == record.AttemptID && assignment.WorkerID == snapshot.WorkerID &&
				assignment.WorkerEpoch == snapshot.WorkerEpoch && assignment.Epoch == record.Command.AssignmentEpoch &&
				assignment.State == domain.AssignmentClaimed && assignment.LeaseExpiresAt.After(now) &&
				assignment.ThreadID == record.Command.ThreadID && reflect.DeepEqual(assignment.Route, record.Command.Route) {
				for _, attempt := range records.Attempts {
					if attempt.ID == record.AttemptID && attempt.AssignmentID == assignment.ID && !attempt.Progress.Terminal() {
						live = true
					}
				}
			}
		}
		currentObservation := false
		for _, observation := range snapshot.Assignments {
			if observation.AssignmentID == record.Command.AssignmentID && observation.AssignmentEpoch == record.Command.AssignmentEpoch && observation.ThreadID == record.Command.ThreadID && observation.WorkspacePath == record.Command.WorkspacePath && observation.Control == domain.ControlPaused {
				currentObservation = true
			}
		}
		if !live || !currentObservation || record.Command.QuotaPoolID != current.ID {
			return nil, nil
		}
		stoppedAt := time.Time{}
		stopID := ""
		sequence := int64(0)
		epochs := record.Command.BucketEpochs
		if record.Delivery == domain.ThrottleDeliveryAcknowledged && record.AcknowledgedAt != nil &&
			(record.Command.Kind == domain.ThrottleCommandDrain || record.Command.Kind == domain.ThrottleCommandHardStop) &&
			(record.Result == domain.ThrottleResultCheckpointed || record.Result == domain.ThrottleResultStopped) &&
			record.Command.AttentionStop == nil {
			stoppedAt = *record.AcknowledgedAt
			stopID = record.Command.ID
		} else {
			for _, observation := range snapshot.Assignments {
				if observation.AssignmentID == record.Command.AssignmentID && observation.AssignmentEpoch == record.Command.AssignmentEpoch &&
					observation.ThreadID == record.Command.ThreadID && observation.WorkspacePath == record.Command.WorkspacePath &&
					observation.Control == domain.ControlPaused && observation.QuotaStop != nil &&
					observation.QuotaStop.ObservedSequence > 0 && observation.QuotaStop.ObservedSequence <= snapshot.Sequence {
					stoppedAt = observation.QuotaStop.StoppedAt
					sequence = observation.QuotaStop.ObservedSequence
					epochs = []domain.QuotaBucketEpoch{{Bucket: observation.QuotaStop.Bucket, Epoch: observation.QuotaStop.Epoch}}
				}
			}
		}
		if stoppedAt.IsZero() || !stoppedAt.Before(now) {
			return nil, nil
		}
		local, err := store.ListBuckets(ctx)
		if err != nil {
			return nil, err
		}
		workers, err := store.LoadWorkerSnapshots(ctx)
		if err != nil {
			return nil, err
		}
		evidenceWorkers := workers
		workers = workerruntime.AuthorizedPlanningSnapshots(cfg.BacklogV2, workers, now)
		states, valid := backlog.FoldQuotaRecoveryStates(current, domain.MergeQuotaObservations(local, workers))
		if !valid {
			return nil, nil
		}
		grant := daemon.IssueCollectorRecoveryGrant(cfg, current, record.Command.Route, epochs, stoppedAt, stopID, sequence, states, now)
		if grant != nil {
			grant.EvidenceDigest = domain.QuotaRecoveryEvidenceDigest(current.AccountID, grant.EvidenceScope, local, evidenceWorkers)
			if grant.EvidenceDigest == "" {
				return nil, nil
			}
		}
		return grant, nil
	}
}
