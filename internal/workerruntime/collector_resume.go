package workerruntime

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/daemon"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// collectorResumeBoundToStop is evaluated again after a restart, before any
// continuation checkpoint or provider effect. An authenticated sender still
// needs current authority for this particular durable stopped attempt.
func collectorResumeBoundToStop(record AttemptRecord, command domain.ThrottleCommand, now time.Time) (bool, string) {
	grant := command.QuotaRecovery
	if grant == nil {
		return false, "collector coordinator resume requires a source-qualified recovery grant"
	}
	if command.CreatedAt.IsZero() || command.CreatedAt.After(now) || !command.CreatedAt.After(grant.StoppedAt) {
		return false, "collector recovery command has invalid post-stop timing"
	}
	if live, why := attemptLive(record, now); !live {
		return false, why
	}
	if record.Phase != PhaseStopped {
		return false, "collector recovery requires a durably stopped attempt"
	}
	if grant.StoppedAt.IsZero() || !grant.Provenance.AcquiredAt.After(grant.StoppedAt) {
		return false, "collector acquisition predates the stop acknowledgement"
	}
	if grant.StopCommandID != "" {
		stop, known := record.ThrottleRequests[grant.StopCommandID]
		ack, accepted := record.ThrottleResults[grant.StopCommandID]
		if !known || !accepted || !ack.Accepted || stop.AttentionStop != nil ||
			(stop.Kind != domain.ThrottleCommandDrain && stop.Kind != domain.ThrottleCommandHardStop) ||
			(ack.Result != domain.ThrottleResultCheckpointed && ack.Result != domain.ThrottleResultStopped) ||
			!ack.AcknowledgedAt.Equal(grant.StoppedAt) || grant.StopObservedSequence != 0 {
			return false, "collector recovery does not match a durable accepted quota stop"
		}
	} else {
		pause := record.LocalThrottle
		if pause == nil || pause.StoppedAt == nil || !pause.StoppedAt.Equal(grant.StoppedAt) ||
			grant.StopObservedSequence <= 0 || record.StopObservedSequence != grant.StopObservedSequence {
			return false, "collector recovery does not match the durable local stop observation"
		}
	}
	if pause := record.LocalThrottle; pause != nil {
		if !pause.RequestedAt.Before(command.CreatedAt) || pause.StoppedAt == nil ||
			pause.StoppedAt.After(grant.StoppedAt) || !grant.Provenance.AcquiredAt.After(*pause.StoppedAt) {
			return false, "a newer or unacknowledged local quota pause vetoes coordinator recovery"
		}
	}
	return true, ""
}

func collectorPauseCoverageCurrent(guard QuotaGuard, route domain.ProviderRoute, pause LocalThrottleRequest) bool {
	collector, ok := guard.(collectorQuotaGuard)
	return ok && collector.CollectorCoveredRoute(route) && pause.CollectorCoverageDigest != "" && pause.CollectorCoverageDigest == collector.CollectorCoverageDigest(route)
}

// CollectorCoverageDigest stays independent of the local producer's liveness.
func (g HostQuotaGuard) CollectorCoverageDigest(route domain.ProviderRoute) string {
	pool, covered := g.Config.CollectorCoverage(route)
	if !covered || !daemon.CollectorPoolCovered(pool) {
		return ""
	}
	return daemon.CollectorRecoveryDigest(g.Config, pool, route)
}

// CoordinatorResumeAllowed verifies the sender's short-lived source authority.
// It cannot mint permission from local credentials or an elapsed reset.
func (g HostQuotaGuard) CoordinatorResumeAllowed(ctx context.Context, command domain.ThrottleCommand) (bool, string, error) {
	pool, covered := g.Config.CollectorCoverage(command.Route)
	if !covered || !daemon.CollectorPoolCovered(pool) {
		if command.QuotaRecovery != nil {
			return false, "collector recovery coverage was removed or changed", nil
		}
		return true, "legacy coordinator authority", nil
	}
	grant := command.QuotaRecovery
	if grant == nil {
		return false, "collector coordinator resume requires a source-qualified recovery grant", nil
	}
	now := g.now()
	p := grant.Provenance
	age := pool.Collector.MaxObservationAge
	if age <= 0 {
		age = daemon.QuotaStaleThreshold(g.Config)
	}
	if grant.EvidenceDigest == "" || !reflect.DeepEqual(grant.EvidenceScope, domain.RecoveryEvidenceScope(pool)) || grant.CoverageDigest == "" || grant.CoverageDigest != daemon.CollectorRecoveryDigest(g.Config, pool, command.Route) ||
		grant.QuotaPoolID != pool.ID || grant.AccountID != pool.AccountID ||
		p.SourceID != pool.Collector.SourceID || p.AcquisitionID == "" || strings.TrimSpace(p.AcquisitionID) != p.AcquisitionID ||
		p.AcquiredAt.IsZero() || p.AcquiredAt.After(now) || command.CreatedAt.Before(p.AcquiredAt) ||
		command.CreatedAt.After(now) || !p.AcquiredAt.After(grant.StoppedAt) ||
		!grant.ExpiresAt.After(now) || grant.ExpiresAt.After(p.AcquiredAt.Add(age)) {
		return false, "collector recovery grant is stale or does not match current coverage and policy", nil
	}
	boundOrigin := false
	for _, origin := range pool.Collector.Origins {
		if origin.WorkerID == p.OriginWorkerID && origin.ProviderInstanceID == grant.OriginProviderInstanceID {
			boundOrigin = true
		}
	}
	if !boundOrigin {
		return false, "collector recovery grant has an unbound source origin", nil
	}
	if g.Buckets != nil {
		states, err := g.Buckets.ListBuckets(ctx)
		if err != nil {
			return false, "local quota state unavailable", err
		}
		if veto, why := daemon.CollectorCoordinatorVeto(g.Config, pool, routeThread(command.Route), states, p.AcquiredAt); veto {
			return false, why, nil
		}
	}
	return true, "current source-qualified coordinator recovery grant", nil
}
