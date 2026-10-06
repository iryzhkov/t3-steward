package backlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The worker's gate cache lives in a directory the agent's own account can
// write in the uncontained lane, so a cache record proves nothing by itself.
// The coordinator is the authority instead: it attests which attempts passed
// a gate on a worker, and it checks every cached report against the passing
// report it recorded for the original attempt. Forging a cached pass then
// requires an authentic pass of the same cache key on the same worker.

// gateCacheOrigins lists the attempts whose passing gate report the
// coordinator recorded from worker within age of now, newest first.
func gateCacheOrigins(gate *domain.TaskGate, records sqlite.CoordinatorRecords, worker string, now time.Time, age time.Duration) []string {
	if gate == nil || age <= 0 || worker == "" {
		return nil
	}
	succeeded := map[string]bool{}
	for _, attempt := range records.Attempts {
		if attempt.Progress == domain.ProgressSucceeded && !attempt.IsSupervisionActivation() {
			succeeded[attempt.ID] = true
		}
	}
	type origin struct {
		attempt string
		at      time.Time
	}
	var origins []origin
	seen := map[string]bool{}
	for _, artifact := range records.Artifacts {
		if artifact.Kind != domain.ArtifactGate || artifact.Name != "gate" || artifact.Producer != "worker:"+worker ||
			!succeeded[artifact.AttemptID] || seen[artifact.AttemptID] || artifact.CreatedAt.After(now) || now.Sub(artifact.CreatedAt) > age {
			continue
		}
		seen[artifact.AttemptID] = true
		origins = append(origins, origin{artifact.AttemptID, artifact.CreatedAt})
	}
	sort.Slice(origins, func(i, j int) bool {
		if !origins[i].at.Equal(origins[j].at) {
			return origins[i].at.After(origins[j].at)
		}
		return origins[i].attempt < origins[j].attempt
	})
	if len(origins) > workerproto.MaxGateCacheOrigins {
		origins = origins[:workerproto.MaxGateCacheOrigins]
	}
	if len(origins) == 0 {
		return nil
	}
	ids := make([]string, len(origins))
	for index, origin := range origins {
		ids[index] = origin.attempt
	}
	return ids
}

// corroborateCachedGate refuses a cached gate report unless the coordinator
// itself recorded the passing, uncached report it claims to replay: same
// worker, cache key, tree and completion time.
func (i CoordinatorResultImporter) corroborateCachedGate(ctx context.Context, records sqlite.CoordinatorRecords, attempt domain.Attempt, worker string, artifacts []domain.Artifact, payloads [][]byte) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %w", ErrInvalidGateEvidence, err)
		}
	}()
	var report GateReport
	found := false
	for index, artifact := range artifacts {
		if artifact.Kind == domain.ArtifactGate && artifact.Name == "gate" {
			if report, err = decodeGateEvidence(payloads[index]); err != nil {
				return err
			}
			found = true
		}
	}
	if !found || !report.Cached {
		return nil
	}
	if report.OriginalAttempt == attempt.ID {
		return errors.New("cached gate names its own attempt as the original")
	}
	var original []domain.Artifact
	for _, artifact := range records.Artifacts {
		if artifact.Kind == domain.ArtifactGate && artifact.Name == "gate" && artifact.AttemptID == report.OriginalAttempt {
			original = append(original, artifact)
		}
	}
	if len(original) != 1 {
		return fmt.Errorf("cached gate original attempt %q has %d recorded gate reports, want 1", report.OriginalAttempt, len(original))
	}
	if original[0].Producer != "worker:"+worker {
		return fmt.Errorf("cached gate original attempt %q ran on another worker", report.OriginalAttempt)
	}
	_, file, err := i.Artifacts.Open(ctx, original[0].ID)
	if err != nil {
		return fmt.Errorf("cached gate original report: %w", err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, GateEvidenceMaxBytes+1))
	if err = errors.Join(readErr, file.Close()); err != nil {
		return fmt.Errorf("cached gate original report: %w", err)
	}
	recorded, err := decodeGateEvidence(raw)
	if err != nil {
		return err
	}
	if recorded.Cached || !recorded.Passed || recorded.OriginalAttempt != report.OriginalAttempt || recorded.CacheKey != report.CacheKey ||
		recorded.TreeHash != report.TreeHash || recorded.Worker != report.Worker || !recorded.CompletedAt.Equal(report.CompletedAt) {
		return fmt.Errorf("cached gate does not match the passing report the coordinator recorded for attempt %q", report.OriginalAttempt)
	}
	return nil
}
