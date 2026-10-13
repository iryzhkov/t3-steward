package sqlite

import "context"

// LoadActiveCoordinatorRecords omits settled-run artifact payloads and retains
// targeted hydration, independently of a tick snapshot.
func (s *Store) LoadActiveCoordinatorRecords(ctx context.Context) (CoordinatorRecords, error) {
	return s.LoadCoordinatorRecords(WithActiveCoordinatorRecords(ctx))
}

type activeCoordinatorRecordsKey struct{}

// WithActiveCoordinatorRecords adapts existing record-reader interfaces for
// coordinator background work. Ordinary command contexts retain full history.
func WithActiveCoordinatorRecords(ctx context.Context) context.Context {
	return context.WithValue(ctx, activeCoordinatorRecordsKey{}, true)
}

func (s *Store) attachArtifactLoaders(records CoordinatorRecords) CoordinatorRecords {
	records.loadRunArtifacts = s.loadRunArtifacts
	records.loadArtifacts = s.LoadArtifacts
	records.loadCommitArtifactPresence = s.loadCommitArtifactPresence
	return records
}

const activeArtifactQuery = `SELECT record FROM coordinator_artifacts
WHERE workflow_run_id IN (
	SELECT id FROM coordinator_workflow_runs
	WHERE COALESCE(json_extract(record, '$.sink.progress'), '')
		NOT IN ('succeeded', 'failed', 'cancelled', 'skipped')
	UNION
	SELECT workflow_run_id FROM (SELECT DISTINCT workflow_run_id FROM coordinator_artifacts) AS artifact_runs
	WHERE NOT EXISTS (
		SELECT 1 FROM coordinator_workflow_runs WHERE id = artifact_runs.workflow_run_id
	)
) ORDER BY id`
