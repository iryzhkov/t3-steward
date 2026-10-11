package sqlite

import (
	"context"
	"fmt"
	"time"
)

// CoordinatorAuditPruneBatch bounds each tick's delete, including index updates.
const CoordinatorAuditPruneBatch = 1000

// Keep the kinds read by ID for durable replay or transition evidence:
// admin-command-* (admin_commands/admin_apply), assignment-released
// (activation-offer-superseded), worker-state-* (worker_reconcile),
// assignment-dispatch-* (dispatch), worker-command-* (worker_commands),
// assignment-lease-renewed (worker_commands), artifact-published (artifacts),
// native-question/native-answer (native_input),
// checkpoint-import-rejected, unknown-assignment-recovered,
// schedule-definition-updated (backlog/schedule_definition), and
// operator-reassessment (supervision_dispatch). Generic insertion also checks
// IDs for duplicate appends; that check does not authorize a state transition.
const prunableCoordinatorAuditKinds = `kind NOT GLOB 'admin-command-*'
 AND kind NOT GLOB 'worker-state-*'
 AND kind NOT GLOB 'assignment-dispatch-*'
 AND kind NOT GLOB 'worker-command-*'
 AND kind NOT IN ('artifact-published', 'assignment-released', 'assignment-lease-renewed',
 'native-question', 'native-answer', 'checkpoint-import-rejected',
 'unknown-assignment-recovered', 'schedule-definition-updated', 'operator-reassessment')`

// Stored timestamps are UTC RFC3339Nano. Pad the fractional part to nine
// digits so seconds with and without a fraction compare chronologically.
const coordinatorAuditTime = `(substr(created_at, 1, 19) || '.' ||
 substr(replace(replace(substr(created_at, 20), '.', ''), 'Z', '') || '000000000', 1, 9))`

const coordinatorMigrationV43 = `CREATE INDEX IF NOT EXISTS coordinator_audit_events_retention
 ON coordinator_audit_events(` + coordinatorAuditTime + `)
 WHERE ` + prunableCoordinatorAuditKinds + `;`

// PruneCoordinatorAuditEvents deletes at most one batch strictly older than
// before. The partial index excludes replay evidence, even when it is ancient.
func (s *Store) PruneCoordinatorAuditEvents(ctx context.Context, before time.Time) (int, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM coordinator_audit_events
 WHERE sequence IN (SELECT sequence FROM coordinator_audit_events
 WHERE `+prunableCoordinatorAuditKinds+` AND `+coordinatorAuditTime+` < ?
 ORDER BY `+coordinatorAuditTime+` LIMIT ?)`,
		before.UTC().Format("2006-01-02T15:04:05.000000000"), CoordinatorAuditPruneBatch)
	if err != nil {
		return 0, fmt.Errorf("prune coordinator audit events: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count pruned coordinator audit events: %w", err)
	}
	return int(count), nil
}
