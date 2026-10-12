package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// AuditPruneBatchSize caps each coordinator boundary's delete to 1,000 rows.
const AuditPruneBatchSize = 1000

// These kinds are replay/authority evidence read by ID, rather than merely
// historical observations: admin-command-* (submission/outcome), assignment-
// released (activation-offer-superseded), assignment-dispatch-*,
// assignment-lease-renewed, worker-state-*, artifact-published,
// checkpoint-import-rejected, unknown-assignment-recovered, native-question,
// native-answer, schedule-definition-created, and schedule-definition-updated.
// Keep the predicate shared with the partial index so protected history is
// never scanned by pruning.
const prunableAuditKinds = `kind NOT GLOB 'admin-command-*'
 AND kind NOT GLOB 'assignment-dispatch-*'
 AND kind NOT GLOB 'worker-state-*'
 AND kind NOT IN (
 'assignment-released', 'assignment-lease-renewed', 'artifact-published',
 'checkpoint-import-rejected', 'unknown-assignment-recovered',
 'native-question', 'native-answer', 'schedule-definition-created', 'schedule-definition-updated'
 )`

// V43 adds only an index. UTC RFC3339Nano timestamps have variable fractional
// widths; removing Z makes their lexicographic order chronological, including
// a whole second compared with a fractional instant within the same second.
const coordinatorMigrationV43 = `
CREATE INDEX IF NOT EXISTS coordinator_audit_events_retention
 ON coordinator_audit_events(rtrim(created_at, 'Z'), sequence)
 WHERE ` + prunableAuditKinds + `;`

const coordinatorMigrationV43Downgrade = `DROP INDEX IF EXISTS coordinator_audit_events_retention;`

const auditPruneCandidates = `SELECT sequence FROM coordinator_audit_events
 WHERE ` + prunableAuditKinds + `
 AND rtrim(created_at, 'Z') < ?
 ORDER BY rtrim(created_at, 'Z'), sequence LIMIT ?`

// PruneAuditEvents deletes at most one batch strictly older than cutoff.
// Replay/authority kinds are retained indefinitely. It does not load JSON
// records, rewrite other tables, or vacuum the database.
func (s *Store) PruneAuditEvents(ctx context.Context, cutoff time.Time) (int64, error) {
	if cutoff.IsZero() {
		return 0, fmt.Errorf("audit retention cutoff must be nonzero")
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM coordinator_audit_events
 WHERE sequence IN (`+auditPruneCandidates+`)`,
		strings.TrimSuffix(cutoff.UTC().Format(time.RFC3339Nano), "Z"), AuditPruneBatchSize)
	if err != nil {
		return 0, fmt.Errorf("prune coordinator audit events: %w", err)
	}
	return result.RowsAffected()
}
