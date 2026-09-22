package sqlite

// Schema 22 replaces the scalar supervision inbox cursor as the authority for
// consumption. An acknowledgement belongs to one event and one activation
// purpose, so interleaved reviewer and repair work cannot consume each other.
//
// The INSERT projects legacy consumed rows into the review purpose (the empty
// purpose used by saved reviewer activations). It therefore preserves decisions
// made before this table existed without acknowledging any repair work.
const coordinatorMigrationV22 = `
CREATE TABLE IF NOT EXISTS coordinator_supervision_inbox_ack (
	event_id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	purpose TEXT NOT NULL,
	acknowledged_at TEXT NOT NULL,
	PRIMARY KEY(event_id, purpose)
);
CREATE INDEX IF NOT EXISTS coordinator_supervision_inbox_ack_run_purpose
	ON coordinator_supervision_inbox_ack(run_id, purpose, event_id);
CREATE TRIGGER IF NOT EXISTS release_supervision_inbox_ack AFTER DELETE ON coordinator_workflow_runs
	BEGIN DELETE FROM coordinator_supervision_inbox_ack WHERE run_id = OLD.id; END;
INSERT OR IGNORE INTO coordinator_supervision_inbox_ack(event_id, run_id, purpose, acknowledged_at)
	SELECT id, run_id, '', 'legacy-consumed'
	FROM coordinator_supervision_inbox
	WHERE consumed = 1;
`
