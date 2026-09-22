package sqlite

// Schema 23 retains the first deterministic activation packaging failure for
// one exact offered assignment epoch. Assignment and activation identity remain
// authoritative in their existing lifecycle records.
const coordinatorMigrationV23 = `
CREATE TABLE IF NOT EXISTS coordinator_activation_dispatch_failures (
	id TEXT PRIMARY KEY,
	assignment_id TEXT NOT NULL,
	assignment_epoch INTEGER NOT NULL,
	run_id TEXT NOT NULL,
	activation_id TEXT NOT NULL,
	record TEXT NOT NULL,
	UNIQUE(assignment_id, assignment_epoch)
);
CREATE INDEX IF NOT EXISTS coordinator_activation_dispatch_failures_run
	ON coordinator_activation_dispatch_failures(run_id, activation_id);
CREATE TRIGGER IF NOT EXISTS release_activation_dispatch_failure AFTER DELETE ON coordinator_assignments
	BEGIN DELETE FROM coordinator_activation_dispatch_failures WHERE assignment_id = OLD.id; END;
`
