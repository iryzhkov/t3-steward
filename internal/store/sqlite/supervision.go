package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Campaign supervision persistence: the durable records of the seam review's
// section 6.2, the transactional readiness fence of its section 2, and the
// revision-fenced decision transactions of its section 3.
//
// Nothing here re-implements supervision reasoning. The state machines and the
// readiness predicate are the frozen domain interface
// (internal/domain/supervision.go); this file reads the rows a guard needs
// inside the caller's transaction, hands them to the pure function, and writes
// what the function returned.
//
// Absence of a row in coordinator_supervision is the unsupervised case, which
// is every run that exists today. No empty record is ever created.
const coordinatorMigrationV18 = `
CREATE TABLE IF NOT EXISTS coordinator_supervision(
	run_id TEXT PRIMARY KEY,
	revision INTEGER NOT NULL,
	activation_epoch INTEGER NOT NULL,
	event_cursor INTEGER NOT NULL,
	record TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS coordinator_supervision_activations(
	id TEXT PRIMARY KEY,
	run_id TEXT NOT NULL,
	epoch INTEGER NOT NULL,
	state TEXT NOT NULL,
	dispatch_identity TEXT NOT NULL DEFAULT '',
	record TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS coordinator_supervision_activations_run
	ON coordinator_supervision_activations(run_id, epoch);
CREATE TRIGGER IF NOT EXISTS immutable_closed_supervision_activation
	BEFORE UPDATE ON coordinator_supervision_activations
	WHEN OLD.state = 'closed'
	BEGIN SELECT RAISE(ABORT,'closed supervision activation is immutable'); END;
CREATE TABLE IF NOT EXISTS coordinator_supervision_gates(
	id TEXT PRIMARY KEY,
	run_id TEXT NOT NULL,
	state TEXT NOT NULL,
	graph_revision INTEGER NOT NULL,
	revision INTEGER NOT NULL,
	evidence_snapshot_id TEXT NOT NULL DEFAULT '',
	record TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS coordinator_supervision_gates_run
	ON coordinator_supervision_gates(run_id, state);
CREATE TABLE IF NOT EXISTS coordinator_supervision_decisions(
	id TEXT PRIMARY KEY,
	run_id TEXT NOT NULL,
	gate_id TEXT NOT NULL,
	request_id TEXT NOT NULL,
	activation_epoch INTEGER NOT NULL,
	record TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS coordinator_supervision_decisions_request
	ON coordinator_supervision_decisions(run_id, request_id);
CREATE INDEX IF NOT EXISTS coordinator_supervision_decisions_gate
	ON coordinator_supervision_decisions(run_id, gate_id);
CREATE TRIGGER IF NOT EXISTS immutable_supervision_decision
	BEFORE UPDATE ON coordinator_supervision_decisions
	BEGIN SELECT RAISE(ABORT,'supervision decisions are append-only'); END;
CREATE TABLE IF NOT EXISTS coordinator_supervision_holds(
	id TEXT PRIMARY KEY,
	run_id TEXT NOT NULL,
	owner TEXT NOT NULL,
	scope_kind TEXT NOT NULL,
	state TEXT NOT NULL,
	record TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS coordinator_supervision_holds_run
	ON coordinator_supervision_holds(run_id, state);
CREATE INDEX IF NOT EXISTS coordinator_supervision_holds_owner
	ON coordinator_supervision_holds(run_id, owner);
CREATE TABLE IF NOT EXISTS coordinator_supervision_incidents(
	id TEXT PRIMARY KEY,
	run_id TEXT NOT NULL,
	state TEXT NOT NULL,
	revision INTEGER NOT NULL,
	gate_id TEXT NOT NULL DEFAULT '',
	record TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS coordinator_supervision_incidents_run
	ON coordinator_supervision_incidents(run_id, state);
CREATE TABLE IF NOT EXISTS coordinator_supervision_outbox(
	id TEXT PRIMARY KEY,
	run_id TEXT NOT NULL,
	activation_id TEXT NOT NULL,
	delivery_state TEXT NOT NULL,
	observed_message_id TEXT NOT NULL DEFAULT '',
	record TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS coordinator_supervision_outbox_delivery
	ON coordinator_supervision_outbox(run_id, delivery_state);
CREATE TABLE IF NOT EXISTS coordinator_supervision_inbox(
	id TEXT PRIMARY KEY,
	run_id TEXT NOT NULL,
	sequence INTEGER NOT NULL,
	consumed INTEGER NOT NULL DEFAULT 0,
	record TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS coordinator_supervision_inbox_sequence
	ON coordinator_supervision_inbox(run_id, sequence);
CREATE INDEX IF NOT EXISTS coordinator_supervision_inbox_pending
	ON coordinator_supervision_inbox(run_id, consumed);
CREATE TABLE IF NOT EXISTS coordinator_supervision_receipts(
	request_id TEXT PRIMARY KEY,
	operation TEXT NOT NULL,
	run_id TEXT NOT NULL,
	payload_sha256 TEXT NOT NULL,
	answer TEXT NOT NULL,
	recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS coordinator_supervision_receipts_run
	ON coordinator_supervision_receipts(run_id, operation);
CREATE TRIGGER IF NOT EXISTS immutable_supervision_receipt
	BEFORE UPDATE ON coordinator_supervision_receipts
	BEGIN SELECT RAISE(ABORT,'supervision idempotency receipts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS release_supervision AFTER DELETE ON coordinator_workflow_runs
	BEGIN
		DELETE FROM coordinator_supervision WHERE run_id = OLD.id;
		DELETE FROM coordinator_supervision_activations WHERE run_id = OLD.id;
		DELETE FROM coordinator_supervision_gates WHERE run_id = OLD.id;
		DELETE FROM coordinator_supervision_decisions WHERE run_id = OLD.id;
		DELETE FROM coordinator_supervision_holds WHERE run_id = OLD.id;
		DELETE FROM coordinator_supervision_incidents WHERE run_id = OLD.id;
		DELETE FROM coordinator_supervision_outbox WHERE run_id = OLD.id;
		DELETE FROM coordinator_supervision_inbox WHERE run_id = OLD.id;
		DELETE FROM coordinator_supervision_receipts WHERE run_id = OLD.id;
		DELETE FROM coordinator_retention_pins WHERE owner = 'supervision:' || OLD.id;
	END;
`

var (
	// ErrSupervisionBlocked reports a dispatch the supervision predicate does
	// not admit. It carries the ordered blocker codes so a caller can render
	// which gate or which hold refused, not merely that supervision said no.
	ErrSupervisionBlocked = errors.New("supervision does not admit this task")
	// ErrSupervisionRequestConflict reports the same idempotency key presented
	// with a different payload. The same key with the same payload replays the
	// first answer instead.
	ErrSupervisionRequestConflict = errors.New("supervision request key was already used with a different payload")
	// ErrSupervisionRecordNotFound reports a supervision operation against a
	// run that has no supervision record, gate, hold, activation or incident of
	// that identity.
	ErrSupervisionRecordNotFound = errors.New("supervision record not found")
)

// SupervisionReadSet is the run-local supervision state a projection fences.
// It is part of the sink's read set so a gate, hold, activation or incident
// changing concurrently invalidates a settlement projection.
type SupervisionReadSet struct {
	Record      *domain.SupervisionRecord `json:"record,omitempty"`
	Gates       []domain.Gate             `json:"gates,omitempty"`
	Holds       []domain.Hold             `json:"holds,omitempty"`
	Activations []domain.Activation       `json:"activations,omitempty"`
	Incidents   []domain.ReviewIncident   `json:"incidents,omitempty"`
}

// SupervisionProjection is the read set plus the append-only decision history,
// which explain and status render but no fence compares.
type SupervisionProjection struct {
	SupervisionReadSet
	Decisions []domain.GateDecision `json:"decisions,omitempty"`
}

// supervisionContext is everything a decision transaction reads before it
// decides: the run, its supervision record, the readiness snapshot built from
// the same rows, and the run's effective task set for closure computation.
type supervisionContext struct {
	Run      domain.WorkflowRun
	Record   domain.SupervisionRecord
	Snapshot domain.SupervisionSnapshot
	Tasks    []domain.Task
}

// supervisionStateTx assembles the run's supervision snapshot inside the
// caller's transaction, the way nodeDependenciesTx does for node edges. It is
// the loader every store-side enforcement point and every other lane shares;
// the exported wrapper is (*Store).LoadSupervisionSnapshot.
//
// A run with no supervision row, and a run row that is absent altogether,
// return an unsupervised snapshot, which domain.SupervisionAdmits admits
// unconditionally. That is today's behaviour and it costs two indexed reads.
//
// RouteAvailable is reported true. Provider admission is not observable from
// the store, and the predicate consults it only for a task already blocked by
// a pending or awaiting-review gate, so reporting it true never admits work a
// gate refuses.
func supervisionStateTx(ctx context.Context, tx *sql.Tx, runID string) (domain.SupervisionSnapshot, error) {
	state, supervised, err := supervisionContextTx(ctx, tx, runID, false)
	if err != nil || !supervised {
		return domain.SupervisionSnapshot{RunID: runID}, err
	}
	return state.Snapshot, nil
}

func supervisionContextTx(ctx context.Context, tx *sql.Tx, runID string, withTasks bool) (supervisionContext, bool, error) {
	var state supervisionContext
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflow_runs WHERE id = ?", runID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		// Imported or synthetic records may have no run row. They are
		// unsupervised by construction.
		return state, false, nil
	}
	if err != nil {
		return state, false, fmt.Errorf("load run %q for supervision: %w", runID, err)
	}
	if err := json.Unmarshal(raw, &state.Run); err != nil {
		return state, false, fmt.Errorf("decode run %q for supervision: %w", runID, err)
	}
	record, found, err := loadSupervisionRecordTx(ctx, tx, runID)
	if err != nil {
		return state, false, err
	}
	if !found {
		if state.Run.Supervision == nil {
			return state, false, nil
		}
		// A run created supervised carries its declared record until the first
		// supervision transaction materializes the row.
		record = *state.Run.Supervision
		record.RunID = runID
	}
	state.Record = record
	gates, err := loadSupervisionGatesTx(ctx, tx, runID)
	if err != nil {
		return state, false, err
	}
	holds, err := loadSupervisionHoldsTx(ctx, tx, runID, domain.HoldActive)
	if err != nil {
		return state, false, err
	}
	state.Snapshot = domain.SupervisionSnapshot{
		RunID:          runID,
		GraphRevision:  state.Run.GraphRevision,
		Supervised:     true,
		RunTerminal:    runSettled(state.Run),
		RunCancelled:   state.Run.Progress == domain.ProgressCancelled,
		RouteAvailable: true,
		Gates:          gates,
		Holds:          holds,
	}
	if withTasks {
		tasks, err := loadWorkflowTasksTx(ctx, tx, state.Run.WorkflowID)
		if err != nil {
			return state, false, err
		}
		state.Tasks = domain.TasksForRun(state.Run, tasks)
	}
	return state, true, nil
}

// runSettled reports terminal settlement the way the sink defines it: the run
// itself, or its sink task, reached a terminal progress state.
func runSettled(run domain.WorkflowRun) bool {
	if run.Sink != nil && run.Sink.Progress.Terminal() {
		return true
	}
	return run.Progress.Terminal()
}

// requireSupervisionAdmitsTx is the store-side enforcement point shared by
// offer creation, claim and start authorization, and admin start. It reads the
// snapshot inside the caller's transaction so the answer and the commit are
// the same serialized event.
//
// expectedGraphRevision fences the answer against an amendment that landed
// after the caller's own read; zero means the caller is not fencing, which is
// correct when the caller reads and commits in this same transaction.
func requireSupervisionAdmitsTx(ctx context.Context, tx *sql.Tx, runID, taskID string, expectedGraphRevision int64) error {
	snapshot, err := supervisionStateTx(ctx, tx, runID)
	if err != nil {
		return err
	}
	verdict := domain.SupervisionAdmits(snapshot, domain.SupervisionQuery{
		TaskID:                taskID,
		ExpectedGraphRevision: expectedGraphRevision,
	})
	if verdict.Admitted {
		return nil
	}
	return supervisionBlockedError(taskID, verdict)
}

func supervisionBlockedError(taskID string, verdict domain.SupervisionVerdict) error {
	parts := make([]string, 0, len(verdict.Blockers))
	for _, blocker := range verdict.Blockers {
		detail := string(blocker.Code)
		switch {
		case blocker.GateID != "":
			detail += " (gate " + blocker.GateID + ")"
		case blocker.HoldID != "":
			detail += " (hold " + blocker.HoldID + ")"
		}
		parts = append(parts, detail)
	}
	return fmt.Errorf("%w: task %q is blocked by %s", ErrSupervisionBlocked, taskID, strings.Join(parts, ", "))
}

// ---------------------------------------------------------------------------
// Row loaders and writers
// ---------------------------------------------------------------------------

func loadSupervisionRecordTx(ctx context.Context, tx *sql.Tx, runID string) (domain.SupervisionRecord, bool, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_supervision WHERE run_id = ?", runID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.SupervisionRecord{}, false, nil
	}
	if err != nil {
		return domain.SupervisionRecord{}, false, fmt.Errorf("load supervision record %q: %w", runID, err)
	}
	var record domain.SupervisionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return domain.SupervisionRecord{}, false, fmt.Errorf("decode supervision record %q: %w", runID, err)
	}
	return record, true, nil
}

func saveSupervisionRecordTx(ctx context.Context, tx *sql.Tx, record domain.SupervisionRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode supervision record %q: %w", record.RunID, err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO coordinator_supervision(run_id, revision, activation_epoch, event_cursor, record)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(run_id) DO UPDATE SET
			revision = excluded.revision,
			activation_epoch = excluded.activation_epoch,
			event_cursor = excluded.event_cursor,
			record = excluded.record`,
		record.RunID, record.Revision, record.ActivationEpoch, record.EventCursor, raw)
	if err != nil {
		return fmt.Errorf("save supervision record %q: %w", record.RunID, err)
	}
	return nil
}

func loadSupervisionRowsTx[T any](ctx context.Context, tx *sql.Tx, query, runID string, extra ...any) ([]T, error) {
	args := append([]any{runID}, extra...)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("load supervision rows for %q: %w", runID, err)
	}
	defer rows.Close()
	var records []T
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var record T
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func loadSupervisionGatesTx(ctx context.Context, tx *sql.Tx, runID string) ([]domain.Gate, error) {
	return loadSupervisionRowsTx[domain.Gate](ctx, tx,
		"SELECT record FROM coordinator_supervision_gates WHERE run_id = ? ORDER BY id", runID)
}

func loadSupervisionGateTx(ctx context.Context, tx *sql.Tx, runID, gateID string) (domain.Gate, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx,
		"SELECT record FROM coordinator_supervision_gates WHERE id = ? AND run_id = ?", gateID, runID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Gate{}, fmt.Errorf("%w: gate %q of run %q", ErrSupervisionRecordNotFound, gateID, runID)
	}
	if err != nil {
		return domain.Gate{}, fmt.Errorf("load gate %q: %w", gateID, err)
	}
	var gate domain.Gate
	if err := json.Unmarshal(raw, &gate); err != nil {
		return domain.Gate{}, fmt.Errorf("decode gate %q: %w", gateID, err)
	}
	return gate, nil
}

func saveSupervisionGateTx(ctx context.Context, tx *sql.Tx, gate domain.Gate) error {
	raw, err := json.Marshal(gate)
	if err != nil {
		return fmt.Errorf("encode gate %q: %w", gate.Definition.ID, err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO coordinator_supervision_gates(id, run_id, state, graph_revision, revision, evidence_snapshot_id, record)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			state = excluded.state, graph_revision = excluded.graph_revision,
			revision = excluded.revision, evidence_snapshot_id = excluded.evidence_snapshot_id,
			record = excluded.record`,
		gate.Definition.ID, gate.RunID, gate.State, gate.GraphRevision, gate.Revision, gate.EvidenceSnapshotID, raw)
	if err != nil {
		return fmt.Errorf("save gate %q: %w", gate.Definition.ID, err)
	}
	return nil
}

func loadSupervisionHoldsTx(ctx context.Context, tx *sql.Tx, runID string, state domain.HoldState) ([]domain.Hold, error) {
	if state == "" {
		return loadSupervisionRowsTx[domain.Hold](ctx, tx,
			"SELECT record FROM coordinator_supervision_holds WHERE run_id = ? ORDER BY id", runID)
	}
	return loadSupervisionRowsTx[domain.Hold](ctx, tx,
		"SELECT record FROM coordinator_supervision_holds WHERE run_id = ? AND state = ? ORDER BY id", runID, string(state))
}

func loadSupervisionHoldTx(ctx context.Context, tx *sql.Tx, runID, holdID string) (domain.Hold, bool, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx,
		"SELECT record FROM coordinator_supervision_holds WHERE id = ? AND run_id = ?", holdID, runID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Hold{}, false, nil
	}
	if err != nil {
		return domain.Hold{}, false, fmt.Errorf("load hold %q: %w", holdID, err)
	}
	var hold domain.Hold
	if err := json.Unmarshal(raw, &hold); err != nil {
		return domain.Hold{}, false, fmt.Errorf("decode hold %q: %w", holdID, err)
	}
	return hold, true, nil
}

func saveSupervisionHoldTx(ctx context.Context, tx *sql.Tx, hold domain.Hold) error {
	raw, err := json.Marshal(hold)
	if err != nil {
		return fmt.Errorf("encode hold %q: %w", hold.ID, err)
	}
	owner := string(hold.Owner.Kind) + ":" + hold.Owner.Principal
	_, err = tx.ExecContext(ctx, `
		INSERT INTO coordinator_supervision_holds(id, run_id, owner, scope_kind, state, record)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET state = excluded.state, record = excluded.record`,
		hold.ID, hold.RunID, owner, string(hold.Scope.Kind), string(hold.State), raw)
	if err != nil {
		return fmt.Errorf("save hold %q: %w", hold.ID, err)
	}
	return nil
}

func loadSupervisionActivationsTx(ctx context.Context, tx *sql.Tx, runID string) ([]domain.Activation, error) {
	return loadSupervisionRowsTx[domain.Activation](ctx, tx,
		"SELECT record FROM coordinator_supervision_activations WHERE run_id = ? ORDER BY id", runID)
}

func saveSupervisionActivationTx(ctx context.Context, tx *sql.Tx, activation domain.Activation) error {
	raw, err := json.Marshal(activation)
	if err != nil {
		return fmt.Errorf("encode activation %q: %w", activation.ID, err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO coordinator_supervision_activations(id, run_id, epoch, state, dispatch_identity, record)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			epoch = excluded.epoch, state = excluded.state,
			dispatch_identity = excluded.dispatch_identity, record = excluded.record`,
		activation.ID, activation.RunID, activation.Epoch, string(activation.State), activation.DispatchIdentity, raw)
	if err != nil {
		return fmt.Errorf("save activation %q: %w", activation.ID, err)
	}
	return nil
}

func loadSupervisionIncidentsTx(ctx context.Context, tx *sql.Tx, runID string) ([]domain.ReviewIncident, error) {
	return loadSupervisionRowsTx[domain.ReviewIncident](ctx, tx,
		"SELECT record FROM coordinator_supervision_incidents WHERE run_id = ? ORDER BY id", runID)
}

func saveSupervisionIncidentTx(ctx context.Context, tx *sql.Tx, incident domain.ReviewIncident) error {
	raw, err := json.Marshal(incident)
	if err != nil {
		return fmt.Errorf("encode incident %q: %w", incident.ID, err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO coordinator_supervision_incidents(id, run_id, state, revision, gate_id, record)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			state = excluded.state, revision = excluded.revision, record = excluded.record`,
		incident.ID, incident.RunID, string(incident.State), incident.Revision, incident.GateID, raw)
	if err != nil {
		return fmt.Errorf("save incident %q: %w", incident.ID, err)
	}
	return nil
}

func loadSupervisionDecisionsTx(ctx context.Context, tx *sql.Tx, runID string) ([]domain.GateDecision, error) {
	return loadSupervisionRowsTx[domain.GateDecision](ctx, tx,
		"SELECT record FROM coordinator_supervision_decisions WHERE run_id = ? ORDER BY id", runID)
}

func supervisionReadSetTx(ctx context.Context, tx *sql.Tx, runID string) (*SupervisionReadSet, error) {
	record, found, err := loadSupervisionRecordTx(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	gates, err := loadSupervisionGatesTx(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	holds, err := loadSupervisionHoldsTx(ctx, tx, runID, "")
	if err != nil {
		return nil, err
	}
	activations, err := loadSupervisionActivationsTx(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	incidents, err := loadSupervisionIncidentsTx(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	if !found && len(gates) == 0 && len(holds) == 0 && len(activations) == 0 && len(incidents) == 0 {
		// An unsupervised run contributes nothing to the fenced read set, so a
		// database without supervision hashes exactly as it did before.
		return nil, nil
	}
	read := &SupervisionReadSet{Gates: gates, Holds: holds, Activations: activations, Incidents: incidents}
	if found {
		read.Record = &record
	}
	return read, nil
}

// ---------------------------------------------------------------------------
// Idempotency receipts
// ---------------------------------------------------------------------------

func supervisionPayloadDigest(payload any) (string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode supervision request payload: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func lookupSupervisionReceiptTx(ctx context.Context, tx *sql.Tx, operation, requestID, digest string) (json.RawMessage, bool, error) {
	var storedOperation, storedDigest string
	var answer []byte
	err := tx.QueryRowContext(ctx,
		"SELECT operation, payload_sha256, answer FROM coordinator_supervision_receipts WHERE request_id = ?",
		requestID).Scan(&storedOperation, &storedDigest, &answer)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("load supervision receipt %q: %w", requestID, err)
	}
	if storedOperation != operation || storedDigest != digest {
		return nil, false, fmt.Errorf("%w: request %q first answered %q", ErrSupervisionRequestConflict, requestID, storedOperation)
	}
	return json.RawMessage(answer), true, nil
}

func recordSupervisionReceiptTx(ctx context.Context, tx *sql.Tx, operation, runID, requestID, digest string, answer any, now time.Time) error {
	raw, err := json.Marshal(answer)
	if err != nil {
		return fmt.Errorf("encode supervision receipt answer: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO coordinator_supervision_receipts(request_id, operation, run_id, payload_sha256, answer, recorded_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		requestID, operation, runID, digest, raw, now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("record supervision receipt %q: %w", requestID, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Decision transactions
// ---------------------------------------------------------------------------

// SupervisionDecision is the answer every supervision decision returns. One
// shape for every operation, because every operation answers the same three
// questions: what the record now is, which outstanding offers the decision
// released, and whether this was a replay of an earlier identical request.
type SupervisionDecision struct {
	Replay              bool                      `json:"replay,omitempty"`
	SupervisionRevision int64                     `json:"supervisionRevision"`
	Gate                *domain.Gate              `json:"gate,omitempty"`
	GateDecision        *domain.GateDecision      `json:"gateDecision,omitempty"`
	Hold                *domain.Hold              `json:"hold,omitempty"`
	Activation          *domain.Activation        `json:"activation,omitempty"`
	Incident            *domain.ReviewIncident    `json:"incident,omitempty"`
	Record              *domain.SupervisionRecord `json:"record,omitempty"`
	// ReleasedAssignmentIDs are the offered assignments this decision narrowed
	// out of readiness and released in the same transaction. An assignment that
	// was already claimed is past the start boundary; it is reported in
	// ClaimedTaskIDs and never rolled back.
	ReleasedAssignmentIDs []string `json:"releasedAssignmentIds,omitempty"`
	ClaimedTaskIDs        []string `json:"claimedTaskIds,omitempty"`
	// AttemptRevision is the highest attempt revision this transaction wrote,
	// so the permitted start boundary is observable rather than argued about.
	AttemptRevision int64 `json:"attemptRevision,omitempty"`
}

type supervisionApply func(ctx context.Context, tx *sql.Tx, state supervisionContext) (SupervisionDecision, error)

// supervisionDecisionTx is the one transaction shape every decision uses.
//
// It replays a receipt, loads the supervision state, applies the operation,
// re-reads the state the operation left, releases every outstanding offer the
// new state no longer admits, bumps the supervision revision and writes the
// receipt, all inside the single SQLite writer that also serializes offer and
// claim. That is what makes a decision linearize against them: the store's own
// transaction is the fence, and the claim's compare-and-set on
// assignment_state = 'offered' stays the boundary a released offer must lose.
func (s *Store) supervisionDecisionTx(ctx context.Context, operation, runID, requestID string, payload any, apply supervisionApply) (SupervisionDecision, error) {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(requestID) == "" {
		return SupervisionDecision{}, errors.New("a supervision decision needs a run and a request key")
	}
	digest, err := supervisionPayloadDigest(payload)
	if err != nil {
		return SupervisionDecision{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SupervisionDecision{}, fmt.Errorf("begin supervision %s: %w", operation, err)
	}
	defer tx.Rollback()

	answer, replayed, err := lookupSupervisionReceiptTx(ctx, tx, operation, requestID, digest)
	if err != nil {
		return SupervisionDecision{}, err
	}
	if replayed {
		var decision SupervisionDecision
		if err := json.Unmarshal(answer, &decision); err != nil {
			return SupervisionDecision{}, fmt.Errorf("decode replayed supervision answer %q: %w", requestID, err)
		}
		decision.Replay = true
		return decision, nil
	}

	state, supervised, err := supervisionContextTx(ctx, tx, runID, true)
	if err != nil {
		return SupervisionDecision{}, err
	}
	if !supervised {
		return SupervisionDecision{}, fmt.Errorf("%w: run %q is not supervised", ErrSupervisionRecordNotFound, runID)
	}
	decision, err := apply(ctx, tx, state)
	if err != nil {
		return SupervisionDecision{}, err
	}

	after, err := supervisionStateTx(ctx, tx, runID)
	if err != nil {
		return SupervisionDecision{}, err
	}
	now := s.now().UTC()
	released, claimed, attemptRevision, err := releaseNarrowedOffersTx(ctx, tx, after, now)
	if err != nil {
		return SupervisionDecision{}, err
	}
	decision.ReleasedAssignmentIDs = released
	decision.ClaimedTaskIDs = claimed
	decision.AttemptRevision = attemptRevision

	record := state.Record
	record.RunID = runID
	record.Revision++
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	if decision.Record != nil {
		// The operation rewrote the record; keep its fields and its revision
		// bump together.
		updated := *decision.Record
		updated.RunID, updated.Revision = runID, record.Revision
		if updated.CreatedAt.IsZero() {
			updated.CreatedAt = record.CreatedAt
		}
		updated.UpdatedAt = now
		record = updated
	}
	if err := saveSupervisionRecordTx(ctx, tx, record); err != nil {
		return SupervisionDecision{}, err
	}
	stored := record
	decision.Record = &stored
	decision.SupervisionRevision = record.Revision

	if err := recordSupervisionReceiptTx(ctx, tx, operation, runID, requestID, digest, decision, now); err != nil {
		return SupervisionDecision{}, err
	}
	if err := tx.Commit(); err != nil {
		return SupervisionDecision{}, fmt.Errorf("commit supervision %s: %w", operation, err)
	}
	return decision, nil
}

// releaseNarrowedOffersTx releases every assignment of the run still in state
// offered whose task the supplied snapshot no longer admits, and reports the
// tasks whose attempt is already claimed.
//
// The release is the same compare-and-set the claim path uses
// (UPDATE ... WHERE assignment_state = 'offered'), so a claim racing a hold has
// exactly one winner and the claim's fence remains the start boundary. The
// released row is left attached to its attempt in state released, which is the
// shape CommitAssignmentPlan knows how to re-arm.
func releaseNarrowedOffersTx(ctx context.Context, tx *sql.Tx, snapshot domain.SupervisionSnapshot, now time.Time) ([]string, []string, int64, error) {
	if !snapshot.Supervised {
		return nil, nil, 0, nil
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT assignment.record FROM coordinator_assignments AS assignment
		JOIN coordinator_attempts AS attempt ON attempt.id = assignment.attempt_id
		WHERE attempt.workflow_run_id = ? AND assignment.assignment_state IN (?, ?)
		ORDER BY assignment.id`,
		snapshot.RunID, string(domain.AssignmentOffered), string(domain.AssignmentClaimed))
	if err != nil {
		return nil, nil, 0, fmt.Errorf("load live assignments of run %q: %w", snapshot.RunID, err)
	}
	var live []domain.Assignment
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, nil, 0, err
		}
		var assignment domain.Assignment
		if err := json.Unmarshal(raw, &assignment); err != nil {
			rows.Close()
			return nil, nil, 0, err
		}
		live = append(live, assignment)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, 0, err
	}

	var released, claimed []string
	var attemptRevision int64
	for _, assignment := range live {
		attempt, err := loadAttemptTx(ctx, tx, assignment.AttemptID)
		if err != nil {
			return nil, nil, 0, err
		}
		verdict := domain.SupervisionAdmits(snapshot, domain.SupervisionQuery{TaskID: attempt.TaskID})
		if verdict.Admitted {
			continue
		}
		if assignment.State == domain.AssignmentClaimed {
			// Past the start boundary. v1 reports it and does not roll it back.
			claimed = append(claimed, attempt.TaskID)
			if attempt.Revision > attemptRevision {
				attemptRevision = attempt.Revision
			}
			continue
		}
		next := assignment
		next.State = domain.AssignmentReleased
		next.LeaseExpiresAt = time.Time{}
		next.UpdatedAt = now
		raw, err := json.Marshal(next)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("encode released assignment %q: %w", next.ID, err)
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE coordinator_assignments SET assignment_state = ?, lease_expires_at = '', record = ?
			WHERE id = ? AND assignment_state = ?`,
			next.State, raw, next.ID, string(domain.AssignmentOffered))
		if err != nil {
			return nil, nil, 0, fmt.Errorf("release offered assignment %q: %w", next.ID, err)
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			// A claim won the race inside this same serialized writer. The
			// claim is the fence; the hold reports the started work.
			claimed = append(claimed, attempt.TaskID)
			continue
		}
		expected := attempt.Revision
		attempt.AssignmentID = ""
		attempt.Progress = domain.ProgressReady
		attempt.Control = domain.ControlUnassigned
		attempt.Revision++
		attempt.UpdatedAt = now
		if err := updateAttemptTx(ctx, tx, attempt, expected); err != nil {
			return nil, nil, 0, err
		}
		if attempt.Revision > attemptRevision {
			attemptRevision = attempt.Revision
		}
		released = append(released, next.ID)
	}
	sort.Strings(released)
	sort.Strings(claimed)
	return released, claimed, attemptRevision, nil
}

// supervisionActorCoversRun reports whether the acting capability is scoped to
// this run. An operator principal is authenticated by the admin transport and
// is not epoch-bound; an overseer acts only at the record's current activation
// epoch, which is how a decision from a revoked epoch is fenced out.
func supervisionActorCoversRun(record domain.SupervisionRecord, actor domain.Actor) bool {
	if actor.Kind == domain.ActorOperator {
		return true
	}
	return actor.Kind == domain.ActorOverseer && actor.ActivationEpoch == record.ActivationEpoch
}
