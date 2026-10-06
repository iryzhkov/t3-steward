package domain

import "time"

// WorkflowLedger is a campaign's opt-in to the milestone ledger the coordinator
// writes into Jocasta, as the author declared it. Everything here is authored
// text copied into the ledger header; none of it is a fact Steward verified.
type WorkflowLedger struct {
	// JocastaProject is the Jocasta project the ledger document is created in,
	// at <project>/handoffs/<run id>.md.
	JocastaProject string `json:"jocastaProject"`
	// Plan is the author's reference to the agreed plan, such as a Jocasta
	// path or an exact jocasta:ID@REVISION reference.
	Plan string `json:"plan,omitempty"`
	// Risk is the author's risk classification: low, medium or high.
	Risk string `json:"risk,omitempty"`
	// Acceptance lists the campaign's acceptance criteria, one line each.
	Acceptance []string `json:"acceptance,omitempty"`
}

// LedgerState is the coordinator's durable progress on one run's ledger. It is
// what makes every ledger write idempotent per run and boundary: a boundary in
// Applied is never written again, and one that was written but not recorded is
// recognised by its marker in the document before anything is appended.
type LedgerState struct {
	RunID   string `json:"runId"`
	Project string `json:"project"`
	Path    string `json:"path"`
	// Revision is the Jocasta revision the coordinator's last write produced
	// or adopted. It is evidence, not a fence: every append re-reads the
	// document and fences on the revision it read.
	Revision int64 `json:"revision,omitempty"`
	// Applied lists the boundaries already in the document, in write order;
	// LastBoundary is the newest of them.
	Applied      []string `json:"applied,omitempty"`
	LastBoundary string   `json:"lastBoundary,omitempty"`
	// Closed is set once the run's closing record is written. A closed ledger
	// is never written again.
	Closed bool `json:"closed,omitempty"`
	// Behind marks a ledger with boundaries Jocasta has not accepted yet. The
	// run is never held for it; the coordinator retries at NextAttemptAt.
	Behind        bool       `json:"behind,omitempty"`
	Failures      int        `json:"failures,omitempty"`
	NextAttemptAt *time.Time `json:"nextAttemptAt,omitempty"`
	LastError     string     `json:"lastError,omitempty"`
	UpdatedAt     time.Time  `json:"updatedAt"`
}
