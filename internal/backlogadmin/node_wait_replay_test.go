package backlogadmin

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// replayRequestCount is how many request identities the coordinator's carrier
// has protected. Every one of them held the exclusive admin lock while it was
// served and holds a share of a store budgeted at 32 MiB and 4096 rows, pruned
// oldest first, which is the fleet's window for recovering a lost answer.
func replayRequestCount(t *testing.T, root string) int {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(root, "admin-replay.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT count(*) FROM requests").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// Reading the coordinator's node waits is a read, and a read must not spend the
// replay budget that exists to recover lost answers to things that had an
// effect.
//
// This is not a theoretical cost. The wait runner of every host that is not the
// coordinator lists node waits on every tick -- four times a minute per host at
// the default snapshot interval -- and each list took a request identity, the
// coordinator's exclusive admin-replay lock and a row holding the whole answer,
// evicting the submission answers the store is for.
func TestListingNodeWaitsDoesNotSpendTheCoordinatorsReplayBudget(t *testing.T) {
	harness := newRemoteHarness(t, true)
	ctx := context.Background()
	if _, err := harness.client.NodeWait(ctx, NodeWaitOperation{Action: "list", Host: "caller-host", Undelivered: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.client.NodeWait(ctx, NodeWaitOperation{Action: "list-task"}); err != nil {
		t.Fatal(err)
	}
	if rows := replayRequestCount(t, harness.replayRoot); rows != 0 {
		t.Fatalf("listing waits wrote %d rows into the carrier's replay store, want none", rows)
	}

	// A registration is an effect and keeps its replay protection, which is what
	// makes a retry of the same registration replay rather than register twice.
	if _, err := harness.client.NodeWait(ctx, NodeWaitOperation{
		Action: "register", Host: "caller-host",
		Request: domain.NodeWaitRequest{ID: "nw-1", ThreadID: "thread"},
	}); err != nil {
		t.Fatal(err)
	}
	if rows := replayRequestCount(t, harness.replayRoot); rows != 1 {
		t.Fatalf("a registration left %d protected requests, want exactly 1", rows)
	}

	// So does the wake transition the delivering host sends: it moves the
	// coordinator's record and must not be applied twice.
	if _, err := harness.client.NodeWait(ctx, NodeWaitOperation{
		Action: NodeWaitTransitionAction, ID: "nw-1", From: "pending", To: "sending",
	}); err != nil {
		t.Fatal(err)
	}
	if rows := replayRequestCount(t, harness.replayRoot); rows != 2 {
		t.Fatalf("a wake transition left %d protected requests, want 2", rows)
	}
}
