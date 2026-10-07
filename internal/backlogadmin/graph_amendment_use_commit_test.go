package backlogadmin

import (
	"context"
	"os"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A3's `campaign rerun --use-commit` prints the reused failed commits from the
// graph amendment answer. The transports project that answer to the rc.115
// shape for strict old clients, which stripped graph.rerunOf.reusedCommits,
// so the receipt never named them. Only a client that knows the field sends
// useCommit; every other request keeps the old shape.
func TestUseCommitAmendmentAnswerKeepsReusedCommits(t *testing.T) {
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), &fullAnswerService{})
	defer stopLocalTransport(t, cancel, done)
	for _, useCommit := range []bool{true, false} {
		got, err := client.AmendGraph(context.Background(), domain.GraphAmendment{ID: "amend", RunID: "r", Operation: "rerun", UseCommit: useCommit})
		if err != nil {
			t.Fatal(err)
		}
		rerun := got.Graph.RerunOf
		if rerun == nil {
			t.Fatalf("useCommit=%t: rerun provenance missing", useCommit)
		}
		if kept := rerun.ReusedCommits != nil && len(*rerun.ReusedCommits) != 0; kept != useCommit {
			t.Fatalf("useCommit=%t: reusedCommits kept=%t", useCommit, kept)
		}
	}
}
