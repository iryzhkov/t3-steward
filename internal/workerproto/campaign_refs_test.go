package workerproto

import (
	"fmt"
	"strings"
	"testing"
)

// The campaign keep list is a complete statement, so it is refused whole. Every
// refusal below would otherwise let a worker delete commits a live campaign
// still needs, which is not a recoverable mistake.
func TestValidateRetainedCampaignRuns(t *testing.T) {
	for name, test := range map[string]struct {
		request SnapshotRequest
		want    string
	}{
		"list without the flag": {
			request: SnapshotRequest{RetainedCampaignRuns: []string{"run-1"}},
			want:    "without the reported flag",
		},
		"above the limit": {
			request: SnapshotRequest{CampaignRefsReported: true, RetainedCampaignRuns: runIDs(MaxRetainedCampaignRuns + 1)},
			want:    "exceed the limit",
		},
		"repeated run": {
			request: SnapshotRequest{CampaignRefsReported: true, RetainedCampaignRuns: []string{"run-1", "run-1"}},
			want:    "repeat",
		},
		"empty identifier": {
			request: SnapshotRequest{CampaignRefsReported: true, RetainedCampaignRuns: []string{""}},
			want:    "not a safe identifier",
		},
		"path traversal": {
			request: SnapshotRequest{CampaignRefsReported: true, RetainedCampaignRuns: []string{".."}},
			want:    "not a safe identifier",
		},
		"path separator": {
			request: SnapshotRequest{CampaignRefsReported: true, RetainedCampaignRuns: []string{"run/1"}},
			want:    "not a safe identifier",
		},
		"control character": {
			request: SnapshotRequest{CampaignRefsReported: true, RetainedCampaignRuns: []string{"run\n1"}},
			want:    "not a safe identifier",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidateSnapshotRequest(test.request)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want one naming %q", err, test.want)
			}
		})
	}
}

// A coordinator that reports nothing is not making a statement, and an empty
// statement is a statement: the two must stay distinguishable on the wire.
func TestRetainedCampaignRunsDistinguishSilenceFromEmptiness(t *testing.T) {
	if err := ValidateSnapshotRequest(SnapshotRequest{}); err != nil {
		t.Fatalf("an older coordinator's silence was refused: %v", err)
	}
	if err := ValidateSnapshotRequest(SnapshotRequest{CampaignRefsReported: true}); err != nil {
		t.Fatalf("an empty statement was refused: %v", err)
	}
	valid := SnapshotRequest{
		ParkedReported:       true,
		CampaignRefsReported: true,
		RetainedCampaignRuns: []string{"run-1", "run:rerun:key"},
	}
	if err := ValidateSnapshotRequest(valid); err != nil {
		t.Fatalf("a valid statement was refused: %v", err)
	}
}

func runIDs(count int) []string {
	runs := make([]string, 0, count)
	for index := range count {
		runs = append(runs, fmt.Sprintf("run-%d", index))
	}
	return runs
}
