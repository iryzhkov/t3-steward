package backlog

import (
	"strings"
	"testing"
)

func TestManifestFixLoopRequiresRetainedReviewReport(t *testing.T) {
	raw := strings.ReplaceAll(fixLoopManifest, "outputs: [review.md]", "outputs: [verdict.json]")
	raw = strings.ReplaceAll(raw, "verdict_line: review.md", "verdict: verdict.json")
	if _, err := ParseManifest([]byte(raw)); err == nil || !strings.Contains(err.Error(), "retain review.md") {
		t.Fatalf("report-less loop must refuse: %v", err)
	}
}
