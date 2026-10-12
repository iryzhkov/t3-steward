package backlog

import (
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestGroupProgress(t *testing.T) {
	records, now := progressFixture()
	for i := range records.WorkflowRuns {
		records.WorkflowRuns[i].Group = "wave-1"
	}
	records.WorkflowRuns[0].Group = "other"
	records.WorkflowRuns[4].UpdatedAt = now.Add(-72 * time.Hour)
	records.Tasks[4].FixLoop = &domain.FixLoopTask{Name: "repair", Round: 1, MaxRounds: 1, Kind: "review"}
	records.ReviewRounds[0].Reviewers[0].Verdict.Verdict = "changes-requested"
	records.ReviewRounds[0].Reviewers = append(records.ReviewRounds[0].Reviewers, records.ReviewRounds[0].Reviewers[0])
	records.ReviewRounds[0].Reviewers[1].Verdict = nil
	doc, err := BuildProgress(records, records.ReviewRounds, true, nil, ProgressFilter{Group: "wave-1"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Group.Total != 5 || doc.Group.Done != 3 || doc.Group.Open != 2 || doc.Group.NeedsAttention != 2 {
		t.Fatalf("%+v", doc.Group)
	}
	var out strings.Builder
	if err := RenderProgress(&out, doc); err != nil {
		t.Fatal(err)
	}
	if len(strings.Split(strings.TrimSpace(out.String()), "\n")) != 6 || !strings.Contains(out.String(), "verdict=changes-requested") {
		t.Fatal(out.String())
	}
	raw, _ := json.Marshal(doc)
	if !strings.Contains(string(raw), `"needsAttention":2`) {
		t.Fatal(string(raw))
	}
	empty, err := BuildProgress(records, nil, false, nil, ProgressFilter{Group: "empty"}, now)
	if err != nil || empty.Group.Total != 0 {
		t.Fatalf("%+v %v", empty, err)
	}
}
