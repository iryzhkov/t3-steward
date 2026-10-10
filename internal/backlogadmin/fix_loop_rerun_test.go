package backlogadmin

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestRerunDetachesReusedVerdictGuard(t *testing.T) {
	for _, verdict := range []string{"accept", "changes-requested", ""} {
		name := verdict
		if name == "" {
			name = "missing"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			service, store, _ := rerunFixture(t)
			records, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for i := range records.Tasks {
				switch records.Tasks[i].Name {
				case "inspect":
					records.Tasks[i].ReviewOutput = &domain.ReviewOutput{VerdictLine: "findings.md"}
				case "implement":
					records.Tasks[i].NeedsVerdict = map[string]string{"inspect": "accept"}
				}
			}
			for i := range records.Attempts {
				if records.Attempts[i].TaskID == "inspect" && verdict != "" {
					records.Attempts[i].ReviewVerdict = &domain.ReviewVerdict{Verdict: verdict}
				}
			}
			if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
				t.Fatal(err)
			}
			result, err := service.AmendGraph(ctx, Principal{ID: "operator"}, rerunRequest("verdict-rerun", "implement"))
			if verdict != "accept" {
				if err == nil || !strings.Contains(err.Error(), "verdict") {
					t.Fatalf("unsatisfied reused guard must refuse rerun: result=%+v err=%v", result, err)
				}
				after, loadErr := store.LoadCoordinatorRecords(ctx)
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				if len(after.WorkflowRuns) != len(records.WorkflowRuns) {
					t.Fatal("refused rerun created a run")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, task := range result.Graph.Tasks {
				if task.Name != "implement" {
					continue
				}
				if len(task.Needs) != 0 || len(task.NeedsVerdict) != 0 {
					t.Fatalf("reused producer remains a guarded dependency: %+v", task)
				}
				if len(task.CarriedInputs) != 1 || task.CarriedInputs[0].Producer != "inspect" {
					t.Fatalf("reused producer output was not carried: %+v", task.CarriedInputs)
				}
				return
			}
			t.Fatal("rerun omitted implementation")
		})
	}
}
