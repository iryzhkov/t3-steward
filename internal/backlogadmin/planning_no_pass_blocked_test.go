package backlogadmin

import (
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestPlanningExplainNoPassPreservesRecordBlockersAndWording(t *testing.T) {
	blockers := []Blocker{{Code: "no-eligible-worker", Detail: "worker is not accepting backlog", WorkerID: "worker"}}
	e := Explanation{Eligible: true, Summary: "task has 1 blocker(s)", Blockers: append([]Blocker(nil), blockers...)}
	(view{}).addPlanningExplanation(&e, &domain.Attempt{Control: domain.ControlUnassigned})
	if e.Eligible || e.Summary != "task has 1 blocker(s); no planning pass yet since coordinator start" {
		t.Fatalf("blocked task before planning: %#v", e)
	}
	if !reflect.DeepEqual(e.Blockers, blockers) {
		t.Fatalf("record blockers changed: got %#v want %#v", e.Blockers, blockers)
	}
	if strings.Contains(e.Summary, "eligible to start") {
		t.Fatalf("no-pass explanation claims eligibility: %s", e.Summary)
	}
}
