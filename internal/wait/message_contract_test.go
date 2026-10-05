package wait

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
)

func TestWakeMessageContractOutcomeControls(t *testing.T) {
	for _, progress := range []domain.ProgressState{domain.ProgressSucceeded, domain.ProgressFailed, domain.ProgressCancelled} {
		w := domain.NodeWait{Request: domain.NodeWaitRequest{ID: "nw", Target: domain.NodeRef{RunID: "run-abc", TaskID: "sink"}}, Observation: &domain.NodeObservation{Target: domain.NodeRef{RunID: "run-abc", TaskID: "sink"}, Progress: progress, ExitCode: 0, Reason: "terminal"}}
		text := nodeWakeProse(w)
		t.Log(text)
		if !strings.Contains(text, "inspect") || !strings.Contains(text, "verdict") || !strings.Contains(text, "t3-steward task result run-abc") {
			t.Error("terminal settlement treated as success", text)
		}
		fields := domain.NodeTrailerFields(*w.Observation)
		if fields["progress"] != string(progress) {
			t.Fatal("machine evidence changed")
		}
	}
	for _, status := range []Status{StatusMet, StatusFailed, StatusGaveUp, StatusTimedOut, StatusCancelled} {
		text := WakeMessage([]Wait{{ID: "w", Name: "check", Status: status, OrTimeout: true, LastOutput: "REJECT", LastExit: 0}})
		if !strings.Contains(text, "inspect") || !strings.Contains(text, "verdict") || strings.Contains(text, "Continue the work that was waiting on this.") {
			t.Error("blind continuation", text)
		}
	}
}
