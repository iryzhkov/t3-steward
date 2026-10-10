package backlogadmin

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A required host capability that no worker reports is a typed, temporary
// capability-missing: campaign check says accepted_waiting and names it, and
// the campaign is not refused, because the host can still be given it.
func TestViabilityReportsAMissingHostCapabilityAsATemporaryWait(t *testing.T) {
	for _, capability := range []string{
		workerproto.CapabilityAskRelay,
		workerproto.CapabilityCoordinatorClient,
		workerproto.CapabilityHuyangTrusted,
		workerproto.GitPushCapability("t3-steward"),
	} {
		t.Run(capability, func(t *testing.T) {
			v := viabilityView(t, nil)
			task := viabilityTaskRequest()
			task.Capabilities = append(task.Capabilities, capability)
			matrix := v.viability(context.Background(), viabilityCatalog(t), ViabilityRequest{Tasks: []ViabilityTask{task}})
			reason, found := candidateReason(t, matrix, ReasonCapabilityMissing)
			if !found {
				t.Fatalf("no capability-missing finding: %+v", matrix.Tasks[0].Candidates)
			}
			if reason.Permanent {
				t.Fatalf("a missing host capability refused the campaign: %+v", reason)
			}
			if !strings.Contains(reason.Detail, capability) || !strings.Contains(reason.Detail, "host capability") {
				t.Fatalf("detail = %q, want the capability named as a host capability", reason.Detail)
			}
			if matrix.Outcome != ViabilityAcceptedWaiting {
				t.Fatalf("outcome = %q, want %q", matrix.Outcome, ViabilityAcceptedWaiting)
			}
			if len(matrix.PermanentReasons()) != 0 {
				t.Fatalf("permanent reasons = %+v", matrix.PermanentReasons())
			}
		})
	}
}

// The same task is ready once the worker's host reports the capability.
func TestViabilityIsReadyWhenTheWorkerReportsTheHostCapability(t *testing.T) {
	v := viabilityView(t, func(v *view) {
		v.workers[0].Inventory.Capabilities = append(v.workers[0].Inventory.Capabilities, workerproto.CapabilityAskRelay)
	})
	task := viabilityTaskRequest()
	task.Capabilities = append(task.Capabilities, workerproto.CapabilityAskRelay)
	matrix := v.viability(context.Background(), viabilityCatalog(t), ViabilityRequest{Tasks: []ViabilityTask{task}})
	if matrix.Outcome != ViabilityReady {
		t.Fatalf("outcome = %q, reasons %+v", matrix.Outcome, matrix.Tasks[0].Candidates)
	}
}

// A missing capability that is not a host capability stays permanent: no
// snapshot will ever supply what the build or the operator does not.
func TestViabilityKeepsAMissingBuildCapabilityPermanent(t *testing.T) {
	for _, capability := range []string{"cuda", "ask-relay", "git-push-"} {
		v := viabilityView(t, nil)
		task := viabilityTaskRequest()
		task.Capabilities = append(task.Capabilities, capability)
		matrix := v.viability(context.Background(), viabilityCatalog(t), ViabilityRequest{Tasks: []ViabilityTask{task}})
		reason, found := candidateReason(t, matrix, ReasonCapabilityMissing)
		if !found || !reason.Permanent || matrix.Outcome != ViabilityImpossible {
			t.Fatalf("%s: reason %+v found %v outcome %q", capability, reason, found, matrix.Outcome)
		}
	}
}

// WorkerMatchReasons is shared with graph-amendment validation, so the
// permanence it reports is what that validation reads.
func TestWorkerMatchReasonsMarksOnlyHostCapabilitiesTemporary(t *testing.T) {
	v := viabilityView(t, nil)
	inventory := v.workers[0].Inventory
	task := viabilityTaskRequest()
	domainTask := domain.Task{
		ID: task.Name, Name: task.Name, Class: task.Class, Routes: task.Routes,
		Placement: domain.Placement{Capabilities: []string{"cuda", workerproto.CapabilityAskRelay}},
	}
	reasons, err := WorkerMatchReasons(domainTask, task.Project, inventory, v.records.QuotaPools, viabilityNow)
	if err != nil {
		t.Fatal(err)
	}
	permanence := map[string]bool{}
	for _, reason := range reasons {
		if reason.Code != ReasonCapabilityMissing {
			continue
		}
		for _, name := range []string{"cuda", workerproto.CapabilityAskRelay} {
			if strings.Contains(reason.Detail, `"`+name+`"`) {
				permanence[name] = reason.Permanent
			}
		}
	}
	if len(permanence) != 2 || !permanence["cuda"] || permanence[workerproto.CapabilityAskRelay] {
		t.Fatalf("permanence = %v from %+v", permanence, reasons)
	}
}
