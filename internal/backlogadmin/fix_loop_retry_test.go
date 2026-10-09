package backlogadmin

import (
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestProductionRetryMustRefuseLoop(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"implement", "review"} {
		for _, progress := range []domain.ProgressState{domain.ProgressFailed, domain.ProgressCancelled} {
			t.Run(kind+"/"+string(progress), func(t *testing.T) {
				now := adminTestNow
				records := sqlite.CoordinatorRecords{
					WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1",
						Progress: domain.ProgressActive, Revision: 2,
						Sink: &domain.SinkTask{ID: domain.SinkTaskID("run-1"), Name: domain.SinkTaskName,
							Progress: domain.ProgressBlocked, Needs: []string{"task-1", "task-2", "task-other"}}}},
					Tasks: []domain.Task{
						{ID: "task-1", WorkflowID: "workflow-1", Name: kind, Class: domain.TaskClassRequired,
							FixLoop: &domain.FixLoopTask{Name: "repair", Round: 1, MaxRounds: 4, Kind: kind}},
						{ID: "task-2", WorkflowID: "workflow-1", Name: "implement-round-2", Class: domain.TaskClassRequired,
							Needs:   []string{"task-1"},
							FixLoop: &domain.FixLoopTask{Name: "repair", Round: 2, MaxRounds: 4, Kind: "implement"}},
						{ID: "task-other", WorkflowID: "workflow-1", Name: "other", Class: domain.TaskClassRequired},
					},
					Attempts: []domain.Attempt{
						{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1",
							Number: 1, Progress: progress, Control: domain.ControlStopped, Revision: 4},
						{ID: "attempt-2", WorkflowRunID: "run-1", TaskID: "task-2",
							Number: 1, Progress: domain.ProgressSkipped, Control: domain.ControlStopped, Revision: 2},
						{ID: "attempt-other", WorkflowRunID: "run-1", TaskID: "task-other",
							Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 2},
					},
				}
				beforeAttempts := append([]domain.Attempt(nil), records.Attempts...)
				beforeRun := records.WorkflowRuns[0]
				command := domain.AdminCommand{ID: "retry-loop", Kind: domain.AdminCommandRetry,
					TargetType: domain.AdminTargetAttempt, TargetID: "attempt-1", ExpectedRevision: 4,
					Reason: "retry stopped loop task", State: domain.AdminCommandPending, CreatedAt: now}
				application, _, err := planAdminCommand(records, nil, nil, command, now)
				if err != nil {
					t.Fatal(err)
				}
				if application.State != domain.AdminCommandRejected || application.NewAttempt != nil ||
					application.Attempt != nil || application.WorkflowRun != nil || len(application.RelatedAttempts) != 0 {
					t.Fatalf("production retry accepted loop task: %+v", application)
				}
				if !strings.Contains(application.Failure, "campaign rerun from "+kind) {
					t.Fatalf("refusal = %q, want loop rerun remedy", application.Failure)
				}
				if !reflect.DeepEqual(records.Attempts, beforeAttempts) || !reflect.DeepEqual(records.WorkflowRuns[0], beforeRun) {
					t.Fatal("rejected retry changed stopped descendants or open run")
				}
			})
		}
	}
}
