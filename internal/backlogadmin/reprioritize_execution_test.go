package backlogadmin

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

func TestExecuteRunReprioritizeIntentAndRestart(t *testing.T) {
	ctx := context.Background()
	store, path, service := reprioritizeSubmissionFixture(t)
	records, err := store.LoadCoordinatorRecords(ctx)
	reprioritizeRequireNoError(t, err)
	// Own setup, preserving A's active-attempt fixture conventions.
	records.Attempts[0].Progress = domain.ProgressBlocked
	records.Attempts[0].Control = domain.ControlUnassigned
	reprioritizeRequireNoError(t, store.SaveCoordinatorRecords(ctx, records))
	request := reprioritizeSubmissionRequest()
	_, err = service.Mutate(ctx, request)
	reprioritizeRequireNoError(t, err)
	intent, trigger, err := planAdminCommand(sqlite.CoordinatorRecords{}, nil, nil, reprioritizeSubmissionCommand(request), adminTestNow.Add(time.Minute))
	reprioritizeRequireNoError(t, err)
	if trigger != nil || intent.Attempt != nil || intent.RelatedAttempts != nil || intent.NewAttempt != nil || intent.WorkflowRun != nil || intent.ExpectedTargetRevision != 3 || intent.State != domain.AdminCommandApplied {
		t.Fatalf("snapshot transition supplied: %#v", intent)
	}
	reprioritizeRequireNoError(t, store.Close())
	store, err = sqlitetest.OpenMigrated(path)
	reprioritizeRequireNoError(t, err)
	defer store.Close()
	service = reprioritizeSubmissionService(t, store)
	service.SetClock(func() time.Time { return adminTestNow.Add(time.Minute) })
	report, err := service.ExecutePendingCommands(ctx)
	reprioritizeRequireNoError(t, err)
	if len(report.Decisions) != 1 || report.Decisions[0].Command.State != domain.AdminCommandApplied {
		t.Fatalf("report: %#v", report)
	}
	after, err := store.LoadCoordinatorRecordsWithAudit(ctx)
	reprioritizeRequireNoError(t, err)
	if after.Attempts[0].AdminImportance == nil || *after.Attempts[0].AdminImportance != 5 || after.Attempts[0].Progress != domain.ProgressBlocked || after.WorkflowRuns[0].Revision != 4 {
		t.Fatalf("state: %#v", after)
	}
	replay, err := service.Mutate(ctx, request)
	reprioritizeRequireNoError(t, err)
	if replay.Command.State != domain.AdminCommandApplied {
		t.Fatal("immutable submission replay")
	}
	again, err := service.ExecutePendingCommands(ctx)
	reprioritizeRequireNoError(t, err)
	unchanged, err := store.LoadCoordinatorRecordsWithAudit(ctx)
	reprioritizeRequireNoError(t, err)
	if len(again.Decisions) != 0 || !reflect.DeepEqual(after, unchanged) {
		t.Fatal("repeat execution wrote state")
	}
}
