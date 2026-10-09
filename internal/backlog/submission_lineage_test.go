package backlog

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

// A submission made from inside a task records the checked parent on its run;
// a refused claim refuses the submission; a submission without a claim, or
// from a coordinator that cannot check one, is root work.
func TestSubmissionRecordsCheckedLineage(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	started := now.Add(-time.Hour)
	parent := domain.SubmissionParent{RunID: "parent-run", TaskID: "parent-task", AttemptID: "parent-attempt"}
	var asked []domain.SubmissionParent
	resolve := func(_ context.Context, claim domain.SubmissionParent) (*domain.RunLineage, error) {
		asked = append(asked, claim)
		if claim.AttemptID == "forged" {
			return nil, errors.New("submission names parent attempt \"forged\", which this coordinator does not have")
		}
		return &domain.RunLineage{
			ParentRunID: claim.RunID, ParentTaskID: claim.TaskID, ParentAttemptID: claim.AttemptID,
			ParentStartedAt: started, RecordedAt: now,
		}, nil
	}
	cases := []struct {
		name    string
		parent  *domain.SubmissionParent
		resolve func(context.Context, domain.SubmissionParent) (*domain.RunLineage, error)
		want    *domain.RunLineage
		refused bool
	}{
		{name: "nested", parent: &parent, resolve: resolve, want: &domain.RunLineage{
			ParentRunID: "parent-run", ParentTaskID: "parent-task", ParentAttemptID: "parent-attempt",
			ParentStartedAt: started, RecordedAt: now,
		}},
		{name: "root", resolve: resolve},
		{name: "no resolver", parent: &parent},
		{name: "forged", parent: &domain.SubmissionParent{RunID: "r", TaskID: "t", AttemptID: "forged"}, resolve: resolve, refused: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			storage := filepath.Join(t.TempDir(), "storage")
			t.Cleanup(func() { _ = removeIngestedTree(storage) })
			service := &SubmissionService{
				StorageRoot: storage, Store: store, MaxBytes: 1 << 20, MaxFiles: 16,
				Now: func() time.Time { return now }, ResolveLineage: test.resolve,
			}
			result, err := service.SubmitDirectory(ctx, DirectorySubmission{
				IdempotencyKey: "lineage-" + strings.ReplaceAll(test.name, " ", "-"), BundleDir: validBundle(t), Parent: test.parent,
			})
			if test.refused {
				if err == nil || !strings.Contains(err.Error(), "submission lineage") {
					t.Fatalf("err = %v, want the forged parent refused", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			records, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, run := range records.WorkflowRuns {
				if run.ID != result.Record.RunID {
					continue
				}
				switch {
				case test.want == nil && run.Lineage != nil:
					t.Fatalf("root run recorded lineage %+v", run.Lineage)
				case test.want != nil && (run.Lineage == nil || *run.Lineage != *test.want):
					t.Fatalf("run lineage = %+v, want %+v", run.Lineage, test.want)
				}
				return
			}
			t.Fatalf("run %q was not recorded", result.Record.RunID)
		})
	}
	if len(asked) != 2 || asked[0] != parent {
		t.Fatalf("resolver was asked %+v, want the nested and forged claims only", asked)
	}
}
