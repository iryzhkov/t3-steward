package backlog

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"path/filepath"
	"testing"
	"time"
)

type interruptedRoleSubmissionStore struct {
	*sqlite.Store
	fail bool
}

func (s *interruptedRoleSubmissionStore) CompleteSubmission(ctx context.Context, key, digest string, at time.Time) (domain.SubmissionRecord, bool, error) {
	if s.fail {
		s.fail = false
		return domain.SubmissionRecord{}, false, errors.New("crash before completion")
	}
	return s.Store.CompleteSubmission(ctx, key, digest, at)
}
func TestRoleSubmissionRecoveryKeepsFirstDurableSelection(t *testing.T) {
	for _, registered := range []bool{false, true} {
		for _, policyMissing := range []bool{false, true} {
			name := map[bool]string{false: "submit", true: "register"}[registered] + map[bool]string{false: "-policy-changed", true: "-policy-missing"}[policyMissing]
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				db, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				store := &interruptedRoleSubmissionStore{Store: db, fail: true}
				resolver := &changingManifestRoles{}
				storage := filepath.Join(t.TempDir(), "storage")
				t.Cleanup(func() { _ = removeIngestedTree(storage) })
				request := DirectorySubmission{BundleDir: roleSubmissionBundle(t), IdempotencyKey: "crash-role", RegisterOnly: registered}
				first := SubmissionService{Store: store, StorageRoot: storage, MaxBytes: 1 << 20, MaxFiles: 16, Roles: resolver}
				if _, err := first.SubmitDirectory(ctx, request); err == nil {
					t.Fatal("crash injection did not fire")
				}
				records, err := db.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if len(records.Tasks) != 1 {
					t.Fatal("first durable task missing")
				}
				if !registered && records.Tasks[0].Routes[0].Model != "chosen" {
					t.Fatal("first durable selection missing")
				}
				before := domain.TaskDigest(records.Tasks[0])
				if policyMissing {
					resolver.err = errors.New("policy unavailable")
				}
				restarted := SubmissionService{Store: store, StorageRoot: storage, MaxBytes: 1 << 20, MaxFiles: 16, Roles: resolver}
				result, err := restarted.SubmitDirectory(ctx, request)
				if err != nil {
					t.Fatalf("cannot recover pending role submission: %v", err)
				}
				if result.Record.State != domain.SubmissionAccepted || !result.Replay {
					t.Fatalf("recovery result: %+v", result)
				}
				records, err = db.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if domain.TaskDigest(records.Tasks[0]) != before {
					t.Fatalf("durable task changed on restart: %+v", records.Tasks[0])
				}
			})
		}
	}
}
