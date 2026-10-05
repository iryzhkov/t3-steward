package backlog

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/review"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestReviewChildStagingPrunePrivateRetention(t *testing.T) {
	f, o, req := newStageOwner(t)
	ctx := context.Background()
	first, err := o.StageDeclared(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	path := stageFile(t, o, first)
	namespace := filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace)
	lock, err := childStageLock(ctx, namespace, first.Checkpoint.Key())
	if err != nil {
		t.Fatal(err)
	}
	// Actual generic pruning while the exclusive owner lock is held must not
	// enumerate/remove this private namespace, regardless of timestamp.
	var protected []string
	stored, err := f.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range stored.WorkflowRuns {
		protected = append(protected, run.ID)
	}
	_, _, err = f.service.Artifacts.Prune(ctx, time.Now().Add(24*time.Hour), protected)
	closeErr := lock.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("active stage pruned", err)
	}
	next, err := o.StageDeclared(ctx, req)
	if err != nil || !reflect.DeepEqual(first, next) {
		t.Fatal("prune changed replay", err)
	}
}
func TestReviewChildStagingPartialConflictAndInputFailure(t *testing.T) {
	for _, kind := range []string{"changed-deadline", "wrong-partial", "source-corrupt", "source-missing"} {
		t.Run(kind, func(t *testing.T) {
			f, o, req := newStageOwner(t)
			ctx := context.Background()
			phase := "request-retained"
			if kind == "source-corrupt" || kind == "source-missing" {
				phase = "allocated"
			}
			fired := false
			o.fault = func(p string) error {
				if p != phase || fired {
					return nil
				}
				fired = true
				if phase == "allocated" {
					for _, a := range f.records.Artifacts {
						if a.ID == f.records.Tasks[0].ReviewRequirements.Criteria.ArtifactID {
							path := filepath.Join(o.admission.Artifacts.SubmissionRoot, a.StoragePath)
							if kind == "source-missing" {
								if err := os.Chmod(filepath.Dir(path), 0700); err != nil {
									t.Fatal(err)
								}
								if err := os.Remove(path); err != nil {
									t.Fatal(err)
								}
							} else {
								if err := os.Chmod(path, 0600); err != nil {
									t.Fatal(err)
								}
								if err := os.WriteFile(path, []byte("corrupt"), 0400); err != nil {
									t.Fatal(err)
								}
							}
						}
					}
					return nil
				}
				return errors.New("real partial intent boundary")
			}
			if _, err := o.StageDeclared(ctx, req); err == nil || !fired {
				t.Fatal("failure boundary not reached", err)
			}
			db := stagingSQL(t, f)
			before := independentDeclaredTables(t, db)
			o.fault = nil
			switch kind {
			case "changed-deadline":
				req.Deadline = req.Deadline.Add(time.Minute)
			case "wrong-partial":
				frozen, found, err := f.store.GetFrozenReviewAuthority(ctx, req.Parent.RunID, req.Parent.TaskID)
				if err != nil || !found {
					t.Fatal(err)
				}
				// Owner identities derive from stored authority and the immutable ID.
				cp, err := f.store.AllocateReviewCheckpoint(ctx, frozen, reviewCheckpoint(req, f.records.Workflows[0].InputManifest.Digest))
				if err != nil {
					t.Fatal(err)
				}
				target := filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace, cp.Key(), "request.json")
				if err = os.Chmod(target, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(target, []byte("wrong partial receipt"), 0400); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := o.StageDeclared(ctx, req); err == nil {
				t.Fatal("partial conflict or corrupt original adopted")
			}
			if before != independentDeclaredTables(t, db) {
				t.Fatal("failed stage changed SQL/native audit")
			}
		})
	}
}
func reviewCheckpoint(req DeclaredChildStageRequest, digest string) review.Checkpoint {
	return review.Checkpoint{ID: req.CheckpointID, HeadCommit: req.HeadCommit, InputDigest: digest}
}
