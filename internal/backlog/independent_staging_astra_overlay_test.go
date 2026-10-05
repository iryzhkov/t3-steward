package backlog

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestIndependentStageDurableMetadata(t *testing.T) {
	for _, kind := range []string{"missing-intent-replay", "late-receipt-corrupt", "late-intent-missing", "late-directory-mode"} {
		t.Run(kind, func(t *testing.T) {
			f, o, req := newStageOwner(t)
			ctx := context.Background()
			var dir string
			if kind == "missing-intent-replay" {
				first, err := o.StageDeclared(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				dir = filepath.Dir(stageFile(t, o, first))
				if err = os.Remove(filepath.Join(dir, "request.json")); err != nil {
					t.Fatal(err)
				}
				fresh, err := NewDeclaredReviewStaging(o.admission, o.store, time.Now)
				if err != nil {
					t.Fatal(err)
				}
				o = fresh
			} else {
				fired := false
				o.fault = func(phase string) error {
					if phase != "receipt-retained" || fired {
						return nil
					}
					fired = true
					entries, err := os.ReadDir(filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace))
					if err != nil {
						return err
					}
					for _, e := range entries {
						if e.Name() != ".locks" {
							dir = filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace, e.Name())
						}
					}
					switch kind {
					case "late-receipt-corrupt":
						p := filepath.Join(dir, "receipt.json")
						if err = os.Chmod(p, 0600); err != nil {
							return err
						}
						if err = os.WriteFile(p, []byte("corrupt receipt"), 0400); err != nil {
							return err
						}
						return os.Chmod(p, 0400)
					case "late-intent-missing":
						return os.Remove(filepath.Join(dir, "request.json"))
					case "late-directory-mode":
						return os.Chmod(dir, 0755)
					}
					return nil
				}
			}
			before, err := f.store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			got, err := o.StageDeclared(ctx, req)
			assertStageNoGraph(t, f, before)
			if err == nil {
				_, buildErr := got.Preparation.Build(got.Admission.Authority, got.Checkpoint, got.Receipt.CreatedAt)
				_, intentErr := os.Stat(filepath.Join(dir, "request.json"))
				receipt, readErr := os.ReadFile(filepath.Join(dir, "receipt.json"))
				info, statErr := os.Stat(dir)
				t.Errorf("accepted damaged durable metadata: build=%v intent=%v receiptBytes=%d read=%v mode=%v stat=%v", buildErr, intentErr, len(receipt), readErr, info.Mode().Perm(), statErr)
			} else {
				t.Logf("refused: %v", err)
			}
		})
	}
}

type independentStagePruneCatalog struct {
	ArtifactCatalog
	expired []domain.Artifact
}

func (c independentStagePruneCatalog) PruneArtifacts(context.Context, time.Time, []string) ([]domain.Artifact, []domain.ArtifactRetentionSkip, error) {
	return c.expired, nil, nil
}
func TestIndependentStagePruneResolvedAlias(t *testing.T) {
	_, o, req := newStageOwner(t)
	first, err := o.StageDeclared(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	target := stageFile(t, o, first)
	root := o.admission.Artifacts.SubmissionRoot
	alias := filepath.Join(root, "review-alias")
	if err = os.Symlink(filepath.Dir(target), alias); err != nil {
		t.Fatal(err)
	}
	a := first.Receipt.Artifacts[0]
	a.StoragePath = "review-alias/" + filepath.Base(target)
	prune := o.admission.Artifacts
	prune.Catalog = independentStagePruneCatalog{ArtifactCatalog: prune.Catalog, expired: []domain.Artifact{a}}
	if _, _, err = prune.Prune(context.Background(), time.Now().Add(time.Hour), nil); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(target); err != nil {
		t.Fatal("resolved alias pruned private stage", err)
	}
	again, err := o.StageDeclared(context.Background(), req)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatal("alias prune disturbed stage", err)
	}
}

func TestIndependentStageProcessLockHelper(t *testing.T) {
	namespace := os.Getenv("M16_PROBE_NAMESPACE")
	if namespace == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	lock, err := childStageLock(ctx, namespace, os.Getenv("M16_PROBE_KEY"))
	if err == nil {
		lock.Close()
		t.Fatal("another process acquired held owner lock")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
func TestIndependentStageProcessLock(t *testing.T) {
	_, o, req := newStageOwner(t)
	first, err := o.StageDeclared(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	namespace := filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace)
	lock, err := childStageLock(context.Background(), namespace, first.Checkpoint.Key())
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestIndependentStageProcessLockHelper$", "-test.v")
	cmd.Env = append(os.Environ(), "M16_PROBE_NAMESPACE="+namespace, "M16_PROBE_KEY="+first.Checkpoint.Key())
	out, childErr := cmd.CombinedOutput()
	closeErr := lock.Close()
	if childErr != nil || closeErr != nil {
		t.Fatalf("process lock %v %v: %s", childErr, closeErr, out)
	}
	fresh, err := NewDeclaredReviewStaging(o.admission, o.store, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	again, err := fresh.StageDeclared(context.Background(), req)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatal("reopened owner changed winner", err)
	}
	t.Logf("separate process excluded; reopened exact replay: %s", out)
}
