package backlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReviewChildStagingReadAfterReceiptCorruption(t *testing.T) {
	f, o, req := newStageOwner(t)
	db := stagingSQL(t, f)
	fired := false
	var before string
	o.fault = func(phase string) error {
		if phase != "receipt-retained" || fired {
			return nil
		}
		fired = true
		namespace := filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace)
		owners, err := os.ReadDir(namespace)
		if err != nil {
			t.Fatal(err)
		}
		for _, owner := range owners {
			if owner.Name() == ".locks" {
				continue
			}
			files, err := os.ReadDir(filepath.Join(namespace, owner.Name()))
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range files {
				if filepath.Ext(entry.Name()) == ".blob" {
					target := filepath.Join(namespace, owner.Name(), entry.Name())
					if err = os.Chmod(target, 0600); err != nil {
						t.Fatal(err)
					}
					if err = os.WriteFile(target, []byte("changed after receipt"), 0400); err != nil {
						t.Fatal(err)
					}
					before = independentDeclaredTables(t, db)
					return nil
				}
			}
		}
		return errors.New("missing real retained boundary")
	}
	if _, err := o.StageDeclared(context.Background(), req); err == nil || !fired {
		t.Fatal("read after receipt corruption returned preparation", err)
	}
	if before != independentDeclaredTables(t, db) {
		t.Fatal("corrupt prepared stage changed SQL")
	}
}
func TestReviewChildStagingWholeManifestExactBytes(t *testing.T) {
	f, o, req := newStageOwner(t)
	ctx := context.Background()
	result, err := o.StageDeclared(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Receipt.Manifest.Entries) != 2 {
		t.Fatal("fixture did not contain whole two-file manifest")
	}
	files, err := o.childStageFiles(ctx, result.Admission, result.Checkpoint, result.Receipt.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		raw, err := os.ReadFile(filepath.Join(o.admission.Artifacts.SubmissionRoot, file.Artifact.StoragePath))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(raw, file.Bytes) {
			t.Fatal("retained original or generated prompt bytes changed")
		}
	}
	// Original non-criteria input is read through its retained descriptor after
	// allocation. A missing/corrupt source must never omit a pin from success.
	before, err := f.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range f.records.Artifacts {
		if a.Name == "inputs/context.md" {
			target := filepath.Join(o.admission.Artifacts.SubmissionRoot, a.StoragePath)
			if err = os.Chmod(target, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(target, []byte("wrong context"), 0400); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err = o.StageDeclared(ctx, req); err == nil {
		t.Fatal("corrupt non-criteria manifest pin ignored")
	}
	assertStageNoGraph(t, f, before)
}
