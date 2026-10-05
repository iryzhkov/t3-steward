package backlog

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestIndependentStageCompleteIntentLoss(t *testing.T) {
	f, o, req := newStageOwner(t)
	ctx := context.Background()
	first, err := o.StageDeclared(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(stageFile(t, o, first))
	receipt, err := os.ReadFile(filepath.Join(dir, "receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dir, "request.json")); err != nil {
		t.Fatal(err)
	}
	db := stagingSQL(t, f)
	before := independentDeclaredTables(t, db)
	// A reconstructed owner must refuse missing immutable intent in a COMPLETE stage.
	fresh, err := NewDeclaredReviewStaging(o.admission, o.store, o.now)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fresh.StageDeclared(ctx, req)
	after, readErr := os.ReadFile(filepath.Join(dir, "receipt.json"))
	if readErr != nil || !reflect.DeepEqual(receipt, after) {
		t.Fatal("receipt changed", readErr)
	}
	if before != independentDeclaredTables(t, db) {
		t.Fatal("SQL changed")
	}
	_, intentErr := os.Lstat(filepath.Join(dir, "request.json"))
	t.Logf("replay error=%v intent-restored=%v same-receipt=%v", err, intentErr == nil, reflect.DeepEqual(first.Receipt, got.Receipt))
	if err == nil || !os.IsNotExist(intentErr) {
		t.Fatal("complete stage silently repaired missing immutable request")
	}
}

func TestIndependentStagePostReceiptCustody(t *testing.T) {
	f, o, req := newStageOwner(t)
	db := stagingSQL(t, f)
	fired := false
	before := ""
	o.fault = func(phase string) error {
		if phase != "receipt-retained" || fired {
			return nil
		}
		fired = true
		if _, err := db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.activationId','late-after-receipt') WHERE id=?", f.records.Assignments[0].ID); err != nil {
			t.Fatal(err)
		}
		before = independentDeclaredTables(t, db)
		return nil
	}
	_, err := o.StageDeclared(context.Background(), req)
	if !fired || err == nil {
		t.Fatal("post-receipt custody returned preparation", err)
	}
	if before != independentDeclaredTables(t, db) {
		t.Fatal("refusal changed SQL")
	}
	t.Logf("post-receipt custody refusal: %v", err)
}

func TestIndependentStagePostReceiptMetadata(t *testing.T) {
	_, o, req := newStageOwner(t)
	fired := false
	o.fault = func(phase string) error {
		if phase != "receipt-retained" || fired {
			return nil
		}
		fired = true
		namespace := filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace)
		entries, err := os.ReadDir(namespace)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.Name() == ".locks" {
				continue
			}
			target := filepath.Join(namespace, entry.Name(), "receipt.json")
			if err = os.Chmod(target, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(target, []byte("corrupt complete receipt"), 0400); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}
	got, err := o.StageDeclared(context.Background(), req)
	t.Logf("post-receipt metadata error=%v preparation-returned=%v", err, got.Receipt.Version != "")
	if !fired || err == nil {
		t.Fatal("corrupt durable receipt returned successful preparation", err)
	}
}
