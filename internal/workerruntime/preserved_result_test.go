package workerruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// preservedRetryFixture is a finished turn whose first collection captured
// the result and then failed to publish it, which is the moment a collection
// retry starts from.
func preservedRetryFixture(t *testing.T) *collectionFixture {
	t.Helper()
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	f.driver.Publisher = &collectionTransientPublisher{CustodyStore: f.custody, failures: 1}
	if err := f.runtime.collect(context.Background(), "assignment-1"); err == nil {
		t.Fatal("the first publication did not fail")
	}
	if record := f.record(t); record.Phase != PhaseCollecting || record.Failure != "" {
		t.Fatalf("first collection = %+v", record)
	}
	if _, err := os.Stat(filepath.Join(f.driver.workspacePath(f.pkg), preservedResultName)); err != nil {
		t.Fatalf("the first collection recorded no preserved result: %v", err)
	}
	return f
}

func TestCollectionRetryReusesPreservedResultWhenDigestMatches(t *testing.T) {
	f := preservedRetryFixture(t)
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatal(err)
	}
	record := f.record(t)
	if record.Phase != PhaseCompleted || record.Failure != "" {
		t.Fatalf("retry did not complete from the preserved workspace: %+v", record)
	}
	// No new turn ran: the thread was read, never started again. Verification
	// still ran once per collection, as it always has.
	if f.process.calls != 2 {
		t.Fatalf("verification runs = %d, want 2", f.process.calls)
	}
	pending, err := f.custody.PendingUploadByPurpose("result")
	if err != nil || pending == nil {
		t.Fatalf("no result in custody: %v", err)
	}
}

func TestCollectionRetryFailsWhenPreservedDigestMismatches(t *testing.T) {
	f := preservedRetryFixture(t)
	if err := os.WriteFile(filepath.Join(f.workspace, "answer.txt"), []byte("written after the turn ended"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatal(err)
	}
	record := f.record(t)
	if record.Phase != PhaseFailed || !strings.HasPrefix(record.Failure, "preserved result digest mismatch: ") ||
		!strings.Contains(record.Failure, `"answer.txt"`) {
		t.Fatalf("mismatch did not fail the attempt: %+v", record)
	}
	if got := domain.ClassifyFailure(record.Failure); got != (domain.FailureClassification{Class: domain.FailureInfrastructure, Code: domain.ReasonPreservedResultMismatch}) {
		t.Fatalf("classification = %s", got)
	}
	// The changed workspace was never captured as the turn's result.
	if f.process.calls != 1 {
		t.Fatalf("verification ran on the changed workspace: %d", f.process.calls)
	}
}

func TestCollectionRetryFailsWhenPreservedWorkspaceIsMissing(t *testing.T) {
	f := preservedRetryFixture(t)
	if err := os.RemoveAll(f.workspace); err != nil {
		t.Fatal(err)
	}
	// The driver refuses on its own, whatever the runtime checked first.
	err := f.driver.verifyPreservedResult(context.Background(), f.pkg, f.workspace, "turn-1")
	var preserved *PreservedResultError
	if !errors.As(err, &preserved) || !preserved.Missing {
		t.Fatalf("verify = %v", err)
	}
	if got := domain.ClassifyFailure(err.Error()); got != (domain.FailureClassification{Class: domain.FailureInfrastructure, Code: domain.ReasonWorkspaceMissing}) {
		t.Fatalf("classification = %s", got)
	}
	_ = f.runtime.collect(context.Background(), "assignment-1")
	record := f.record(t)
	if !strings.Contains(record.Failure, "workspace is missing; outputs cannot be collected") ||
		(record.Phase != PhaseFailed && record.Phase != PhaseCompleted) {
		t.Fatalf("missing workspace did not fail the attempt: %+v", record)
	}
	if f.process.calls != 1 {
		t.Fatalf("verification ran without a workspace: %d", f.process.calls)
	}
}

func TestPreservedResultOfAnEarlierTurnIsReplaced(t *testing.T) {
	f := preservedRetryFixture(t)
	path := filepath.Join(f.driver.workspacePath(f.pkg), preservedResultName)
	if err := os.WriteFile(filepath.Join(f.workspace, "answer.txt"), []byte("the next turn's answer"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.driver.verifyPreservedResult(context.Background(), f.pkg, f.workspace, "turn-2"); err != nil {
		t.Fatalf("a later turn was compared with an earlier one: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the earlier turn's record was kept: %v", err)
	}
	f.driver.recordPreservedResult(context.Background(), f.pkg, f.workspace, "turn-2")
	recorded, found, err := readPreservedResult(path)
	if err != nil || !found || recorded.Turn != "turn-2" || recorded.Identity != f.pkg.Identity {
		t.Fatalf("record = %+v, %t, %v", recorded, found, err)
	}
	if err := f.driver.verifyPreservedResult(context.Background(), f.pkg, f.workspace, "turn-2"); err != nil {
		t.Fatalf("unchanged workspace refused: %v", err)
	}
}

func TestPreservedResultRecordThatCannotBeTrustedIsRefused(t *testing.T) {
	f := preservedRetryFixture(t)
	path := filepath.Join(f.driver.workspacePath(f.pkg), preservedResultName)
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := f.driver.verifyPreservedResult(context.Background(), f.pkg, f.workspace, "turn-1")
	var preserved *PreservedResultError
	if !errors.As(err, &preserved) || preserved.Missing {
		t.Fatalf("unreadable record = %v", err)
	}
	other := f.pkg
	other.Identity.AssignmentEpoch++
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	f.driver.recordPreservedResult(context.Background(), f.pkg, f.workspace, "turn-1")
	if err := os.Rename(path, filepath.Join(f.driver.workspacePath(other), preservedResultName)); err != nil {
		if err := os.MkdirAll(f.driver.workspacePath(other), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path, filepath.Join(f.driver.workspacePath(other), preservedResultName)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.driver.verifyPreservedResult(context.Background(), other, f.workspace, "turn-1"); !errors.As(err, &preserved) {
		t.Fatalf("another execution's record = %v", err)
	}
}
