package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCollectionFix1MissingTurnRetainsRecoveryEvidence(t *testing.T) {
	f := newCollectionFixture(t, 1024, 4096, 100, 2000)
	c := &fix1CountingControl{recordingT3: f.control}
	f.driver.T3 = c
	c.thread.TurnID = ""
	c.message = FailedMarker + "\noriginal task failed\n"
	original := bytes.Clone(c.archive)
	_ = f.runtime.collect(context.Background(), "assignment-1")
	if f.record(t).Phase != PhaseFailed {
		t.Fatal("real archive boundary absent")
	}
	exports := c.exports
	path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot collectedTurn
	if err = json.Unmarshal(before, &snapshot); err != nil || !snapshot.RecoveryOnly || snapshot.TurnID != "" || snapshot.Identity == nil || *snapshot.Identity != f.pkg.Identity || !bytes.Equal(snapshot.Archive, original) || snapshot.Message != c.message {
		t.Fatalf("missing-turn recovery fabricated or lost: %v", err)
	}
	f.reopen(t)
	for i := 0; i < 3; i++ {
		if err = f.runtime.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) || f.record(t).Phase != PhaseCompleted || c.exports != exports || c.settles != 1 || f.process.calls != 0 {
		t.Fatalf("missing-turn evidence/replay: %v", err)
	}
}

func TestCollectionFix1GenuineSuccessPreservesFailedRead(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	thread := *f.control.thread
	failedArchive := []byte("{}")
	if _, _, _, err := f.driver.settleCollectedTurn(f.pkg, thread, "failed read", failedArchive, "not finished", ""); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
	failedRaw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = f.driver.settleCollectedTurn(f.pkg, thread, "genuine finished", f.control.archive, "", ""); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(filepath.Dir(path), "collected-turn-recovery-*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("failed recovery missing: %v %v", files, err)
	}
	recovery, err := os.ReadFile(files[0])
	if err != nil || !bytes.Equal(recovery, failedRaw) {
		t.Fatalf("failed bytes lost on successful observation: %v", err)
	}
	var old collectedTurn
	if err = json.Unmarshal(recovery, &old); err != nil || !old.RecoveryOnly {
		t.Fatal("recovery became successful authority")
	}
	message, archive, failure, err := f.driver.settleCollectedTurn(f.pkg, thread, "", []byte("{}"), "not finished", "")
	if err != nil || message != "genuine finished" || failure != "" || !bytes.Equal(archive, f.control.archive) {
		t.Fatalf("genuine witness recovery failed: %q %q %v", message, failure, err)
	}
}
