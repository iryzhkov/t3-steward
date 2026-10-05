package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRepair1IndependentLaterTurnRetention(t *testing.T) {
	f := newCollectionFixture(t, 1024, 4096, 100, 2000)
	c := &fix1CountingControl{recordingT3: f.control}
	f.driver.T3 = c
	c.message = FailedMarker + "\noriginal failed turn\n"
	originalArchive := bytes.Clone(c.archive)
	f.driver.Publisher = independentPrefixPublisher{f.custody}
	firstErr := f.runtime.collect(context.Background(), "assignment-1")
	if firstErr == nil || f.record(t).Phase != PhaseCollecting {
		t.Fatalf("transient publication fixture: %v", firstErr)
	}
	path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var snap collectedTurn
	if err = json.Unmarshal(original, &snap); err != nil || !snap.RecoveryOnly || !bytes.Equal(snap.Archive, originalArchive) {
		t.Fatal("failed observation not retained")
	}
	c.thread.TurnID = "turn-2"
	c.message = "genuine later success"
	c.archive = []byte(strings.ReplaceAll(string(c.archive), "turn-1", "turn-2"))
	f.driver.Publisher = f.custody
	secondErr := f.runtime.collect(context.Background(), "assignment-1")
	if f.record(t).Phase != PhaseFailed {
		t.Fatalf("real later archive boundary not hit: %v", secondErr)
	}
	f.reopen(t)
	if err = f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(filepath.Dir(path), "collected-turn*.json"))
	if err != nil {
		t.Fatal(err)
	}
	retained := false
	for _, p := range paths {
		raw, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		if bytes.Equal(raw, original) {
			retained = true
		}
	}
	t.Logf("first=%v second=%v phase=%s originalTurn=turn-1 laterTurn=turn-2 originalArchiveBytes=%d snapshotFiles=%d originalExactJSONRetained=%v verification=%d", firstErr, secondErr, f.record(t).Phase, len(originalArchive), len(paths), retained, f.process.calls)
	if !retained {
		t.Error("original failed recovery evidence discarded when a different turn became successful")
	}
}

func TestRepair1IndependentRecoveryConflictRefuses(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	thread := *f.control.thread
	if _, _, _, err := f.driver.settleCollectedTurn(f.pkg, thread, "failed", []byte("{}"), "original failure", ""); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	recovery := filepath.Join(filepath.Dir(path), "collected-turn-recovery-"+shortDigest(before)+".json")
	foreign := []byte("{}")
	if err = os.WriteFile(recovery, foreign, 0600); err != nil {
		t.Fatal(err)
	}
	_, _, _, err = f.driver.settleCollectedTurn(f.pkg, thread, "genuine success", f.control.archive, "", "")
	after, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	old, e := os.ReadFile(recovery)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("replacement=%v originalUnchanged=%v conflictingRecoveryUnchanged=%v", err, bytes.Equal(before, after), bytes.Equal(foreign, old))
	if err == nil || !bytes.Equal(before, after) || !bytes.Equal(foreign, old) {
		t.Fatal("conflicting recovery replaced or source lost")
	}
	var permanent *permanentCollectionFailure
	if errors.As(err, &permanent) {
		t.Fatal("retention refusal became permanent size failure")
	}
}
