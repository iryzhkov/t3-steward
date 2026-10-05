package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func fix2NoEffects(t *testing.T, f *collectionFixture, c *fix1CountingControl, err error) {
	t.Helper()
	var permanent *permanentCollectionFailure
	p, pendingErr := f.custody.PendingUploadByPurpose("result")
	if err == nil || errors.As(err, &permanent) || pendingErr != nil || p != nil || f.process.calls != 0 || c.settles != 0 {
		t.Fatalf("unsupported durability effects: err=%v pending=%v pendingErr=%v verifies=%d settles=%d", err, p != nil, pendingErr, f.process.calls, c.settles)
	}
}

func TestCollectionFix2ActiveDurabilityRetryReopen(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("ordinary-user permission boundary required")
	}
	for _, successful := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed", true: "successful"}[successful], func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			c := &fix1CountingControl{recordingT3: f.control}
			f.driver.T3 = c
			if !successful {
				c.message = FailedMarker + "\noriginal failed turn\n"
			}
			dir := f.driver.workspacePath(f.pkg)
			path := filepath.Join(dir, "collected-turn.json")
			if err := os.Chmod(dir, 0300); err != nil {
				t.Fatal(err)
			}
			defer os.Chmod(dir, 0700)
			err := f.runtime.collect(context.Background(), "assignment-1")
			fix2NoEffects(t, f, c, err)
			raw, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			for i := 0; i < 2; i++ {
				err = f.runtime.collect(context.Background(), "assignment-1")
				fix2NoEffects(t, f, c, err)
				after, e := os.ReadFile(path)
				if e != nil || !bytes.Equal(raw, after) || f.record(t).Phase != PhaseCollecting {
					t.Fatal("retry changed evidence or phase", e)
				}
			}
			if err = os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			f.reopen(t)
			if err = f.runtime.collect(context.Background(), "assignment-1"); err != nil {
				t.Fatal(err)
			}
			after, e := os.ReadFile(path)
			wantVerifies := 0
			if successful {
				wantVerifies = 1
			}
			if e != nil || !bytes.Equal(raw, after) || f.record(t).Phase != PhaseCompleted || c.settles != 1 || f.process.calls != wantVerifies {
				t.Fatalf("restored recovery: %v phase=%s settles=%d verifies=%d", e, f.record(t).Phase, c.settles, f.process.calls)
			}
			f.reopen(t)
			if err = f.runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			after, e = os.ReadFile(path)
			if e != nil || !bytes.Equal(raw, after) || c.settles != 1 || f.process.calls != wantVerifies {
				t.Fatal("reopen changed exact retained evidence/effects", e)
			}
		})
	}
}

func TestCollectionFix2RecoveryCopyDurabilityRetryReopen(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("ordinary-user permission boundary required")
	}
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	c := &fix1CountingControl{recordingT3: f.control}
	f.driver.T3 = c
	thread := *c.thread
	if _, _, _, err := f.driver.settleCollectedTurn(f.pkg, thread, "failed", []byte("{}"), "original failure", ""); err != nil {
		t.Fatal(err)
	}
	dir := f.driver.workspacePath(f.pkg)
	path := filepath.Join(dir, "collected-turn.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(dir, "collected-turn-recovery-"+shortDigest(raw)+".json")
	if err = os.Chmod(dir, 0300); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0700)
	// Reach the real post-link failure and then the identical-existing shortcut.
	for i := 0; i < 3; i++ {
		if err = privateBytes(copyPath, raw); err == nil {
			t.Fatal("unproven recovery copy authorized")
		}
		copied, e := os.ReadFile(copyPath)
		if e != nil || !bytes.Equal(raw, copied) {
			t.Fatal("visible copy changed", e)
		}
		err = f.runtime.collect(context.Background(), "assignment-1")
		fix2NoEffects(t, f, c, err)
		active, e := os.ReadFile(path)
		if e != nil || !bytes.Equal(active, raw) {
			t.Fatal("source changed", e)
		}
	}
	if err = os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	if err = f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatal(err)
	}
	copied, e := os.ReadFile(copyPath)
	if e != nil || !bytes.Equal(raw, copied) || c.settles != 1 || f.process.calls != 1 || f.record(t).Phase != PhaseCompleted {
		t.Fatalf("restored equal-copy recovery: %v phase=%s", e, f.record(t).Phase)
	}
	var old collectedTurn
	if err = json.Unmarshal(copied, &old); err != nil || !old.RecoveryOnly || old.Identity == nil || *old.Identity != f.pkg.Identity {
		t.Fatal("recovery identity laundered", err)
	}
	f.reopen(t)
	if err = f.runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	copied, e = os.ReadFile(copyPath)
	if e != nil || !bytes.Equal(raw, copied) || c.settles != 1 || f.process.calls != 1 {
		t.Fatal("reopen changed exact recovery/effects", e)
	}
}

func TestCollectionFix2PreservesOriginalJSONAcrossFailures(t *testing.T) {
	for _, nextFailure := range []string{"", "another failure"} {
		t.Run(nextFailure, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			thread := *f.control.thread
			if _, _, _, err := f.driver.settleCollectedTurn(f.pkg, thread, "first", []byte("original archive"), "failed", ""); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			// A valid JSON record need not be the canonical Marshal encoding.
			raw = append([]byte(" \n"), raw...)
			raw = append(raw, []byte(" \n")...)
			if err = os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			thread.TurnID = "later"
			if _, _, _, err = f.driver.settleCollectedTurn(f.pkg, thread, "later", f.control.archive, nextFailure, ""); err != nil {
				t.Fatal(err)
			}
			copied, err := os.ReadFile(filepath.Join(filepath.Dir(path), "collected-turn-recovery-"+shortDigest(raw)+".json"))
			if err != nil || !bytes.Equal(raw, copied) {
				t.Fatal("original JSON bytes not retained", err)
			}
		})
	}
}

func TestCollectionFix2DifferentTurnRetentionRefusals(t *testing.T) {
	for _, obstacle := range []string{"conflict", "symlink", "binding", "permission"} {
		t.Run(obstacle, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			c := &fix1CountingControl{recordingT3: f.control}
			f.driver.T3 = c
			thread := *c.thread
			if _, _, _, err := f.driver.settleCollectedTurn(f.pkg, thread, "first", []byte("{}"), "failed", ""); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
			if obstacle == "binding" {
				var snap collectedTurn
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err = json.Unmarshal(raw, &snap); err != nil {
					t.Fatal(err)
				}
				snap.Identity.DispatchToken = "foreign"
				raw, err = json.Marshal(snap)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			copyPath := filepath.Join(filepath.Dir(path), "collected-turn-recovery-"+shortDigest(raw)+".json")
			switch obstacle {
			case "conflict":
				if err = os.WriteFile(copyPath, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err = os.Symlink(filepath.Join(t.TempDir(), "absent"), copyPath); err != nil {
					t.Fatal(err)
				}
			case "permission":
				if os.Geteuid() == 0 {
					t.Fatal("ordinary-user boundary required")
				}
				if err = os.Chmod(filepath.Dir(path), 0500); err != nil {
					t.Fatal(err)
				}
				defer os.Chmod(filepath.Dir(path), 0700)
			}
			c.thread.TurnID = "later"
			err = f.runtime.collect(context.Background(), "assignment-1")
			fix2NoEffects(t, f, c, err)
			after, e := os.ReadFile(path)
			if e != nil || !bytes.Equal(raw, after) || f.record(t).Phase != PhaseCollecting {
				t.Fatal("refusal altered original evidence/phase", e)
			}
		})
	}
}
