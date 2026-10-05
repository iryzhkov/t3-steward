package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRepair2IndependentLegacySuccessfulWitness(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
	original := collectedTurn{ThreadID: f.pkg.Identity.ThreadID, TurnID: f.control.thread.TurnID, Message: "legacy finished", Archive: f.control.archive}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0400); err != nil {
		t.Fatal(err)
	}
	message, archive, failure, err := f.driver.settleCollectedTurn(f.pkg, *f.control.thread, "unfinished", []byte("{}"), "not finished", "")
	after, e := os.ReadFile(path)
	t.Logf("legacy no identity read-only recovered=%v exactBytes=%v", err == nil && failure == "" && message == original.Message && bytes.Equal(archive, original.Archive), bytes.Equal(raw, after))
	if err != nil || e != nil || failure != "" || message != original.Message || !bytes.Equal(archive, original.Archive) || !bytes.Equal(raw, after) {
		t.Fatal("legacy successful witness refused", err, e)
	}
}

func TestRepair2IndependentUnknownJSONAndBinding(t *testing.T) {
	for _, mutation := range []string{"unknown-fields", "missing-identity", "wrong-thread"} {
		t.Run(mutation, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			c := &fix1CountingControl{recordingT3: f.control}
			f.driver.T3 = c
			thread := *c.thread
			thread.TurnID = ""
			if _, _, _, e := f.driver.settleCollectedTurn(f.pkg, thread, "first", []byte("exact original archive"), "failed", ""); e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
			raw, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			var fields map[string]json.RawMessage
			if e = json.Unmarshal(raw, &fields); e != nil {
				t.Fatal(e)
			}
			if mutation == "missing-identity" {
				delete(fields, "identity")
			}
			if mutation == "wrong-thread" {
				fields["threadId"] = json.RawMessage(`"foreign"`)
			}
			fields["futureRecoveryMetadata"] = json.RawMessage(`{"preserve":["all", 1]}`)
			raw, e = json.MarshalIndent(fields, "", "  ")
			if e != nil {
				t.Fatal(e)
			}
			raw = append(raw, '\n')
			if e = os.WriteFile(path, raw, 0600); e != nil {
				t.Fatal(e)
			}
			if mutation == "unknown-fields" {
				thread.TurnID = "known"
				_, _, failure, e := f.driver.settleCollectedTurn(f.pkg, thread, "later failure", []byte("later archive"), "still failed", "")
				retained, re := os.ReadFile(filepath.Join(filepath.Dir(path), "collected-turn-recovery-"+shortDigest(raw)+".json"))
				t.Logf("empty-to-known later failure exact unknown JSON retained=%v", bytes.Equal(raw, retained))
				if e != nil || re != nil || failure != "still failed" || !bytes.Equal(raw, retained) {
					t.Fatal("retention", e, re)
				}
			} else {
				e = f.runtime.collect(context.Background(), "assignment-1")
				fix2NoEffects(t, f, c, e)
				after, re := os.ReadFile(path)
				t.Logf("binding=%s refused=%v unchanged=%v verification=%d settlement=%d", mutation, e != nil, bytes.Equal(raw, after), f.process.calls, c.settles)
				if re != nil || !bytes.Equal(raw, after) || f.record(t).Phase != PhaseCollecting {
					t.Fatal("binding refusal mutated evidence", re)
				}
			}
		})
	}
}

func TestRepair2IndependentSharedPrivateReplay(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("ordinary user required")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "receipt")
	data := []byte("exact private bytes")
	if e := privateBytes(path, data); e != nil {
		t.Fatal(e)
	}
	if e := os.Chmod(path, 0400); e != nil {
		t.Fatal(e)
	}
	if e := privateBytes(path, data); e != nil {
		t.Fatal("read-only legitimate replay", e)
	}
	if e := os.Chmod(path, 0000); e != nil {
		t.Fatal(e)
	}
	e := privateBytes(path, data)
	if e == nil {
		t.Fatal("unreadable file accepted")
	}
	if e = os.Chmod(path, 0600); e != nil {
		t.Fatal(e)
	}
	if e = privateBytes(path, []byte("foreign")); e == nil {
		t.Fatal("conflict accepted")
	}
	if e = os.Chmod(dir, 0300); e != nil {
		t.Fatal(e)
	}
	defer os.Chmod(dir, 0700)
	for i := 0; i < 2; i++ {
		if e = privateBytes(path, data); e == nil {
			t.Fatal("unproven directory replay accepted")
		}
	}
	if e = os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	if e = privateBytes(path, data); e != nil {
		t.Fatal("restored replay", e)
	}
	link := filepath.Join(dir, "symlink")
	if e = os.Symlink(path, link); e != nil {
		t.Fatal(e)
	}
	if e = privateBytes(link, data); e == nil {
		t.Fatal("symlink adopted")
	}
	raw, re := os.ReadFile(path)
	if re != nil || !bytes.Equal(raw, data) {
		t.Fatal("shared helper changed original", re)
	}
	t.Log("0400 legitimate replay passes; 0000/conflict/0300 repeated/symlink refuse; restored permissions replay passes with exact bytes")
}
