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

func TestRepair1ReviewRecoveryReplacement(t *testing.T) {
	for _, firstTurn := range []string{"turn-1", "", "turn-old"} {
		t.Run("first="+firstTurn, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			thread := *f.control.thread
			thread.TurnID = firstTurn
			message := "original failed observation"
			archive := []byte("original failed archive bytes")
			if _, _, _, err := f.driver.settleCollectedTurn(f.pkg, thread, message, archive, "original failure", ""); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			thread.TurnID = "turn-1"
			if _, _, _, err = f.driver.settleCollectedTurn(f.pkg, thread, "genuine finished", f.control.archive, "", ""); err != nil {
				t.Fatal(err)
			}
			files, err := filepath.Glob(filepath.Join(filepath.Dir(path), "collected-turn*.json"))
			if err != nil {
				t.Fatal(err)
			}
			retained := false
			for _, file := range files {
				raw, e := os.ReadFile(file)
				if e != nil {
					t.Fatal(e)
				}
				if bytes.Equal(raw, before) {
					retained = true
				}
			}
			t.Logf("firstTurn=%q laterTurn=%q snapshots=%d exactOriginalRetained=%v", firstTurn, thread.TurnID, len(files), retained)
			if !retained {
				t.Error("genuine later success deleted original recovery-only evidence")
			}
		})
	}
}

func TestRepair1ReviewRecoveryWriteRefusal(t *testing.T) {
	for _, obstacle := range []string{"conflict", "symlink", "mode"} {
		t.Run(obstacle, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			c := &fix1CountingControl{recordingT3: f.control}
			f.driver.T3 = c
			thread := *c.thread
			if _, _, _, err := f.driver.settleCollectedTurn(f.pkg, thread, "failed", []byte("{}"), "original failure", ""); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var recorded collectedTurn
			if err = json.Unmarshal(before, &recorded); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(recorded)
			if err != nil {
				t.Fatal(err)
			}
			recovery := filepath.Join(filepath.Dir(path), "collected-turn-recovery-"+shortDigest(raw)+".json")
			switch obstacle {
			case "conflict":
				if err = os.WriteFile(recovery, []byte("conflicting evidence"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err = os.Symlink(filepath.Join(t.TempDir(), "absent"), recovery); err != nil {
					t.Fatal(err)
				}
			case "mode":
				if os.Geteuid() == 0 {
					t.Fatal("mode refusal requires ordinary user")
				}
				if err = os.Chmod(filepath.Dir(path), 0500); err != nil {
					t.Fatal(err)
				}
				defer os.Chmod(filepath.Dir(path), 0700)
			}
			err = f.driver.Collect(context.Background(), f.pkg, f.workspace)
			var permanent *permanentCollectionFailure
			after, readErr := os.ReadFile(path)
			pending, pendingErr := f.custody.PendingUploadByPurpose("result")
			t.Logf("obstacle=%s refused=%v unchanged=%v verifies=%d settles=%d pending=%v", obstacle, err != nil, bytes.Equal(before, after), f.process.calls, c.settles, pending != nil)
			if err == nil || errors.As(err, &permanent) || readErr != nil || !bytes.Equal(before, after) || f.process.calls != 0 || c.settles != 0 || pendingErr != nil || pending != nil {
				t.Fatalf("unsupported recovery publication: %v", err)
			}
		})
	}
}

func TestRepair1ReviewUnprovenDirectoryDurability(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("permission boundary requires ordinary user")
	}
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	c := &fix1CountingControl{recordingT3: f.control}
	f.driver.T3 = c
	c.message = FailedMarker + "\noriginal failed turn\n"
	dir := f.driver.workspacePath(f.pkg)
	if err := os.Chmod(dir, 0300); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0700)
	first := f.driver.Collect(context.Background(), f.pkg, f.workspace)
	path := filepath.Join(dir, "collected-turn.json")
	raw, readErr := os.ReadFile(path)
	if first == nil || readErr != nil {
		t.Fatalf("did not reach post-link directory-open failure: first=%v read=%v", first, readErr)
	}
	if p, e := f.custody.PendingUploadByPurpose("result"); e != nil || p != nil {
		t.Fatalf("first refusal published: %v", e)
	}
	reopened, e := os.Open(dir)
	if e == nil {
		reopened.Close()
		t.Fatal("directory unexpectedly readable")
	}
	second := f.driver.Collect(context.Background(), f.pkg, f.workspace)
	p, pendingErr := f.custody.PendingUploadByPurpose("result")
	after, afterErr := os.ReadFile(path)
	t.Logf("firstRefused=%v snapshotVisible=%v directoryStillUnopenable=%v retryErr=%v published=%v settles=%d unchanged=%v", first != nil, readErr == nil, e != nil, second, p != nil, c.settles, bytes.Equal(raw, after))
	if pendingErr != nil || afterErr != nil {
		t.Fatal(pendingErr, afterErr)
	}
	if second == nil || p != nil || c.settles != 0 {
		t.Error("retry authorized custody/settlement from snapshot whose directory durability remains unproven")
	}
}
