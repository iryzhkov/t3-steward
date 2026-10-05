package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRepair2ReviewLargeRecoveryReplay(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("ordinary-user permissions required")
	}
	for _, large := range []bool{false, true} {
		t.Run(map[bool]string{false: "small-control", true: "large"}[large], func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			c := &fix1CountingControl{recordingT3: f.control}
			f.driver.T3 = c
			thread := *c.thread
			archive := []byte("{}")
			if large {
				archive = append([]byte("{\"padding\":\""), bytes.Repeat([]byte("x"), 13<<20)...)
				archive = append(archive, []byte("\"}")...)
			}
			if !json.Valid(archive) {
				t.Fatal("archive fixture")
			}
			if _, _, _, err := f.driver.settleCollectedTurn(f.pkg, thread, "original failed", archive, "original failure", ""); err != nil {
				t.Fatal(err)
			}
			dir := f.driver.workspacePath(f.pkg)
			active := filepath.Join(dir, "collected-turn.json")
			raw, err := os.ReadFile(active)
			if err != nil {
				t.Fatal(err)
			}
			recovery := filepath.Join(dir, "collected-turn-recovery-"+shortDigest(raw)+".json")
			if err = os.Chmod(dir, 0300); err != nil {
				t.Fatal(err)
			}
			defer os.Chmod(dir, 0700)
			first := privateBytes(recovery, raw)
			copyRaw, e := os.ReadFile(recovery)
			if first == nil || e != nil || !bytes.Equal(copyRaw, raw) {
				t.Fatalf("post-link fixture: %v %v", first, e)
			}
			if err = os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			// Real collection reopens the journal, sees the earlier failed snapshot,
			// and must adopt the identical recovery copy before genuine success.
			f.reopen(t)
			retry := f.runtime.collect(context.Background(), "assignment-1")
			copyRaw, e = os.ReadFile(recovery)
			if e != nil || !bytes.Equal(copyRaw, raw) {
				t.Fatal("original recovery changed", e)
			}
			pending, pe := f.custody.PendingUploadByPurpose("result")
			t.Logf("large=%v jsonBytes=%d firstRefused=%v retry=%v phase=%s verifies=%d settles=%d published=%v exactRecovery=true", large, len(raw), first != nil, retry, f.record(t).Phase, f.process.calls, c.settles, pending != nil)
			if pe != nil {
				t.Fatal(pe)
			}
			if retry != nil || f.record(t).Phase != PhaseCompleted || f.process.calls != 1 || c.settles != 1 || pending == nil {
				t.Error("restored permissions did not recover identical valid snapshot")
			}
		})
	}
}
