package workerruntime

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestCollectionFix3ForeignRecoveryRefusal(t *testing.T) {
	for _, obstacle := range []string{"oversized-sparse", "same-length-conflict"} {
		t.Run(obstacle, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			c := &fix1CountingControl{recordingT3: f.control}
			f.driver.T3 = c
			if _, _, _, err := f.driver.settleCollectedTurn(f.pkg, *c.thread, "original", []byte("{}"), "failed", ""); err != nil {
				t.Fatal(err)
			}
			active := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
			raw, err := os.ReadFile(active)
			if err != nil {
				t.Fatal(err)
			}
			recovery := filepath.Join(filepath.Dir(active), "collected-turn-recovery-"+shortDigest(raw)+".json")
			foreign := append([]byte(nil), raw...)
			foreign[len(foreign)-1] ^= 1
			if err = os.WriteFile(recovery, foreign, 0600); err != nil {
				t.Fatal(err)
			}
			if obstacle == "oversized-sparse" {
				if err = os.Truncate(recovery, 1<<40); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Stat(recovery)
			if err != nil {
				t.Fatal(err)
			}
			f.reopen(t)
			for i := 0; i < 2; i++ {
				err = f.runtime.collect(context.Background(), "assignment-1")
				fix2NoEffects(t, f, c, err)
				retained, re := os.ReadFile(active)
				after, se := os.Stat(recovery)
				if re != nil || se != nil || !bytes.Equal(raw, retained) || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) || f.record(t).Phase != PhaseCollecting {
					t.Fatal("refusal mutated original or foreign recovery", re, se)
				}
			}
			// Read only the supplied-size prefix; the 1TiB foreign tail is never read.
			file, err := os.Open(recovery)
			if err != nil {
				t.Fatal(err)
			}
			prefix := make([]byte, len(foreign))
			n, err := file.Read(prefix)
			file.Close()
			if err != nil || n != len(prefix) || !bytes.Equal(prefix, foreign) {
				t.Fatal("foreign prefix changed", err)
			}
			t.Logf("%s remains unchanged; repeated collection refuses with zero verification/publication/settlement", obstacle)
		})
	}
}

func TestCollectionFix3StreamedPrivateReplay(t *testing.T) {
	for _, size := range []int{0, 1, 32768, 32769, (17 << 20) + 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "receipt")
			data := bytes.Repeat([]byte("x"), size)
			if err := privateBytes(path, data); err != nil {
				t.Fatal(err)
			}
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if err = privateBytes(path, data); err != nil {
				t.Fatal(err)
			}
			if size > 0 {
				conflicting := append([]byte(nil), data...)
				conflicting[len(conflicting)-1] ^= 1
				if err = privateBytes(path, conflicting); err == nil {
					t.Fatal("late same-size conflict accepted")
				}
			}
			if err = privateBytes(path, append(append([]byte(nil), data...), 'x')); err == nil {
				t.Fatal("length conflict accepted")
			}
			after, err := os.Stat(path)
			raw, re := os.ReadFile(path)
			if err != nil || re != nil || !os.SameFile(before, after) || !bytes.Equal(raw, data) || !before.ModTime().Equal(after.ModTime()) {
				t.Fatal("replay replaced evidence", err, re)
			}
		})
	}
}
