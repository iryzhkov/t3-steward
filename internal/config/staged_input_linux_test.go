//go:build linux

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/testutil"
	"golang.org/x/sys/unix"
)

func TestStagedAbsentInputEventsWithoutTimestampSignal(t *testing.T) {
	for _, kind := range []string{"create/remove", "move in/out", "missing ancestor", "parent move", "unrelated"} {
		t.Run(kind, func(t *testing.T) {
			parent := testutil.RealTempDir(t)
			path := filepath.Join(parent, "optional")
			if kind == "missing ancestor" {
				path = filepath.Join(path, "projection")
			}
			input, err := observeStagedInput(path, 1024, true)
			if err != nil {
				t.Fatalf("path=%s err=%v", path, err)
			}
			defer input.close()
			switch kind {
			case "create/remove", "missing ancestor":
				target := filepath.Join(parent, "optional")
				stagedWrite(t, target, []byte("{}"))
				if err := os.Remove(target); err != nil {
					t.Fatal(err)
				}
			case "move in/out":
				other := filepath.Join(parent, "other")
				stagedWrite(t, other, []byte("{}"))
				if err := os.Rename(other, path); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(path, other); err != nil {
					t.Fatal(err)
				}
			case "parent move":
				if err := os.Rename(parent, parent+".moved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(parent+".moved", parent); err != nil {
					t.Fatal(err)
				}
			case "unrelated":
				other := filepath.Join(parent, "other")
				stagedWrite(t, other, []byte("{}"))
				if err := os.Remove(other); err != nil {
					t.Fatal(err)
				}
			}
			// Model a coarse clock deterministically: make the saved timestamps
			// equal to the final observation. UtimesNano cannot restore ctime.
			// All real identity/ownership observations and queued events remain.
			var final unix.Stat_t
			if err := unix.Stat(parent, &final); err != nil {
				t.Fatal(err)
			}
			last := len(input.dirs) - 1
			input.dirs[last].Mtim = final.Mtim
			input.dirs[last].Ctim = final.Ctim
			err = input.check()
			if kind == "unrelated" {
				if err != nil {
					t.Fatalf("unrelated event refused: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "changed absent input") {
				t.Fatalf("transient event accepted without timestamp signal: %v", err)
			}
		})
	}
}

func TestStagedAbsentInputWatchFailsClosed(t *testing.T) {
	input, err := observeStagedInput(filepath.Join(testutil.RealTempDir(t), "optional"), 1024, true)
	if err != nil {
		t.Fatal(err)
	}
	input.close()
	if err := input.check(); err == nil || !strings.Contains(err.Error(), "watch unavailable") {
		t.Fatalf("lost watch accepted: %v", err)
	}
}
