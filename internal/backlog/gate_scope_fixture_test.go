package backlog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

func gateScenarioID(root, scenario string) string {
	return fmt.Sprintf("gate-%x", sha256.Sum256([]byte(root+"\x00"+scenario)))
}

var gateBarrierCounts = struct {
	sync.Mutex
	counts map[string]int
}{counts: make(map[string]int)}

// Only qualification opts in: both independent processes hold live children
// at the same scenario/iteration barrier before either runner may clean up.
func gateScenarioBarrier(t *testing.T) string {
	t.Helper()
	root, side := os.Getenv("T3_GATE_BARRIER"), os.Getenv("T3_GATE_SIDE")
	if root == "" {
		return ""
	}
	if side != "left" && side != "right" {
		t.Fatal("invalid qualification side")
	}
	peer := "left"
	if side == peer {
		peer = "right"
	}
	gateBarrierCounts.Lock()
	gateBarrierCounts.counts[t.Name()]++
	n := gateBarrierCounts.counts[t.Name()]
	gateBarrierCounts.Unlock()
	key := fmt.Sprintf("%s-%d-", t.Name(), n)
	return fmt.Sprintf("touch %q; peer=%q; while [ ! -e \"$peer\" ]; do check_live || exit 1; sleep 0.01; done; date -u +%%s.%%N >> %q\n",
		filepath.Join(root, key+side), filepath.Join(root, key+peer), filepath.Join(root, key+side))
}

// The acknowledgement is issued only after inotifywait reports successful
// watch registration, while its process is alive. Status lives outside the repo.
// Descendants have no runner pipes open, so the launcher can return to cleanup.
func gateWatchLauncher(status, watcher, barrier string, bound time.Duration) string {
	ticks := int(bound / (10 * time.Millisecond))
	return fmt.Sprintf(`status=%q
: > "$status"
rm -f "$status.done" "$status.ready"
: > "$status.started"
({ %s; } > "$status.event" 2> "$status" < /dev/null; code=$?; echo "$code" > "$status.done"; if [ "$code" = 0 ]; then printf 'bad\n' > out.txt; fi) >/dev/null 2>&1 < /dev/null &
child=$!
remaining=%d
check_live() {
 remaining=$((remaining-1))
 if [ "$remaining" -le 0 ] || [ -e "$status.done" ] || ! kill -0 "$child" 2>/dev/null; then
  echo "watch readiness failed: timeout or premature exit" >&2
  cat "$status" >&2
  kill "$child" 2>/dev/null
  wait "$child" 2>/dev/null
  return 1
 fi
}
while ! grep -qx 'Watches established.' "$status"; do check_live || exit 1; sleep 0.01; done
check_live || exit 1
date -u +%%s.%%N > "$status.ready"
%s
echo "watch ready"
`, status, watcher, ticks, barrier)
}

func realGateScenario(t *testing.T) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	id := gateScenarioID(root, t.Name())
	unit := processScopeUnit(fmt.Sprintf("verify-%s-gate-0", id))
	t.Cleanup(func() {
		out, err := exec.Command("systemctl", "--user", "kill", "--kill-who=all", "--signal=KILL", unit).CombinedOutput()
		if err != nil && !strings.Contains(string(out), "not loaded") && !strings.Contains(string(out), "not found") {
			t.Errorf("cleanup %s: %s %v", unit, out, err)
		}
		if exec.Command("systemctl", "--user", "is-active", "--quiet", unit).Run() == nil {
			t.Errorf("lingering unit %s", unit)
		}
	})
	t.Logf("scenario attempt=%s unit=%s root=%s", id, unit, root)
	return id, filepath.Join(root, "watch"), gateScenarioBarrier(t)
}

func requireGateWatchReady(t *testing.T, status string) {
	t.Helper()
	ready, err := os.ReadFile(status + ".ready")
	if err != nil {
		out, _ := os.ReadFile(status)
		t.Fatalf("watch not ready: %v: %s", err, out)
	}
	t.Logf("watch ready at=%s status=%s", strings.TrimSpace(string(ready)), status)
}

func TestGateScenarioIdentity(t *testing.T) {
	a, b := gateScenarioID(t.TempDir(), "scenario"), gateScenarioID(t.TempDir(), "scenario")
	unit := func(id string) string { return processScopeUnit("verify-" + id + "-gate-0") }
	if a == b || unit(a) == unit(b) {
		t.Fatal("invocation identities collided")
	}
	root := t.TempDir()
	if gateScenarioID(root, "scenario") != gateScenarioID(root, "scenario") {
		t.Fatal("identity unstable")
	}
}

func TestGateWatchReadiness(t *testing.T) {
	if _, err := exec.LookPath("inotifywait"); err != nil {
		t.Skip("inotifywait unavailable")
	}
	for _, mode := range []string{"registered", "startup-failure", "never-ready"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			writeTestFile(t, root, "big.bin", "data")
			release, ack := filepath.Join(root, "release"), filepath.Join(root, "ack")
			watcher := fmt.Sprintf("touch %q; while [ ! -e %q ]; do sleep 0.01; done; LC_ALL=C inotifywait -e open big.bin", release+".waiting", release)
			if mode == "startup-failure" {
				watcher = "echo deliberate-startup-failure >&2; exit 7"
			}
			if mode == "never-ready" {
				watcher = "echo deliberate-never-ready >&2; exec sleep 120"
			}
			status := filepath.Join(root, "status")
			// Keep all descendants in an owned real scope, including negative paths.
			requireUserSystemd(t)
			id, _, _ := realGateScenario(t)
			cmd := gateWatchLauncher(status, watcher, "", testtiming.Bound(500*time.Millisecond))
			if mode == "registered" {
				cmd += fmt.Sprintf("touch %q; cat big.bin >/dev/null; while [ ! -e \"$status.done\" ]; do remaining=$((remaining-1)); [ \"$remaining\" -gt 0 ] || exit 1; sleep 0.01; done; wait \"$child\"\n", ack)
			}
			ctx, cancel := context.WithTimeout(context.Background(), testtiming.Bound(5*time.Second))
			defer cancel()
			done := make(chan error, 1)
			joined := false
			defer func() {
				cancel()
				if !joined {
					<-done
				}
			}()
			var output strings.Builder
			go func() {
				_, err := (SystemdScopeRunner{}).Run(ctx, ProcessRequest{ID: "verify-" + id + "-gate-0", Dir: root, Program: "/bin/sh", Args: []string{"-c", cmd}, Log: &output, KillRemaining: true})
				done <- err
			}()
			if mode == "registered" {
				for {
					if _, err := os.Stat(release + ".waiting"); err == nil {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("watcher did not reach release barrier")
					case <-time.After(10 * time.Millisecond):
					}
				}
				// Explicit release proves the acknowledgement cannot precede registration.
				if _, err := os.Stat(status + ".ready"); !os.IsNotExist(err) {
					t.Fatal("ack before release")
				}
				if err := os.WriteFile(release, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-done:
				joined = true
				if exec.Command("systemctl", "--user", "is-active", "--quiet", processScopeUnit("verify-"+id+"-gate-0")).Run() == nil {
					t.Fatal("descendants survived cleanup")
				}
				if mode == "registered" {
					if err != nil {
						t.Fatalf("launcher: %v %s", err, output.String())
					}
					requireGateWatchReady(t, status)
					if _, err := os.Stat(ack); err != nil {
						t.Fatal(err)
					}
					if got := strings.TrimSpace(readAbsoluteTestFile(t, filepath.Join(root, "out.txt"))); got != "bad" {
						t.Fatalf("open missed: %s", got)
					}
				} else {
					t.Logf("bounded readiness failure: %v\n%s", err, output.String())
					if err == nil || !strings.Contains(output.String(), "deliberate-") || !strings.Contains(output.String(), "watch readiness failed") {
						t.Fatalf("missing failure evidence: %v %s", err, output.String())
					}
				}
			case <-time.After(testtiming.Bound(5 * time.Second)):
				t.Fatal("readiness did not terminate within bound")
			}
		})
	}
}
