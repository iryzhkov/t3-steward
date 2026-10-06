package backlog

import (
	"context"
	"testing"
	"time"
)

// A systemd service start changes INVOCATION_ID, JOURNAL_STREAM and similar
// variables that no gate command reads. They must not reach the cache key, or
// every worker restart reruns an unchanged gate.
func TestH2GateCacheSurvivesServiceRestartEnvironment(t *testing.T) {
	dir := h2GateRepository(t)
	storage := t.TempDir()
	t.Setenv("INVOCATION_ID", "aaaa")
	t.Setenv("JOURNAL_STREAM", "8:1111")
	t.Setenv("SYSTEMD_EXEC_PID", "100")
	first, err := AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, GateCacheAge: time.Hour}.
		Finalize(context.Background(), h2GateRequest(dir, "first"))
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, first.StorageDir)
	if h2ReadGate(t, storage, first).Cached {
		t.Fatal("first gate reported cached")
	}

	t.Setenv("INVOCATION_ID", "bbbb")
	t.Setenv("JOURNAL_STREAM", "8:2222")
	t.Setenv("SYSTEMD_EXEC_PID", "200")
	restartRunner := &directRunner{}
	second, err := AttemptFinalizer{StorageRoot: storage, Processes: restartRunner, GateCacheAge: time.Hour}.
		Finalize(context.Background(), h2GateRequest(dir, "second"))
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, second.StorageDir)
	if cached := h2ReadGate(t, storage, second); !cached.Cached || cached.OriginalAttempt != "first" || len(restartRunner.calls) != 1 {
		t.Fatalf("cache miss after simulated restart: cached=%v original=%q calls=%d", cached.Cached, cached.OriginalAttempt, len(restartRunner.calls))
	}

	// A variable that does change what the commands do still misses.
	t.Setenv("GOFLAGS", "-tags=h2-restart-probe")
	changedRunner := &directRunner{}
	third, err := AttemptFinalizer{StorageRoot: storage, Processes: changedRunner, GateCacheAge: time.Hour}.
		Finalize(context.Background(), h2GateRequest(dir, "third"))
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, third.StorageDir)
	if h2ReadGate(t, storage, third).Cached || len(changedRunner.calls) != 2 {
		t.Fatalf("GOFLAGS change reused the cache: calls=%d", len(changedRunner.calls))
	}
}

func TestGateEnvironmentDigestAllowlist(t *testing.T) {
	base := []string{"PATH=/usr/bin", "HOME=/home/w", "INVOCATION_ID=a", "JOURNAL_STREAM=1", "MANAGERPID=1", "MEMORY_PRESSURE_WATCH=/x", "HYPRLAND_INSTANCE_SIGNATURE=a", "T3_SECRET=s1"}
	same := []string{"T3_SECRET=s2", "HYPRLAND_INSTANCE_SIGNATURE=b", "MEMORY_PRESSURE_WATCH=/y", "MANAGERPID=2", "JOURNAL_STREAM=2", "INVOCATION_ID=b", "HOME=/home/w", "PATH=/usr/bin"}
	if gateEnvironmentDigest(base) != gateEnvironmentDigest(same) {
		t.Fatal("volatile variables changed the digest")
	}
	for _, changed := range []string{"PATH=/opt/bin", "GOFLAGS=-race", "CGO_ENABLED=0", "CC=clang", "LC_ALL=C", "LANG=C", "TMPDIR=/t", "GIT_CONFIG_GLOBAL=/g", "MAKEFLAGS=-j4", "LD_PRELOAD=/x.so"} {
		name := changed[:len(changed)-len(changed[indexEquals(changed):])]
		var env []string
		for _, entry := range base {
			if entry[:indexEquals(entry)] != name {
				env = append(env, entry)
			}
		}
		if gateEnvironmentDigest(append(env, changed)) == gateEnvironmentDigest(base) {
			t.Fatalf("%s did not change the digest", changed)
		}
	}
}

func indexEquals(entry string) int {
	for i := range entry {
		if entry[i] == '=' {
			return i
		}
	}
	return len(entry)
}
