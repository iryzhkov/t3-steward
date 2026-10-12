package workerruntime

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
)

func TestWorkerWithoutCachesKeepsItsCatalogRevision(t *testing.T) {
	// Exact pre-cache JSON encoding of this worker and empty catalog at the pinned base.
	legacy := `{"WorkerID":"w","Worker":{"Containment":null,"Address":"host","Epoch":"","AcceptBacklog":true,"Capabilities":null,"Providers":null,"Credential":"credential","CPUClass":"","Executors":{"Slots":0,"CPUUnits":0,"MemoryMB":0,"ScratchMB":0}},"Projects":[],"Profiles":[]}`
	expected := fmt.Sprintf("%x", sha256.Sum256([]byte(legacy)))
	for _, caches := range [][]config.V2Cache{nil, {}} {
		settings := config.BacklogV2{Workers: map[string]config.V2Worker{"w": {Address: "host", AcceptBacklog: true, Credential: "credential", Caches: caches}}}
		binding, err := BuildWorkerBinding(settings, "w", time.Now())
		if err != nil || binding.CatalogRevision != expected {
			t.Fatalf("binding=%+v err=%v want=%s", binding, err, expected)
		}
	}
}

func TestCacheKeysSeparateToolchainVersions(t *testing.T) {
	root := BuildCacheRoot(t.TempDir())
	c := config.V2Cache{Name: "go", Toolchain: config.V2CacheToolchain{Command: "go", VersionArgs: []string{"env", "GOVERSION"}}, Env: map[string]string{"GOCACHE": "go-build", "GOMODCACHE": "mod"}, MaxMB: 100}
	version := "go1.25/unsafe"
	runner := func(ctx context.Context, command string, args []string) ([]byte, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > cacheVersionTimeout || command != "go" || len(args) != 2 {
			t.Fatal("query contract")
		}
		return []byte(version), nil
	}
	first, skip := ResolveBuildCaches(context.Background(), root, []config.V2Cache{c}, runner)
	if len(skip) != 0 || len(first) != 2 {
		t.Fatalf("%v %v", first, skip)
	}
	same, _ := ResolveBuildCaches(context.Background(), root, []config.V2Cache{c}, runner)
	version = "go1.25-unsafe"
	second, _ := ResolveBuildCaches(context.Background(), root, []config.V2Cache{c}, runner)
	if first["GOCACHE"] != same["GOCACHE"] || first["GOCACHE"] == second["GOCACHE"] {
		t.Fatal("version key separation")
	}
	info, err := os.Stat(first["GOCACHE"])
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("mode: %v %v", info, err)
	}
	for _, relative := range []string{"go", filepath.Dir(first["GOCACHE"])[len(root)+1:], first["GOCACHE"][len(root)+1:]} {
		attack := BuildCacheRoot(t.TempDir())
		link := filepath.Join(attack, relative)
		if err := os.MkdirAll(filepath.Dir(link), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), link); err != nil {
			t.Fatal(err)
		}
		env, skips := ResolveBuildCaches(context.Background(), attack, []config.V2Cache{c}, func(context.Context, string, []string) ([]byte, error) { return []byte("go1.25/unsafe"), nil })
		if len(env) != 0 || skips["go"] == "" {
			t.Fatal("symlink accepted")
		}
	}
}

func TestProjectedCatalogRejectsInvalidCaches(t *testing.T) {
	host, projection := catalogHostFixture(t)
	projection.Worker.Caches = []config.V2Cache{{Name: "go", Toolchain: config.V2CacheToolchain{Command: "go"}, Env: map[string]string{"GOCACHE": "../escape"}, MaxMB: 1}}
	if _, err := projection.Settings(host.Bootstrap, host.Home); err == nil || !strings.Contains(err.Error(), "cache \"go\"") {
		t.Fatalf("projection refusal: %v", err)
	}
}

func TestCacheWithFailingVersionQueryIsSkipped(t *testing.T) {
	for _, output := range []string{"error", "", "timeout", string(make([]byte, cacheVersionLimit+1))} {
		t.Run(fmt.Sprintf("size-%d-%s", len(output), output[:min(len(output), 7)]), func(t *testing.T) {
			caches := []config.V2Cache{
				{Name: "bad", Toolchain: config.V2CacheToolchain{Command: "bad"}, Env: map[string]string{"BAD": "data"}, MaxMB: 1},
				{Name: "good", Toolchain: config.V2CacheToolchain{Command: "good"}, Env: map[string]string{"GOOD": "data"}, MaxMB: 1},
			}
			runner := func(ctx context.Context, command string, _ []string) ([]byte, error) {
				if command == "good" {
					return []byte("v1"), nil
				}
				if output == "error" {
					return nil, os.ErrNotExist
				}
				if output == "timeout" {
					return nil, context.DeadlineExceeded
				}
				return []byte(output), nil
			}
			env, skip := ResolveBuildCaches(context.Background(), BuildCacheRoot(t.TempDir()), caches, runner)
			if len(env) != 1 || env["GOOD"] == "" || skip["bad"] == "" {
				t.Fatalf("%v %v", env, skip)
			}
		})
	}
	b := &cacheOutput{}
	if _, err := b.Write(make([]byte, cacheVersionLimit+1)); err == nil || b.Len() != 0 {
		t.Fatal("unbounded output")
	}
}
