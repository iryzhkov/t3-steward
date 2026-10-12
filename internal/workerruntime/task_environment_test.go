package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func goTaskCache() []config.V2Cache {
	return []config.V2Cache{{Name: "go", Toolchain: config.V2CacheToolchain{Command: "go", VersionArgs: []string{"env", "GOVERSION"}}, Env: map[string]string{"GOCACHE": "go-build", "GOMODCACHE": "mod"}, MaxMB: 4096}}
}

func cacheDispatchFixture(t *testing.T) (*LocalDriver, workerproto.ExecutionPackage, *recordingT3) {
	t.Helper()
	pkg := testPackage()
	pkg.Environment.Type = backlog.EnvironmentFresh
	pkg.Environment.Repository, pkg.Environment.Ref, pkg.Environment.T3Project = "", "", ""
	catalog, err := backlog.NewProjectCatalog([]backlog.ProjectDefinition{{Name: "steward", Type: backlog.EnvironmentFresh, SetupProfile: "go"}}, []backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	control := &recordingT3{}
	driver, err := NewLocalDriver(LocalDriver{
		Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs"), BuildCacheRoot: BuildCacheRoot(root), Caches: goTaskCache(), CacheVersionRunner: func(context.Context, string, []string) ([]byte, error) { return []byte("go-test-version\n"), nil }},
		Catalog: catalog, Workspace: backlog.WorkspacePreparer{Processes: successfulProcessRunner{}},
		Source: mapArtifactSource{pkg.Prompt.ID: []byte("prompt")}, Publisher: &recordingPublisher{}, T3: control,
	})
	if err != nil {
		t.Fatal(err)
	}
	return driver, pkg, control
}

func assertTaskCacheEnvironment(t *testing.T, driver *LocalDriver, pkg workerproto.ExecutionPackage, got map[string]string) {
	t.Helper()
	cacheEnv, skipped := ResolveBuildCaches(context.Background(), driver.Config.BuildCacheRoot, driver.Config.Caches, driver.Config.CacheVersionRunner)
	if len(skipped) != 0 {
		t.Fatalf("skipped: %v", skipped)
	}
	want := pkg.Identity.TaskEnvironment()
	for key, value := range cacheEnv {
		if !strings.HasPrefix(value, driver.Config.BuildCacheRoot+string(filepath.Separator)) {
			t.Fatalf("cache outside root: %s", value)
		}
		want[key] = value
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("environment=%v want=%v", got, want)
	}
}

func TestTaskEnvironmentGainsOnlyDeclaredCacheVariables(t *testing.T) {
	for _, caches := range []bool{false, true} {
		t.Run(map[bool]string{false: "none", true: "declared"}[caches], func(t *testing.T) {
			t.Parallel()
			driver, pkg, control := cacheDispatchFixture(t)
			if !caches {
				driver.Config.Caches = nil
			}
			workspace, err := driver.Prepare(context.Background(), pkg)
			if err != nil {
				t.Fatal(err)
			}
			if err := driver.CreateThread(context.Background(), pkg, workspace); err != nil {
				t.Fatal(err)
			}
			assertTaskCacheEnvironment(t, driver, pkg, control.created[0].Environment)
			if control.created[0].WorktreePath != workspace {
				t.Fatal("workspace redirected")
			}
		})
	}
}

func TestRecoveryDispatchWithoutPrepareGetsCacheEnvironment(t *testing.T) {
	t.Parallel()
	driver, pkg, control := cacheDispatchFixture(t)
	workspace, err := driver.Prepare(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	// A restarted driver reuses only the configuration and durable workspace.
	restarted := *driver
	if err := restarted.CreateThread(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	assertTaskCacheEnvironment(t, &restarted, pkg, control.created[0].Environment)
}

func TestContainedDispatchStaysUncached(t *testing.T) {
	t.Parallel()
	driver, pkg, host := cacheDispatchFixture(t)
	workspace, err := driver.Prepare(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	scoped := &recordingT3{}
	pkg.Environment.DirectoryBindings = []directoryresource.Binding{{}}
	// The raw driver isolates this test from catalog binding validation.
	driver.Catalog = nil
	driver.ScopedT3 = attachFunc(func(context.Context, workerproto.ExecutionPackage) (T3Control, error) { return scoped, nil })
	driver.Config.CacheVersionRunner = func(context.Context, string, []string) ([]byte, error) {
		t.Fatal("contained version query")
		return nil, nil
	}
	var log bytes.Buffer
	driver.Log = slog.New(slog.NewTextHandler(&log, nil))
	if err := driver.CreateThread(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if len(host.created) != 0 || len(scoped.created) != 1 {
		t.Fatal("wrong control dispatched")
	}
	if !reflect.DeepEqual(scoped.created[0].Environment, pkg.Identity.TaskEnvironment()) {
		t.Fatal("contained cache injected")
	}
	if scoped.created[0].WorktreePath != "/workspace" {
		t.Fatal("contained path changed")
	}
	if strings.Count(log.String(), "build caches skipped") != 1 || !strings.Contains(log.String(), "does not mount") {
		t.Fatalf("log=%s", log.String())
	}
}

func TestSkippedCacheLogsOnceAndDispatches(t *testing.T) {
	t.Parallel()
	driver, pkg, control := cacheDispatchFixture(t)
	workspace, err := driver.Prepare(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	driver.Config.CacheVersionRunner = func(context.Context, string, []string) ([]byte, error) { return nil, errors.New("version unavailable") }
	var log bytes.Buffer
	driver.Log = slog.New(slog.NewTextHandler(&log, nil))
	if err := driver.CreateThread(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(control.created[0].Environment, pkg.Identity.TaskEnvironment()) {
		t.Fatal("failed cache injected")
	}
	for _, text := range []string{"build cache skipped", "cache=go", "version unavailable", "attempt=" + pkg.Identity.AttemptID, "thread=" + pkg.Identity.ThreadID} {
		if strings.Count(log.String(), text) != 1 {
			t.Fatalf("missing or repeated %q: %s", text, log.String())
		}
	}
}

func TestSecondTaskReusesFirstTasksGOCACHE(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go binary on PATH")
	}
	driver, pkg, control := cacheDispatchFixture(t)
	driver.Config.CacheVersionRunner = nil
	var outputs [2]string
	var caches [2]string
	for i := range outputs {
		pkg.Identity.AttemptID = []string{"cache-attempt-first", "cache-attempt-second"}[i]
		pkg.Identity.ThreadID = []string{"cache-thread-first", "cache-thread-second"}[i]
		workspace, err := driver.Prepare(context.Background(), pkg)
		if err != nil {
			t.Fatal(err)
		}
		if err := driver.CreateThread(context.Background(), pkg, workspace); err != nil {
			t.Fatal(err)
		}
		environment := control.created[i].Environment
		caches[i] = environment["GOCACHE"]
		if caches[i] == "" {
			t.Fatal("real Go cache unresolved")
		}
		for path, content := range map[string]string{"go.mod": "module example.invalid/cacheproof\n\ngo 1.25\n", "cacheproof.go": "package cacheproof\nfunc Value() int { return 42 }\n"} {
			if err := os.WriteFile(filepath.Join(workspace, path), []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
		}
		// Remove attempt-specific source paths from Go compilation cache keys.
		cmd := exec.Command("go", "build", "-trimpath", "-x", ".")
		cmd.Dir = workspace
		merged := map[string]string{}
		for _, entry := range os.Environ() {
			key, value, _ := strings.Cut(entry, "=")
			merged[key] = value
		}
		for key, value := range environment {
			merged[key] = value
		}
		for key, value := range map[string]string{"GOTOOLCHAIN": "local", "GOPROXY": "off", "GOFLAGS": "-mod=mod -p=3", "GOMAXPROCS": "3", "GOWORK": "off"} {
			merged[key] = value
		}
		for key, value := range merged {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("build %d: %v\n%s", i, err, output)
		}
		outputs[i] = string(output)
	}
	if caches[0] != caches[1] {
		t.Fatalf("cache changed: %v", caches)
	}
	compileActions := func(output string) int {
		count := 0
		for _, line := range strings.Split(output, "\n") {
			if strings.Contains(line, "/compile ") && strings.Contains(line, "-p example.invalid/cacheproof ") {
				count++
			}
		}
		return count
	}
	first, second := compileActions(outputs[0]), compileActions(outputs[1])
	if first == 0 || second >= first {
		t.Fatalf("target compile actions first=%d second=%d\nfirst:\n%s\nsecond:\n%s", first, second, outputs[0], outputs[1])
	}
}
