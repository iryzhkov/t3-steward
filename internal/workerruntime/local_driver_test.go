package workerruntime

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type mapArtifactSource map[string][]byte

func (s mapArtifactSource) OpenArtifact(_ context.Context, object workerproto.ArtifactObject) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s[object.ID])), nil
}

type recordingPublisher struct {
	results     []PublishedResult
	checkpoints int
}

func (p *recordingPublisher) PublishResult(_ context.Context, _ workerproto.ExecutionPackage, result PublishedResult) error {
	p.results = append(p.results, result)
	return nil
}

func (p *recordingPublisher) PublishCheckpoint(_ context.Context, _ workerproto.ExecutionPackage, path string, data []byte) (*domain.CheckpointMetadata, error) {
	p.checkpoints++
	result := checkpointMetadata(path, data, runtimeTestNow)
	result.ArtifactID = "checkpoint-1"
	return &result, nil
}

type recordingT3 struct {
	thread         *domain.Thread
	created        []t3control.NewThreadInput
	projectID      string
	resolveProject string
	resolveErr     error
	warns          []domain.Warning
	resumes        []string
	stops          int
	message        string
	archive        []byte
}

func (c *recordingT3) GetThread(context.Context, string) (*domain.Thread, error) {
	if c.thread == nil {
		return nil, nil
	}
	copy := *c.thread
	return &copy, nil
}

func (c *recordingT3) ResolveProjectID(_ context.Context, project string) (string, error) {
	c.resolveProject = project
	if c.resolveErr != nil {
		return "", c.resolveErr
	}
	if c.projectID == "" {
		return "resolved-project-id", nil
	}
	return c.projectID, nil
}

func (c *recordingT3) CreateAndStartThread(_ context.Context, input t3control.NewThreadInput) (string, error) {
	c.created = append(c.created, input)
	c.thread = &domain.Thread{ID: input.ThreadID, Running: true, ModelSelection: input.ModelSelection}
	return input.ThreadID, nil
}
func (c *recordingT3) StopThread(context.Context, domain.Thread, t3control.StopMode) error {
	c.stops++
	c.thread.Running = false
	return nil
}
func (c *recordingT3) WaitStopped(context.Context, string, time.Duration) (*domain.Thread, bool, error) {
	return c.thread, c.thread == nil || !c.thread.Running, nil
}
func (c *recordingT3) WarnThread(_ context.Context, _ domain.Thread, warning domain.Warning) error {
	c.warns = append(c.warns, warning)
	return nil
}
func (c *recordingT3) ResumeThread(_ context.Context, _ domain.Thread, prompt string) error {
	c.resumes = append(c.resumes, prompt)
	return nil
}
func (c *recordingT3) LastAssistantMessage(context.Context, string) (string, error) {
	return c.message, nil
}
func (c *recordingT3) ExportThread(context.Context, string) ([]byte, error) {
	return append([]byte(nil), c.archive...), nil
}

type successfulProcessRunner struct{}

func (successfulProcessRunner) Run(_ context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	if request.Log != nil {
		_, _ = request.Log.Write([]byte("ok\n"))
	}
	return backlog.ProcessResult{ExitCode: 0}, nil
}
func (successfulProcessRunner) Kill(string) error { return nil }

type staticRepositoryCache struct {
	path string
}

func (c staticRepositoryCache) Prepare(context.Context, string, io.Writer) (backlog.CachedRepository, error) {
	return backlog.CachedRepository{Path: c.path}, nil
}

func TestLocalDriverBindsCatalogArtifactsWorkspaceAndT3(t *testing.T) {
	repository, commit := makeGitRepository(t)
	pkg := testPackage()
	pkg.Environment.Repository = "https://example.com/steward.git"
	pkg.Environment.Ref = commit
	pkg.Environment.RequiredCredentials = []string{"github-token"}
	pkg.StaticInputs = []workerproto.ArtifactObject{testArtifact("input-1", "context.md", "context")}
	manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
	if err != nil {
		t.Fatal(err)
	}
	_ = manifest
	catalog, err := backlog.NewProjectCatalog(
		[]backlog.ProjectDefinition{{
			Name: "steward", Repository: "https://example.com/steward.git", DefaultRef: commit,
			T3ProjectTemplate: "development", SetupProfile: "go", RequiredCredentials: []string{"github-token"},
		}},
		[]backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}},
	)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	artifactRoot := filepath.Join(root, "artifacts")
	runsRoot := filepath.Join(root, "runs")
	publisher := &recordingPublisher{}
	control := &recordingT3{message: "finished\nBACKLOG STATUS: done", archive: []byte("{}")}
	source := mapArtifactSource{
		pkg.Prompt.ID:          []byte("prompt"),
		pkg.StaticInputs[0].ID: []byte("context"),
	}
	driver, err := NewLocalDriver(LocalDriver{
		Config: LocalDriverConfig{
			CatalogRevision: "catalog-1", ArtifactRoot: artifactRoot,
			RunsRoot: runsRoot,
		},
		Catalog: catalog,
		Workspace: backlog.WorkspacePreparer{
			Cache:     staticRepositoryCache{path: repository},
			Processes: successfulProcessRunner{},
		},
		Finalizer: backlog.AttemptFinalizer{Processes: successfulProcessRunner{}, Now: func() time.Time { return runtimeTestNow }, NewID: func(string) string { return "verification-1" }},
		Source:    source, Publisher: publisher, T3: control, Now: func() time.Time { return runtimeTestNow },
		Credentials: EnvironmentCredentialChecker{Lookup: func(name string) (string, bool) {
			return "resolved-secret", name == "T3_STEWARD_CREDENTIAL_GITHUB_TOKEN"
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := driver.Prepare(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	input, err := os.ReadFile(filepath.Join(workspace, ".t3", "inputs", "context.md"))
	if err != nil || string(input) != "context" {
		t.Fatalf("materialized input=%q err=%v", input, err)
	}
	if err := driver.CreateThread(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if len(control.created) != 1 || control.created[0].ThreadID != "thread-1" ||
		control.created[0].DispatchToken != "dispatch-1" || control.created[0].WorktreePath != workspace ||
		control.created[0].Prompt != "prompt" {
		t.Fatalf("create input = %+v", control.created)
	}
	if state, err := driver.ObserveThread(context.Background(), pkg); err != nil || state != backlog.DispatchThreadActive {
		t.Fatalf("observe=%q err=%v", state, err)
	}
	control.thread.Running = false
	if err := driver.Collect(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if len(publisher.results) != 1 || len(publisher.results[0].Finalized.Artifacts) != 1 ||
		string(publisher.results[0].ThreadArchive) != "{}" {
		t.Fatalf("published = %+v", publisher.results)
	}
	if err := driver.Cleanup(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	driver.Credentials = nil
	if _, err := driver.Prepare(context.Background(), pkg); err == nil || !strings.Contains(err.Error(), "resolver is unavailable") {
		t.Fatalf("missing credential resolver error = %v", err)
	}
}

func TestLocalDriverResolvesProjectBeforeCreate(t *testing.T) {
	pkg := testPackage()
	root := t.TempDir()
	control := &recordingT3{projectID: "project-uuid"}
	driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: root}, T3: control}
	cachePath := filepath.Join(root, "objects", pkg.Prompt.SHA256)
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cachePath, []byte("prompt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := driver.CreateThread(context.Background(), pkg, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if control.resolveProject != pkg.Environment.T3Project {
		t.Fatalf("resolved project = %q, want %q", control.resolveProject, pkg.Environment.T3Project)
	}
	if len(control.created) != 1 || control.created[0].ProjectID != "project-uuid" {
		t.Fatalf("create input = %+v", control.created)
	}
	control.resolveErr = io.EOF
	if err := driver.CreateThread(context.Background(), pkg, t.TempDir()); err == nil || !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("resolver error = %v", err)
	}
	if len(control.created) != 1 {
		t.Fatalf("create proceeded after resolver failure: %+v", control.created)
	}
}

func TestLocalDriverNoExternalEffectsModeAndCorruption(t *testing.T) {
	_, commit := makeGitRepository(t)
	pkg := testPackage()
	pkg.Environment.Repository = "https://example.com/steward.git"
	pkg.Environment.Ref = commit
	catalog, err := backlog.NewProjectCatalog(
		[]backlog.ProjectDefinition{{Name: "steward", Repository: "https://example.com/steward.git", DefaultRef: commit, T3ProjectTemplate: "development", SetupProfile: "go"}},
		[]backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}},
	)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	publisher := &recordingPublisher{}
	driver, err := NewLocalDriver(LocalDriver{
		Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs"), DryRun: true},
		Catalog: catalog, Source: mapArtifactSource{pkg.Prompt.ID: []byte("prompt")},
		Publisher: publisher, Now: func() time.Time { return runtimeTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := driver.Prepare(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.CreateThread(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if state, err := driver.ObserveThread(context.Background(), pkg); err != nil || state != backlog.DispatchThreadActive {
		t.Fatalf("no-effects observe=%q err=%v", state, err)
	}
	if err := driver.StopThread(context.Background(), pkg); err != nil {
		t.Fatal(err)
	}
	if state, err := driver.ObserveThread(context.Background(), pkg); err != nil || state != backlog.DispatchThreadStopped {
		t.Fatalf("no-effects stopped observe=%q err=%v", state, err)
	}
	if err := driver.Resume(context.Background(), pkg, domain.ThrottleCommand{Reason: "recovered"}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := driver.Checkpoint(context.Background(), pkg, domain.ThrottleCommand{Reason: "quota"})
	if err != nil || checkpoint == nil || publisher.checkpoints != 1 {
		t.Fatalf("checkpoint=%+v publishes=%d err=%v", checkpoint, publisher.checkpoints, err)
	}

	corruptRoot := t.TempDir()
	corruptDriver, err := NewLocalDriver(LocalDriver{
		Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(corruptRoot, "artifacts"), RunsRoot: filepath.Join(corruptRoot, "runs"), DryRun: true},
		Catalog: catalog, Source: mapArtifactSource{pkg.Prompt.ID: []byte("wrong")},
		Publisher: publisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := corruptDriver.Prepare(context.Background(), pkg); err == nil {
		t.Fatal("corrupt downloaded artifact accepted")
	}
}

func TestLocalDriverRejectsStaleCatalogAndUnsafeCleanup(t *testing.T) {
	_, commit := makeGitRepository(t)
	pkg := testPackage()
	pkg.Environment.Repository = "https://example.com/steward.git"
	pkg.Environment.Ref = commit
	catalog, err := backlog.NewProjectCatalog(
		[]backlog.ProjectDefinition{{Name: "steward", Repository: "https://example.com/steward.git", DefaultRef: commit, T3ProjectTemplate: "development", SetupProfile: "go"}},
		[]backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}},
	)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	driver, err := NewLocalDriver(LocalDriver{
		Config:  LocalDriverConfig{CatalogRevision: "catalog-new", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs"), DryRun: true},
		Catalog: catalog, Source: mapArtifactSource{pkg.Prompt.ID: []byte("prompt")},
		Publisher: &recordingPublisher{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Prepare(context.Background(), pkg); err == nil {
		t.Fatal("stale catalog package accepted")
	}
	if err := driver.Cleanup(context.Background(), pkg, t.TempDir()); err == nil {
		t.Fatal("cleanup outside attempt root accepted")
	}
}

func makeGitRepository(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q", root},
		{"-C", root, "config", "user.email", "test@example.com"},
		{"-C", root, "config", "user.name", "Test"},
	} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"-C", root, "add", "README.md"}, {"-C", root, "commit", "-qm", "fixture"}} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	output, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, ".git"), string(bytes.TrimSpace(output))
}
