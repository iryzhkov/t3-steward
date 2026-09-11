package backlog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestLegacySubmissionSourceIngestsUnchangedMarkdownAndReplays(t *testing.T) {
	root := t.TempDir()
	drop := filepath.Join(root, "drop")
	if err := os.MkdirAll(drop, 0o700); err != nil {
		t.Fatal(err)
	}
	writeLegacySubmission(t, filepath.Join(drop, "task-one.md"), "t3-steward development", true, "first prompt")
	writeLegacySubmission(t, filepath.Join(drop, "disabled.md"), "t3-steward development", false, "disabled prompt")

	store, err := sqlite.OpenMigrated(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	storage := filepath.Join(root, "bundles")
	t.Cleanup(func() { _ = removeIngestedTree(storage) })
	service := &SubmissionService{
		StorageRoot: storage, Store: store,
		MaxBytes: 1 << 20, MaxFiles: 10,
	}
	aliases, err := LegacyProjectAliases(map[string]string{"steward": "t3-steward development"})
	if err != nil {
		t.Fatal(err)
	}
	source := LegacySubmissionSource{
		Dir: drop, Submitter: service, ProjectAliases: aliases,
		MaxBytes: 1 << 20, MaxFiles: 10, AllowedUID: uint32(os.Getuid()),
	}

	first := source.Tick(context.Background())
	if len(first.Errors) != 0 || len(first.Accepted) != 1 || first.Accepted[0].Replay ||
		len(first.Skipped) != 1 || first.Skipped[0] != "disabled" {
		t.Fatalf("first report = %+v", first)
	}
	second := source.Tick(context.Background())
	if len(second.Errors) != 0 || len(second.Accepted) != 1 || !second.Accepted[0].Replay {
		t.Fatalf("second report = %+v", second)
	}

	writeLegacySubmission(t, filepath.Join(drop, "task-one.md"), "t3-steward development", true, "changed prompt")
	changed := source.Tick(context.Background())
	if len(changed.Accepted) != 0 || len(changed.Errors) != 1 ||
		!strings.Contains(changed.Errors[0].Error(), "different content") {
		t.Fatalf("changed report = %+v", changed)
	}
}

func TestLegacySubmissionSourceProjectMappingAndValidation(t *testing.T) {
	if _, err := LegacyProjectAliases(map[string]string{
		"one": "shared", "two": "shared",
	}); err == nil || !strings.Contains(err.Error(), "multiple") {
		t.Fatalf("duplicate alias error = %v", err)
	}
	aliases, err := LegacyProjectAliases(map[string]string{"steward": "development"})
	if err != nil {
		t.Fatal(err)
	}
	if aliases["steward"] != "steward" || aliases["development"] != "steward" {
		t.Fatalf("aliases = %+v", aliases)
	}

	root := t.TempDir()
	writeLegacySubmission(t, filepath.Join(root, "unknown.md"), "unknown", true, "prompt")
	source := LegacySubmissionSource{
		Dir: root, Submitter: rejectingSingleTaskSubmitter{}, ProjectAliases: aliases,
		MaxBytes: 1 << 20, MaxFiles: 10, AllowedUID: uint32(os.Getuid()),
	}
	report := source.Tick(context.Background())
	if len(report.Errors) != 1 || !strings.Contains(report.Errors[0].Error(), "unmapped project") {
		t.Fatalf("report = %+v", report)
	}
}

type rejectingSingleTaskSubmitter struct{}

func (rejectingSingleTaskSubmitter) SubmitSingleTask(context.Context, SingleTaskSubmission) (SubmissionResult, error) {
	return SubmissionResult{}, nil
}

func writeLegacySubmission(t *testing.T, path, project string, enabled bool, prompt string) {
	t.Helper()
	raw := "---\nproject: " + project + "\ntitle: test\nimportance: 3\ndifficulty: 3\nmax_turns: 2\nenabled: "
	if enabled {
		raw += "true\n"
	} else {
		raw += "false\n"
	}
	raw += "---\n" + prompt + "\n"
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLegacySubmissionSourceBoundsFilesAndRejectsLinks(t *testing.T) {
	root := t.TempDir()
	writeLegacySubmission(t, filepath.Join(root, "one.md"), "development", true, "one")
	writeLegacySubmission(t, filepath.Join(root, "two.md"), "development", true, "two")
	source := LegacySubmissionSource{
		Dir: root, Submitter: rejectingSingleTaskSubmitter{},
		ProjectAliases: map[string]string{"development": "steward"},
		MaxBytes: 1 << 20, MaxFiles: 1, AllowedUID: uint32(os.Getuid()),
	}
	report := source.Tick(context.Background())
	if len(report.Errors) != 1 || !strings.Contains(report.Errors[0].Error(), "exceeds 1 files") {
		t.Fatalf("file bound report = %+v", report)
	}

	linkRoot := t.TempDir()
	target := filepath.Join(t.TempDir(), "target.md")
	writeLegacySubmission(t, target, "development", true, "linked")
	if err := os.Symlink(target, filepath.Join(linkRoot, "link.md")); err != nil {
		t.Fatal(err)
	}
	source.Dir = linkRoot
	source.MaxFiles = 10
	report = source.Tick(context.Background())
	if len(report.Errors) != 1 || !strings.Contains(report.Errors[0].Error(), "not a regular file") {
		t.Fatalf("link report = %+v", report)
	}

	source.Dir = root
	source.MaxFiles = 10
	source.MaxBytes = 8
	report = source.Tick(context.Background())
	if len(report.Errors) != 1 || !strings.Contains(report.Errors[0].Error(), "exceed 8 bytes") {
		t.Fatalf("byte bound report = %+v", report)
	}

	source.MaxBytes = 1 << 20
	if err := os.Chmod(root, 0o777); err != nil {
		t.Fatal(err)
	}
	report = source.Tick(context.Background())
	if len(report.Errors) != 1 || !strings.Contains(report.Errors[0].Error(), "group/world writable") {
		t.Fatalf("directory mode report = %+v", report)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	source.AllowedUID = uint32(os.Getuid() + 1)
	report = source.Tick(context.Background())
	if len(report.Errors) != 1 || !strings.Contains(report.Errors[0].Error(), "owner is not authorized") {
		t.Fatalf("directory owner report = %+v", report)
	}
}

func TestLegacySubmissionSourceInstalledFixtures(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"t3-backlog-default.md", "t3-job-enqueue.md"} {
		raw, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	sink := &recordingSingleTaskSubmitter{}
	source := LegacySubmissionSource{
		Dir: root, Submitter: sink,
		ProjectAliases: map[string]string{
			"t3-steward development": "steward",
			"nightly maintenance":    "nightly",
		},
		MaxBytes: 1 << 20, MaxFiles: 10, AllowedUID: uint32(os.Getuid()),
	}
	report := source.Tick(context.Background())
	if len(report.Errors) != 0 || len(report.Accepted) != 2 || len(sink.requests) != 2 {
		t.Fatalf("fixture report = %+v requests=%+v", report, sink.requests)
	}
	if sink.requests[0].Task.Project != "steward" ||
		sink.requests[1].Task.Project != "nightly" {
		t.Fatalf("mapped fixture projects = %q, %q",
			sink.requests[0].Task.Project, sink.requests[1].Task.Project)
	}
}

type recordingSingleTaskSubmitter struct {
	requests []SingleTaskSubmission
}

func (s *recordingSingleTaskSubmitter) SubmitSingleTask(_ context.Context, request SingleTaskSubmission) (SubmissionResult, error) {
	s.requests = append(s.requests, request)
	return SubmissionResult{}, nil
}
