package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func TestCampaignCommitFetchCandidateReports(t *testing.T) {
	cases := []struct {
		name          string
		before, after map[string]string
		accept        bool
		evidence      string
	}{
		{"root-add", nil, map[string]string{"continuation.md": "new", "handoff.md": "new"}, false, ""},
		{"nested-add", nil, map[string]string{".t3/inputs/unit/continuation.md": "new", ".t3/dependencies/task/handoff.md": "new", "tasks/deep/handoff.md": "new"}, false, ""},
		{"edit", map[string]string{"continuation.md": "old", "tasks/handoff.md": "old"}, map[string]string{"continuation.md": "new", "tasks/handoff.md": "new"}, false, ""},
		{"delete", map[string]string{"handoff.md": "old", "tasks/continuation.md": "old"}, nil, false, ""},
		{"rename", map[string]string{"handoff.md": "same"}, map[string]string{"task/continuation.md": "same"}, false, ""},
		{"newline", nil, map[string]string{"task\nname/handoff.md": "new"}, false, ""},
		{"mixed", nil, map[string]string{"handoff.md": "new", "source.go": "package fixture"}, true, ""},
		{"report-to-source", map[string]string{"handoff.md": "same"}, map[string]string{"source.txt": "same"}, true, ""},
		{"docs", nil, map[string]string{"docs/guide.md": "new"}, true, ""},
		{"case-sensitive", nil, map[string]string{"Handoff.md": "new"}, true, ""},
		{"unchanged", nil, nil, true, ""},
		{"missing-base", nil, map[string]string{"source.txt": "new"}, false, strings.Repeat("1", 40)},
		{"invalid-base", nil, map[string]string{"source.txt": "new"}, false, "--HEAD"},
		{"empty-base", nil, map[string]string{"source.txt": "new"}, false, "empty"},
		{"tree-base", nil, map[string]string{"source.txt": "new"}, false, "tree"},
		{"unrelated-base", nil, map[string]string{"source.txt": "new"}, false, "unrelated"},
		{"submodule", nil, map[string]string{"handoff.md": "new"}, true, "submodule"},
	}
	for _, transport := range []string{"retained", "worker-fallback"} {
		for _, tc := range cases {
			t.Run(transport+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				source := t.TempDir()
				git := func(args ...string) string {
					return candidateGit(t, append([]string{"-C", source}, args...)...)
				}
				write := func(root, name, content string) {
					t.Helper()
					target := filepath.Join(root, name)
					if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(target, []byte(content), 0600); err != nil {
						t.Fatal(err)
					}
				}
				git("init", "-q")
				git("config", "user.name", "Fixture")
				git("config", "user.email", "fixture@example.invalid")
				write(source, "seed.txt", "seed")
				for name, content := range tc.before {
					write(source, name, content)
				}
				git("add", ".")
				git("commit", "-qm", "seed")
				seed := git("rev-parse", "HEAD")
				if tc.evidence == "submodule" {
					git("update-index", "--add", "--cacheinfo", "160000,"+seed+",module")
					git("commit", "-qm", "submodule base")
				}
				base := git("rev-parse", "HEAD")
				destination := filepath.Join(t.TempDir(), "destination")
				candidateGit(t, "clone", "-q", source, destination)
				for name := range tc.before {
					if err := os.Remove(filepath.Join(source, name)); err != nil {
						t.Fatal(err)
					}
				}
				for name, content := range tc.after {
					write(source, name, content)
				}
				if tc.evidence == "submodule" {
					git("update-index", "--cacheinfo", "160000,"+base+",module")
				}
				if tc.evidence == "submodule" {
					git("add", "handoff.md")
				} else {
					git("add", "-A")
				}
				if len(tc.before)+len(tc.after) > 0 {
					git("commit", "-qm", "candidate")
				}
				head := git("rev-parse", "HEAD")
				git("update-ref", "refs/heads/imported", head)
				bundle := filepath.Join(t.TempDir(), "candidate.bundle")
				bundleArgs := []string{"bundle", "create", bundle, "refs/heads/imported"}
				// Retained bundles can carry the base; fallback bundles require it locally.
				if transport == "worker-fallback" && head != base {
					bundleArgs = append(bundleArgs, "^"+base)
				}
				git(bundleArgs...)
				raw, err := os.ReadFile(bundle)
				if err != nil {
					t.Fatal(err)
				}
				recordedBase := base
				switch tc.evidence {
				case "tree":
					recordedBase = git("rev-parse", base+"^{tree}")
				case "unrelated":
					tree := git("rev-parse", base+"^{tree}")
					recordedBase = git("commit-tree", tree, "-m", "unrelated root")
					candidateGit(t, "-C", destination, "fetch", "-q", source, recordedBase)
				case "empty":
					recordedBase = ""
				case "", "submodule":
				default:
					recordedBase = tc.evidence
				}
				candidateGit(t, "-C", destination, "config", "diff.ignoreSubmodules", "all")
				write(destination, "seed.txt", "local unstaged edit")
				write(destination, "staged.txt", "local staged edit")
				candidateGit(t, "-C", destination, "add", "staged.txt")
				snapshot := func() string {
					return candidateGit(t, "-C", destination, "rev-parse", "HEAD") + "\n" +
						candidateGit(t, "-C", destination, "write-tree") + "\n" +
						candidateGit(t, "-C", destination, "status", "--porcelain=v1") + "\n" +
						candidateGit(t, "-C", destination, "diff", "--binary")
				}
				before := snapshot()
				var out bytes.Buffer
				cli := campaignCLI{stdout: &out, exportCommit: func(_ context.Context, request backlogadmin.CommitExportRequest) (backlogadmin.ArtifactContent, error) {
					return backlogadmin.ArtifactContent{
						Provenance: &backlog.CommitProvenance{WorkflowRunID: "run", TaskID: "task", Name: "implementation", Commit: head, Base: recordedBase},
						Metadata:   backlogadmin.ArtifactMetadata{Size: int64(len(raw)), SHA256: exportCLIHash(raw)},
						Content:    io.NopCloser(bytes.NewReader(raw)),
					}, nil
				}}
				err = cli.run(context.Background(), []string{"commit", "export", "run/task/implementation", "--into", destination, "--branch", "imported"})
				if tc.accept {
					if err != nil {
						t.Fatal(err)
					}
					if got := candidateGit(t, "-C", destination, "rev-parse", "refs/heads/imported"); got != head {
						t.Fatalf("head %s", got)
					}
				} else {
					if err == nil {
						t.Fatal("accepted refused candidate")
					}
					if tc.evidence == "" && (!strings.Contains(err.Error(), "report-only") || !strings.Contains(err.Error(), head)) {
						t.Fatalf("not actionable: %v", err)
					}
					if out.Len() != 0 {
						t.Fatalf("success receipt on refusal: %s", out.String())
					}
					refs := candidateGit(t, "-C", destination, "for-each-ref", "--format=%(refname)", "refs/heads/imported")
					if refs != "" {
						t.Fatalf("created branch: %s", refs)
					}
				}
				if after := snapshot(); after != before {
					t.Fatalf("destination changed:\nbefore %s\nafter %s", before, after)
				}
			})
		}
	}
}
