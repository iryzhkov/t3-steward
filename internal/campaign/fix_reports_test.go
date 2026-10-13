package campaign

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestFixReportPathsAndLegacyInputs(t *testing.T) {
	for _, rounds := range []int{1, 2} {
		s, o := fixFixture()
		o.Lineage.RoundsDeclared = rounds
		u, err := GenerateFixChain(s, o)
		reportReject(t, err != nil, err)
		m := fixManifest(t, u)
		for _, file := range u.Files {
			if file.Path == "inputs/fix/rules.md" {
				reportReject(t, !strings.Contains(string(file.Content), "Tests named in the original brief must still exist and pass; removal or weakening is a regression."), "missing named-test integrity instruction")
			}
		}
		reportReject(t, !reflect.DeepEqual(m.Tasks["fix1"].InputsFrom["run-source/review"], []string{"review.md", "verdict.json"}), "legacy review names changed")
		for name, task := range m.Tasks {
			want := []string{"artifacts/continuation.md", "artifacts/handoff.md", "artifacts/verification.log"}
			if strings.HasPrefix(name, "review") {
				want = []string{"artifacts/continuation.md", "artifacts/review.md", "artifacts/verdict.json"}
				reportReject(t, task.ReviewOutput.Verdict != "artifacts/verdict.json" || !slices.Contains(task.Verify, "head -1 artifacts/review.md | grep -Eq '^VERDICT: (ACCEPT|CHANGES_REQUESTED)$'"), task)
				reportReject(t, len(task.Commits) != 0, "review declares commits")
			}
			reportReject(t, !reflect.DeepEqual(task.Outputs, want), task.Outputs)
			reportReject(t, !slices.Contains(task.Verify, fixReportPathCheck), "missing collision verification")
			steps := task.Preflight.Steps
			reportReject(t, len(steps) != 1 || steps[0].FailurePolicy != backlog.PreflightPolicyRequirePass || !reflect.DeepEqual(steps[0].Command, []string{"sh", "-c", fixReportPathCheck}), steps)
			for from, inputs := range task.InputsFrom {
				if strings.Contains(from, "/") {
					continue
				}
				upstream := m.Tasks[from]
				for _, input := range inputs {
					if input == "fix" || input == "gate/log.txt" {
						continue
					}
					reportReject(t, !slices.Contains(upstream.Outputs, input), fmt.Sprintf("%s consumes undeclared %s from %s", name, input, from))
				}
			}
		}
	}
}

func reportReject(t *testing.T, bad bool, detail ...any) {
	t.Helper()
	if bad {
		t.Fatal(detail...)
	}
}

func reportGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	b, err := cmd.CombinedOutput()
	reportReject(t, err != nil, fmt.Sprintf("git %v: %v: %s", args, err, b))
	return strings.TrimSpace(string(b))
}

func reportCheck(dir, command string) (string, error) {
	cmd := exec.Command("sh", "-c", command)
	cmd.Dir = dir
	b, err := cmd.CombinedOutput()
	return string(b), err
}

// Exercise actual output capture and downstream materialization, with legacy
// tracked root reports present. Neither a source fix nor a no-op changes them.
func TestFixReportsRetainedOutsideSourceAndNoOpCommits(t *testing.T) {
	s, o := fixFixture()
	u, err := GenerateFixChain(s, o)
	reportReject(t, err != nil, err)
	m := fixManifest(t, u)
	repo := t.TempDir()
	reportGit(t, repo, "init", "-b", "main")
	reportGit(t, repo, "config", "user.name", "Fixture")
	reportGit(t, repo, "config", "user.email", "fixture@example.test")
	for _, p := range []string{"continuation.md", "handoff.md", "source.go"} {
		writeBaselineFile(t, repo, p, "legacy\n")
	}
	reportGit(t, repo, "add", ".")
	reportGit(t, repo, "commit", "-m", "base")
	base := reportGit(t, repo, "rev-parse", "HEAD")
	storage := t.TempDir()
	t.Cleanup(func() { baselineCleanup(t, storage) })
	refs := backlog.CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	preceding := base
	for _, name := range []string{"fix1", "fix2"} {
		taskSpec := m.Tasks[name]
		output, err := reportCheck(repo, taskSpec.Preflight.Steps[0].Command[2])
		reportReject(t, err != nil, fmt.Sprintf("%s: %v", output, err))
		text := "source fixed\n"
		if name == "fix2" {
			text = "no-op: preceding review accepted " + preceding + "\n"
		}
		task := domain.Task{ID: name, Name: name, Outputs: []domain.ArtifactDeclaration{{Name: "fix", Commit: &domain.CommitOutput{Revision: "HEAD"}}}}
		for _, p := range taskSpec.Outputs {
			writeBaselineFile(t, repo, p, text)
			task.Outputs = append(task.Outputs, domain.ArtifactDeclaration{Name: p})
		}
		if name == "fix1" {
			writeBaselineFile(t, repo, "source.go", "fixed\n")
			reportGit(t, repo, "add", "source.go")
			reportGit(t, repo, "commit", "-m", "source fix")
		}
		head := reportGit(t, repo, "rev-parse", "HEAD")
		reportReject(t, name == "fix2" && head != preceding, "no-op changed preceding commit")
		for _, command := range []string{fixReportPathCheck, "git diff --quiet && git diff --cached --quiet"} {
			output, err := reportCheck(repo, command)
			reportReject(t, err != nil, fmt.Sprintf("%s: %v", output, err))
		}
		diff := reportGit(t, repo, "diff", "--name-only", base, head)
		reportReject(t, diff != "source.go", fmt.Sprintf("reports entered diff: %q", diff))
		finalizer := backlog.AttemptFinalizer{StorageRoot: storage, CampaignRefs: refs}
		result, err := finalizer.Finalize(context.Background(), backlog.AttemptFinalization{
			Task: task, Attempt: domain.Attempt{ID: "attempt-" + name, TaskID: name, WorkflowRunID: "run-fixture"},
			WorkspaceDir: repo, ExplicitSuccess: true, Repository: repo, BaseCommit: preceding,
		})
		reportReject(t, err != nil || !result.Completion.VerificationPassed || result.Completion.Failure != "", err, result.Completion)
		provenance, err := refs.Resolve("run-fixture", name, "fix")
		reportReject(t, err != nil || provenance.Commit != head, fmt.Sprintf("commit provenance: %+v %v", provenance, err))
		consumer := domain.Task{ID: "consumer", Name: "consumer", Needs: []string{name}, DependencyInputs: map[string][]string{name: {"artifacts/handoff.md", "artifacts/verification.log"}}}
		downstream := t.TempDir()
		t.Cleanup(func() { baselineCleanup(t, downstream) })
		_, err = backlog.MaterializeDependencies(downstream, storage, "run-fixture", consumer, []domain.Task{task}, result.Artifacts)
		reportReject(t, err != nil, err)
		for _, p := range taskSpec.Outputs {
			found := false
			for _, a := range result.Artifacts {
				if a.Name != p {
					continue
				}
				b, e := os.ReadFile(filepath.Join(storage, filepath.FromSlash(a.StoragePath)))
				reportReject(t, e != nil || string(b) != text, fmt.Sprintf("retained %s: %q %v", p, b, e))
				found = true
			}
			reportReject(t, !found, fmt.Sprintf("missing retained report %s", p))
		}
		matches, err := filepath.Glob(filepath.Join(downstream, ".t3/dependencies/*/artifacts/handoff.md"))
		reportReject(t, err != nil || len(matches) != 1, fmt.Sprintf("dependency report: %v %v", matches, err))
		preceding = head
	}
	reviewSpec := m.Tasks["review2"]
	reviewer := domain.Task{ID: "review2", Name: "review2", ReviewOutput: reviewSpec.ReviewOutput}
	for _, p := range reviewSpec.Outputs {
		text := "VERDICT: ACCEPT\n"
		if p == reviewSpec.ReviewOutput.Verdict {
			text = "{\"verdict\":\"accept\",\"blocking_findings\":0,\"finding_titles\":[]}\n"
		}
		writeBaselineFile(t, repo, p, text)
		reviewer.Outputs = append(reviewer.Outputs, domain.ArtifactDeclaration{Name: p})
	}
	for _, command := range reviewSpec.Verify {
		output, err := reportCheck(repo, command)
		reportReject(t, err != nil, fmt.Sprintf("%s: %v", output, err))
	}
	finalizer := backlog.AttemptFinalizer{StorageRoot: storage}
	result, err := finalizer.Finalize(context.Background(), backlog.AttemptFinalization{
		Task: reviewer, Attempt: domain.Attempt{ID: "attempt-review", TaskID: "review2", WorkflowRunID: "run-fixture"},
		WorkspaceDir: repo, ExplicitSuccess: true,
	})
	reportReject(t, err != nil || result.Completion.Failure != "", fmt.Sprintf("review capture: %+v %v", result, err))
	consumer := domain.Task{ID: "next-fix", Needs: []string{"review2"}, DependencyInputs: map[string][]string{"review2": m.Tasks["fix2"].InputsFrom["review2"]}}
	_, err = backlog.MaterializeDependencies(repo, storage, "run-fixture", consumer, []domain.Task{reviewer}, result.Artifacts)
	reportReject(t, err != nil, err)
	head := reportGit(t, repo, "rev-parse", "HEAD")
	reportReject(t, head != preceding, "review created a commit")
	output, err := reportCheck(repo, "git diff --quiet && git diff --cached --quiet")
	reportReject(t, err != nil, fmt.Sprintf("%s: %v", output, err))
	t.Cleanup(func() { baselineCleanup(t, repo) })
}

func TestFixReportTrackedCollisionStopsBeforeSourceEdits(t *testing.T) {
	for _, p := range []string{"artifacts/continuation.md", "artifacts/handoff.md", "artifacts/verification.log", "artifacts/review.md", "artifacts/verdict.json"} {
		t.Run(p, func(t *testing.T) {
			repo := t.TempDir()
			reportGit(t, repo, "init", "-b", "main")
			writeBaselineFile(t, repo, p, "existing product file\n")
			writeBaselineFile(t, repo, "source.go", "unchanged\n")
			reportGit(t, repo, "config", "user.name", "Fixture")
			reportGit(t, repo, "config", "user.email", "fixture@example.test")
			reportGit(t, repo, "add", p)
			reportGit(t, repo, "commit", "-m", "existing tracked report")
			output, err := reportCheck(repo, fixReportPathCheck)
			reportReject(t, err == nil || !strings.Contains(output, "report-path collision: "+p), fmt.Sprintf("%q %v", output, err))
			b, err := os.ReadFile(filepath.Join(repo, "source.go"))
			reportReject(t, err != nil || string(b) != "unchanged\n", "source changed")
			b, err = os.ReadFile(filepath.Join(repo, p))
			reportReject(t, err != nil || string(b) != "existing product file\n", "tracked report changed")
		})
	}
}

func TestFixReportStagedCollisionWithSourceEdit(t *testing.T) {
	s, o := fixFixture()
	u, err := GenerateFixChain(s, o)
	reportReject(t, err != nil, err)
	task := fixManifest(t, u).Tasks["fix1"]
	for _, p := range []string{"artifacts/continuation.md", "artifacts/handoff.md", "artifacts/verification.log", "artifacts/review.md", "artifacts/verdict.json"} {
		t.Run(p, func(t *testing.T) {
			repo := t.TempDir()
			reportGit(t, repo, "init", "-b", "main")
			reportGit(t, repo, "config", "user.name", "Fixture")
			reportGit(t, repo, "config", "user.email", "fixture@example.test")
			writeBaselineFile(t, repo, "source.go", "original\n")
			reportGit(t, repo, "add", "source.go")
			reportGit(t, repo, "commit", "-m", "base")
			writeBaselineFile(t, repo, "source.go", "fixed\n")
			writeBaselineFile(t, repo, p, "report\n")
			reportGit(t, repo, "add", "source.go", p)
			output, err := reportCheck(repo, task.Verify[len(task.Verify)-1])
			reportReject(t, err == nil || !strings.Contains(output, "report-path collision: "+p), output, err)
			reportReject(t, reportGit(t, repo, "diff", "--cached", "--name-only") != p+"\nsource.go", "check changed index")
			b, err := os.ReadFile(filepath.Join(repo, "source.go"))
			reportReject(t, err != nil || string(b) != "fixed\n", "check changed source")
		})
	}
}
