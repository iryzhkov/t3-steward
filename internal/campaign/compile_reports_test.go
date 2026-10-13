package campaign

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCompiledReportsCloseAcrossTemplates(t *testing.T) {
	cases := []CompilePlan{parseCompileFixture(t, compilePlanFixture), parseCompileFixture(t, compileRolesPlanFixture)}
	for _, raw := range []string{
		compileRoutingHead + "routes: {execute: {instance: codex, model: gpt-6.1-sol}}\nroles: {review: {effort: high}}\n" + compileRoutingUnits,
		planningFixture, strings.Replace(planningFixture, "ref: "+planningRef, "type: fresh", 1),
	} {
		p, err := ParseCompilePlan("plan.md", []byte(raw))
		reportReject(t, err != nil, err)
		cases = append(cases, p)
	}
	for _, plan := range cases {
		for _, unit := range plan.Units {
			m := compiledManifest(t, unit)
			for name, task := range m.Tasks {
				want := compilerReports().outputs(name == "review")
				if name == "plan" {
					want = append([]string{"plan.md", "risks.md"}, want...)
				}
				reportReject(t, !reflect.DeepEqual(task.Outputs, want), name, task.Outputs)
				reportReject(t, len(slices.Compact(append([]string(nil), task.Outputs...))) != len(want), "duplicate outputs")
				if name != "implement" {
					reportReject(t, len(task.Commits) != 0, name, "commits")
				}
				if name == "review" {
					reportReject(t, task.ReviewOutput == nil || task.ReviewOutput.VerdictLine != compilerReports().Review || task.ReviewOutput.Verdict != "", task.ReviewOutput)
				} else {
					reportReject(t, task.ReviewOutput != nil, "producer verdict")
				}
				for from, selectors := range task.InputsFrom {
					upstream := m.Tasks[from]
					for _, selector := range selectors {
						reportReject(t, !slices.Contains(upstream.Outputs, selector) && !(from == "implement" && selector == compiledCommitName), "undeclared selector", selector)
					}
				}
				fresh := m.Environment.Type == "fresh"
				check := compileReportCheck(fresh)
				steps := task.Preflight.Steps
				reportReject(t, len(steps) != 1 || steps[0].ID != "report-path-collision" || steps[0].Kind != "check" ||
					steps[0].FailurePolicy != "require-pass" || steps[0].Timeout != 2*time.Minute || steps[0].MaxOutputBytes != 4096 ||
					!reflect.DeepEqual(steps[0].Command, []string{"sh", "-c", check}), steps)
				reportReject(t, !slices.Contains(task.Verify, check) || slices.Contains(task.Verify, compileCleanSourceCheck) == fresh, task.Verify)
				prompt := compiledFile(t, unit, task.PromptFile)
				for _, text := range []string{check, compilerReports().Handoff, compilerReports().Verification, compilerReports().Continuation, "baseline evidence", "worker verification"} {
					reportReject(t, !strings.Contains(prompt, text), name, "missing", text)
				}
				for _, root := range []string{"`handoff.md`", "`continuation.md`", "`review.md`"} {
					reportReject(t, strings.Contains(prompt, root), root)
				}
				if fresh {
					reportReject(t, strings.Contains(check, "git "), "fresh guard uses Git")
				}
			}
		}
	}
	// Preserve command bytes/order and deduplicate only exact generated checks.
	check := compileReportCheck(false)
	authored := []string{"printf 'authored  command'", check, compileCleanSourceCheck}
	reportReject(t, !reflect.DeepEqual(compileReportChecks(authored, check, false), authored), "verify changed")
}

func TestCompiledReportCollisionRefusals(t *testing.T) {
	paths := append(compilerReports().outputs(true), "review.md", "artifacts", "../x", strings.Repeat("x", 256), "plan.md, plan.md")
	for _, path := range append(paths, "artifacts/verification.log/child", "artifacts/review.md/child") {
		raw := strings.Replace(planningFixture, "plan.md, risks.md", path, 1)
		p, err := ParseCompilePlan("plan.md", []byte(raw))
		var located *CompilePlanError
		reportReject(t, err == nil || len(p.Units) != 0, "accepted", path)
		reportReject(t, !errors.As(err, &located) || located.Line == 0, err)
	}
	raw := strings.Replace(planningFixture, "plan.md, risks.md", "notes/plan.md, artifacts/product.md", 1)
	_, err := ParseCompilePlan("plan.md", []byte(raw))
	reportReject(t, err != nil, err)
}

func compilerReportRepo(t *testing.T) (string, string) {
	t.Helper()
	repo := t.TempDir()
	reportGit(t, repo, "init", "-b", "main")
	reportGit(t, repo, "config", "user.name", "Fixture")
	reportGit(t, repo, "config", "user.email", "fixture@example.test")
	for _, p := range []string{"source.go", "continuation.md", "handoff.md", "review.md", "artifacts/product.md"} {
		writeBaselineFile(t, repo, p, "legacy\n")
	}
	reportGit(t, repo, "add", ".")
	reportGit(t, repo, "commit", "-m", "base")
	return repo, reportGit(t, repo, "rev-parse", "HEAD")
}

func TestCompiledReportGuardsProtectSource(t *testing.T) {
	check := compileReportCheck(false)
	for _, p := range compilerReports().outputs(true) {
		for _, kind := range []string{"staged", "tracked", "descendant", "symlink", "directory", "fifo"} {
			t.Run(p+"/"+kind, func(t *testing.T) {
				repo, _ := compilerReportRepo(t)
				target := p
				switch kind {
				case "descendant":
					target += "/child"
					fallthrough
				case "staged", "tracked":
					writeBaselineFile(t, repo, target, "product bytes\n")
					reportGit(t, repo, "add", target)
					if kind == "tracked" {
						reportGit(t, repo, "commit", "-m", "tracked report")
					}
				case "fifo":
					reportReject(t, syscall.Mkfifo(filepath.Join(repo, p), 0o600) != nil, "fifo")
				case "symlink":
					reportReject(t, os.Symlink("../source.go", filepath.Join(repo, p)) != nil, "symlink")
				case "directory":
					reportReject(t, os.Mkdir(filepath.Join(repo, p), 0o755) != nil, "directory")
				}
				writeBaselineFile(t, repo, "source.go", "meaningful staged source\n")
				reportGit(t, repo, "add", "source.go")
				head := reportGit(t, repo, "rev-parse", "HEAD")
				index := reportGit(t, repo, "ls-files", "--stage")
				diff := reportGit(t, repo, "diff", "--binary", "HEAD")
				output, err := reportCheck(repo, check)
				reportReject(t, err == nil || !strings.Contains(output, "report-path collision: "+p) || len(output) > 4096, output, err)
				reportReject(t, head != reportGit(t, repo, "rev-parse", "HEAD") || index != reportGit(t, repo, "ls-files", "--stage") ||
					diff != reportGit(t, repo, "diff", "--binary", "HEAD"), "guard mutated source/index")
				if kind != "symlink" && kind != "directory" && kind != "fifo" {
					b, e := os.ReadFile(filepath.Join(repo, target))
					reportReject(t, e != nil || string(b) != "product bytes\n", e)
				}
			})
		}
	}
	for _, kind := range []string{"blob", "blob-absent", "gitlink", "parent-symlink", "parent-file", "invalid-index", "invalid-repo"} {
		t.Run(kind, func(t *testing.T) {
			repo, base := compilerReportRepo(t)
			switch kind {
			case "blob":
				reportGit(t, repo, "rm", "artifacts/product.md")
				if err := os.Remove(filepath.Join(repo, "artifacts")); err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				writeBaselineFile(t, repo, "artifacts", "product parent\n")
				reportGit(t, repo, "add", "artifacts")
			case "blob-absent":
				reportGit(t, repo, "rm", "artifacts/product.md")
				blob := reportGit(t, repo, "rev-parse", base+":source.go")
				reportGit(t, repo, "update-index", "--add", "--cacheinfo", "100644,"+blob+",artifacts")
			case "gitlink":
				reportGit(t, repo, "rm", "artifacts/product.md")
				reportGit(t, repo, "update-index", "--add", "--cacheinfo", "160000,"+base+",artifacts")
			case "parent-symlink", "parent-file":
				reportGit(t, repo, "rm", "artifacts/product.md")
				if err := os.Remove(filepath.Join(repo, "artifacts")); err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				if kind == "parent-file" {
					writeBaselineFile(t, repo, "artifacts", "parent\n")
				} else {
					reportReject(t, os.Symlink(t.TempDir(), filepath.Join(repo, "artifacts")) != nil, "symlink")
				}
			case "invalid-index":
				writeBaselineFile(t, repo, ".git/index", "corrupt")
			case "invalid-repo":
				repo = t.TempDir()
			}
			output, err := reportCheck(repo, check)
			reportReject(t, err == nil || !strings.HasPrefix(output, "report-path "), output, err)
		})
	}
	repo, _ := compilerReportRepo(t)
	writeBaselineFile(t, repo, compilerReports().Handoff, "untracked report\n")
	output, err := reportCheck(repo, check)
	reportReject(t, err != nil, output, err)
	// Current staging differs from the cached baseline; final verification must catch it.
	step := compiledManifest(t, parseCompileFixture(t, compilePlanFixture).Units[0]).Tasks["implement"].Preflight.Steps
	now := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC)
	engine := backlog.PreflightEngine{Runner: compilerGuardRunner{}, Now: func() time.Time { return now }}
	request := backlog.PreflightRequest{TaskID: "implement", AttemptID: "a", WorkspaceDir: repo, WorkerID: "w", SourceRevision: "head",
		EnvironmentDigest: "env", Steps: step, Freshness: time.Hour}
	first, err := engine.Run(context.Background(), request)
	reportReject(t, err != nil || !first.MayLaunch(), err, first)
	reportGit(t, repo, "add", compilerReports().Handoff)
	request.Cached = first.Receipts
	cached, err := engine.Run(context.Background(), request)
	reportReject(t, err != nil || !cached.MayLaunch() || !slices.Contains(cached.Reused, "report-path-collision"), cached, err)
	output, err = reportCheck(repo, check)
	reportReject(t, err == nil, "final guard missed staged collision", output)
	request.Cached = nil
	current, err := engine.Run(context.Background(), request)
	reportReject(t, err != nil || current.MayLaunch(), current, err)
}

type compilerGuardRunner struct{}

func (compilerGuardRunner) Run(ctx context.Context, r backlog.ProcessRequest) (backlog.ProcessResult, error) {
	cmd := exec.CommandContext(ctx, r.Program, r.Args...)
	cmd.Dir = r.Dir
	b, err := cmd.CombinedOutput()
	if err != nil {
		return backlog.ProcessResult{ExitCode: 1, Output: string(b)}, &backlog.ProcessExitError{ExitCode: 1, Err: err}
	}
	return backlog.ProcessResult{Output: string(b)}, nil
}

func TestCompiledReportsRetainAndMaterialize(t *testing.T) { compilerReportCapture(t, false) }
func TestCompiledFreshRolesWriteTheirOwnLogs(t *testing.T) {
	compilerReportCapture(t, true)
	for _, p := range append(compilerReports().outputs(true), "artifacts") {
		for _, link := range []bool{false, true} {
			dir := t.TempDir()
			reportReject(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, p)), 0o755) != nil, p)
			if link {
				reportReject(t, os.Symlink(t.TempDir(), filepath.Join(dir, p)) != nil, p)
			} else {
				reportReject(t, os.Mkdir(filepath.Join(dir, p), 0o755) != nil, p)
				if p == "artifacts" { // A real parent directory is allowed.
					output, err := reportCheck(dir, compileReportCheck(true))
					reportReject(t, err != nil, output, err)
					continue
				}
			}
			output, err := reportCheck(dir, compileReportCheck(true))
			reportReject(t, err == nil, p, output)
		}
	}
}

func compilerReportCapture(t *testing.T, fresh bool) {
	t.Helper()
	raw := planningFixture
	producerName := "plan"
	if !fresh {
		raw = string(readCompileFixture(t))
		producerName = "implement"
	} else {
		raw = strings.Replace(raw, "ref: "+planningRef, "type: fresh", 1)
	}
	plan, err := ParseCompilePlan("plan", []byte(raw))
	reportReject(t, err != nil, err)
	m := compiledManifest(t, plan.Units[0])
	repo, base := t.TempDir(), ""
	if !fresh {
		repo, base = compilerReportRepo(t)
		writeBaselineFile(t, repo, "source.go", "implemented\n")
		reportGit(t, repo, "add", "source.go")
		reportGit(t, repo, "commit", "-m", "source")
	}
	head := base
	if !fresh {
		head = reportGit(t, repo, "rev-parse", "HEAD")
	}
	storage := t.TempDir()
	t.Cleanup(func() { baselineCleanup(t, storage) })
	refs := backlog.CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	finalizer := backlog.AttemptFinalizer{StorageRoot: storage, CampaignRefs: refs}
	taskFor := func(name string) domain.Task {
		spec := m.Tasks[name]
		task := domain.Task{ID: name, Name: name, ReviewOutput: spec.ReviewOutput}
		for _, p := range spec.Outputs {
			task.Outputs = append(task.Outputs, domain.ArtifactDeclaration{Name: p})
		}
		for _, c := range spec.Commits {
			task.Outputs = append(task.Outputs, domain.ArtifactDeclaration{Name: c.Name, Commit: &domain.CommitOutput{Revision: c.Revision}})
		}
		return task
	}
	capture := func(name, workspace, log string) (domain.Task, backlog.FinalizedAttempt) {
		task := taskFor(name)
		output, err := reportCheck(workspace, compileReportCheck(fresh))
		reportReject(t, err != nil, output, err)
		for _, p := range m.Tasks[name].Outputs {
			text := name + " evidence\n"
			if p == compilerReports().Verification {
				text = log
			}
			if p == compilerReports().Review {
				text = "VERDICT: ACCEPT\nreviewed " + head + "\n"
			}
			writeBaselineFile(t, workspace, p, text)
		}
		for _, command := range compileReportChecks(nil, compileReportCheck(fresh), fresh) {
			output, err := reportCheck(workspace, command)
			reportReject(t, err != nil, output, err)
			log += fmt.Sprintf("command: %s\nexit: 0\noutput:\n%s", command, output)
		}
		if fresh {
			log += fmt.Sprintf("plan input SHA-256: %x\n", sha256.Sum256([]byte(raw)))
		} else {
			log += "HEAD: " + head + "\n"
		}
		writeBaselineFile(t, workspace, compilerReports().Verification, log)
		result, err := finalizer.Finalize(context.Background(), backlog.AttemptFinalization{
			Task: task, Attempt: domain.Attempt{ID: "attempt-" + name, TaskID: name, WorkflowRunID: "run-reports"},
			WorkspaceDir: workspace, ExplicitSuccess: true, Repository: workspace, BaseCommit: base,
		})
		reportReject(t, err != nil || result.Completion.Failure != "", result.Completion, err)
		for _, p := range m.Tasks[name].Outputs {
			found := false
			for _, a := range result.Artifacts {
				if a.Name != p {
					continue
				}
				b, e := os.ReadFile(filepath.Join(storage, filepath.FromSlash(a.StoragePath)))
				expected, e2 := os.ReadFile(filepath.Join(workspace, p))
				digest := fmt.Sprintf("%x", sha256.Sum256(b))
				reportReject(t, e != nil || e2 != nil || string(b) != string(expected) || a.Size != int64(len(b)) || a.SHA256 != digest, a, e, e2)
				if p == compilerReports().Review {
					verdict, e := backlog.ParseReviewVerdict(*task.ReviewOutput, b)
					reportReject(t, e != nil || verdict.Verdict != "accept", verdict, e)
				}
				found = true
			}
			reportReject(t, !found, "missing retained", p)
		}
		return task, result
	}
	producerLog := "producer tested " + head + "\n"
	if fresh {
		producerLog = "no verification command declared\ninput digest fixture\n"
	}
	producer, result := capture(producerName, repo, producerLog)
	producerBytes, err := os.ReadFile(filepath.Join(repo, compilerReports().Verification))
	reportReject(t, err != nil || !strings.Contains(string(producerBytes), "exit: 0"), err)
	producerLog = string(producerBytes)
	if fresh {
		reportReject(t, !strings.Contains(producerLog, fmt.Sprintf("%x", sha256.Sum256([]byte(raw)))), "missing actual input digest")
	}
	if !fresh {
		provenance, e := refs.Resolve("run-reports", producerName, compiledCommitName)
		reportReject(t, e != nil || provenance.Commit != head || reportGit(t, repo, "diff", "--name-only", base, head) != "source.go", provenance, e)
		reportReject(t, backlog.ValidateCampaignCandidate(context.Background(), repo, head, base) != nil ||
			backlog.ValidateCampaignCandidate(context.Background(), repo, head, head) != nil, "source/no-op candidate rejected")
	}
	consumer := domain.Task{ID: "review", Name: "review", Needs: []string{producerName}, DependencyInputs: m.Tasks["review"].InputsFrom}
	downstream := t.TempDir()
	t.Cleanup(func() { baselineCleanup(t, downstream) })
	_, err = backlog.MaterializeDependencies(downstream, storage, "run-reports", consumer, []domain.Task{producer}, result.Artifacts)
	reportReject(t, err != nil, err)
	mounts, err := filepath.Glob(filepath.Join(downstream, ".t3/dependencies/*/artifacts/verification.log"))
	reportReject(t, err != nil || len(mounts) != 1, mounts, err)
	mounted, err := os.ReadFile(mounts[0])
	reportReject(t, err != nil || string(mounted) != producerLog, err)
	// Missing/corrupt H/V fail without publishing a partial dependency directory.
	for _, name := range []string{compilerReports().Handoff, compilerReports().Verification} {
		for _, corrupt := range []bool{false, true} {
			artifacts := append([]domain.Artifact(nil), result.Artifacts...)
			for i, a := range artifacts {
				if a.Name == name {
					if corrupt {
						artifacts[i].SHA256 = strings.Repeat("0", 64)
					} else {
						artifacts = append(artifacts[:i], artifacts[i+1:]...)
					}
					break
				}
			}
			failed := t.TempDir()
			_, e := backlog.MaterializeDependencies(failed, storage, "run-reports", consumer, []domain.Task{producer}, artifacts)
			reportReject(t, e == nil, "accepted missing/corrupt", name)
			entries, _ := os.ReadDir(filepath.Join(failed, ".t3/dependencies"))
			reportReject(t, len(entries) != 0, "partial dependency published")
		}
	}
	reviewWorkspace := downstream
	if !fresh {
		reviewWorkspace = repo
	}
	reviewer, reviewResult := capture("review", reviewWorkspace, "reviewer own checks "+head+"\n")
	reviewerBytes, err := os.ReadFile(filepath.Join(reviewWorkspace, compilerReports().Verification))
	reportReject(t, err != nil || string(reviewerBytes) == producerLog || !strings.Contains(string(reviewerBytes), "exit: 0"), "reviewer did not write its own command log")
	reportReject(t, len(m.Tasks["review"].Commits) != 0 || len(reviewer.Outputs) != 4 || len(reviewResult.Artifacts) < 4, reviewer)
	mountedAfter, err := os.ReadFile(mounts[0])
	reportReject(t, err != nil || string(mountedAfter) != producerLog, "incoming log overwritten")
	if !fresh {
		reportReject(t, reportGit(t, repo, "rev-parse", "HEAD") != head, "review commit")
		for _, p := range []string{"continuation.md", "handoff.md", "review.md"} {
			b, e := os.ReadFile(filepath.Join(repo, p))
			reportReject(t, e != nil || string(b) != "legacy\n", p)
		}
		// The unchanged candidate API rejects report-only work.
		writeBaselineFile(t, repo, "handoff.md", "report only\n")
		reportGit(t, repo, "add", "handoff.md")
		reportGit(t, repo, "commit", "-m", "report only")
		reportReject(t, backlog.ValidateCampaignCandidate(context.Background(), repo, reportGit(t, repo, "rev-parse", "HEAD"), head) == nil, "report-only accepted")
	}
}
