package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/review"
)

func reviewCatalog() []backlogadmin.Project {
	return []backlogadmin.Project{{Name: "scratch", Type: "fresh", Workers: []backlogadmin.ProjectWorker{{Worker: "w", Routes: []backlogadmin.ProjectRoute{
		{Instance: "a", Model: "full", ProviderFamily: "claude", Tier: "executor"},
		{Instance: "b", Model: "full", ProviderFamily: "openai", Tier: "executor"},
		{Instance: "a", Model: "cheap", ProviderFamily: "claude", Tier: "economy"},
		{Instance: "b", Model: "cheap", ProviderFamily: "openai", Tier: "economy"},
		{Instance: "alias", Model: "full", ProviderFamily: "claude", Tier: "executor"},
	}}}}}
}

func TestReviewRoundCompositionAndIsolation(t *testing.T) {
	plan := filepath.Join(t.TempDir(), "plan.md")
	if err := os.WriteFile(plan, []byte("UNTRUSTED PLAN SECRET"), 0600); err != nil {
		t.Fatal(err)
	}
	args, err := parseReviewArgs([]string{"--plan", plan, "--project", "scratch", "--reviewer", "a/full", "--independent", "b/full", "--swarm", "security,errors,tests", "--swarm-model", "a/cheap", "--swarm-model", "b/cheap", "--judge", "b/full", "--no-notify"})
	if err != nil {
		t.Fatal(err)
	}
	dir, err := buildReviewCampaign(args, reviewCatalog(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	bundle, err := campaign.Load(dir, campaign.DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Manifest.Tasks) != 6 {
		t.Fatalf("tasks %d", len(bundle.Manifest.Tasks))
	}
	for name, task := range bundle.Manifest.Tasks {
		prompt, err := os.ReadFile(filepath.Join(dir, task.PromptFile))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(prompt), "UNTRUSTED PLAN SECRET") {
			t.Fatal("pasted input")
		}
		if !strings.Contains(string(prompt), ".t3/inputs/") || !strings.Contains(string(prompt), "untrusted") {
			t.Fatal("missing trust instructions")
		}
		if strings.HasPrefix(name, "independent") && (len(task.Needs) > 0 || len(task.InputsFrom) > 0) {
			t.Fatal("anchored independent reviewer")
		}
		if name == "judge" && (len(task.Needs) != 3 || len(task.InputsFrom) != 3) {
			t.Fatal("judge must see only swarm")
		}
	}
	if bundle.Manifest.Review == nil || len(bundle.Manifest.Review.Reviewers) != 6 {
		t.Fatal("missing round")
	}
}

func TestReviewConstraintsRefuseInsteadOfWeakening(t *testing.T) {
	for _, args := range [][]string{
		{"--project", "scratch", "--reviewer", "a/full", "--reviewer", "alias/full", "--no-notify"},
		{"--project", "scratch", "--reviewer", "a/full", "--judge", "b/cheap", "--no-notify"},
		{"--project", "scratch", "--reviewer", "a/full", "--reviewer", "b/full", "--swarm", "security", "--no-notify"},
		{"--project", "scratch", "--reviewer", "a/full", "--reviewer", "b/full", "--swarm", "invented", "--no-notify"},
		{"--project", "scratch", "--reviewer", "unknown/model", "--no-notify"},
		{"--diff", "main..HEAD", "--reviewer", "a/full", "--reviewer", "b/full", "--no-notify"},
		{"--project", "scratch", "--reviewer", "a/full", "--reviewer", "b/full", "--deadline", "0s", "--no-notify"},
	} {
		a, err := parseReviewArgs(args)
		if err == nil {
			var dir string
			dir, err = buildReviewCampaign(a, reviewCatalog(), time.Now())
			if dir != "" {
				os.RemoveAll(dir)
			}
		}
		if err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

// The command submits a real ingested campaign and the fake fleet completes
// its six tasks. Evidence then crosses the durable collection/result boundary.
func TestReviewCommandFullRound(t *testing.T) {
	for _, failure := range []string{"", "malformed", "timeout"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			store, err := sqlite.OpenMigrated(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			plan := filepath.Join(t.TempDir(), "plan.md")
			if err := os.WriteFile(plan, []byte("plan evidence"), 0600); err != nil {
				t.Fatal(err)
			}
			h := newTaskRunHarness()
			h.projects = reviewCatalog()
			task := h.cli()
			storage := t.TempDir()
			t.Cleanup(func() {
				_ = filepath.WalkDir(storage, func(path string, entry os.DirEntry, err error) error {
					if err == nil && entry.IsDir() {
						return os.Chmod(path, 0700)
					}
					return err
				})
			})
			service := backlog.SubmissionService{Store: store, StorageRoot: storage, MaxBytes: 4 << 20, MaxFiles: 100}
			resultRoot := t.TempDir()
			payloads := map[string]string{}
			task.campaign.submissions = func() (adminSubmissionService, error) {
				return submissionFunc(func(ctx context.Context, request backlogadmin.LocalSubmissionRequest, body io.Reader, _ int64) (backlogadmin.LocalSubmissionResponse, error) {
					result, err := service.SubmitArchive(ctx, backlog.ArchiveSubmission{IdempotencyKey: request.IdempotencyKey, Archive: body})
					if err != nil {
						return backlogadmin.LocalSubmissionResponse{}, err
					}
					round, err := store.GetReviewRound(ctx, result.Record.RunID)
					if err != nil {
						return backlogadmin.LocalSubmissionResponse{}, err
					}
					if len(round.Reviewers) != 6 || round.WorkflowRunID != result.Record.RunID {
						t.Fatal("round not atomically bound to campaign")
					}
					records, err := store.LoadCoordinatorRecords(ctx)
					if err != nil {
						return backlogadmin.LocalSubmissionResponse{}, err
					}
					now := time.Now().UTC()
					for i := range records.Attempts {
						attempt := &records.Attempts[i]
						attempt.Progress = domain.ProgressSucceeded
						attempt.CompletedAt = &now
						var member review.Reviewer
						for _, r := range round.Reviewers {
							if r.TaskID == attempt.TaskID {
								member = r
							}
						}
						if failure == "timeout" && member.ID == "independent-2" {
							attempt.Progress = domain.ProgressActive
							attempt.CompletedAt = nil
							continue
						}
						v := review.Verdict{Schema: review.Schema, Verdict: "accept", Findings: []review.Finding{}, InputManifestDigest: round.InputManifestDigest, ReviewerRoute: member.Route}
						raw, _ := json.Marshal(v)
						if failure == "malformed" && member.ID == "independent-2" {
							raw = []byte("{broken")
						}
						for _, doc := range []string{"review.md", "verdict.json"} {
							id := member.ID + "-" + doc
							content := "review accepted"
							if doc == "verdict.json" {
								content = string(raw)
							}
							payloads[id] = content
							records.Artifacts = append(records.Artifacts, domain.Artifact{ID: id, WorkflowRunID: round.WorkflowRunID, TaskID: member.TaskID, AttemptID: attempt.ID, Kind: domain.ArtifactOutput, Name: doc, Size: int64(len(content)), SHA256: strings.Repeat("a", 64), StoragePath: id, CreatedAt: now})
						}
					}
					if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
						return backlogadmin.LocalSubmissionResponse{}, err
					}
					collector := backlog.ReviewCollector{Store: store, Results: resultRoot, Open: func(_ context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
						return domain.Artifact{ID: id}, io.NopCloser(strings.NewReader(payloads[id])), nil
					}}
					collectAt := now
					if failure == "timeout" {
						collectAt = round.Deadline.Add(time.Second)
					}
					if err := collector.Tick(ctx, collectAt); err != nil {
						return backlogadmin.LocalSubmissionResponse{}, err
					}
					return backlogadmin.LocalSubmissionResponse{RunID: round.ID, Key: request.IdempotencyKey}, nil
				}), nil
			}
			var out bytes.Buffer
			cli := reviewCLI{task: task, result: reviewResultCLI{results: resultRoot, stdout: &out, query: func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
				r, err := store.GetReviewRound(ctx, q.RoundID)
				return backlogadmin.Response{ReviewRound: &r}, err
			}}}
			a, err := parseReviewArgs([]string{"--plan", plan, "--project", "scratch", "--reviewer", "a/full", "--independent", "b/full", "--swarm", "security,errors,tests", "--swarm-model", "a/cheap", "--swarm-model", "b/cheap", "--judge", "b/full", "--wait", "--json"})
			if err != nil {
				t.Fatal(err)
			}
			err = cli.run(ctx, a)
			if failure == "" && err != nil {
				t.Fatal(err)
			}
			if failure != "" {
				code, ok := err.(exitCodeError)
				if !ok || code.code != 2 {
					t.Fatalf("collection failure exit: %v", err)
				}
			}
			var reply review.Reply
			if err := json.Unmarshal(out.Bytes(), &reply); err != nil {
				t.Fatal(err)
			}
			if len(reply.Reviewers) != 6 || reply.SummaryPath == "" {
				t.Fatal("incomplete short reply")
			}
			if failure != "" && reply.CombinedVerdict != "reject" {
				t.Fatal("failure became acceptance")
			}
		})
	}
}

func TestReviewNotificationAndOutsideT3Refusal(t *testing.T) {
	h := newTaskRunHarness()
	h.projects = reviewCatalog()
	a, err := parseReviewArgs([]string{"--project", "scratch", "--reviewer", "a/full", "--reviewer", "b/full"})
	if err != nil {
		t.Fatal(err)
	}
	c := reviewCLI{task: h.cli()}
	if err := c.run(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if len(h.notified) != 1 || h.notified[0].Request.ThreadID != h.thread {
		t.Fatal("caller not notified")
	}
	if h.notified[0].Request.Target.RunID != "run-1" {
		t.Fatal("wrong round notification")
	}
	h = newTaskRunHarness()
	h.projects = reviewCatalog()
	h.thread = ""
	h.threadErr = fmt.Errorf("no current T3 thread")
	c = reviewCLI{task: h.cli()}
	if err := c.run(context.Background(), a); err == nil || !strings.Contains(err.Error(), "--no-notify") {
		t.Fatalf("outside T3: %v", err)
	}
	if len(h.archives) != 0 {
		t.Fatal("submitted before resolving notification")
	}
}

func TestReviewUnusedSwarmRouteIsNotSilentlyIgnored(t *testing.T) {
	a, err := parseReviewArgs([]string{"--project", "scratch", "--reviewer", "a/full", "--swarm", "security", "--swarm-model", "a/cheap", "--swarm-model", "b/full", "--judge", "b/full", "--no-notify"})
	if err != nil {
		t.Fatal(err)
	}
	dir, err := buildReviewCampaign(a, reviewCatalog(), time.Now())
	if dir != "" {
		os.RemoveAll(dir)
	}
	if err == nil {
		t.Fatal("unused executor-tier swarm route silently accepted")
	}
}

func TestReviewRiskSelectsDefaultLenses(t *testing.T) {
	for _, risk := range []string{"routine", "risky"} {
		a, err := parseReviewArgs([]string{"--project", "scratch", "--reviewer", "a/full", "--judge", "b/full", "--swarm", "--swarm-model", "a/cheap", "--swarm-model", "b/cheap", "--risk", risk, "--no-notify"})
		if err != nil {
			t.Fatal(err)
		}
		dir, err := buildReviewCampaign(a, reviewCatalog(), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		bundle, err := campaign.Load(dir, campaign.DefaultLimits)
		os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
		want := 5
		if risk == "risky" {
			want = 9
		}
		if len(bundle.Manifest.Tasks) != want {
			t.Fatalf("%s: %d tasks", risk, len(bundle.Manifest.Tasks))
		}
	}
}

func TestReviewGateExit(t *testing.T) {
	r := review.Round{ID: "round-gate", InputManifestDigest: strings.Repeat("a", 64), Reviewers: []review.Reviewer{{ID: "r", Role: "independent", Required: true, Route: "a/full"}}}
	if err := r.Initialize(time.Now()); err != nil {
		t.Fatal(err)
	}
	v := review.Verdict{Schema: review.Schema, Verdict: "accept-with-changes", InputManifestDigest: r.InputManifestDigest, ReviewerRoute: "a/full", Findings: []review.Finding{{ID: "f", Title: "minor", Severity: "low", Evidence: []string{"file.go:1"}, Recommendation: "fix"}}}
	raw, _ := json.Marshal(v)
	if err := r.ApplyResult("r", review.Result{State: "succeeded", ReviewMD: "minor", VerdictJSON: raw}, time.Now()); err != nil {
		t.Fatal(err)
	}
	c := reviewResultCLI{results: t.TempDir(), stdout: os.Stdout, query: func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
		return backlogadmin.Response{ReviewRound: &r}, nil
	}}
	if err := c.run(context.Background(), []string{r.ID}); err != nil {
		t.Fatal(err)
	}
	err := c.run(context.Background(), []string{r.ID, "--gate"})
	code, ok := err.(exitCodeError)
	if !ok || code.code != 3 {
		t.Fatalf("gate: %v", err)
	}
}
