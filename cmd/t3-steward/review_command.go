package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/compat"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
	"gopkg.in/yaml.v3"
)

const reviewUsage = `Usage: t3-steward review [--plan FILE]... [--diff BASE..HEAD | --diff-file FILE]
 [--criteria FILE] [--project P] [--reviewer INSTANCE/MODEL]...
 [--independent INSTANCE/MODEL] [--model INSTANCE/MODEL] [--judge INSTANCE/MODEL]
 [--swarm LENS,...] [--swarm-model INSTANCE/MODEL]... [--risk routine|risky]
 [--deadline D] [--wait | --no-notify | --notify-thread ID] [--gate] [--json]
 t3-steward review result <round> [--wait] [--gate] [--json]

Inputs are snapshotted as files. --diff requires a catalog git project matching
the current checkout; refs are resolved once and reviewers receive HEAD plus diff.
Explicit routes require catalog provider_family and tier metadata. At least one
independent reviewer is required. Independent reviewers plus judge must include
two provider families when the project offers two. Every reviewer is a new task,
never the caller's session. Swarm requires economy routes and an executor judge.
--swarm default uses lenses for the risk (default routine). Deadline defaults to 4h.
Inside T3, the current thread is notified by default. Outside T3, specify --wait,
--no-notify or --notify-thread ID. --wait collects via review result --wait.
--gate requires --wait on submission and rejects anything other than accept.
Exit 0: all results valid, whatever verdict; 2: collection failed; 3: gate rejected;
1: pending, timeout of the client wait, invalid flags or submission failure.
See docs/review.md. Role policy and in-task review are not implemented.
`

type reviewArgs struct {
	plans, reviewers, swarmModels                                       []string
	diff, diffFile, criteria, project, judge, swarm, risk, notifyThread string
	deadline                                                            time.Duration
	wait, noNotify, gate, asJSON                                        bool
}
type reviewStrings []string

func (v *reviewStrings) String() string     { return strings.Join(*v, ",") }
func (v *reviewStrings) Set(s string) error { *v = append(*v, s); return nil }

func parseReviewArgs(args []string) (reviewArgs, error) {
	var a reviewArgs
	f := flag.NewFlagSet("review", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.Var((*reviewStrings)(&a.plans), "plan", "")
	f.Var((*reviewStrings)(&a.reviewers), "reviewer", "")
	f.Var((*reviewStrings)(&a.reviewers), "model", "")
	f.Var((*reviewStrings)(&a.reviewers), "independent", "")
	f.Var((*reviewStrings)(&a.swarmModels), "swarm-model", "")
	f.StringVar(&a.diff, "diff", "", "")
	f.StringVar(&a.diffFile, "diff-file", "", "")
	f.StringVar(&a.criteria, "criteria", "", "")
	f.StringVar(&a.project, "project", "", "")
	f.StringVar(&a.judge, "judge", "", "")
	f.StringVar(&a.swarm, "swarm", "", "")
	f.StringVar(&a.risk, "risk", "routine", "")
	f.StringVar(&a.notifyThread, "notify-thread", "", "")
	f.DurationVar(&a.deadline, "deadline", 4*time.Hour, "")
	f.BoolVar(&a.wait, "wait", false, "")
	f.BoolVar(&a.noNotify, "no-notify", false, "")
	f.BoolVar(&a.gate, "gate", false, "")
	f.BoolVar(&a.asJSON, "json", false, "")
	// A bare --swarm selects defaults; the documented value form also works.
	for i, arg := range args {
		if arg == "--swarm" && (i+1 == len(args) || strings.HasPrefix(args[i+1], "--")) {
			args = append([]string(nil), args...)
			args[i] = "--swarm=default"
		}
	}
	if err := f.Parse(args); err != nil {
		return a, err
	}
	if f.NArg() != 0 {
		return a, errors.New("review accepts flags only; try review --help")
	}
	if a.deadline <= 0 || a.deadline > 7*24*time.Hour {
		return a, errors.New("--deadline must be positive and at most 168h")
	}
	if a.risk != "routine" && a.risk != "risky" {
		return a, errors.New("--risk must be routine or risky")
	}
	if a.diff != "" && (a.diffFile != "" || a.project == "") {
		return a, errors.New("--diff requires --project and excludes --diff-file")
	}
	modes := 0
	for _, set := range []bool{a.wait, a.noNotify, a.notifyThread != ""} {
		if set {
			modes++
		}
	}
	if modes > 1 {
		return a, errors.New("choose only one of --wait, --no-notify, --notify-thread")
	}
	if a.gate && !a.wait {
		return a, errors.New("--gate requires --wait on review submission")
	}
	return a, nil
}

func reviewProject(name string, projects []backlogadmin.Project) (backlogadmin.Project, error) {
	if name == "" {
		for _, p := range projects {
			if p.Type == "fresh" {
				if name != "" {
					return backlogadmin.Project{}, errors.New("multiple fresh catalog projects; pass --project P")
				}
				name = p.Name
			}
		}
	}
	for _, p := range projects {
		if p.Name == name {
			return p, nil
		}
	}
	return backlogadmin.Project{}, errors.New("review requires a catalog project; pass --project P (see t3-steward projects)")
}

func reviewRoutes(project backlogadmin.Project) (map[string]backlogadmin.ProjectRoute, int, error) {
	routes := map[string]backlogadmin.ProjectRoute{}
	families := map[string]bool{}
	for _, w := range project.Workers {
		for _, r := range w.Routes {
			key := r.Instance + "/" + r.Model
			if old, ok := routes[key]; ok && (old.ProviderFamily != r.ProviderFamily || old.Tier != r.Tier) {
				return nil, 0, fmt.Errorf("conflicting catalog review metadata for %s", key)
			}
			if r.ProviderFamily == "" {
				return nil, 0, fmt.Errorf("provider diversity cannot be verified: catalog route %s lacks provider_family; configure backlog_v2.review_routes", key)
			}
			routes[key] = r
			if r.ProviderFamily != "" {
				families[r.ProviderFamily] = true
			}
		}
	}
	return routes, len(families), nil
}

func buildReviewCampaign(a reviewArgs, projects []backlogadmin.Project, now time.Time) (string, error) {
	project, err := reviewProject(a.project, projects)
	if err != nil {
		return "", err
	}
	routes, families, err := reviewRoutes(project)
	if err != nil {
		return "", err
	}
	round := review.Round{ID: "pending", Risk: a.risk, Deadline: now.Add(a.deadline).UTC(), TemplateVersion: review.TemplateVersion}
	add := func(id, role, route string, required bool) error {
		if !review.ValidRoute(route) {
			return fmt.Errorf("route %q must be INSTANCE/MODEL", route)
		}
		m, ok := routes[route]
		if !ok || m.ProviderFamily == "" || m.Tier == "" {
			return fmt.Errorf("route %s lacks catalog review metadata; configure backlog_v2.review_routes provider_family and tier", route)
		}
		round.Reviewers = append(round.Reviewers, review.Reviewer{ID: id, Role: role, Route: route, Required: required, ProviderFamily: m.ProviderFamily, Tier: m.Tier})
		return nil
	}
	for i, r := range a.reviewers {
		if err := add(fmt.Sprintf("independent-%d", i+1), "independent", r, true); err != nil {
			return "", err
		}
	}
	lenses := []string{}
	if a.swarm != "" {
		lensSet := a.swarm
		if lensSet == "default" {
			lensSet = "security,errors,tests"
			if a.risk == "risky" {
				lensSet = "security,concurrency,errors,rollout,tests,resources,docs"
			}
		}
		seen := map[string]bool{}
		for _, lens := range strings.Split(lensSet, ",") {
			switch lens {
			case "security", "concurrency", "errors", "rollout", "tests", "resources", "docs":
			default:
				return "", fmt.Errorf("unknown swarm lens %q", lens)
			}
			if seen[lens] {
				return "", fmt.Errorf("duplicate swarm lens %s", lens)
			}
			seen[lens] = true
			lenses = append(lenses, lens)
		}
		if len(a.swarmModels) == 0 {
			return "", errors.New("swarm needs explicit --swarm-model INSTANCE/MODEL")
		}
		for _, route := range a.swarmModels {
			m, ok := routes[route]
			if !ok || m.Tier != "economy" || m.ProviderFamily == "" {
				return "", fmt.Errorf("swarm route %s requires catalog economy-tier metadata", route)
			}
		}
		previous := ""
		for i, lens := range lenses {
			index := i % len(a.swarmModels)
			if len(a.swarmModels) > 1 {
				for offset := 0; offset < len(a.swarmModels); offset++ {
					candidate := (index + offset) % len(a.swarmModels)
					if routes[a.swarmModels[candidate]].ProviderFamily != previous {
						index = candidate
						break
					}
				}
			}
			route := a.swarmModels[index]
			if err := add("swarm-"+lens, "swarm:"+lens, route, false); err != nil {
				return "", err
			}
			previous = routes[route].ProviderFamily
		}
	} else if len(a.swarmModels) > 0 {
		return "", errors.New("--swarm-model requires --swarm")
	}
	if a.judge != "" {
		if err := add("judge", "judge", a.judge, true); err != nil {
			return "", err
		}
	}
	if err := review.ValidateSelection(round, families); err != nil {
		return "", err
	}
	files := append([]string(nil), a.plans...)
	if a.diffFile != "" {
		files = append(files, a.diffFile)
	}
	if a.criteria != "" {
		files = append(files, a.criteria)
	}
	generated := ""
	if a.diff != "" {
		if project.Type == "fresh" || project.Repository == "" {
			return "", errors.New("--diff requires a catalog git project")
		}
		checkout, err := readGitCheckout()
		if err != nil {
			return "", err
		}
		if normalizeRepository(checkout.Remote) != normalizeRepository(project.Repository) {
			return "", errors.New("--diff checkout remote does not match --project repository")
		}
		base, head, ok := strings.Cut(a.diff, "..")
		if !ok || base == "" || head == "" || strings.Contains(head, "..") {
			return "", errors.New("--diff must be BASE..HEAD")
		}
		resolve := func(ref string) (string, error) {
			raw, err := exec.Command("git", "rev-parse", "--verify", "--end-of-options", ref+"^{commit}").Output()
			return strings.TrimSpace(string(raw)), err
		}
		round.BaseCommit, err = resolve(base)
		if err != nil {
			return "", fmt.Errorf("resolve diff base: %w", err)
		}
		round.HeadCommit, err = resolve(head)
		if err != nil {
			return "", fmt.Errorf("resolve diff head: %w", err)
		}
		generated, err = os.MkdirTemp("", "t3-review-diff-")
		if err != nil {
			return "", err
		}
		defer os.RemoveAll(generated)
		// Resolve only the temporary directory we created. Caller inputs still
		// pass unchanged through ingestion's strict symlink refusal.
		generated, err = filepath.EvalSymlinks(generated)
		if err != nil {
			return "", fmt.Errorf("resolve generated diff directory: %w", err)
		}
		path := filepath.Join(generated, "review.diff")
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return "", err
		}
		// Limit the process output before snapshotting, not after allocating it.
		cmd := exec.Command("git", "diff", "--no-ext-diff", "--no-textconv", round.BaseCommit, round.HeadCommit, "--")
		cmd.Stdout = &reviewBoundedWriter{writer: f, remaining: pinnedinput.MaxFileBytes}
		runErr := cmd.Run()
		closeErr := f.Close()
		if runErr != nil {
			return "", fmt.Errorf("generate bounded diff: %w", runErr)
		}
		if closeErr != nil {
			return "", closeErr
		}
		files = append(files, path)
	}
	snapshot, err := pinnedinput.SnapshotFiles(files)
	if err != nil {
		return "", err
	}
	round.InputManifestDigest = snapshot.Manifest.Digest
	manifest := backlog.Manifest{Version: backlog.ManifestVersion, Name: "review-round", Class: domain.TaskClassRequired,
		Environment: backlog.ManifestEnvironment{Project: project.Name, Type: backlog.EnvironmentFresh, Scope: backlog.EnvironmentScopeTask},
		Tasks:       map[string]backlog.ManifestTask{}, Review: &round}
	if project.Type != "fresh" {
		manifest.Environment.Type = backlog.EnvironmentGit
		manifest.Environment.Ref = round.HeadCommit
		if manifest.Environment.Ref == "" {
			manifest.Environment.Ref = project.DefaultRef
		}
		if manifest.Environment.Ref == "" {
			return "", errors.New("catalog git project needs default_ref or --diff")
		}
	}
	for _, e := range snapshot.Manifest.Entries {
		manifest.Inputs = append(manifest.Inputs, e.Name)
	}
	prompts := map[string]string{}
	for _, member := range round.Reviewers {
		instance, model, _ := strings.Cut(member.Route, "/")
		t := backlog.ManifestTask{PromptFile: "prompts/" + member.ID + ".md", Outputs: []string{"review.md", "verdict.json"}, MaxTurns: 1, Deadline: &round.Deadline,
			Routes: []backlog.ManifestRoute{{Instance: instance, Model: model, QuotaPool: routes[member.Route].QuotaPool}}}
		if member.Role == "judge" {
			t.InputsFrom = map[string][]string{}
			for _, lens := range lenses {
				id := "swarm-" + lens
				t.Needs = append(t.Needs, id)
				t.InputsFrom[id] = []string{"verdict.json"}
			}
		}
		prompt, err := review.Prompt(member, snapshot.Manifest)
		if err != nil {
			return "", err
		}
		composed := backlog.FirstTurnPrompt(prompt, t.OutputDeclarations())
		if compat.TurnInputLength(composed) > compat.MaxTurnInputLength {
			return "", errors.New("composed reviewer prompt exceeds the M7b turn input limit; reduce input manifest size")
		}
		prompts[t.PromptFile] = prompt
		manifest.Tasks[member.ID] = t
	}
	raw, err := yaml.Marshal(manifest)
	if err != nil {
		return "", err
	}
	root, err := os.MkdirTemp("", "t3-steward-review-")
	if err != nil {
		return "", err
	}
	success := false
	defer func() {
		if !success {
			os.RemoveAll(root)
		}
	}()
	if err := os.MkdirAll(filepath.Join(root, "prompts"), 0700); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(root, campaign.ManifestFileName), raw, 0600); err != nil {
		return "", err
	}
	for path, prompt := range prompts {
		if err := os.WriteFile(filepath.Join(root, path), []byte(prompt), 0600); err != nil {
			return "", err
		}
	}
	if err := snapshot.Write(root); err != nil {
		return "", err
	}
	success = true
	return root, nil
}

type reviewBoundedWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *reviewBoundedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, errors.New("diff exceeds 1 MiB input limit; split the review")
	}
	n, err := w.writer.Write(p)
	w.remaining -= int64(n)
	return n, err
}

func cmdReviewSubmit(g globalFlags, args []string) error {
	a, err := parseReviewArgs(args)
	if err != nil {
		return err
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	cli, err := newReviewCLI(cfg)
	if err != nil {
		return err
	}
	return cli.run(context.Background(), a)
}

type reviewCLI struct {
	task   taskRunCLI
	result reviewResultCLI
}

func newReviewCLI(cfg config.Config) (reviewCLI, error) {
	task := newTaskRunCLI(cfg)
	results, err := cfg.ResolveResultsDir()
	if err != nil {
		return reviewCLI{}, err
	}
	return reviewCLI{task: task, result: reviewResultCLI{results: results, stdout: os.Stdout, query: task.query}}, nil
}
func (c reviewCLI) run(ctx context.Context, a reviewArgs) error {
	thread := ""
	if !a.wait && !a.noNotify {
		requested := a.notifyThread
		if requested == "" {
			requested = "current"
		}
		var err error
		thread, err = c.task.campaign.campaignNotifyThread("review", requested)
		if err != nil {
			return fmt.Errorf("%w; outside T3 pass --wait, --no-notify or --notify-thread ID", err)
		}
	}
	projects, err := c.task.projects(ctx)
	if err != nil {
		return err
	}
	dir, err := buildReviewCampaign(a, projects, time.Now())
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	bundle, plan, err := c.task.campaign.prepare(dir)
	if err != nil {
		return err
	}
	matrix, err := c.task.campaign.checkViability(ctx, plan, bundle, "")
	if err != nil {
		return err
	}
	if matrix.Outcome == backlogadmin.ViabilityImpossible {
		return campaignImpossible(matrix)
	}
	if c.task.campaign.submissions == nil {
		return errors.New("coordinator submission transport is unavailable")
	}
	client, err := c.task.campaign.submissions()
	if err != nil {
		return err
	}
	sum := sha256.Sum256(bundle.Archive)
	key := "review-" + hex.EncodeToString(sum[:])
	response, err := client.SubmitArchive(ctx, backlogadmin.LocalSubmissionRequest{IdempotencyKey: key, Principal: c.task.campaign.submissionPrincipal()}, bytes.NewReader(bundle.Archive), int64(len(bundle.Archive)))
	if err != nil {
		return err
	}
	if a.wait {
		resultArgs := []string{response.RunID, "--wait", "--timeout", a.deadline.String()}
		if a.asJSON {
			resultArgs = append(resultArgs, "--json")
		}
		if a.gate {
			resultArgs = append(resultArgs, "--gate")
		}
		return c.result.run(ctx, resultArgs)
	}
	notification, err := c.task.campaign.attachWake(ctx, key, response.RunID, thread, false)
	if err != nil {
		return fmt.Errorf("round %s submitted; collect with t3-steward review result %s --wait; notification failed: %w", response.RunID, response.RunID, err)
	}
	if a.asJSON {
		return encodeCampaignJSON(c.task.stdout, struct {
			Schema string                `json:"schema"`
			Round  string                `json:"round"`
			Notify *campaignNotification `json:"notify,omitempty"`
		}{Schema: "review-submit/v1", Round: response.RunID, Notify: notification})
	}
	fmt.Fprintf(c.task.stdout, "round %s\nresult: t3-steward review result %s --wait\n", response.RunID, response.RunID)
	return nil
}
