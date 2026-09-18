package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"gopkg.in/yaml.v3"
)

// taskRunHarness is one whole "task run" with every outside fact faked: the
// checkout, the coordinator's projects view, the readiness check, the
// submission and the thread resolution. Nothing here reaches a coordinator, a
// git binary or the network.
type taskRunHarness struct {
	checkout     gitCheckout
	checkoutErr  error
	projects     []backlogadmin.Project
	projectsErr  error
	release      string
	matrix       backlogadmin.ViabilityMatrix
	thread       string
	threadErr    error
	defaultModel string
	// submitErr is what the coordinator refuses the submission with, for the
	// refusals this CLI has to explain rather than pass through.
	submitErr error

	stdin  *strings.Reader
	stdout bytes.Buffer
	stderr bytes.Buffer

	// observed
	queries   []backlogadmin.QueryKind
	archives  [][]byte
	requests  []backlogadmin.LocalSubmissionRequest
	viability []backlogadmin.ViabilityRequest
	notified  []backlogadmin.NodeWaitOperation
	replay    bool
	// accepted is what the coordinator has already recorded under each key,
	// so that a second submission is answered by the coordinator's own rule:
	// the same key with the same content replays, the same key with different
	// content is refused.
	accepted map[string]string
}

func newTaskRunHarness() *taskRunHarness {
	return &taskRunHarness{
		checkout: gitCheckout{
			Remote: "git@github.com:iryzhkov/t3-steward.git",
			Branch: "main", Upstream: "origin/main",
		},
		projects: []backlogadmin.Project{
			{
				Name: "steward", Repository: "https://github.com/iryzhkov/t3-steward",
				DefaultRef: "main",
				Workers: []backlogadmin.ProjectWorker{{
					Worker: "omarchy-pc", Advertises: true, Enrolled: true, Ready: true, State: "observed",
					Routes: []backlogadmin.ProjectRoute{
						{Instance: "t3-primary", Model: "claude-haiku-4-5", QuotaPool: "pool-claude"},
						{Instance: "t3-primary", Model: "opus", QuotaPool: "pool-claude"},
					},
				}},
			},
			{
				Name: "other", Repository: "https://example.invalid/other.git", DefaultRef: "main",
			},
		},
		matrix: backlogadmin.ViabilityMatrix{Outcome: backlogadmin.ViabilityReady},
		thread: "thread-1",
	}
}

func (h *taskRunHarness) cli() taskRunCLI {
	var stdin io.Reader = strings.NewReader("")
	if h.stdin != nil {
		stdin = h.stdin
	}
	return taskRunCLI{
		campaign: campaignCLI{
			limits: campaign.DefaultLimits,
			stdout: io.Discard, stderr: &h.stderr,
			principal: "tester",
			submissions: func() (adminSubmissionService, error) {
				return submissionFunc(func(_ context.Context, request backlogadmin.LocalSubmissionRequest, body io.Reader, _ int64) (backlogadmin.LocalSubmissionResponse, error) {
					raw, err := io.ReadAll(body)
					if err != nil {
						return backlogadmin.LocalSubmissionResponse{}, err
					}
					h.archives = append(h.archives, raw)
					h.requests = append(h.requests, request)
					if h.submitErr != nil {
						return backlogadmin.LocalSubmissionResponse{}, h.submitErr
					}
					digest := fmt.Sprintf("%x", sha256.Sum256(raw))
					if h.accepted == nil {
						h.accepted = map[string]string{}
					}
					recorded, known := h.accepted[request.IdempotencyKey]
					switch {
					case known && recorded != digest:
						return backlogadmin.LocalSubmissionResponse{}, domain.ErrSubmissionConflict
					case !known:
						h.accepted[request.IdempotencyKey] = digest
					}
					return backlogadmin.LocalSubmissionResponse{
						Key: request.IdempotencyKey, WorkflowID: "workflow-1", RunID: "run-1",
						State: "accepted", Replay: h.replay || known, Digest: digest,
					}, nil
				}), nil
			},
			viability: func(_ context.Context, request backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
				h.viability = append(h.viability, request)
				return h.matrix, nil
			},
			notify: func(_ context.Context, operation backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
				h.notified = append(h.notified, operation)
				return backlogadmin.NodeWaitResponse{Waits: []domain.NodeWait{{
					Request: operation.Request, Delivery: "pending",
				}}}, nil
			},
			resolveThread: func(string) (string, error) { return h.thread, h.threadErr },
		},
		query: func(_ context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
			h.queries = append(h.queries, query.Kind)
			if query.Kind == backlogadmin.QueryStatus {
				return backlogadmin.Response{
					Version: backlogadmin.Version, Kind: query.Kind,
					Status: &backlogadmin.Status{Runtime: backlogadmin.RuntimeStatus{Release: h.release}},
				}, nil
			}
			if query.Kind != backlogadmin.QueryProjects {
				return backlogadmin.Response{}, fmt.Errorf("unexpected query kind %q", query.Kind)
			}
			if h.projectsErr != nil {
				return backlogadmin.Response{}, h.projectsErr
			}
			response := backlogadmin.Response{Version: backlogadmin.Version, Kind: query.Kind}
			for _, project := range h.projects {
				if query.Filter.Project == "" || project.Name == query.Filter.Project {
					response.Projects = append(response.Projects, project)
				}
			}
			return response, nil
		},
		checkout:     func() (gitCheckout, error) { return h.checkout, h.checkoutErr },
		defaultModel: h.defaultModel,
		stdin:        stdin,
		stdout:       &h.stdout,
		stderr:       &h.stderr,
	}
}

func (h *taskRunHarness) run(args ...string) error {
	return h.cli().run(context.Background(), args)
}

func (h *taskRunHarness) record(t *testing.T) taskRunRecord {
	t.Helper()
	var record taskRunRecord
	if err := json.Unmarshal(h.stdout.Bytes(), &record); err != nil {
		t.Fatalf("--json did not print one document: %v\n%s", err, h.stdout.String())
	}
	return record
}

// manifest reads back the workflow.yaml of the archive that was submitted,
// which is the only proof that the campaign directory the CLI built says what
// the caller asked for.
func (h *taskRunHarness) manifest(t *testing.T) (backlog.Manifest, map[string]string) {
	t.Helper()
	if len(h.archives) != 1 {
		t.Fatalf("archives submitted = %d, want 1", len(h.archives))
	}
	return h.archiveAt(t, 0)
}

// archiveAt reads back one of several submitted archives, for a test that
// starts the same task twice and compares what each start sent.
func (h *taskRunHarness) archiveAt(t *testing.T, index int) (backlog.Manifest, map[string]string) {
	t.Helper()
	if index >= len(h.archives) {
		t.Fatalf("archive %d was never submitted; %d were", index, len(h.archives))
	}
	files := map[string]string{}
	reader := tar.NewReader(bytes.NewReader(h.archives[index]))
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		files[header.Name] = string(raw)
	}
	var manifest backlog.Manifest
	if err := yaml.Unmarshal([]byte(files["workflow.yaml"]), &manifest); err != nil {
		t.Fatalf("workflow.yaml is not a manifest: %v\n%s", err, files["workflow.yaml"])
	}
	return manifest, files
}

type submissionFunc func(context.Context, backlogadmin.LocalSubmissionRequest, io.Reader, int64) (backlogadmin.LocalSubmissionResponse, error)

func (f submissionFunc) SubmitArchive(ctx context.Context, request backlogadmin.LocalSubmissionRequest, body io.Reader, size int64) (backlogadmin.LocalSubmissionResponse, error) {
	return f(ctx, request, body, size)
}

// The whole point of the verb: one call from a checkout starts one task, and
// every derived value is printed so that the caller can see what it agreed to.
func TestTaskRunDerivesProjectRefRouteAndKeyFromTheCheckout(t *testing.T) {
	h := newTaskRunHarness()
	if err := h.run("--model", "claude-haiku-4-5", "--json", "--", "summarise the diff"); err != nil {
		t.Fatal(err)
	}
	record := h.record(t)
	if record.Run != "run-1" || len(record.Tasks) != 1 || record.Tasks[0] != "task" {
		t.Fatalf("record = %+v", record)
	}
	if record.Project != "steward" || record.Ref != "main" {
		t.Fatalf("project = %q ref = %q, want the checkout's project and branch", record.Project, record.Ref)
	}
	if record.Route.Instance != "t3-primary" || record.Route.Model != "claude-haiku-4-5" ||
		record.Route.QuotaPool != "pool-claude" {
		t.Fatalf("route = %+v, want the pool the worker advertises and no invented one", record.Route)
	}
	if !strings.HasPrefix(record.IdempotencyKey, "run-") || len(record.IdempotencyKey) != len("run-")+16 {
		t.Fatalf("idempotencyKey = %q, want run- plus sixteen hex characters", record.IdempotencyKey)
	}
	if record.Replayed {
		t.Fatal("a first submission reported replayed")
	}
	if record.Check != string(backlogadmin.ViabilityReady) {
		t.Fatalf("check = %q", record.Check)
	}
	if record.Notify == nil || record.Notify.ThreadID != "thread-1" {
		t.Fatalf("notify = %+v, want the calling thread registered by default", record.Notify)
	}
	if record.Result != "t3-steward task result run-1" {
		t.Fatalf("result command = %q", record.Result)
	}

	manifest, files := h.manifest(t)
	if manifest.Version != backlog.ManifestVersion || manifest.Environment.Project != "steward" ||
		manifest.Environment.Ref != "main" || manifest.Environment.Type != backlog.EnvironmentGit {
		t.Fatalf("manifest environment = %+v", manifest.Environment)
	}
	if len(manifest.Routes) != 1 || manifest.Routes[0].Instance != "t3-primary" ||
		manifest.Routes[0].Model != "claude-haiku-4-5" || manifest.Routes[0].QuotaPool != "pool-claude" {
		t.Fatalf("manifest routes = %+v", manifest.Routes)
	}
	if manifest.Class != domain.TaskClassSurplus {
		t.Fatalf("class = %q, want the surplus default", manifest.Class)
	}
	if files["prompts/task.md"] != "summarise the diff\n" {
		t.Fatalf("prompt = %q", files["prompts/task.md"])
	}
	if len(h.viability) != 1 || h.viability[0].Tasks[0].Project != "steward" {
		t.Fatalf("the readiness check did not run before submission: %+v", h.viability)
	}
	if len(h.notified) != 1 || h.notified[0].Request.ThreadID != "thread-1" {
		t.Fatalf("notification = %+v", h.notified)
	}
}

// The key is what makes a repeat safe, so it must depend on the inputs and on
// nothing else: the same command twice is the same run, a different prompt is
// a different run.
func TestTaskRunIdempotencyKeyIsStableAndPromptSensitive(t *testing.T) {
	first := newTaskRunHarness()
	if err := first.run("--model", "opus", "--json", "--", "one"); err != nil {
		t.Fatal(err)
	}
	again := newTaskRunHarness()
	again.replay = true
	if err := again.run("--model", "opus", "--json", "--", "one"); err != nil {
		t.Fatal(err)
	}
	other := newTaskRunHarness()
	if err := other.run("--model", "opus", "--json", "--", "two"); err != nil {
		t.Fatal(err)
	}
	keyOne, keyAgain, keyTwo := first.record(t), again.record(t), other.record(t)
	if keyOne.IdempotencyKey != keyAgain.IdempotencyKey {
		t.Fatalf("the same command produced %q and %q", keyOne.IdempotencyKey, keyAgain.IdempotencyKey)
	}
	if keyOne.IdempotencyKey == keyTwo.IdempotencyKey {
		t.Fatalf("a different prompt reused key %q", keyOne.IdempotencyKey)
	}
	if !keyAgain.Replayed {
		t.Fatal("a replayed submission did not report replayed: true")
	}
}

// The key covers what will run and not how the route was found, so the archive
// must not vary by that either. These two commands are the same task to the
// caller and to the key -- the same project, ref, instance, model, prompt,
// class and turns -- so if the path that derived the route changes the
// submitted manifest, the second start is refused for a difference the caller
// never made, and the refusal blames --worker and --name, which are not the
// cause.
func TestTaskRunSubmitsOneArchiveWhicheverPathDerivedTheRoute(t *testing.T) {
	h := newTaskRunHarness()
	if err := h.run("--model", "opus", "--", "work"); err != nil {
		t.Fatalf("the catalog-derived start failed: %v", err)
	}
	h.stdout.Reset()
	if err := h.run("--project", "steward", "--model", "t3-primary/opus", "--json", "--", "work"); err != nil {
		t.Fatalf("the same task started with an explicit route was refused: %v", err)
	}
	if len(h.requests) != 2 || len(h.archives) != 2 {
		t.Fatalf("submissions = %d, archives = %d, want 2 of each", len(h.requests), len(h.archives))
	}
	if h.requests[0].IdempotencyKey != h.requests[1].IdempotencyKey {
		t.Fatalf("the two starts carry keys %q and %q, so this test no longer says anything",
			h.requests[0].IdempotencyKey, h.requests[1].IdempotencyKey)
	}
	if !bytes.Equal(h.archives[0], h.archives[1]) {
		catalog, _ := h.archiveAt(t, 0)
		explicit, _ := h.archiveAt(t, 1)
		t.Fatalf("one key %s, two archives: the catalog path submitted route %+v and the explicit path %+v",
			h.requests[0].IdempotencyKey, catalog.Routes, explicit.Routes)
	}
	if record := h.record(t); !record.Replayed {
		t.Fatalf("the second start did not replay the first: %+v", record)
	}
}

// Uncommitted work is not sent, and a ref nobody else can fetch is refused
// rather than submitted and failed on the worker half an hour later.
func TestTaskRunRefusesARefTheFleetCannotFetch(t *testing.T) {
	detached := newTaskRunHarness()
	detached.checkout = gitCheckout{Remote: "git@github.com:iryzhkov/t3-steward.git"}
	err := detached.run("--model", "opus", "--", "work")
	if err == nil || !strings.Contains(err.Error(), "push first or pass --ref") {
		t.Fatalf("detached HEAD error = %v", err)
	}

	ahead := newTaskRunHarness()
	ahead.checkout.Ahead = 2
	err = ahead.run("--model", "opus", "--", "work")
	if err == nil || !strings.Contains(err.Error(), "push first or pass --ref") {
		t.Fatalf("unpushed branch error = %v", err)
	}

	// A dirty tree is a warning, not a refusal: the ref is fetchable, the
	// uncommitted changes simply do not travel.
	dirty := newTaskRunHarness()
	dirty.checkout.Dirty = true
	if err := dirty.run("--model", "opus", "--json", "--", "work"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dirty.stderr.String(), "uncommitted") {
		t.Fatalf("a dirty tree was not warned about: %q", dirty.stderr.String())
	}
	if record := dirty.record(t); record.Run != "run-1" {
		t.Fatalf("the warning stopped the run: %+v", record)
	}
}

// Every ambiguity contract 4 names is refused, and each refusal says what to
// pass instead.
func TestTaskRunRefusesEveryAmbiguity(t *testing.T) {
	t.Run("no project matches the remote", func(t *testing.T) {
		h := newTaskRunHarness()
		h.checkout.Remote = "git@github.com:iryzhkov/unknown.git"
		err := h.run("--model", "opus", "--", "work")
		if err == nil || !strings.Contains(err.Error(), "--project") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("several projects match the remote", func(t *testing.T) {
		h := newTaskRunHarness()
		h.projects[1].Repository = "https://github.com/iryzhkov/t3-steward.git"
		err := h.run("--model", "opus", "--", "work")
		if err == nil || !strings.Contains(err.Error(), "--project") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("a model several instances offer", func(t *testing.T) {
		h := newTaskRunHarness()
		h.projects[0].Workers[0].Routes = append(h.projects[0].Workers[0].Routes,
			backlogadmin.ProjectRoute{Instance: "t3-secondary", Model: "opus", QuotaPool: "pool-two"})
		err := h.run("--model", "opus", "--", "work")
		if err == nil || !strings.Contains(err.Error(), "INSTANCE/MODEL") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("no model and no default", func(t *testing.T) {
		h := newTaskRunHarness()
		err := h.run("--", "work")
		if err == nil || !strings.Contains(err.Error(), "t3-primary/opus") {
			t.Fatalf("the refusal does not list the routes on offer: %v", err)
		}
	})
	t.Run("a model no eligible worker offers", func(t *testing.T) {
		h := newTaskRunHarness()
		err := h.run("--model", "gpt-5", "--", "work")
		if err == nil || !strings.Contains(err.Error(), "t3-primary/opus") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("two prompt sources", func(t *testing.T) {
		h := newTaskRunHarness()
		file := filepath.Join(t.TempDir(), "prompt.md")
		if err := os.WriteFile(file, []byte("from the file"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := h.run("--model", "opus", "--prompt-file", file, "--", "inline")
		if err == nil || !strings.Contains(err.Error(), "exactly one") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("no prompt at all", func(t *testing.T) {
		h := newTaskRunHarness()
		err := h.run("--model", "opus")
		if err == nil || !strings.Contains(err.Error(), "prompt") {
			t.Fatalf("error = %v", err)
		}
	})
}

// A run nobody will hear about is never started by accident, and opting out is
// explicit.
func TestTaskRunRefusesWhenNoThreadResolvesUnlessNoNotify(t *testing.T) {
	h := newTaskRunHarness()
	h.threadErr = errors.New("no T3 session")
	err := h.run("--model", "opus", "--", "work")
	if err == nil || !strings.Contains(err.Error(), "--no-notify") {
		t.Fatalf("error = %v", err)
	}
	if len(h.archives) != 0 {
		t.Fatal("a run was submitted that nobody is listening for")
	}

	quiet := newTaskRunHarness()
	quiet.threadErr = errors.New("no T3 session")
	if err := quiet.run("--model", "opus", "--no-notify", "--json", "--", "work"); err != nil {
		t.Fatal(err)
	}
	if record := quiet.record(t); record.Notify != nil {
		t.Fatalf("notify = %+v, want none", record.Notify)
	}
	if len(quiet.notified) != 0 {
		t.Fatal("--no-notify still registered a wait")
	}
}

// Fan-out is one run with one task per file, named by the file stem, sharing
// every other derived value.
func TestTaskRunFanOutMakesOneTaskPerFile(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"alpha.md": "first", "beta.md": "second", "gamma.md": "third",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	h := newTaskRunHarness()
	if err := h.run("--model", "opus", "--fan-out", filepath.Join(dir, "*.md"), "--json"); err != nil {
		t.Fatal(err)
	}
	record := h.record(t)
	if strings.Join(record.Tasks, ",") != "alpha,beta,gamma" {
		t.Fatalf("tasks = %v", record.Tasks)
	}
	manifest, files := h.manifest(t)
	if len(manifest.Tasks) != 3 {
		t.Fatalf("manifest tasks = %+v", manifest.Tasks)
	}
	if files["prompts/beta.md"] != "second\n" {
		t.Fatalf("beta prompt = %q", files["prompts/beta.md"])
	}
	if len(h.viability) != 1 || len(h.viability[0].Tasks) != 3 {
		t.Fatalf("the whole fan-out was not checked at once: %+v", h.viability)
	}
}

// accepted_waiting is a success: the run exists and the caller may end its
// turn. It must not look like a failure in the record or in the exit code.
func TestTaskRunTreatsAcceptedWaitingAsSuccess(t *testing.T) {
	h := newTaskRunHarness()
	h.matrix = backlogadmin.ViabilityMatrix{Outcome: backlogadmin.ViabilityAcceptedWaiting}
	if err := h.run("--model", "opus", "--json", "--", "work"); err != nil {
		t.Fatal(err)
	}
	if record := h.record(t); record.Check != string(backlogadmin.ViabilityAcceptedWaiting) || record.Run != "run-1" {
		t.Fatalf("record = %+v", record)
	}
}

// An impossible campaign is refused before a run id is spent, with the
// coordinator's own permanent reasons.
func TestTaskRunRefusesAnImpossibleTaskBeforeSubmitting(t *testing.T) {
	h := newTaskRunHarness()
	h.matrix = backlogadmin.ViabilityMatrix{
		Outcome: backlogadmin.ViabilityImpossible,
		Tasks: []backlogadmin.ViabilityTaskResult{{
			Task: "task", Outcome: backlogadmin.ViabilityImpossible,
			Reasons: []backlogadmin.ViabilityReason{{
				Code: backlogadmin.ReasonUnknownProject, Detail: "no such project", Permanent: true,
			}},
		}},
	}
	err := h.run("--model", "opus", "--", "work")
	if err == nil || !strings.Contains(err.Error(), "unknown-project") {
		t.Fatalf("error = %v", err)
	}
	if len(h.archives) != 0 {
		t.Fatal("an impossible task was submitted anyway")
	}
}

// The flags that are not derived still have to arrive in the manifest.
func TestTaskRunCarriesOutputsVerifyClassAndMaxTurns(t *testing.T) {
	h := newTaskRunHarness()
	if err := h.run(
		"--model", "t3-primary/opus", "--worker", "omarchy-pc", "--name", "Nightly sweep",
		"--outputs", "report.md,notes.md", "--verify", "test -s report.md", "--verify", "go test ./...",
		"--class", "required", "--max-turns", "12", "--json", "--", "work",
	); err != nil {
		t.Fatal(err)
	}
	manifest, _ := h.manifest(t)
	task := manifest.Tasks["task"]
	if strings.Join(task.Outputs, ",") != "report.md,notes.md" {
		t.Fatalf("outputs = %v", task.Outputs)
	}
	if len(task.Verify) != 2 || task.Verify[1] != "go test ./..." {
		t.Fatalf("verify = %v", task.Verify)
	}
	if manifest.Class != domain.TaskClassRequired || task.MaxTurns != 12 {
		t.Fatalf("class = %q maxTurns = %d", manifest.Class, task.MaxTurns)
	}
	if len(manifest.Routes) != 1 || manifest.Routes[0].Host != "omarchy-pc" {
		t.Fatalf("--worker did not pin the route: %+v", manifest.Routes)
	}
	if manifest.Name != "nightly-sweep" {
		t.Fatalf("manifest name = %q, want the name as a manifest-legal slug", manifest.Name)
	}
}

// The prompt may come from stdin, which is how a wrapper script passes one.
func TestTaskRunReadsThePromptFromStdin(t *testing.T) {
	h := newTaskRunHarness()
	h.stdin = strings.NewReader("piped work\nsecond line\n")
	if err := h.run("--model", "opus", "--prompt-file", "-", "--json"); err != nil {
		t.Fatal(err)
	}
	_, files := h.manifest(t)
	if files["prompts/task.md"] != "piped work\nsecond line\n" {
		t.Fatalf("prompt = %q", files["prompts/task.md"])
	}
}

// defaults.model is the only place a route may come from when the caller names
// none, and it is read from the client configuration rather than guessed.
func TestTaskRunUsesTheConfiguredDefaultModel(t *testing.T) {
	h := newTaskRunHarness()
	h.defaultModel = "t3-primary/opus"
	if err := h.run("--json", "--", "work"); err != nil {
		t.Fatal(err)
	}
	if record := h.record(t); record.Route.Model != "opus" || record.Route.Instance != "t3-primary" {
		t.Fatalf("route = %+v", record.Route)
	}
}

// rc69RefusedProjectsQuery is what a coordinator of the previous release
// answers when it is asked for a query kind it does not have. The refusal
// crosses the transport as this text (bd5362b:internal/backlogadmin/service.go,
// ErrInvalidQuery), so this is what the client has to recognise.
var errRC69RefusesTheProjectsQuery = errors.New(`coordinator refused the query: invalid query: kind "projects" or target is invalid`)

// A start that names its project and its whole route needs nothing from the
// catalog to decide what will run, so a coordinator that cannot answer the
// projects query must not stop it: that query is the one thing in this verb a
// coordinator of the previous release does not have, and a mixed-release fleet
// is exactly when starting one task matters. The catalog is still asked, for
// the quota pool alone, so that this path and the derived path submit the same
// archive; here the refusal is swallowed and the pool stays empty.
func TestTaskRunStartsAFullyExplicitRouteWhenTheCatalogIsRefused(t *testing.T) {
	h := newTaskRunHarness()
	h.projectsErr = errRC69RefusesTheProjectsQuery
	if err := h.run("--project", "steward", "--model", "t3-primary/claude-haiku-4-5",
		"--worker", "omarchy-pc", "--json", "--", "summarise the diff"); err != nil {
		t.Fatal(err)
	}
	// The refusal costs nothing beyond the one query: it is never explained,
	// because nothing on this path was waiting for the answer.
	for _, kind := range h.queries {
		if kind == backlogadmin.QueryStatus {
			t.Fatalf("the swallowed refusal was explained anyway: %v", h.queries)
		}
	}
	record := h.record(t)
	if record.Project != "steward" || record.Route.Instance != "t3-primary" ||
		record.Route.Model != "claude-haiku-4-5" || record.Route.Worker != "omarchy-pc" {
		t.Fatalf("record = %+v", record)
	}
	// The pool is not invented here. The coordinator resolves it from the
	// worker's inventory, which is where the catalog would have read it.
	if record.Route.QuotaPool != "" {
		t.Fatalf("a route derived without the catalog named a quota pool: %+v", record.Route)
	}
	manifest, _ := h.manifest(t)
	if len(manifest.Routes) != 1 || manifest.Routes[0].Instance != "t3-primary" ||
		manifest.Routes[0].Model != "claude-haiku-4-5" || manifest.Routes[0].QuotaPool != "" ||
		manifest.Routes[0].Host != "omarchy-pc" || manifest.Environment.Project != "steward" {
		t.Fatalf("manifest route = %+v, environment = %+v", manifest.Routes, manifest.Environment)
	}
}

// A qualified defaults.model names the route as completely as the flag does.
func TestTaskRunStartsAQualifiedDefaultModelWhenTheCatalogIsRefused(t *testing.T) {
	h := newTaskRunHarness()
	h.projectsErr = errRC69RefusesTheProjectsQuery
	h.defaultModel = "t3-primary/opus"
	if err := h.run("--project", "steward", "--json", "--", "work"); err != nil {
		t.Fatal(err)
	}
	if record := h.record(t); record.Route.Instance != "t3-primary" ||
		record.Route.Model != "opus" || record.Route.QuotaPool != "" {
		t.Fatalf("route = %+v", record.Route)
	}
}

// The same explicit start against a coordinator that does answer the catalog
// reads the pool the instance advertises, which is what makes the two paths
// submit one archive.
func TestTaskRunReadsTheAdvertisedPoolForAnExplicitRoute(t *testing.T) {
	h := newTaskRunHarness()
	if err := h.run("--project", "steward", "--model", "t3-primary/claude-haiku-4-5",
		"--json", "--", "summarise the diff"); err != nil {
		t.Fatal(err)
	}
	if record := h.record(t); record.Route.QuotaPool != "pool-claude" {
		t.Fatalf("route = %+v, want the pool the instance advertises", record.Route)
	}
	manifest, _ := h.manifest(t)
	if len(manifest.Routes) != 1 || manifest.Routes[0].QuotaPool != "pool-claude" {
		t.Fatalf("manifest route = %+v", manifest.Routes)
	}
}

// When the catalog is what the start needs, an older coordinator's refusal is
// explained rather than passed through: "invalid query" reads like a client
// bug, and the agent needs the release, the release that has the query and the
// two flags that avoid it.
func TestTaskRunExplainsACoordinatorWithoutTheProjectsQuery(t *testing.T) {
	h := newTaskRunHarness()
	h.projectsErr = errRC69RefusesTheProjectsQuery
	h.release = "v0.11.0-rc.69"
	err := h.run("--model", "claude-haiku-4-5", "--", "work")
	if err == nil {
		t.Fatal("a start that needs the catalog succeeded against a coordinator without it")
	}
	for _, want := range []string{
		"v0.11.0-rc.69", `"projects" query`, projectsQueryRelease,
		"--project NAME", "--model INSTANCE/MODEL", "invalid query",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not name %q: %v", want, err)
		}
	}
	if len(h.archives) != 0 {
		t.Fatal("a run was submitted after the catalog could not be read")
	}
}

// Only that one refusal is explained. Any other failure of the query is the
// caller's to read as it stands.
func TestTaskRunPassesAnyOtherProjectsFailureThrough(t *testing.T) {
	h := newTaskRunHarness()
	h.projectsErr = errors.New("coordinator unavailable: dial unix: no such file")
	err := h.run("--model", "claude-haiku-4-5", "--", "work")
	if err == nil || err.Error() != h.projectsErr.Error() {
		t.Fatalf("error = %v, want the transport failure unchanged", err)
	}
}

// The key covers what will run; --worker and --name are outside it by
// contract and inside the archive in fact, so re-running the same prompt with
// a different one of them reaches the coordinator with the same key and a
// different digest. The coordinator's refusal names neither flag, which leaves
// the caller with nothing to change.
func TestTaskRunNamesTheFlagsOutsideTheIdempotencyKeyWhenTheContentDiffers(t *testing.T) {
	h := newTaskRunHarness()
	h.submitErr = domain.ErrSubmissionConflict
	err := h.run("--model", "claude-haiku-4-5", "--worker", "omarchy-pc", "--", "work")
	if err == nil {
		t.Fatal("a conflicting submission succeeded")
	}
	for _, want := range []string{
		domain.ErrSubmissionConflict.Error(), "--worker", "--name", "--idempotency-key",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not name %q: %v", want, err)
		}
	}
}

// The explanation is attached to that one refusal. Any other failure of the
// submission is the caller's to read as it stands.
func TestTaskRunPassesAnyOtherSubmissionFailureThrough(t *testing.T) {
	h := newTaskRunHarness()
	h.submitErr = errors.New("coordinator unavailable: dial unix: no such file")
	err := h.run("--model", "claude-haiku-4-5", "--", "work")
	if err == nil || err.Error() != h.submitErr.Error() {
		t.Fatalf("error = %v, want the transport failure unchanged", err)
	}
}

func TestTaskRunTextRecordNamesTheResultCommand(t *testing.T) {
	h := newTaskRunHarness()
	if err := h.run("--model", "opus", "--", "work"); err != nil {
		t.Fatal(err)
	}
	text := h.stdout.String()
	for _, want := range []string{
		"run run-1", "project steward", "ref main",
		"route omarchy-pc t3-primary/opus", "t3-steward task result run-1",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("the text record does not name %q:\n%s", want, text)
		}
	}
}
