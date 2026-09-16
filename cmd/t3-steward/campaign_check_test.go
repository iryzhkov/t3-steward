package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// campaignCheckCLI arms the readiness seam and leaves the submission seam
// disarmed, so a check that packed and sent a bundle would fail the test.
func campaignCheckCLI(t *testing.T, out *bytes.Buffer, answer func(backlogadmin.ViabilityRequest) backlogadmin.ViabilityMatrix) (campaignCLI, *[]backlogadmin.ViabilityRequest) {
	t.Helper()
	requests := &[]backlogadmin.ViabilityRequest{}
	cli := campaignTestCLI(t, out)
	cli.viability = func(_ context.Context, request backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
		*requests = append(*requests, request)
		return answer(request), nil
	}
	return cli, requests
}

func campaignImpossibleMatrix(request backlogadmin.ViabilityRequest) backlogadmin.ViabilityMatrix {
	matrix := backlogadmin.ViabilityMatrix{
		SchemaVersion: backlogadmin.ViabilityMatrixSchemaVersion,
		Outcome:       backlogadmin.ViabilityImpossible,
	}
	for _, task := range request.Tasks {
		matrix.Tasks = append(matrix.Tasks, backlogadmin.ViabilityTaskResult{
			Task: task.Name, Outcome: backlogadmin.ViabilityImpossible,
			Reasons: []backlogadmin.ViabilityReason{{
				Code: backlogadmin.ReasonUnknownProject, Permanent: true,
				Detail: "this coordinator has no project \"t3-steward\"",
			}},
			Candidates: []backlogadmin.ViabilityCandidate{},
		})
	}
	return matrix
}

func campaignWaitingOnQuota(request backlogadmin.ViabilityRequest) backlogadmin.ViabilityMatrix {
	matrix := backlogadmin.ViabilityMatrix{
		SchemaVersion: backlogadmin.ViabilityMatrixSchemaVersion,
		Outcome:       backlogadmin.ViabilityAcceptedWaiting,
	}
	for _, task := range request.Tasks {
		matrix.Tasks = append(matrix.Tasks, backlogadmin.ViabilityTaskResult{
			Task: task.Name, Outcome: backlogadmin.ViabilityAcceptedWaiting,
			Candidates: []backlogadmin.ViabilityCandidate{{
				Worker: "homelab", Outcome: backlogadmin.ViabilityAcceptedWaiting,
				Reasons: []backlogadmin.ViabilityReason{{
					Code: backlogadmin.ReasonQuotaClosed, Detail: "quota pool \"pool-1\" admission is closed",
				}},
			}},
		})
	}
	return matrix
}

func TestCampaignCheckAsksTheCoordinatorAndPacksNothing(t *testing.T) {
	root := campaignFixture(t)
	var out bytes.Buffer
	cli, requests := campaignCheckCLI(t, &out, campaignReadyMatrix)
	if err := cli.run(context.Background(), []string{"check", root}); err != nil {
		t.Fatal(err)
	}
	if len(*requests) != 1 {
		t.Fatalf("viability queries = %d, want 1", len(*requests))
	}
	request := (*requests)[0]
	if len(request.Tasks) != 2 {
		t.Fatalf("tasks = %+v, want review and implement", request.Tasks)
	}
	for _, task := range request.Tasks {
		if task.Project != "t3-steward" {
			t.Fatalf("task %q project = %q", task.Name, task.Project)
		}
	}
	if request.BundleBytes <= 0 || request.BundleFiles <= 0 {
		t.Fatalf("the request did not describe the bundle it would send: %+v", request)
	}
	if !strings.Contains(out.String(), "example-campaign is ready") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestCampaignCheckNarrowsToOneTask(t *testing.T) {
	root := campaignFixture(t)
	var out bytes.Buffer
	cli, requests := campaignCheckCLI(t, &out, campaignReadyMatrix)
	if err := cli.run(context.Background(), []string{"check", root, "--task", "implement"}); err != nil {
		t.Fatal(err)
	}
	request := (*requests)[0]
	if len(request.Tasks) != 1 || request.Tasks[0].Name != "implement" {
		t.Fatalf("tasks = %+v", request.Tasks)
	}

	var refused bytes.Buffer
	unknown, _ := campaignCheckCLI(t, &refused, campaignReadyMatrix)
	err := unknown.run(context.Background(), []string{"check", root, "--task", "absent"})
	if err == nil || !strings.Contains(err.Error(), "no task named") {
		t.Fatalf("error = %v", err)
	}
}

func TestCampaignCheckRefusesAnImpossibleCampaign(t *testing.T) {
	root := campaignFixture(t)
	var out bytes.Buffer
	cli, _ := campaignCheckCLI(t, &out, campaignImpossibleMatrix)
	err := cli.run(context.Background(), []string{"check", root, "--json"})
	if err == nil {
		t.Fatal("an impossible campaign was reported as acceptable")
	}
	if class := backlogadmin.ClassOf(err); class != backlogadmin.ClassRejected {
		t.Fatalf("class = %q, want %q", class, backlogadmin.ClassRejected)
	}
	if backlogadmin.ExitCodeFor(err) != 8 {
		t.Fatalf("exit code = %d, want 8", backlogadmin.ExitCodeFor(err))
	}
	if !strings.Contains(err.Error(), backlogadmin.ReasonUnknownProject) {
		t.Fatalf("error does not name the permanent reason: %v", err)
	}
	// The document is still printed, so an agent reading --json gets the matrix
	// as well as the refusal.
	var document campaignCheck
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatalf("invalid JSON %q: %v", out.String(), err)
	}
	if document.SchemaVersion != campaignCheckSchemaVersion ||
		document.Matrix.Outcome != backlogadmin.ViabilityImpossible {
		t.Fatalf("document = %+v", document)
	}
}

const campaignSupervisedFixtureManifest = `version: 2
name: supervised-example
environment:
  project: t3-steward
inputs:
  - inputs/plan.md
routes:
  - instance: codex
    model: gpt-5.6-sol
    quota_pool: codex-main
supervision:
  route:
    instance: claudeAgent
    model: claude-fable-5-1
    quota_pool: claude-main
  prompt_file: prompts/overseer.md
  max_activations: 4
  max_turns_per_activation: 3
  activation_deadline: 2h
gates:
  review_gate:
    after: [review]
    before: [implement]
tasks:
  review:
    prompt_file: prompts/review.md
    outputs: [review.md]
  implement:
    prompt_file: prompts/implement.md
    needs: [review]
    inputs_from:
      review: [review.md]
`

// campaignSupervisedFixture is the same small campaign with an overseer and one
// gate, which is the only difference the supervision half of check reads.
func campaignSupervisedFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range map[string]string{
		"workflow.yaml":        campaignSupervisedFixtureManifest,
		"inputs/plan.md":       "the plan\n",
		"prompts/review.md":    "review the plan\n",
		"prompts/implement.md": "implement the plan\n",
		"prompts/overseer.md":  "decide the review gate on the evidence\n",
	} {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// campaignOverseerFleet is one worker hosting the overseer route, with the
// campaign supervision capability or without it.
func campaignOverseerFleet(capable bool) []domain.WorkerInventory {
	inventory := domain.WorkerInventory{
		ID: "homelab", AcceptBacklog: true, Health: domain.WorkerHealthReady,
		Providers: []domain.WorkerProviderInventory{{
			InstanceID: "claudeAgent", Models: []string{"claude-fable-5-1"},
			QuotaPoolID: "claude-main", Available: true,
		}, {
			InstanceID: "codex", Models: []string{"gpt-5.6-sol"},
			QuotaPoolID: "codex-main", Available: true,
		}},
	}
	if capable {
		inventory.Capabilities = []string{workerproto.CapabilityCampaignSupervision}
	}
	return []domain.WorkerInventory{inventory}
}

// campaignSupervisionMatrix answers a check with the coordinator's own
// supervision evaluation over one fleet, so this test exercises the rule rather
// than a hand-written verdict.
func campaignSupervisionMatrix(fleet []domain.WorkerInventory) func(backlogadmin.ViabilityRequest) backlogadmin.ViabilityMatrix {
	return func(request backlogadmin.ViabilityRequest) backlogadmin.ViabilityMatrix {
		matrix := campaignReadyMatrix(request)
		if request.Supervision == nil {
			return matrix
		}
		// This fleet has a supervisor admin client, so the fleet's own capability
		// is what these cases are about.
		matrix.Reasons = backlogadmin.SupervisionViabilityReasons(*request.Supervision, fleet, true)
		for _, reason := range matrix.Reasons {
			if reason.Permanent {
				matrix.Outcome = backlogadmin.ViabilityImpossible
			}
		}
		return matrix
	}
}

// TestCampaignCheckRefusesSupervisionNoWorkerCanRun is the admission half of the
// overseer route: a campaign whose gates no configured worker could ever decide
// is impossible, not merely waiting, because no amount of waiting installs a
// capability on a host.
func TestCampaignCheckRefusesSupervisionNoWorkerCanRun(t *testing.T) {
	root := campaignSupervisedFixture(t)
	var out bytes.Buffer
	cli, requests := campaignCheckCLI(t, &out, campaignSupervisionMatrix(campaignOverseerFleet(false)))
	err := cli.run(context.Background(), []string{"check", root})
	if err == nil {
		t.Fatal("a campaign whose overseer no worker can run was reported as acceptable")
	}
	if class := backlogadmin.ClassOf(err); class != backlogadmin.ClassRejected {
		t.Fatalf("class = %q, want %q", class, backlogadmin.ClassRejected)
	}
	if backlogadmin.ExitCodeFor(err) != 8 {
		t.Fatalf("exit code = %d, want 8", backlogadmin.ExitCodeFor(err))
	}
	for _, want := range []string{backlogadmin.ReasonCapabilityMissing, workerproto.CapabilityCampaignSupervision} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err.Error(), want)
		}
	}

	// The requirement the coordinator was asked about is the declared overseer
	// route, carried once for the run rather than per task.
	if len(*requests) != 1 {
		t.Fatalf("viability queries = %d, want 1", len(*requests))
	}
	supervision := (*requests)[0].Supervision
	if supervision == nil {
		t.Fatal("the request said nothing about the campaign's declared overseer")
	}
	if supervision.Route.ProviderInstanceID != "claudeAgent" || supervision.Route.Model != "claude-fable-5-1" ||
		supervision.RequiredCapability != workerproto.CapabilityCampaignSupervision {
		t.Fatalf("supervision requirement = %+v", *supervision)
	}
}

// TestCampaignCheckAcceptsSupervisionOneWorkerCanRun is the same campaign on a
// fleet that can run it, which is what keeps the refusal above from being a
// refusal of supervision itself.
func TestCampaignCheckAcceptsSupervisionOneWorkerCanRun(t *testing.T) {
	root := campaignSupervisedFixture(t)
	var out bytes.Buffer
	cli, _ := campaignCheckCLI(t, &out, campaignSupervisionMatrix(campaignOverseerFleet(true)))
	if err := cli.run(context.Background(), []string{"check", root}); err != nil {
		t.Fatalf("a runnable supervised campaign was refused: %v", err)
	}
	if !strings.Contains(out.String(), "supervised-example is ready") {
		t.Fatalf("output = %q", out.String())
	}
}

// TestCampaignCheckUnsupervisedSaysNothingAboutSupervision keeps the request
// unchanged for every campaign that declares no overseer.
func TestCampaignCheckUnsupervisedSaysNothingAboutSupervision(t *testing.T) {
	root := campaignFixture(t)
	var out bytes.Buffer
	cli, requests := campaignCheckCLI(t, &out, campaignSupervisionMatrix(campaignOverseerFleet(false)))
	if err := cli.run(context.Background(), []string{"check", root}); err != nil {
		t.Fatal(err)
	}
	if (*requests)[0].Supervision != nil {
		t.Fatalf("an unsupervised campaign asked about an overseer: %+v", (*requests)[0].Supervision)
	}
}

// TestCampaignSubmitRefusesAnImpossibleCampaign is the zero-workflow case on
// the client: nothing is packed, nothing is sent, and no run can exist.
func TestCampaignSubmitRefusesAnImpossibleCampaign(t *testing.T) {
	root := campaignFixture(t)
	fake := &fakeSubmissionService{}
	var out bytes.Buffer
	cli, _ := campaignCheckCLI(t, &out, campaignImpossibleMatrix)
	cli.submissions = func() (adminSubmissionService, error) { return fake, nil }
	err := cli.run(context.Background(), []string{"submit", root, "--idempotency-key", "campaign-1"})
	if err == nil {
		t.Fatal("submit accepted a campaign that can never run")
	}
	if fake.size != 0 || len(fake.raw) != 0 {
		t.Fatalf("a refused submission still sent %d bytes", len(fake.raw))
	}
}

// TestCampaignSubmitProceedsWhileWaiting states that a temporary obstruction is
// not a refusal: the run is created and the reasons are reported.
func TestCampaignSubmitProceedsWhileWaiting(t *testing.T) {
	root := campaignFixture(t)
	fake := &fakeSubmissionService{}
	var out bytes.Buffer
	cli, _ := campaignCheckCLI(t, &out, campaignWaitingOnQuota)
	cli.submissions = func() (adminSubmissionService, error) { return fake, nil }
	if err := cli.run(context.Background(), []string{"submit", root, "--idempotency-key", "campaign-1"}); err != nil {
		t.Fatal(err)
	}
	if len(fake.raw) == 0 {
		t.Fatal("a temporary obstruction stopped the submission")
	}
	for _, want := range []string{"accepted_waiting", backlogadmin.ReasonQuotaClosed} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output %q does not contain %q", out.String(), want)
		}
	}
}

func TestCampaignSubmitAllowUnverifiedIsAuditedAndLoud(t *testing.T) {
	root := campaignFixture(t)
	fake := &fakeSubmissionService{}
	var out bytes.Buffer
	cli := campaignTestCLI(t, &out)
	cli.submissions = func() (adminSubmissionService, error) { return fake, nil }
	cli.principal = "local:1000"
	// The readiness seam stays disarmed: reaching it would fail the test, which
	// is what "skips the client-side check" has to mean.
	if err := cli.run(context.Background(), []string{
		"submit", root, "--idempotency-key", "campaign-1",
		"--allow-unverified", "--reason", "coordinator is being rebuilt",
	}); err != nil {
		t.Fatal(err)
	}
	if !fake.request.Unverified ||
		fake.request.UnverifiedReason != "coordinator is being rebuilt" ||
		fake.request.Principal != "local:1000" {
		t.Fatalf("audit fields = %+v", fake.request)
	}
	for _, want := range []string{"warning", "local:1000", "coordinator is being rebuilt"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output %q does not contain %q", out.String(), want)
		}
	}
}

// D5. The warning is advice, not part of the result. Printed on stdout ahead of
// the JSON document it made that document unparseable, which turns an advisory
// into a failure for exactly the readers --json exists for.
func TestCampaignSubmitJSONStaysParseableWithAllowUnverified(t *testing.T) {
	root := campaignFixture(t)
	fake := &fakeSubmissionService{}
	var out, errs bytes.Buffer
	cli := campaignTestCLI(t, &out)
	cli.stderr = &errs
	cli.submissions = func() (adminSubmissionService, error) { return fake, nil }
	cli.principal = "local:1000"
	if err := cli.run(context.Background(), []string{
		"submit", root, "--idempotency-key", "campaign-1", "--json",
		"--allow-unverified", "--reason", "coordinator is being rebuilt",
	}); err != nil {
		t.Fatal(err)
	}
	var document any
	decoder := json.NewDecoder(bytes.NewReader(out.Bytes()))
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("stdout is not one JSON document: %v; stdout was %q", err, out.String())
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		t.Fatalf("stdout carries more than the result document: %q", out.String())
	}
	if strings.Contains(out.String(), "warning") {
		t.Fatalf("the warning reached stdout: %q", out.String())
	}
	for _, want := range []string{"warning", "local:1000", "coordinator is being rebuilt"} {
		if !strings.Contains(errs.String(), want) {
			t.Fatalf("stderr %q does not contain %q", errs.String(), want)
		}
	}
}

func TestCampaignAllowUnverifiedRequiresAReason(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantError string
	}{
		{
			name:      "a skipped check needs a reason",
			args:      []string{"./demo", "--idempotency-key", "k", "--allow-unverified"},
			wantError: "requires --reason",
		},
		{
			name:      "a reason alone means nothing",
			args:      []string{"./demo", "--idempotency-key", "k", "--reason", "why"},
			wantError: "only meaningful with --allow-unverified",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseCampaignArgs("submit", test.args, false, true)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want %q", err, test.wantError)
			}
		})
	}
	for _, command := range []string{"validate", "plan", "check"} {
		_, err := parseCampaignArgs(command, []string{"./demo", "--allow-unverified"}, false, false)
		if err == nil || !strings.Contains(err.Error(), "does not accept --allow-unverified") {
			t.Fatalf("%s error = %v", command, err)
		}
	}
	for _, command := range []string{"validate", "plan", "submit"} {
		_, err := parseCampaignArgs(command, []string{"./demo", "--task", "x"}, false, false)
		if err == nil || !strings.Contains(err.Error(), "does not accept --task") {
			t.Fatalf("%s error = %v", command, err)
		}
	}
}

// TestCampaignHelpDocumentsEveryVerb keeps the help contract enforceable rather
// than aspirational.
func TestCampaignHelpDocumentsEveryVerb(t *testing.T) {
	var out bytes.Buffer
	if err := printCampaignHelp(&out, nil); err != nil {
		t.Fatal(err)
	}
	help := out.String()
	for _, want := range []string{
		// Every verb, with its class: offline, read-only and live, or mutating.
		"Offline, reaches no coordinator",
		"Read-only and live, asks the coordinator and creates nothing",
		"Mutating, checks first and creates one workflow and one run",
		"explain is read-only and live",
		// Outcomes and what they mean for submission.
		"ready", "accepted_waiting", "impossible",
		// The escape hatch, stated plainly.
		"--allow-unverified", "Agents should not use it",
		// Exit codes, JSON availability and idempotency.
		"Exit codes", "schemaVersion", "--idempotency-key",
		// Configuration and credential references.
		"backlog_v2.coordinator_client", "secretref:f03-admin/",
		// A complete copyable example, and where the recovery commands are.
		"t3-steward campaign check demo --json",
		"t3-steward campaign help readiness",
		// The transport note another subagent added must survive.
		"Talking to the coordinator",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("campaign help does not mention %q", want)
		}
	}

	// The readiness topic carries the full tables the usage points at.
	var topic bytes.Buffer
	if err := printCampaignHelp(&topic, []string{"help", "readiness"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"ready", "accepted_waiting", "impossible",
		backlogadmin.ReasonCatalogDigestMismatch, backlogadmin.ReasonRefNotFound,
		backlogadmin.ReasonNoConfiguredRoute, backlogadmin.ReasonQuotaClosed,
		"git ls-remote --exit-code -- <repository> <ref>",
		"t3-steward worker enroll", "Exit codes", "schemaVersion",
	} {
		if !strings.Contains(topic.String(), want) {
			t.Fatalf("campaign help readiness does not mention %q", want)
		}
	}
}

// TestCampaignPlanProjectionCarriesRequirements states that the request sent to
// the coordinator carries what the plan knows and nothing about the bundle's
// contents.
func TestCampaignPlanProjectionCarriesRequirements(t *testing.T) {
	plan := campaign.Plan{
		Name:        "demo",
		Environment: campaign.Environment{Project: "t3-steward", Ref: "main"},
		Tasks: []campaign.Task{{
			Name: "implement", Class: "required",
			Placement: campaign.Placement{Hosts: []string{"homelab"}, Requires: []string{"git"}},
			Resources: campaign.Resources{MinCPUClass: "high"},
			Routes: []campaign.Route{{
				Instance: "t3-primary", Model: "opus", QuotaPool: "pool-1",
				Options: []campaign.RouteOption{{Name: "effort", Value: "high"}},
			}},
			ResourceLocks: []string{"repo:t3-steward"},
		}},
	}
	request, err := campaignViabilityRequest(plan, 2048, 7, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(request.Tasks) != 1 {
		t.Fatalf("tasks = %+v", request.Tasks)
	}
	task := request.Tasks[0]
	if task.Project != "t3-steward" || task.Ref != "main" || string(task.Class) != "required" ||
		len(task.Hosts) != 1 || len(task.Capabilities) != 1 ||
		string(task.Resources.MinCPUClass) != "high" || len(task.ResourceLocks) != 1 {
		t.Fatalf("task = %+v", task)
	}
	if len(task.Routes) != 1 || task.Routes[0].ProviderInstanceID != "t3-primary" ||
		task.Routes[0].Model != "opus" || task.Routes[0].QuotaPoolID != "pool-1" ||
		task.Routes[0].Options["effort"] != "high" {
		t.Fatalf("routes = %+v", task.Routes)
	}
	if request.BundleBytes != 2048 || request.BundleFiles != 7 {
		t.Fatalf("bundle description = %d bytes, %d files", request.BundleBytes, request.BundleFiles)
	}
}
