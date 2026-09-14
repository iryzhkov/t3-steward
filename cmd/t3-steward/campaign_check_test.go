package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
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
