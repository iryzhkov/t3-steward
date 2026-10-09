package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Feedback 135: on a worker without a coordinator client "t3-steward ask"
// failed with a bare transport error and the task was not parked. It is now a
// typed block: the agent is told in so many words that nothing was asked and
// the task is not parked, nothing reaches the coordinator, and the exit code
// is the client-configuration code rather than a generic failure.
func TestAskOnAClientlessWorkerBlocksVisiblyAndParksNothing(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	previous := askCoordinatorReach
	askCoordinatorReach = func(config.Config) error { return errors.New("no coordinator client on agent-a") }
	t.Cleanup(func() { askCoordinatorReach = previous })

	identity, err := resolveTaskIdentity(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := parseAskArgs([]string{"pick one", "--option", "alpha", "--option", "beta"})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = runAsk(ctx, cfg, spec, identity, &out)
	if err == nil {
		t.Fatal("an ask on a client-less worker succeeded")
	}
	for _, want := range []string{askRelayUnavailableCode + ":", "NOT parked", "no question was sent",
		"no coordinator client on agent-a", "placement.requires: [" + workerproto.CapabilityAskRelay + "]"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error does not say %q: %v", want, err)
		}
	}
	if !strings.HasPrefix(err.Error(), "client-configuration: "+askRelayUnavailableCode+": ") {
		t.Fatalf("the typed reason does not lead the message: %v", err)
	}
	if class := backlogadmin.ClassOf(err); class != backlogadmin.ClassClientConfiguration || backlogadmin.ExitCode(class) != 3 {
		t.Fatalf("class %q exit %d, want client-configuration and 3", class, backlogadmin.ExitCode(class))
	}
	if strings.Contains(out.String(), "End this turn now") || out.Len() != 0 {
		t.Fatalf("a blocked ask told the agent it was parked: %q", out.String())
	}
	waits, err := store.ListTaskWaits(ctx)
	if err != nil || len(waits) != 0 {
		t.Fatalf("a blocked ask registered waits: %+v err=%v", waits, err)
	}

	// With a route to the coordinator the same ask parks the task, so the
	// block is the missing route and nothing else.
	askCoordinatorReach = askCoordinatorRoute
	if err := runAsk(ctx, cfg, spec, identity, &out); err != nil {
		t.Fatalf("the same ask with a route: %v", err)
	}
	if !strings.Contains(out.String(), "End this turn now") {
		t.Fatalf("the agent was not told to end its turn: %s", out.String())
	}
}

// The real reachability check blocks on a host that is neither a coordinator
// nor a configured client, before any carrier is built.
func TestAskBlocksOnAHostWithoutAnyCoordinatorRoute(t *testing.T) {
	cfg := config.Default()
	cfg.StatePath = filepath.Join(t.TempDir(), "state.db")
	spec, err := parseAskArgs([]string{"pick one", "--option", "alpha", "--option", "beta"})
	if err != nil {
		t.Fatal(err)
	}
	err = runAsk(context.Background(), cfg, spec, taskIdentity{AttemptID: "attempt-1"}, &bytes.Buffer{})
	if err == nil || !strings.HasPrefix(err.Error(), "client-configuration: "+askRelayUnavailableCode+": ") {
		t.Fatalf("err = %v, want %s", err, askRelayUnavailableCode)
	}
}

// "campaign check" carries a declared host capability to the coordinator, and
// prints the coordinator's temporary capability-missing as what the campaign
// waits for, not as a refusal.
func TestCampaignCheckCarriesAndNamesAMissingHostCapability(t *testing.T) {
	plan := campaign.Plan{
		Environment: campaign.Environment{Project: "t3-steward"},
		Tasks: []campaign.Task{{Name: "implement", Placement: campaign.Placement{
			Requires: []string{workerproto.CapabilityAskRelay, workerproto.GitPushCapability("t3-steward")},
		}}},
	}
	request, err := campaignViabilityRequest(plan, 0, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := request.Tasks[0].Capabilities; !slices.Contains(got, workerproto.CapabilityAskRelay) || !slices.Contains(got, "git-push-t3-steward") {
		t.Fatalf("readiness request capabilities = %v", got)
	}

	reason := backlogadmin.ViabilityReason{Code: backlogadmin.ReasonCapabilityMissing,
		Detail: `worker "agent-a" lacks capability "ask-relay-v1"; it is a host capability the worker reports when its host provides it`}
	matrix := backlogadmin.ViabilityMatrix{
		Outcome: backlogadmin.ViabilityAcceptedWaiting,
		Tasks: []backlogadmin.ViabilityTaskResult{{Task: "implement", Outcome: backlogadmin.ViabilityAcceptedWaiting,
			Candidates: []backlogadmin.ViabilityCandidate{{Worker: "agent-a", Outcome: backlogadmin.ViabilityAcceptedWaiting,
				Reasons: []backlogadmin.ViabilityReason{reason}}}}},
	}
	var out bytes.Buffer
	if err := renderCampaignCheck(&out, campaignCheck{Name: "demo", Matrix: matrix}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"campaign demo is accepted_waiting: submit accepts it and it waits, queued, for capability-missing to clear",
		`temporary  capability-missing: worker "agent-a" lacks capability "ask-relay-v1"`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output does not contain %q:\n%s", want, out.String())
		}
	}
	if len(matrix.PermanentReasons()) != 0 {
		t.Fatalf("a waiting matrix has permanent reasons: %+v", matrix.PermanentReasons())
	}
}

// Amendment validation reads configuration, which cannot say whether a host
// will provide a host capability, so a task requiring one is accepted there
// and placement decides from the real snapshot; an unknown capability is
// still refused.
func TestGraphTaskValidatorDefersHostCapabilitiesToPlacement(t *testing.T) {
	root := t.TempDir()
	cfg := qualificationConfig(root)
	cfg.Path = filepath.Join(root, "config.yaml")
	writeReloadConfig(t, cfg.Path, cfg)
	loaded, err := config.LoadFile(cfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	validate := graphTaskValidator(loaded.BacklogV2)
	workflow := domain.Workflow{ID: "workflow-1", Project: "steward",
		Environment: domain.ExecutionEnvironment{Type: "git", Scope: "task", Ref: "main"}}
	task := func(capabilities ...string) domain.Task {
		return domain.Task{ID: "task-1", WorkflowID: workflow.ID, Name: "implement",
			Placement: domain.Placement{Hosts: []string{qualificationWorkerID()}, Capabilities: capabilities},
			Routes:    []domain.ProviderRoute{{ProviderInstanceID: "test", Model: "test", QuotaPoolID: "pool"}}}
	}
	for _, capability := range []string{workerproto.CapabilityAskRelay, workerproto.CapabilityCoordinatorClient,
		workerproto.CapabilityHuyangTrusted, workerproto.GitPushCapability("steward")} {
		if err := validate(workflow, task(capability)); err != nil {
			t.Fatalf("host capability %q refused: %v", capability, err)
		}
	}
	if err := validate(workflow, task("gpu")); err == nil || !strings.Contains(err.Error(), "no configured worker supports") {
		t.Fatalf("an unknown capability was accepted: %v", err)
	}
}
