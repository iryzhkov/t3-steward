package campaign

// How the supervisor identity reaches the overseer's CLI.
//
// On a live supervised qualification both gates were accepted by the overseer's
// own activations, and both decision receipts named actor kind operator with
// the worker host's admin principal. The identity reached the thread only
// through T3's thread environment, which the coordinator sends only when
// t3.send_thread_environment is on; it is off by default and off on the fleet,
// because no tested T3 release verifies the field. The CLI therefore fell back
// to the host's own coordinator client and acted with full operator authority.
//
// This test follows the identity the other way: from the activation the
// coordinator dispatched, through the execution package the worker is offered,
// to the record that package tells the worker to write into the activation
// workspace, which is where the CLI finds it with no environment at all.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const overseerCredentialReference = "secretref:f03-admin/campaign-supervisor"

func TestActivationWorkspaceCarriesTheSupervisorIdentity(t *testing.T) {
	ctx := context.Background()
	run := newOverseerRun(t)
	review := overseerReadyGate(ctx, t, run)
	run.observe(t, "event-analysis-review", review.Gate.Definition.ID, "the analysis review is ready")
	plan := run.advance(t, domain.ActivationEventTriggerFired)
	attempt, assignment := run.dispatch(t, plan, []domain.WorkerSnapshot{baselineSnapshot()})

	// The coordinator's half: the activation records the principal its overseer
	// authenticates as.
	state, err := run.supervision.LoadSupervisionActivationState(ctx, run.fixture.supervised)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.Principal != overseerPrincipal {
		t.Fatalf("activation principal = %q, want %q", state.Activation.Principal, overseerPrincipal)
	}

	// The package half: the offer the worker is given carries the same principal
	// and the credential reference its CLI resolves.
	pkg := activationPackage(t, run, state, attempt, assignment)
	if pkg.Supervision == nil {
		t.Fatal("the execution package carries no activation")
	}
	if pkg.Supervision.Principal != overseerPrincipal ||
		pkg.Supervision.CredentialReference != overseerCredentialReference {
		t.Fatalf("package activation = %#v, want the supervisor principal and credential", pkg.Supervision)
	}

	// The workspace half: the record the worker writes, which the CLI reads with
	// no environment and no flag. It names the same principal the coordinator
	// recorded, which is the whole chain the qualification found broken.
	content, declared, err := pkg.Supervision.ActivationIdentityRecord()
	if err != nil {
		t.Fatalf("render the workspace identity record: %v", err)
	}
	if !declared {
		t.Fatal("the activation declares no credential, so its CLI has no identity to present")
	}
	values, err := workerproto.ParseSupervisorIdentityFile(content)
	if err != nil {
		t.Fatalf("parse the workspace identity record: %v", err)
	}
	if values[workerproto.SupervisorClientEnvironment] != state.Activation.Principal {
		t.Fatalf("the workspace record names %q, the coordinator recorded %q",
			values[workerproto.SupervisorClientEnvironment], state.Activation.Principal)
	}
	if values[workerproto.SupervisorCredentialEnvironment] != overseerCredentialReference {
		t.Fatalf("the workspace record names credential %q, want %q",
			values[workerproto.SupervisorCredentialEnvironment], overseerCredentialReference)
	}
	// The record is a selector and never a secret: it cannot be replayed as
	// authority by anything that reads it.
	if strings.Contains(content, pkg.Supervision.LeaseToken) ||
		strings.Contains(content, pkg.Identity.DispatchToken) {
		t.Fatal("the workspace identity record carries a token")
	}

	// The environment stays as a second channel rather than the only one.
	environment := pkg.Supervision.ActivationEnvironment()
	if environment[workerproto.SupervisorClientEnvironment] != state.Activation.Principal {
		t.Fatalf("activation environment = %#v, want the same principal", environment)
	}

	// The prompt states the fallback in the exact form a command needs, because
	// the discovery above depends on the working directory.
	if !strings.Contains(pkg.Supervision.Prompt, "--supervisor-credential "+overseerCredentialReference) {
		t.Fatalf("the activation prompt does not state the credential flag:\n%s", pkg.Supervision.Prompt)
	}

	// And the activation the CLI would decide under holds a durable remaining
	// cost, so quota planning admits it instead of skipping it as inconsistent.
	if assignment.Estimate == nil || assignment.Estimate.RemainingCost <= 0 {
		t.Fatalf("activation assignment estimate = %#v, want a durable remaining cost", assignment.Estimate)
	}
}

// activationPackage renders the execution package this activation's worker is
// offered, from the durable records the coordinator holds.
func activationPackage(
	t *testing.T,
	run *overseerRun,
	state backlog.SupervisionActivationState,
	attempt domain.Attempt,
	assignment domain.Assignment,
) workerproto.ExecutionPackage {
	t.Helper()
	ctx := context.Background()
	records := reload(ctx, t, run.fixture.store)
	admin, err := run.fixture.store.LoadSupervisionAdminState(ctx, run.fixture.supervised)
	if err != nil {
		t.Fatal(err)
	}
	gates := make([]backlog.ActivationGateFacts, 0, len(admin.Gates))
	for _, facts := range admin.Gates {
		gates = append(gates, backlog.ActivationGateFacts{Gate: facts.Gate, Evidence: facts.Evidence})
	}
	var workflow domain.Workflow
	var workflowRun domain.WorkflowRun
	for _, candidate := range records.WorkflowRuns {
		if candidate.ID == run.fixture.supervised {
			workflowRun = candidate
		}
	}
	for _, candidate := range records.Workflows {
		if candidate.ID == workflowRun.WorkflowID {
			workflow = candidate
		}
	}
	inbox := backlog.CoalesceSupervisionEvents(run.fixture.supervised, state.Record.EventCursor, state.Pending)
	dispatch := backlog.ActivationDispatch{
		Identity: state.Activation.DispatchIdentity, Epoch: state.Activation.Epoch,
		LeaseToken: state.Activation.LeaseToken, RequiredCapability: backlog.SupervisionWorkerCapability,
	}
	if state.Activation.LeaseExpiresAt != nil {
		dispatch.LeaseExpiresAt = state.Activation.LeaseExpiresAt.UTC()
	}
	if state.Activation.Deadline != nil {
		dispatch.Deadline = state.Activation.Deadline.UTC()
	}
	pkg, err := backlog.BuildActivationPackage(backlog.ActivationPackageInput{
		Workflow: workflow, Run: workflowRun, Record: state.Record, Activation: state.Activation,
		Dispatch: dispatch, Attempt: attempt, Assignment: assignment,
		Triggers: inbox.Triggers, Tasks: domain.TasksForRun(workflowRun, records.Tasks),
		Attempts: records.Attempts, Gates: gates, Artifacts: records.Artifacts,
		SupervisorPrincipal:           overseerPrincipal,
		SupervisorCredentialReference: overseerCredentialReference,
		CoordinatorID:                 "coordinator-1",
		CoordinatorEpoch:              1,
		CatalogRevision:               "catalog-1",
		PrepareTimeout:                time.Minute,
		VerificationTimeout:           time.Minute,
		MaxArtifactBytes:              1 << 20,
		MaxTotalBytes:                 2 << 20,
		Now:                           run.now,
	})
	if err != nil {
		t.Fatalf("build the activation package: %v", err)
	}
	return pkg
}
