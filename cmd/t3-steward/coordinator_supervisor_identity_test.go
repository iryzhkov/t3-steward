package main

// The identity path of an overseer, end to end: the credential the coordinator
// puts on an activation, the environment the worker starts the thread with, the
// client the CLI then authenticates as, and the scope the coordinator resolves
// for it.
//
// Every link existed except the middle two. The activation named the credential
// in its prompt and the CLI had no way to use it, so an overseer on a worker
// host authenticated as that host's ordinary admin client and was refused every
// supervision operation.

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const (
	supervisorClientName      = "supervisor:normandy"
	supervisorCredentialRef   = "secretref:f03-admin/supervisor-normandy"
	workerHostClientName      = "admin:omarchy-pc"
	workerHostCredentialRef   = "secretref:f03-admin/omarchy-pc"
	supervisorIdentityTestRun = "run-supervised"
)

// referenceAdminCredentials resolves each admin credential reference to the
// client it belongs to, which is what the coordinator's remote server checks a
// frame's sender against.
type referenceAdminCredentials struct{}

func (referenceAdminCredentials) ResolveAdmin(reference string) (backlogadmin.AdminCredentials, error) {
	principal := workerHostClientName
	if reference == supervisorCredentialRef {
		principal = supervisorClientName
	}
	return backlogadmin.AdminCredentials{
		ClientPrincipal: principal, ClientKeyID: principal + "-key",
		ClientSecret:         []byte("0123456789abcdef-client"),
		CoordinatorPrincipal: "coordinator:normandy", CoordinatorKeyID: "coordinator-key",
		CoordinatorSecret: []byte("0123456789abcdef-coord"),
	}, nil
}

func supervisorIdentityTestConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.StatePath = filepath.Join(t.TempDir(), "state.sqlite")
	cfg.BacklogV2.CoordinatorClient = config.V2CoordinatorClient{
		CoordinatorID: "normandy-coordinator", Address: "normandy", Connection: "ssh",
		RemoteCommand: "t3-steward", Credential: workerHostCredentialRef,
		RequestTimeout: config.Duration(30 * time.Second),
		MessageLimits:  config.V2MessageLimits{MaxBytes: 1 << 20, MaxFiles: 100, MaxArtifactBytes: 1 << 20},
	}
	return cfg
}

// supervisorIdentityScope is the coordinator's own answer about which principal
// may act on the run, read from the activation it recorded.
type supervisorIdentityScope struct{ principal string }

func (s supervisorIdentityScope) SupervisorScope(_ context.Context, principal, runID string) (backlogadmin.SupervisorScope, error) {
	if principal != s.principal || runID != supervisorIdentityTestRun {
		return backlogadmin.SupervisorScope{}, nil
	}
	return backlogadmin.SupervisorScope{RunID: supervisorIdentityTestRun, ActivationEpoch: 2}, nil
}

func (supervisorIdentityScope) ArtifactRun(context.Context, string) (string, error) {
	return supervisorIdentityTestRun, nil
}

// An overseer started from an activation package authenticates as the fleet's
// supervisor client and is authorized on its run; the same host's ordinary
// admin client, running the same command, is not.
func TestOverseerEnvironmentAuthenticatesAsTheSupervisorClient(t *testing.T) {
	withAdminCredentials(t, referenceAdminCredentials{})
	cfg := supervisorIdentityTestConfig(t)

	// The coordinator's half: the activation carries the supervisor client and
	// the credential reference its CLI must present.
	settings := coordinatorActivationSettings{
		CoordinatorID: "normandy-coordinator", CoordinatorEpoch: 1,
		SupervisorClient:              supervisorClientName,
		SupervisorCredentialReference: supervisorCredentialRef,
	}
	if !settings.configured() {
		t.Fatal("the supervisor settings are not configured")
	}
	activation := workerproto.SupervisionActivation{
		ActivationID: "activation-1", RunID: supervisorIdentityTestRun, Epoch: 2,
		Principal:           settings.SupervisorClient,
		CredentialReference: settings.SupervisorCredentialReference,
	}

	// The worker's half: the thread environment the activation is started with.
	environment := activation.ActivationEnvironment()
	if environment[workerproto.SupervisorCredentialEnvironment] != supervisorCredentialRef ||
		environment[workerproto.SupervisorClientEnvironment] != supervisorClientName {
		t.Fatalf("activation environment = %#v", environment)
	}
	lookup := func(name string) (string, bool) {
		value, ok := environment[name]
		return value, ok
	}

	// The CLI's half: the transport it builds inside that thread.
	identity := supervisorIdentityFromEnvironment(lookup)
	if !identity.declared() {
		t.Fatal("the overseer's environment declares no supervisor identity")
	}
	overseer, err := newCoordinatorTransportAs(cfg, identity)
	if err != nil {
		t.Fatalf("build the overseer transport: %v", err)
	}
	if overseer.principal.ID != supervisorClientName {
		t.Fatalf("overseer authenticates as %q, want %q", overseer.principal.ID, supervisorClientName)
	}
	// The same host without that environment stays its own admin client.
	ownClient, err := newCoordinatorTransport(cfg)
	if err != nil {
		t.Fatalf("build this host's own transport: %v", err)
	}
	if ownClient.principal.ID != workerHostClientName {
		t.Fatalf("this host authenticates as %q, want %q", ownClient.principal.ID, workerHostClientName)
	}

	// The coordinator's authorizer: it resolves scope under the relayed form of
	// the principal it recorded on the activation, which must be the relayed
	// form of the client the CLI just presented.
	recorded := settings.principalID()
	if recorded != backlogadmin.RelayedPrincipalID(overseer.principal.ID) {
		t.Fatalf("the coordinator recorded %q, the CLI presents %q",
			recorded, backlogadmin.RelayedPrincipalID(overseer.principal.ID))
	}
	authorizer := backlogadmin.SupervisorAuthorizer{
		Scope:    supervisorIdentityScope{principal: recorded},
		Delegate: refuseEveryAdminAction{},
	}
	decision := backlogadmin.Action{
		Kind:            backlogadmin.SupervisionDecisionKind,
		WorkflowRunID:   supervisorIdentityTestRun,
		ActivationEpoch: 2,
	}
	supervisor := backlogadmin.Principal{
		ID:    backlogadmin.RelayedPrincipalID(overseer.principal.ID),
		Roles: []string{backlogadmin.SupervisorRole},
	}
	if err := authorizer.Authorize(context.Background(), supervisor, decision); err != nil {
		t.Fatalf("the overseer's own principal was refused its decision: %v", err)
	}
	ordinary := backlogadmin.Principal{
		ID:    backlogadmin.RelayedPrincipalID(ownClient.principal.ID),
		Roles: []string{backlogadmin.SupervisorRole},
	}
	if err := authorizer.Authorize(context.Background(), ordinary, decision); err == nil {
		t.Fatal("this host's ordinary admin client was authorized to decide a gate")
	}
}

// refuseEveryAdminAction stands for the delegate behind the supervisor
// authorizer. It refuses everything, so an authorization that succeeds below is
// the supervisor scope's own answer and never the delegate's.
type refuseEveryAdminAction struct{}

func (refuseEveryAdminAction) Authorize(context.Context, backlogadmin.Principal, backlogadmin.Action) error {
	return errRefusedByDelegate
}

var errRefusedByDelegate = backlogadmin.ErrInvalidQuery

// A credential that resolves to a different admin client than the activation
// named is refused on this host, rather than travelling and coming back as an
// unauthorized-scope refusal that an expired activation produces too.
func TestOverseerRefusesAMismatchedSupervisorCredential(t *testing.T) {
	withAdminCredentials(t, referenceAdminCredentials{})
	cfg := supervisorIdentityTestConfig(t)
	_, err := newCoordinatorTransportAs(cfg, supervisorIdentity{
		CredentialReference: workerHostCredentialRef,
		ExpectedPrincipal:   supervisorClientName,
	})
	if backlogadmin.ClassOf(err) != backlogadmin.ClassClientConfiguration {
		t.Fatalf("class = %q (%v)", backlogadmin.ClassOf(err), err)
	}
}

// A value outside the admin credential namespace never becomes a credential
// lookup, whatever the environment says.
func TestOverseerRefusesACredentialOutsideTheAdminNamespace(t *testing.T) {
	withAdminCredentials(t, referenceAdminCredentials{})
	cfg := supervisorIdentityTestConfig(t)
	_, err := newCoordinatorTransportAs(cfg, supervisorIdentity{
		CredentialReference: "secretref:f03-worker/omarchy-pc",
	})
	if backlogadmin.ClassOf(err) != backlogadmin.ClassClientConfiguration {
		t.Fatalf("class = %q (%v)", backlogadmin.ClassOf(err), err)
	}
}
