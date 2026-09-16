package main

// The channel that does not depend on the provider: the record the worker wrote
// into the activation workspace.
//
// The environment path tested in coordinator_supervisor_identity_test.go only
// reaches the CLI when t3.send_thread_environment is on, and it is off by
// default and off on the fleet because no tested T3 release verifies the field.
// With neither channel the CLI fell back to the host's own coordinator client
// and the overseer decided gates with full operator authority, which is what
// these tests are about.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// writeActivationWorkspace lays out one activation workspace with the record
// the worker writes into it, and makes a directory inside it the working
// directory, so the discovery below has to walk up to find it.
func writeActivationWorkspace(t *testing.T, activation workerproto.SupervisionActivation, mode os.FileMode) string {
	t.Helper()
	workspace := t.TempDir()
	content, declared, err := activation.ActivationIdentityRecord()
	if err != nil {
		t.Fatal(err)
	}
	if !declared {
		t.Fatal("the activation declares no credential, so there is no record to write")
	}
	if err := os.MkdirAll(filepath.Join(workspace, workerproto.SupervisorIdentityDir), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(workspace, filepath.FromSlash(workerproto.SupervisorIdentityFile))
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	inside := filepath.Join(workspace, "evidence")
	if err := os.MkdirAll(inside, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(inside)
	return workspace
}

func supervisorTestActivation() workerproto.SupervisionActivation {
	return workerproto.SupervisionActivation{
		ActivationID: "activation-1", RunID: supervisorIdentityTestRun, Epoch: 2,
		Principal:           supervisorClientName,
		CredentialReference: supervisorCredentialRef,
	}
}

// Started in the activation workspace with no environment and no flag, the CLI
// authenticates as the supervisor principal.
func TestOverseerWorkspaceRecordAuthenticatesAsTheSupervisorClient(t *testing.T) {
	withAdminCredentials(t, referenceAdminCredentials{})
	cfg := supervisorIdentityTestConfig(t)
	writeActivationWorkspace(t, supervisorTestActivation(), 0o600)

	// No flag, and the two environment variables are unset, which is the fleet's
	// configuration. The identity therefore comes from the workspace or from
	// nowhere.
	t.Setenv(workerproto.SupervisorCredentialEnvironment, "")
	t.Setenv(workerproto.SupervisorClientEnvironment, "")
	identity, err := resolveSupervisorIdentity("")
	if err != nil {
		t.Fatalf("resolve the supervisor identity: %v", err)
	}
	if !identity.declared() {
		t.Fatal("the activation workspace declares no supervisor identity")
	}
	if identity.ExpectedPrincipal != supervisorClientName {
		t.Fatalf("expected principal = %q, want %q", identity.ExpectedPrincipal, supervisorClientName)
	}
	overseer, err := newCoordinatorTransportAs(cfg, identity)
	if err != nil {
		t.Fatalf("build the overseer transport: %v", err)
	}
	if overseer.principal.ID != supervisorClientName {
		t.Fatalf("overseer authenticates as %q, want %q", overseer.principal.ID, supervisorClientName)
	}
}

// An ordinary task workspace has no such record, and the CLI stays this host's
// own admin client there.
func TestTaskWorkspaceWithoutASupervisionRecordStaysTheHostAdminClient(t *testing.T) {
	withAdminCredentials(t, referenceAdminCredentials{})
	cfg := supervisorIdentityTestConfig(t)

	// A task workspace carries the task identity record and nothing else.
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, domain.TaskIdentityDir), 0o700); err != nil {
		t.Fatal(err)
	}
	content, err := domain.RenderTaskIdentityFile(map[string]string{
		domain.TaskWaitEnvWorkflowRunID: "run-1", domain.TaskWaitEnvTaskID: "task-build",
		domain.TaskWaitEnvAttemptID: "attempt-1", domain.TaskWaitEnvAttemptRevision: "3",
		domain.TaskWaitEnvAssignmentID: "assignment-1", domain.TaskWaitEnvThreadID: "thread-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, filepath.FromSlash(domain.TaskIdentityFile)),
		[]byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(workspace)

	identity, err := resolveSupervisorIdentity("")
	if err != nil {
		t.Fatalf("resolve the supervisor identity: %v", err)
	}
	if identity.declared() {
		t.Fatalf("a task workspace produced a supervisor identity: %#v", identity)
	}
	transport, err := newCoordinatorTransportAs(cfg, identity)
	if err != nil {
		t.Fatalf("build the transport: %v", err)
	}
	if transport.principal.ID != workerHostClientName {
		t.Fatalf("a task authenticates as %q, want this host's own %q",
			transport.principal.ID, workerHostClientName)
	}
}

// An explicit flag wins over the workspace, so an operator deciding a gate by
// hand from inside an activation workspace still signs with the credential
// named on the command line.
func TestSupervisorCredentialFlagWinsOverTheWorkspaceRecord(t *testing.T) {
	writeActivationWorkspace(t, supervisorTestActivation(), 0o600)
	identity, err := resolveSupervisorIdentity(workerHostCredentialRef)
	if err != nil {
		t.Fatalf("resolve the supervisor identity: %v", err)
	}
	if identity.CredentialReference != workerHostCredentialRef || identity.ExpectedPrincipal != "" {
		t.Fatalf("identity = %#v, want the flag's credential and no expected principal", identity)
	}
}

// Every command family other than supervision reaches the coordinator as this
// host's own admin client, even when it is run inside an activation workspace.
// The discovered identity is refused there exactly as the flag is: it is a
// parameter of one seam, and no other command can reach a transport built from
// one.
func TestNonSupervisionCommandsIgnoreTheWorkspaceRecord(t *testing.T) {
	withAdminCredentials(t, referenceAdminCredentials{})
	cfg := supervisorIdentityTestConfig(t)
	writeActivationWorkspace(t, supervisorTestActivation(), 0o600)
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		t.Fatalf("build this host's own transport: %v", err)
	}
	if transport.principal.ID != workerHostClientName {
		t.Fatalf("a command run inside an activation workspace authenticates as %q, want %q",
			transport.principal.ID, workerHostClientName)
	}
}

// A record anyone could have written is refused rather than read, because it
// decides which admin client a command presents.
func TestSupervisorWorkspaceRecordMustBePrivate(t *testing.T) {
	writeActivationWorkspace(t, supervisorTestActivation(), 0o644)
	if _, err := resolveSupervisorIdentity(""); err == nil {
		t.Fatal("a world-readable supervisor identity record was accepted")
	}
}
