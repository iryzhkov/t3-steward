package workerruntime

// The worker's half of the overseer identity: a private record in the
// activation workspace saying which admin client the CLI must present.
//
// The thread environment carries the same two selectors, but only when the
// deployment turned t3.send_thread_environment on, and the fleet leaves it off
// because no tested T3 release verifies the field. With neither channel the
// overseer's CLI fell back to its host's own coordinator client, and every
// decision receipt named an operator instead of the supervisor.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const (
	supervisionTestPrincipal  = "supervisor:normandy"
	supervisionTestCredential = "secretref:f03-admin/supervisor-normandy"
)

// testActivationPackage is the package a worker is offered for one overseer
// activation: no outputs, no verification, a fresh task-scoped workspace.
func testActivationPackage() workerproto.ExecutionPackage {
	pkg := testPackage()
	pkg.Outputs = nil
	pkg.Verification = nil
	pkg.Environment.Scope = "task"
	pkg.Environment.SetupProfile = "supervision-activation"
	pkg.RequiredCapabilities = []string{workerproto.CapabilityCampaignSupervision}
	pkg.Supervision = &workerproto.SupervisionActivation{
		ActivationID: "activation-1", RunID: pkg.Identity.WorkflowRunID, Epoch: 2,
		RecordRevision: 4, GraphRevision: 1,
		Principal:           supervisionTestPrincipal,
		CredentialReference: supervisionTestCredential,
		LeaseToken:          "lease-1", LeaseExpiresAt: runtimeTestNow.Add(time.Hour),
		MaxTurns: 3, Prompt: "review the analysis gate",
		Actions: []workerproto.SupervisionAction{{
			Name: "decide", Command: []string{"t3-steward", "campaign", "supervision", "decide"},
		}},
	}
	return pkg
}

// Preparing an activation writes the record, private, carrying selectors and no
// secret. It is written before any thread exists, because the CLI inside that
// thread is what reads it.
func TestActivationWorkspaceCarriesThePrivateSupervisorIdentity(t *testing.T) {
	workspace := t.TempDir()
	driver := &LocalDriver{}
	pkg := testActivationPackage()
	if err := driver.writeSupervisionIdentity(pkg, workspace); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(workspace, filepath.FromSlash(workerproto.SupervisorIdentityFile))
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("supervision identity mode = %v, want a regular 0600 file", info.Mode())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	values, err := workerproto.ParseSupervisorIdentityFile(string(raw))
	if err != nil {
		t.Fatalf("parse the record back: %v", err)
	}
	if values[workerproto.SupervisorClientEnvironment] != supervisionTestPrincipal ||
		values[workerproto.SupervisorCredentialEnvironment] != supervisionTestCredential {
		t.Fatalf("record = %#v, want the activation's own principal and credential reference", values)
	}
	if pkg.Identity.DispatchToken == "" {
		t.Fatal("the fixture has no dispatch token, so its absence proves nothing")
	}
	if strings.Contains(string(raw), pkg.Identity.DispatchToken) ||
		strings.Contains(string(raw), pkg.Supervision.LeaseToken) {
		t.Fatal("the supervision identity record carries a token")
	}
	// Re-preparing the same activation keeps it private.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := driver.writeSupervisionIdentity(pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if info, err = os.Lstat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("rewritten supervision identity mode = %v (%v)", info.Mode(), err)
	}
}

// An ordinary task package writes no such record: only an activation has a
// supervisor identity to present.
func TestTaskPackageWritesNoSupervisorIdentity(t *testing.T) {
	workspace := t.TempDir()
	if err := (&LocalDriver{}).writeSupervisionIdentity(testPackage(), workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(workspace, workerproto.SupervisorIdentityDir)); !os.IsNotExist(err) {
		t.Fatalf("a task workspace holds a supervision identity directory: %v", err)
	}
}

// A deployment that configured no supervisor credential writes nothing rather
// than an empty record, because an empty record would say the CLI has an
// identity to present when it has none.
func TestActivationWithoutACredentialWritesNoRecord(t *testing.T) {
	workspace := t.TempDir()
	pkg := testActivationPackage()
	pkg.Supervision.CredentialReference = ""
	if err := (&LocalDriver{}).writeSupervisionIdentity(pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(workspace, workerproto.SupervisorIdentityDir)); !os.IsNotExist(err) {
		t.Fatalf("an activation with no credential left a record: %v", err)
	}
}

// The record leaves the workspace before the activation's turn is finalized.
func TestSupervisionIdentityIsRemovedBeforeAnythingIsFinalized(t *testing.T) {
	workspace := t.TempDir()
	driver := &LocalDriver{}
	if err := driver.writeSupervisionIdentity(testActivationPackage(), workspace); err != nil {
		t.Fatal(err)
	}
	if err := driver.removeSupervisionIdentity(workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(workspace, workerproto.SupervisorIdentityDir)); !os.IsNotExist(err) {
		t.Fatalf("the supervision identity survived collection: %v", err)
	}
}
