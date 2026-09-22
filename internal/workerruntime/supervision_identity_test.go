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
	"context"
	"crypto/sha256"
	"encoding/hex"
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
func TestActivationPrepareMaterializesVerifiedEvidenceAndPrompt(t *testing.T) {
	root := t.TempDir()
	pkg := testActivationPackage()
	prompt := []byte("authored supervisor prompt")
	evidence := []byte("{\"activationId\":\"activation-1\",\"gates\":[\"gate-1\"]}")
	object := func(id, path string, data []byte) workerproto.ArtifactObject {
		sum := sha256.Sum256(data)
		return workerproto.ArtifactObject{
			ID: id, Path: path, Kind: "input", MediaType: "application/octet-stream",
			Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:]),
		}
	}
	pkg.Prompt = object("supervision-prompt", "prompt.md", prompt)
	pkg.StaticInputs = []workerproto.ArtifactObject{
		object("activation-evidence", "inputs/supervision-evidence.json", evidence),
	}
	pkg.Limits.MaxArtifactBytes = 1 << 20
	driver := &LocalDriver{
		Config: LocalDriverConfig{ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
		Source: mapArtifactSource{pkg.Prompt.ID: prompt, pkg.StaticInputs[0].ID: evidence},
	}
	workspace, err := driver.Prepare(context.Background(), pkg)
	if err != nil {
		t.Fatalf("prepare activation: %v", err)
	}
	for path, want := range map[string][]byte{
		"inputs/supervision-prompt.md":     prompt,
		"inputs/supervision-evidence.json": evidence,
	} {
		fullPath := filepath.Join(workspace, filepath.FromSlash(path))
		got, err := os.ReadFile(fullPath)
		if err != nil || string(got) != string(want) {
			t.Fatalf("materialized %s = %q, err %v", path, got, err)
		}
		info, err := os.Lstat(fullPath)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("materialized %s mode = %v, err %v", path, info.Mode(), err)
		}
	}
	if info, err := os.Lstat(workspace); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("activation workspace mode = %v, err %v", info.Mode(), err)
	}
	evidencePath := filepath.Join(workspace, "inputs", "supervision-evidence.json")
	outside := filepath.Join(root, "outside")
	if err := os.WriteFile(outside, []byte("outside unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(evidencePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, evidencePath); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Prepare(context.Background(), pkg); err == nil {
		t.Fatal("re-prepare followed a model-writable evidence symlink")
	}
	if got, err := os.ReadFile(outside); err != nil || string(got) != "outside unchanged" {
		t.Fatalf("outside target changed to %q, err %v", got, err)
	}

	corruptRoot := t.TempDir()
	corrupt := &LocalDriver{
		Config: LocalDriverConfig{ArtifactRoot: filepath.Join(corruptRoot, "artifacts"), RunsRoot: filepath.Join(corruptRoot, "runs")},
		Source: mapArtifactSource{pkg.Prompt.ID: prompt, pkg.StaticInputs[0].ID: []byte("wrong evidence")},
	}
	if _, err := corrupt.Prepare(context.Background(), pkg); err == nil {
		t.Fatal("prepare accepted evidence with the wrong digest and size")
	}
}

func TestActivationRenderedPromptNamesEvidenceAndFitsCap(t *testing.T) {
	pkg := testActivationPackage()
	pkg.Supervision.Prompt = strings.Repeat("bounded evidence line\n", 1200)
	legacy := ActivationPrompt(*pkg.Supervision)
	if strings.Contains(legacy, "inputs/supervision-evidence.json") ||
		!strings.Contains(legacy, pkg.Supervision.Prompt) {
		t.Fatalf("legacy activation prompt invented evidence files or lost inline evidence: %s", legacy)
	}
	pkg.Supervision.EvidenceFiles = true
	rendered := ActivationPrompt(*pkg.Supervision)
	if len(rendered) > 32<<10 {
		t.Fatalf("model-bound prompt is %d bytes, exceeds 32768", len(rendered))
	}
	for _, required := range []string{"inputs/supervision-evidence.json", "inputs/supervision-prompt.md", "Read only the subjects needed"} {
		if !strings.Contains(rendered, required) {
			t.Fatalf("model-bound prompt does not name %q: %s", required, rendered)
		}
	}
}

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
