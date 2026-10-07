//go:build linux

package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
)

// A contained run the memory limit killed has no T3 outcome left to capture.
// A forced quiesce, which failure collection and cleanup use, must still stop
// its unit and keep the named cause; otherwise the attempt can never end.
func TestForcedQuiesceStopsAContainedRunTheMemoryLimitKilled(t *testing.T) {
	root := t.TempDir()
	journal, err := providercontainment.CanonicalRoot(filepath.Join(root, "journal"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(journal, 0700); err != nil {
		t.Fatal(err)
	}
	pkg := testPackage()
	pkg.Environment.DirectoryBindings = []directoryresource.Binding{{}}
	pkg.ResourceDemand = &domain.ResourceDemand{CPUUnits: 2, MemoryMB: 6000}
	manager := ContainedT3{Supervisor: providercontainment.Supervisor{Root: journal, Executable: "/bin/true"}}
	launch := providercontainment.Launch{ExecutionID: pkg.Identity.ThreadID, Spec: providercontainment.Spec{
		WorkerID: pkg.WorkerID, Directories: pkg.Environment.DirectoryBindings,
		Control: &directoryresource.Identity{}, Limits: containedLimits(pkg),
	}}
	key := sha256.Sum256([]byte(launch.Spec.WorkerID + "\x00" + launch.ExecutionID))
	unit := "t3-contained-" + hex.EncodeToString(key[:]) + ".service"
	if err := os.Mkdir(filepath.Join(journal, unit), 0700); err != nil {
		t.Fatal(err)
	}
	intent, err := json.Marshal(struct {
		Launch     providercontainment.Launch `json:"launch"`
		Executable string                     `json:"executable"`
	}{launch, manager.Supervisor.Executable})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(intent)
	digest := hex.EncodeToString(sum[:])
	if err := os.WriteFile(filepath.Join(journal, unit, "intent.json"), intent, 0600); err != nil {
		t.Fatal(err)
	}
	path, err := manager.recordPath(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := privateJSON(path+".preparation", containedPreparation{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Launch: launch}); err != nil {
		t.Fatal(err)
	}
	attachment, err := json.Marshal(ContainedAttachment{WorkerID: pkg.WorkerID, Identity: pkg.Identity, Launch: launch, InvocationID: "invocation", EnvironmentID: "environment"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, attachment, 0600); err != nil {
		t.Fatal(err)
	}
	// A systemctl stand-in: the unit failed with the memory limit's result.
	bin := t.TempDir()
	stopped := filepath.Join(bin, "stopped")
	script := "#!/bin/sh\nif [ \"$2\" = stop ]; then : > " + stopped + "; exit 0; fi\n" +
		"printf 'LoadState=loaded\\nDescription=t3-containment:" + digest + "\\nActiveState=failed\\nSubState=failed\\nInvocationID=invocation\\nResult=oom-kill\\nCPUQuotaPerSecUSec=2s\\nMemoryMax=6291456000\\n'\n"
	if err := os.WriteFile(filepath.Join(bin, "systemctl"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	if err := manager.Quiesce(context.Background(), pkg, false); err == nil {
		t.Fatal("an unforced quiesce adopted a run whose outcome it cannot capture")
	}
	if err := manager.Quiesce(context.Background(), pkg, true); err != nil {
		t.Fatalf("forced quiesce of a memory-killed run: %v", err)
	}
	if _, err := os.Stat(stopped); err != nil {
		t.Fatalf("the unit was never stopped: %v", err)
	}
	observation, err := manager.Supervisor.Observe(context.Background(), launch)
	if err != nil || !observation.Stopped || observation.Failure != "contained run exceeded its 6000 MB memory reservation" {
		t.Fatalf("stopped observation %+v %v", observation, err)
	}
	retained, err := manager.Attach(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	if message, err := retained.LastAssistantMessage(context.Background(), pkg.Identity.ThreadID); err != nil || !strings.Contains(message, "6000 MB memory reservation") {
		t.Fatalf("retained outcome %q %v does not name the reservation", message, err)
	}
}
