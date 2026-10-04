package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"reflect"
	"strings"
	"testing"
)

func TestInitialTitleDispatchPreservesProviderInput(t *testing.T) {
	legacy := preflightTestPackage()
	driver, control, workspace := preflightDriver(t, legacy, &preflightStubRunner{})
	if err := driver.CreateThread(context.Background(), legacy, workspace); err != nil {
		t.Fatal(err)
	}
	pkg := legacy
	pkg.Display = &workerproto.SessionDisplay{WorkflowName: "Ship café", TaskName: "Build"}
	pkg.RequiredCapabilities = []string{workerproto.PackageCapabilitySessionDisplay}
	if err := driver.CreateThread(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if len(control.created) != 2 {
		t.Fatalf("provider calls %+v", control.created)
	}
	old, new := control.created[0], control.created[1]
	if old.Title != workerproto.InitialSessionTitle(legacy) || new.Title != workerproto.InitialSessionTitle(pkg) || old.Title == new.Title {
		t.Fatalf("titles %q / %q", old.Title, new.Title)
	}
	new.Title = old.Title
	if !reflect.DeepEqual(old, new) {
		t.Fatal("display changed dispatch identity, environment, prompt or provider selection")
	}
}
func TestDisplayTitlePreflightFailureHasNoProviderEffect(t *testing.T) {
	pkg := preflightTestPackage(preflightCheckStep("require-pass"))
	pkg.Display = &workerproto.SessionDisplay{WorkflowName: "Ship", TaskName: "Build"}
	pkg.RequiredCapabilities = append(pkg.RequiredCapabilities, workerproto.PackageCapabilitySessionDisplay)
	runner := &preflightStubRunner{exit: 2, output: "failed"}
	driver, control, workspace := preflightDriver(t, pkg, runner)
	if err := driver.CreateThread(context.Background(), pkg, workspace); err == nil {
		t.Fatal("failed preflight allowed dispatch")
	}
	if len(control.created) != 0 || len(control.managedProjects) != 0 || len(runner.requests) != 1 {
		t.Fatal("provider effect before preflight")
	}
}
func TestInitialSupervisionTitleDispatch(t *testing.T) {
	for _, display := range []*workerproto.SessionDisplay{nil, {WorkflowName: "Release café"}} {
		pkg := testActivationPackage()
		pkg.Display = display
		if display != nil {
			pkg.RequiredCapabilities = append(pkg.RequiredCapabilities, workerproto.PackageCapabilitySessionDisplay)
		}
		driver, control, workspace := preflightDriver(t, pkg, &preflightStubRunner{})
		if err := driver.CreateThread(context.Background(), pkg, workspace); err != nil {
			t.Fatal(err)
		}
		if len(control.created) != 1 || control.created[0].Title != workerproto.InitialSessionTitle(pkg) || !strings.Contains(control.created[0].Title, "supervision:") {
			t.Fatalf("activation dispatch %+v", control.created)
		}
		if control.created[0].ThreadID != pkg.Identity.ThreadID || control.created[0].DispatchToken != pkg.Identity.DispatchToken {
			t.Fatal("title changed activation identity")
		}
	}
}
