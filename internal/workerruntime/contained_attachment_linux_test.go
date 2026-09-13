//go:build linux

package workerruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
)

func TestContainedAttachmentRecoveryAndFences(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "t3-attach-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	journal := filepath.Join(root, "journal")
	control := filepath.Join(root, "control")
	for _, p := range []string{journal, control} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	file, identity, err := directoryresource.Open(directoryresource.Registration{WorkerID: "worker-a", ResourceID: "control", Revision: "1", Path: control})
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	if err := os.WriteFile(filepath.Join(control, "token"), []byte("owned"), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(control, "api.sock"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(control, "api.sock"), 0600); err != nil {
		t.Fatal(err)
	}
	var environment atomic.Value
	environment.Store("environment-a")
	var calls atomic.Int64
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/.well-known/t3/environment" {
			json.NewEncoder(w).Encode(map[string]string{"environmentId": environment.Load().(string)})
			return
		}
		if r.Header.Get("Authorization") != "Bearer owned" {
			w.WriteHeader(401)
			return
		}
		if r.URL.Path == "/api/auth/session" {
			fmt.Fprint(w, `{"authenticated":true}`)
			return
		}
		fmt.Fprint(w, `{"threads":[],"projects":[]}`)
	})}
	go server.Serve(listener)
	defer server.Close()
	pkg := testPackage()
	pkg.Environment.DirectoryBindings = []directoryresource.Binding{{}}
	record := ContainedAttachment{WorkerID: pkg.WorkerID, Identity: pkg.Identity, InvocationID: "invocation-a", EnvironmentID: "environment-a",
		Launch: providercontainment.Launch{ExecutionID: pkg.Identity.ThreadID, Spec: providercontainment.Spec{WorkerID: pkg.WorkerID, Control: &identity, Directories: pkg.Environment.DirectoryBindings}}}
	var invocation atomic.Value
	invocation.Store("invocation-a")
	manager := ContainedT3{Supervisor: providercontainment.Supervisor{Root: journal}, Timeout: time.Second,
		observe: func(context.Context, providercontainment.Launch) (providercontainment.SupervisorObservation, error) {
			return providercontainment.SupervisorObservation{State: "active/running", InvocationID: invocation.Load().(string)}, nil
		}}
	ctx := context.Background()
	if _, err := manager.Attach(ctx, pkg); err == nil {
		t.Fatal("unprepared execution attached")
	}
	if calls.Load() != 0 {
		t.Fatal("missing receipt contacted API")
	}
	if err := manager.Remember(ctx, pkg, record); err != nil {
		t.Fatal(err)
	}
	if err := manager.Remember(ctx, pkg, record); err != nil {
		t.Fatalf("identical receipt: %v", err)
	}
	// A new manager reads the durable receipt after a worker restart.
	recovered := ContainedT3{Supervisor: manager.Supervisor, Timeout: time.Second, observe: manager.observe}
	attached, err := recovered.Attach(ctx, pkg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := attached.ListThreads(ctx); err != nil {
		t.Fatal(err)
	}
	// Existing clients also fence their next operation, not just initial Attach.
	invocation.Store("invocation-b")
	before := calls.Load()
	if _, err := attached.ListThreads(ctx); err == nil {
		t.Fatal("changed invocation used")
	}
	if calls.Load() != before {
		t.Fatal("changed invocation contacted API")
	}
	if _, err := recovered.Attach(ctx, pkg); err == nil {
		t.Fatal("changed invocation attached")
	}
	invocation.Store("invocation-a")
	changed := pkg
	changed.Identity.AssignmentEpoch++
	if _, err := recovered.Attach(ctx, changed); err == nil {
		t.Fatal("wrong assignment epoch attached")
	}
	environment.Store("environment-b")
	if _, err := recovered.Attach(ctx, pkg); err == nil {
		t.Fatal("foreign environment attached")
	}
	environment.Store("environment-a")
	other := record
	other.EnvironmentID = "environment-b"
	environment.Store("environment-b")
	if err := manager.Remember(ctx, pkg, other); err == nil {
		t.Fatal("foreign receipt overwritten")
	}
	environment.Store("environment-a")
	for _, state := range []string{"recovery-required", "active/exited", "inactive/dead", "failed/failed", "stopped"} {
		refused := manager
		refused.observe = func(context.Context, providercontainment.Launch) (providercontainment.SupervisorObservation, error) {
			return providercontainment.SupervisorObservation{State: state, InvocationID: "invocation-a", Stopped: state == "stopped"}, nil
		}
		if _, err := refused.Attach(ctx, pkg); err == nil {
			t.Fatalf("state %s attached", state)
		}
	}
	// Malformed storage is preserved as recovery-required; never recreated.
	path, err := manager.recordPath(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Attach(ctx, pkg); err == nil {
		t.Fatal("incomplete receipt accepted")
	}
}

func TestAttachmentTransportDropsResponseOnInvocationLoss(t *testing.T) {
	checks := 0
	transport := attachmentTransport{
		check: func(context.Context) error {
			checks++
			if checks == 2 {
				return fmt.Errorf("invocation lost")
			}
			return nil
		},
		next: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
		}),
	}
	req, err := http.NewRequestWithContext(context.Background(), "POST", "http://contained-t3/api/orchestration/dispatch", nil)
	if err != nil {
		t.Fatal(err)
	}
	if response, err := transport.RoundTrip(req); err == nil || response != nil {
		t.Fatal("uncertain response accepted")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
