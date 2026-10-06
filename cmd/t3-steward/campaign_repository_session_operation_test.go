package main

import (
	"context"
	"os/exec"
	"slices"
	"sync"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The repository probe, exact-ref resolution and commit export share one
// session dialer. Over SSH the worker operation is the remote argument, so
// the probe and ref resolution must start the control operation and commit
// export the artifact-send operation, which is where the producing worker
// serves the bundle.
func TestRepositorySessionsStartTheirWorkerOperationOverSSH(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Coordinator.ID = "coordinator"
	worker := cfg.BacklogV2.Workers["normandy"]
	if worker.Address == "" || worker.Connection != "" {
		t.Fatalf("default normandy worker is not an SSH worker: %+v", worker)
	}
	settings := cfg.BacklogV2

	var mu sync.Mutex
	var calls [][]string
	factory := workerproto.CommandFactory(func(ctx context.Context, name string, args ...string) *exec.Cmd {
		mu.Lock()
		calls = append(calls, append([]string{name}, args...))
		mu.Unlock()
		return exec.CommandContext(ctx, "false")
	})
	observer := newCoordinatorRepositoryObserver(settings, &testProtocolCredentialResolver{}, 7, factory)
	refs := newCoordinatorRepositoryRefResolver(settings, &testProtocolCredentialResolver{}, 7, factory, nil)

	for _, tc := range []struct {
		name      string
		operation string
		dial      func(context.Context) (any, func() error, error)
	}{
		{"repository probe", coordinatorWorkerControlOperation, func(ctx context.Context) (any, func() error, error) {
			return observer.dialWorker(ctx, "normandy")
		}},
		{"exact-ref resolution", coordinatorWorkerControlOperation, func(ctx context.Context) (any, func() error, error) {
			return refs.dialWorker(ctx, "normandy")
		}},
		{"commit export", coordinatorWorkerArtifactSendOperation, func(ctx context.Context) (any, func() error, error) {
			return observer.dialWorkerOperation(ctx, "normandy", coordinatorWorkerArtifactSendOperation)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			calls = nil
			mu.Unlock()
			dialed, closer, err := tc.dial(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if closer != nil {
				defer closer()
			}
			client, ok := dialed.(*workerproto.Client)
			if !ok {
				t.Fatalf("dialed %T, want *workerproto.Client", dialed)
			}
			// The fake ssh exits non-zero; the call only has to start it.
			_, _ = client.Catalog(context.Background(), nil)
			mu.Lock()
			defer mu.Unlock()
			if len(calls) == 0 {
				t.Fatal("no ssh command was started")
			}
			for _, call := range calls {
				if !slices.Contains(call, tc.operation) {
					t.Fatalf("ssh command %q does not start operation %q", call, tc.operation)
				}
				if tc.operation != coordinatorWorkerControlOperation && slices.Contains(call, coordinatorWorkerControlOperation) {
					t.Fatalf("ssh command %q starts the control operation", call)
				}
			}
		})
	}
}
