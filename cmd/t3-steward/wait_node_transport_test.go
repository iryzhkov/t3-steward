package main

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// Only a host that is not the coordinator reads node waits over a transport.
// The coordinator's own store is the authority, and a second reader of the same
// rows on the same machine would be one more way for a wake to be sent twice.
func TestOnlyANonCoordinatorHostReadsNodeWaitsOverTheTransport(t *testing.T) {
	client := config.V2CoordinatorClient{
		CoordinatorID: "normandy-coordinator", Address: "test.invalid",
		Connection: "ssh", Credential: "secretref:f03-admin/test",
	}
	for _, tc := range []struct {
		name   string
		mode   string
		client config.V2CoordinatorClient
		want   bool
	}{
		{"a worker that administers a coordinator", "worker", client, true},
		{"the coordinator itself", "coordinator", client, false},
		{"a host with no coordinator at all", "worker", config.V2CoordinatorClient{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.BacklogV2.Mode = tc.mode
			cfg.BacklogV2.CoordinatorClient = tc.client
			runner := wait.New(nil, nil, nil)
			configureNodeWaitTransport(runner, cfg)
			if got := runner.NodeStore != nil; got != tc.want {
				t.Fatalf("node wait transport configured = %t, want %t", got, tc.want)
			}
		})
	}
}
