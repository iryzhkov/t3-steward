package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

func TestCheckTaskWaitIdentityHostKinds(t *testing.T) {
	for _, tc := range []struct {
		name        string
		client      bool
		persistent  bool
		coordinator bool
		want        string
	}{
		{name: "pure client", client: true, want: "ok"},
		{name: "plain host", want: "ok"},
		{name: "worker without identity", client: true, persistent: true, want: "FAIL"},
		{name: "worker without client or identity", persistent: true, want: "FAIL"},
		{name: "coordinator without identity", coordinator: true, want: "FAIL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := writeTaskWaitBootstrap(t, "", "")
			cfg := config.Default()
			if tc.client {
				cfg.BacklogV2.CoordinatorClient.CoordinatorID = "test-coordinator"
				cfg.BacklogV2.CoordinatorClient.Address = "test-host"
				cfg.BacklogV2.CoordinatorClient.Credential = "secretref:f03-admin/client"
			}
			if tc.coordinator {
				cfg.BacklogV2.Coordinator.ID = "test-coordinator"
			}
			if tc.persistent {
				path := filepath.Join(home, ".config/t3-steward/persistent-worker.yaml")
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("backlog_v2: {}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			level, text := checkTaskWaitIdentity(cfg)
			if level != tc.want {
				t.Fatalf("check = %s %q, want %s", level, text, tc.want)
			}
			if tc.want == "ok" && text != "task wakes and ask relays: nothing to deliver on this host (not a worker)" {
				t.Fatalf("client check = %q", text)
			}
			if tc.want == "FAIL" && !strings.Contains(text, errNoTaskWorkerIdentity.Error()) {
				t.Fatalf("missing identity check = %q", text)
			}
			// The diagnostic classification must not change runtime fencing.
			runner := wait.New(nil, nil, nil)
			err := configureTaskWaitTransport(runner, cfg)
			if !errors.Is(err, errNoTaskWorkerIdentity) || runner.DisableTaskWaitRuntime != tc.client {
				t.Fatalf("runtime err=%v disabled=%v, want disabled=%v", err, runner.DisableTaskWaitRuntime, tc.client)
			}
		})
	}
}

func TestCheckTaskWaitIdentityConfiguredWorkers(t *testing.T) {
	t.Run("local worker identity", func(t *testing.T) {
		writeTaskWaitBootstrap(t, "", "")
		cfg := config.Default()
		cfg.BacklogV2.LocalWorker.ID = "worker"
		if level, text := checkTaskWaitIdentity(cfg); level != "ok" || !strings.Contains(text, "delivered here as worker worker") {
			t.Fatalf("check = %s %q", level, text)
		}
	})
	t.Run("bootstrap identity", func(t *testing.T) {
		writeTaskWaitBootstrap(t, "worker", "test-coordinator")
		if level, text := checkTaskWaitIdentity(config.Default()); level != "ok" || !strings.Contains(text, "delivered here as worker worker") {
			t.Fatalf("check = %s %q", level, text)
		}
	})
}
