package config

import (
	"strings"
	"testing"
	"time"
)

// clientHostConfig is the usual shape of a host that administers a remote
// coordinator: no coordinator, no worker, only a client block.
func clientHostConfig() Config {
	cfg := Default()
	cfg.BacklogV2.CoordinatorClient = V2CoordinatorClient{
		CoordinatorID: "normandy-coordinator",
		Address:       "normandy",
		Credential:    "secretref:f03-admin/omarchy-pc",
	}
	return cfg
}

func TestCoordinatorClientDefaultsAndAcceptance(t *testing.T) {
	cfg := clientHostConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	client := cfg.BacklogV2.CoordinatorClient
	if client.Connection != "ssh" || client.RemoteCommand != "t3-steward" {
		t.Fatalf("defaults = %+v", client)
	}
	if client.RequestTimeout.D() != 30*time.Second {
		t.Fatalf("request timeout = %s", client.RequestTimeout.D())
	}
	if client.MessageLimits.MaxBytes != 4<<20 || client.MessageLimits.MaxArtifactBytes != 1<<30 ||
		client.MessageLimits.MaxFiles != 1000 {
		t.Fatalf("message limits = %+v", client.MessageLimits)
	}
	// An absent block stays absent and valid: a coordinator-local host needs none.
	bare := Default()
	if err := bare.Validate(); err != nil {
		t.Fatal(err)
	}
	if bare.BacklogV2.CoordinatorClient.Configured() {
		t.Fatal("an undeclared client block reported itself configured")
	}
}

func TestCoordinatorClientRefusals(t *testing.T) {
	tests := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"missing coordinator id": {
			mutate: func(c *Config) { c.BacklogV2.CoordinatorClient.CoordinatorID = "" },
			want:   "coordinator_client.coordinator_id",
		},
		"missing address": {
			mutate: func(c *Config) { c.BacklogV2.CoordinatorClient.Address = "" },
			want:   "coordinator_client.address",
		},
		"unsupported connection": {
			mutate: func(c *Config) { c.BacklogV2.CoordinatorClient.Connection = "http" },
			want:   "coordinator_client.connection must be ssh",
		},
		"negative timeout": {
			mutate: func(c *Config) { c.BacklogV2.CoordinatorClient.RequestTimeout = Duration(-time.Second) },
			want:   "coordinator_client.request_timeout must be positive",
		},
		"non-positive limit": {
			mutate: func(c *Config) { c.BacklogV2.CoordinatorClient.MessageLimits.MaxBytes = -1 },
			want:   "coordinator_client.message_limits",
		},
		"missing credential": {
			mutate: func(c *Config) { c.BacklogV2.CoordinatorClient.Credential = "" },
			want:   "credential reference must be trimmed",
		},
		"worker credential": {
			mutate: func(c *Config) {
				c.BacklogV2.CoordinatorClient.Credential = "secretref:f02-protocol/omarchy-pc"
			},
			want: "is a worker protocol reference",
		},
		"foreign namespace": {
			mutate: func(c *Config) { c.BacklogV2.CoordinatorClient.Credential = "inline-secret" },
			want:   "must start with secretref:f03-admin/",
		},
		"unnamed client": {
			mutate: func(c *Config) { c.BacklogV2.CoordinatorClient.Credential = "secretref:f03-admin/" },
			want:   "must name a client",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := clientHostConfig()
			test.mutate(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want one naming %q", err, test.want)
			}
			// No refusal may teach an agent to open a remote shell instead.
			if err != nil && strings.Contains(err.Error(), "ssh "+cfg.BacklogV2.CoordinatorClient.Address) {
				t.Fatalf("error teaches a remote shell: %v", err)
			}
		})
	}
}

func TestCoordinatorRefusesItsOwnClientBlock(t *testing.T) {
	cfg := validBacklogV2Config(t)
	cfg.BacklogV2.CoordinatorClient = V2CoordinatorClient{
		CoordinatorID: "normandy", Address: "normandy", Credential: "secretref:f03-admin/normandy",
	}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "administers itself over its own socket") {
		t.Fatalf("error = %v", err)
	}
}

func TestAdminAndWorkerCredentialsAreNotInterchangeable(t *testing.T) {
	// A worker may not be given an admin credential.
	cfg := validBacklogV2Config(t)
	worker := cfg.BacklogV2.Workers["normandy"]
	worker.Credential = "secretref:f03-admin/normandy"
	cfg.BacklogV2.Workers["normandy"] = worker
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "is a coordinator admin reference") {
		t.Fatalf("worker with admin credential: %v", err)
	}
	// An admin client may not be given a worker credential.
	coordinator := validBacklogV2Config(t)
	coordinator.BacklogV2.Coordinator.AdminClients = map[string]V2AdminClient{
		"omarchy-pc": {Credential: "secretref:f02-protocol/omarchy-pc"},
	}
	err = coordinator.Validate()
	if err == nil || !strings.Contains(err.Error(), "is a worker protocol reference") {
		t.Fatalf("admin client with worker credential: %v", err)
	}
	// The accepted shape passes.
	accepted := validBacklogV2Config(t)
	accepted.BacklogV2.Coordinator.AdminClients = map[string]V2AdminClient{
		"omarchy-pc": {Credential: "secretref:f03-admin/omarchy-pc"},
	}
	if err := accepted.Validate(); err != nil {
		t.Fatal(err)
	}
}
