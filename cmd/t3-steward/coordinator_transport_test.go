package main

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
)

// teachesRemoteShell reports whether a message would teach an agent to run
// "ssh <coordinator> t3-steward ...". No help text, example or error may.
func teachesRemoteShell(message string) bool {
	return strings.Contains(message, "ssh ") && strings.Contains(message, "t3-steward ")
}

func transportTestConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.StatePath = filepath.Join(t.TempDir(), "state.sqlite")
	return cfg
}

type fixedAdminCredentials struct {
	credentials backlogadmin.AdminCredentials
	err         error
}

func (f fixedAdminCredentials) ResolveAdmin(string) (backlogadmin.AdminCredentials, error) {
	return f.credentials, f.err
}

func withAdminCredentials(t *testing.T, resolver backlogadmin.AdminCredentialResolver) {
	t.Helper()
	previous := adminCredentials
	adminCredentials = resolver
	t.Cleanup(func() { adminCredentials = previous })
}

func completeAdminCredentials() backlogadmin.AdminCredentials {
	return backlogadmin.AdminCredentials{
		ClientPrincipal: "admin:omarchy-pc", ClientKeyID: "admin-key-1",
		ClientSecret:         []byte("0123456789abcdef-client"),
		CoordinatorPrincipal: "coordinator:normandy", CoordinatorKeyID: "coordinator-key-1",
		CoordinatorSecret: []byte("0123456789abcdef-coord"),
	}
}

func TestTransportSelectionWithoutACoordinatorOrAClient(t *testing.T) {
	cfg := transportTestConfig(t)
	_, err := newCoordinatorTransport(cfg)
	if backlogadmin.ClassOf(err) != backlogadmin.ClassClientConfiguration {
		t.Fatalf("class = %q (%v)", backlogadmin.ClassOf(err), err)
	}
	if backlogadmin.ExitCodeFor(err) != 3 {
		t.Fatalf("exit code = %d, want 3", backlogadmin.ExitCodeFor(err))
	}
	// The error must name both places to configure, and neither may be a shell.
	for _, want := range []string{"backlog_v2.coordinator_client", config.CoordinatorClientBootstrapPath, "secretref:f03-admin/"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %v, want it to name %q", err, want)
		}
	}
	if teachesRemoteShell(err.Error()) {
		t.Fatalf("error teaches a remote shell: %v", err)
	}
}

func TestTransportSelectionPrefersTheConfiguredClient(t *testing.T) {
	withAdminCredentials(t, fixedAdminCredentials{credentials: completeAdminCredentials()})
	cfg := transportTestConfig(t)
	cfg.BacklogV2.CoordinatorClient = config.V2CoordinatorClient{
		CoordinatorID: "normandy-coordinator", Address: "normandy", Connection: "ssh",
		RemoteCommand: "t3-steward", Credential: "secretref:f03-admin/omarchy-pc",
		RequestTimeout: config.Duration(30 * time.Second),
		MessageLimits:  config.V2MessageLimits{MaxBytes: 1 << 20, MaxFiles: 100, MaxArtifactBytes: 1 << 20},
	}
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	description := transport.client.Describe()
	if description.Carrier != backlogadmin.CarrierRemote || description.CoordinatorID != "normandy-coordinator" {
		t.Fatalf("description = %+v", description)
	}
	if strings.Contains(description.Endpoint, "secretref") {
		t.Fatalf("endpoint leaks a credential reference: %q", description.Endpoint)
	}
	if len(transport.principal.Roles) != 1 || transport.principal.Roles[0] != backlogadmin.RemoteAdminRole {
		t.Fatalf("principal = %+v", transport.principal)
	}
}

func TestTransportSelectionUsesTheSocketOnACoordinator(t *testing.T) {
	cfg := transportTestConfig(t)
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy-coordinator"
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	description := transport.client.Describe()
	if description.Carrier != backlogadmin.CarrierLocal ||
		!strings.HasSuffix(description.Endpoint, ".admin.sock") {
		t.Fatalf("description = %+v", description)
	}
	if len(transport.principal.Roles) != 1 || transport.principal.Roles[0] != backlogadmin.LocalAdminRole {
		t.Fatalf("principal = %+v", transport.principal)
	}
}

func TestTransportSelectionReportsAnUnresolvableCredential(t *testing.T) {
	withAdminCredentials(t, fixedAdminCredentials{err: errors.New("reference is unavailable")})
	cfg := transportTestConfig(t)
	cfg.BacklogV2.CoordinatorClient = config.V2CoordinatorClient{
		CoordinatorID: "normandy-coordinator", Address: "normandy", Connection: "ssh",
		RemoteCommand: "t3-steward", Credential: "secretref:f03-admin/omarchy-pc",
		RequestTimeout: config.Duration(30 * time.Second),
		MessageLimits:  config.V2MessageLimits{MaxBytes: 1 << 20, MaxFiles: 100, MaxArtifactBytes: 1 << 20},
	}
	_, err := newCoordinatorTransport(cfg)
	if backlogadmin.ClassOf(err) != backlogadmin.ClassClientConfiguration {
		t.Fatalf("class = %q (%v)", backlogadmin.ClassOf(err), err)
	}
}

func TestCoordinatorExchangeAcceptsOnlyOneFixedOperation(t *testing.T) {
	if err := run([]string{"coordinator-exchange"}); err == nil ||
		!strings.Contains(err.Error(), "exactly one fixed operation") {
		t.Fatalf("missing operation error = %v", err)
	}
	if err := run([]string{"coordinator-exchange", "query", "extra"}); err == nil ||
		!strings.Contains(err.Error(), "exactly one fixed operation") {
		t.Fatalf("extra operation error = %v", err)
	}
	for _, operation := range []string{"status", "query;id", "--config"} {
		if err := cmdCoordinatorExchange(globalFlags{}, operation); err == nil ||
			!strings.Contains(err.Error(), "operation must be one of") {
			t.Fatalf("operation %q error = %v", operation, err)
		}
	}
}

func TestCoordinatorExchangeRequiresExplicitLocalConfig(t *testing.T) {
	if err := cmdCoordinatorExchange(globalFlags{}, "query"); err == nil ||
		!strings.Contains(err.Error(), "explicit operator-controlled --config") {
		t.Fatalf("implicit config error = %v", err)
	}
}

func TestCoordinatorExchangeRejectsAuthorityFlags(t *testing.T) {
	for name, flags := range map[string]globalFlags{
		"dry run":    {dryRun: true},
		"no dry run": {noDryRun: true},
		"log level":  {logLevel: "debug"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := cmdCoordinatorExchange(flags, "query"); err == nil ||
				!strings.Contains(err.Error(), "only the local --config override") {
				t.Fatalf("authority flag error = %v", err)
			}
		})
	}
}

func TestCoordinatorExchangeIgnoresSSHOriginalCommand(t *testing.T) {
	// The variable is set to something that would be catastrophic if it were
	// ever read and forwarded. The command must refuse on its own grounds.
	t.Setenv("SSH_ORIGINAL_COMMAND", "rm -rf / ; t3-steward backlog cancel run/task")
	err := cmdCoordinatorExchange(globalFlags{}, "query")
	if err == nil || !strings.Contains(err.Error(), "explicit operator-controlled --config") {
		t.Fatalf("error = %v", err)
	}
}

func TestRemoteAdminRoleIsAuthorizedExceptForWorkerEnrollment(t *testing.T) {
	authorizer := localAdminAuthorizer{}
	remote := backlogadmin.Principal{ID: "remote:admin:omarchy-pc", Roles: []string{backlogadmin.RemoteAdminRole}}
	local := backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}}
	ctx := context.Background()
	for _, kind := range []backlogadmin.QueryKind{
		backlogadmin.QueryStatus, "node-wait", "graph-amendment", backlogadmin.QueryArtifacts,
	} {
		if err := authorizer.Authorize(ctx, remote, backlogadmin.Action{Kind: kind}); err != nil {
			t.Fatalf("remote-admin refused %q: %v", kind, err)
		}
	}
	err := authorizer.Authorize(ctx, remote, backlogadmin.Action{Kind: "worker-enrollment"})
	if err == nil || !strings.Contains(err.Error(), "may not enroll workers") {
		t.Fatalf("remote worker enrollment error = %v", err)
	}
	if teachesRemoteShell(err.Error()) {
		t.Fatalf("error teaches a remote shell: %v", err)
	}
	// The local peer keeps every operation, including enrollment.
	if err := authorizer.Authorize(ctx, local, backlogadmin.Action{Kind: "worker-enrollment"}); err != nil {
		t.Fatalf("local-admin refused enrollment: %v", err)
	}
	// A principal with neither role is refused outright.
	worker := backlogadmin.Principal{ID: "ssh:omarchy-pc", Roles: []string{"worker"}}
	if err := authorizer.Authorize(ctx, worker, backlogadmin.Action{Kind: backlogadmin.QueryStatus}); err == nil {
		t.Fatal("a worker principal was authorized for an admin operation")
	}
}

func TestHelpNeverTeachesARemoteShell(t *testing.T) {
	for name, text := range map[string]string{
		"root":                 usage,
		"backlog":              backlogUsage,
		"schedules":            schedulesUsage,
		"coordinator":          coordinatorUsage,
		"coordinator-exchange": coordinatorExchangeUsage,
	} {
		t.Run(name, func(t *testing.T) {
			if teachesRemoteShell(text) {
				t.Fatal("help teaches a remote shell")
			}
		})
	}
}
