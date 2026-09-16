package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
)

// reportTransportError prints the versioned error envelope when the command was
// asked for JSON. It always returns the error unchanged, so the process exit
// code still carries the class and the human line still reaches stderr.
func reportTransportError(args []string, err error) error {
	if err == nil {
		return nil
	}
	envelope, classified := backlogadmin.NewTransportErrorEnvelope(err)
	if !classified || !requestsJSON(args) {
		return err
	}
	_ = json.NewEncoder(os.Stdout).Encode(envelope)
	return err
}

func requestsJSON(args []string) bool {
	for _, argument := range args {
		if argument == "--json" {
			return true
		}
	}
	return false
}

// coordinatorTransportHelp is appended to the help of every command family that
// talks to the coordinator, so that one answer to "where does this go, what can
// fail and what do I run next" is given in exactly one wording.
//
// It deliberately never shows "ssh <coordinator> t3-steward ...": teaching that
// would undo the authority boundary the remote carrier exists to draw.
const coordinatorTransportHelp = `
Talking to the coordinator

  These commands reach the coordinator through one transport. On the coordinator
  host that is its owner-only Unix socket. On any other host it is an SSH session
  to the coordinator's restricted coordinator-exchange command, selected by
  configuring a coordinator client. Run "t3-steward coordinator identity" to see
  which one answered before you submit anything.

  Read-only commands (status, list, show, graph, diagnose, task show, events,
  explain, artifacts, artifact show/get, commands, command show, schedules list,
  show and history) change nothing. Every other command below is mutating.

  Exit codes:
    0  the coordinator answered
    3  client configuration: this host cannot form a request
    4  authentication: the coordinator refused the principal or the signature
    5  unavailable: no coordinator answered
    6  timeout: the request deadline expired with no answer
    7  protocol: version or limit mismatch, or a malformed frame
    8  rejected: the coordinator answered and refused the request
    1  anything else

  --json is available on every command that prints a result. A failure that
  reached the transport also prints {"version":"backlog.admin/v1","kind":"error",
  "class":"...","operation":"...","message":"..."} on standard output.

  Configuration on a host that is not the coordinator, in either place, with the
  configuration file winning when both exist:
    backlog_v2.coordinator_client: coordinator_id, address, connection: ssh,
      remote_command, credential, request_timeout, message_limits
    ~/.config/t3-steward/coordinator-client.json, written by UpKeeper, mode 0600
  The credential is a reference of the form secretref:f03-admin/<client>. Its
  value is resolved at use and never stored in configuration. Worker references
  (secretref:f02-protocol/<host>) are refused here, and admin references are
  refused for workers.

  Idempotency: pass --idempotency-key on submission, --request-id on a schedule
  definition or a graph amendment, and --command-id on a revision-fenced control.
  Repeating a request with the same key returns the first answer rather than
  performing the work twice, including when the first answer was lost in transit.

  Common failures. Permanent until something changes: a missing or invalid client
  block (3), a credential that does not resolve or no longer matches (4), a
  request the coordinator refused on its merits such as a stale revision fence
  (8). Usually temporary: the coordinator is not running or its host is briefly
  unreachable (5), and a deadline that expired under load (6).

  Recovery:
    t3-steward coordinator identity --json   Prove which coordinator answers.
    Re-run the command with the same --idempotency-key, --request-id or
    --command-id. That is safe: it returns the first answer if one was produced.
`

// coordinatorTransportSummary is the short form, for help that must stay small
// enough to enter agent context.
const coordinatorTransportSummary = `
Talking to the coordinator

  On the coordinator host these commands use its owner-only socket; on any other
  host they use an SSH session to its restricted coordinator-exchange command,
  selected by a coordinator client in backlog_v2.coordinator_client or in the
  UpKeeper-owned ~/.config/t3-steward/coordinator-client.json, whose credential
  is a secretref:f03-admin/<client> reference. Run
  "t3-steward coordinator identity" to see which coordinator answers.

  Exit codes beyond the generic 1: 3 client configuration, 4 authentication,
  5 unavailable, 6 timeout, 7 protocol, 8 refused by the coordinator. 3, 4 and 8
  are permanent until something changes; 5 and 6 are usually temporary. Re-running
  with the same --idempotency-key is always safe.
  Full explanation: t3-steward backlog help
`

// coordinatorTransport is the one place a client to the coordinator is built.
// Every command family that talks to the coordinator goes through it, so the
// carrier is chosen once and reported the same way everywhere.
type coordinatorTransport struct {
	client    backlogadmin.CoordinatorAdminTransport
	principal backlogadmin.Principal
}

// adminCredentials resolves an admin credential reference. Tests replace it;
// nothing else does.
var adminCredentials backlogadmin.AdminCredentialResolver = backlogadmin.AdminResolver{}

// missingCoordinatorClient names the configuration block to add. It never
// suggests opening a shell on the coordinator host: that is the authority
// story this transport exists to remove.
func missingCoordinatorClient(path string) error {
	return &backlogadmin.TransportError{
		Class: backlogadmin.ClassClientConfiguration,
		Err: fmt.Errorf(
			"this host runs no coordinator (no admin socket at %s) and has no coordinator client. "+
				"Configure one in either place: a backlog_v2.coordinator_client block in the configuration "+
				"file, which always wins, or the UpKeeper-owned ~/%s (mode 0600, schema_version 1). "+
				"Both name the coordinator id, its ssh destination and a secretref:f03-admin/<client> credential",
			path, config.CoordinatorClientBootstrapPath),
	}
}

// newCoordinatorTransport selects the carrier. A declared coordinator_client
// block selects the remote carrier; a coordinator-local client keeps the
// owner-only socket.
func newCoordinatorTransport(cfg config.Config) (coordinatorTransport, error) {
	// No override: every command family other than supervision reaches the
	// coordinator as this host's own admin client, which is what keeps a
	// supervisor credential out of operations it must never sign.
	return newCoordinatorTransportAs(cfg, supervisorIdentity{})
}

// newCoordinatorTransportAs selects the carrier, optionally under a supervisor
// client identity.
//
// The override reaches only the remote carrier's credential. Everything else
// about the client -- the coordinator it names, its address and its restricted
// remote command -- is this host's configuration and is unchanged, because the
// override answers "who is calling", not "which coordinator".
//
// On the coordinator's own owner-only socket the override is inert, and
// deliberately so: that carrier authenticates by peer UID and already grants
// the full local-admin role, which is strictly stronger than any supervisor
// credential. Presenting one there would narrow nothing and prove nothing.
func newCoordinatorTransportAs(cfg config.Config, identity supervisorIdentity) (coordinatorTransport, error) {
	if err := identity.validate(); err != nil {
		return coordinatorTransport{}, err
	}
	if client := cfg.BacklogV2.CoordinatorClient; client.Configured() {
		reference := client.Credential
		if identity.declared() {
			reference = identity.CredentialReference
		}
		credentials, err := adminCredentials.ResolveAdmin(reference)
		if err != nil {
			return coordinatorTransport{}, &backlogadmin.TransportError{
				Class: backlogadmin.ClassClientConfiguration, Coordinator: client.CoordinatorID, Err: err,
			}
		}
		if err := identity.checkResolved(credentials); err != nil {
			return coordinatorTransport{}, err
		}
		remote, err := backlogadmin.NewSSHClient(backlogadmin.SSHClientConfig{
			CoordinatorID:      client.CoordinatorID,
			Address:            client.Address,
			RemoteCommand:      client.RemoteCommand,
			Credentials:        credentials,
			RequestTimeout:     client.RequestTimeout.D(),
			MaxResponseBytes:   client.MessageLimits.MaxBytes,
			MaxArtifactBytes:   client.MessageLimits.MaxArtifactBytes,
			MaxSubmissionBytes: client.MessageLimits.MaxBytes,
		})
		if err != nil {
			return coordinatorTransport{}, err
		}
		return coordinatorTransport{
			client: remote,
			principal: backlogadmin.Principal{
				ID:    credentials.ClientPrincipal,
				Roles: []string{backlogadmin.RemoteAdminRole},
			},
		}, nil
	}
	socketPath, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		return coordinatorTransport{}, &backlogadmin.TransportError{
			Class: backlogadmin.ClassClientConfiguration, Err: err,
		}
	}
	// A host that is neither a coordinator nor a configured client is told
	// which block is missing, rather than being left with a bare dial failure
	// that reads like a broken installation.
	if cfg.BacklogV2.Mode != "coordinator" {
		if _, err := os.Stat(socketPath); errors.Is(err, os.ErrNotExist) {
			return coordinatorTransport{}, missingCoordinatorClient(socketPath)
		}
	}
	return coordinatorTransport{
		client: backlogadmin.LocalClient{
			Path:               socketPath,
			CoordinatorID:      cfg.BacklogV2.Coordinator.ID,
			MaxResponseBytes:   cfg.BacklogV2.MessageLimits.MaxBytes,
			MaxArtifactBytes:   cfg.BacklogV2.MessageLimits.MaxArtifactBytes,
			MaxSubmissionBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
			RequestTimeout:     cfg.BacklogV2.Transport.RequestTimeout.D(),
		},
		principal: backlogadmin.Principal{
			ID:    fmt.Sprintf("local:%d", os.Getuid()),
			Roles: []string{backlogadmin.LocalAdminRole},
		},
	}, nil
}
