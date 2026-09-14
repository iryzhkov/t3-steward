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

// coordinatorTransport is the one place a client to the coordinator is built.
// Every command family that talks to the coordinator goes through it, so the
// carrier is chosen once and reported the same way everywhere.
type coordinatorTransport struct {
	client    backlogadmin.CoordinatorAdminTransport
	principal backlogadmin.Principal
}

// adminCredentials resolves an admin credential reference. Tests replace it;
// nothing else does.
var adminCredentials backlogadmin.AdminCredentialResolver = backlogadmin.EnvironmentAdminCredentialResolver{}

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
	if client := cfg.BacklogV2.CoordinatorClient; client.Configured() {
		credentials, err := adminCredentials.ResolveAdmin(client.Credential)
		if err != nil {
			return coordinatorTransport{}, &backlogadmin.TransportError{
				Class: backlogadmin.ClassClientConfiguration, Coordinator: client.CoordinatorID, Err: err,
			}
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
