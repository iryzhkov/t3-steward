package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
)

var coordinatorExchangeUsage = `t3-steward coordinator-exchange --config PATH [operation]

The restricted SSH endpoint of the coordinator. It is invoked by a forced
command in authorized_keys, never by a person and never by an agent.

The flag comes before the optional operation word, because global flags are
parsed after the command word.

With no operation word one key serves a client for every operation, and the
operation is taken from the signed request frame. With an operation word the key
serves that one operation only, which is the tighter arrangement when an
operator wants key-level narrowing; the client then needs one key per operation
it uses.

Operations: ` + coordinatorExchangeOperations + `

It reads one signed request frame on standard input and writes one signed
response frame on standard output. SSH_ORIGINAL_COMMAND is never read and never
forwarded. The coordinator identity, the accepted admin clients and their
credential references come only from the --config file the operator pinned in
the forced command.

Exit codes: 0 answered, 1 refused.
`

var coordinatorExchangeOperations = strings.Join(backlogadmin.Operations(), ", ")

// cmdCoordinatorExchange is the fixed SSH-command boundary of the coordinator,
// built as a sibling of worker-exchange. Identity comes only from validated
// local configuration, and no part of the SSH invocation reaches a shell.
//
// The operation word is optional. Omitted, the operation is taken from the
// signed frame, which lets one key serve a client that needs more than one
// operation; a client cannot vary its ssh destination per operation, so a
// pinned word would otherwise limit it to one. Given, it pins the key to that
// operation and the frame must agree.
func cmdCoordinatorExchange(g globalFlags, operation string) error {
	if operation != "" && !backlogadmin.ValidOperation(operation) {
		return fmt.Errorf("coordinator-exchange operation must be one of %s", coordinatorExchangeOperations)
	}
	if g.dryRun || g.noDryRun || g.logLevel != "" {
		return errors.New("coordinator-exchange permits only the local --config override")
	}
	if !g.configExplicit {
		return errors.New("coordinator-exchange requires an explicit operator-controlled --config path")
	}
	cfg, err := config.LoadFile(g.configPath)
	if err != nil {
		return err
	}
	if cfg.BacklogV2.Mode != "coordinator" {
		return errors.New("coordinator-exchange requires backlog_v2.mode=coordinator")
	}
	if len(cfg.BacklogV2.Coordinator.AdminClients) == 0 {
		return errors.New("coordinator-exchange requires at least one backlog_v2.coordinator.admin_clients entry")
	}
	clients := make(map[string]backlogadmin.AdminCredentials, len(cfg.BacklogV2.Coordinator.AdminClients))
	supervisors := make(map[string]bool)
	approvers := make(map[string]bool)
	for principal, client := range cfg.BacklogV2.Coordinator.AdminClients {
		credentials, err := adminCredentials.ResolveAdmin(client.Credential)
		if err != nil {
			return err
		}
		clients[principal] = credentials
		if client.Supervisor {
			supervisors[principal] = true
		}
		if client.Approver {
			approvers[principal] = true
		}
	}
	socketPath, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		return err
	}
	replay, err := backlogadmin.OpenRemoteReplayStore(filepath.Dir(socketPath), cfg.BacklogV2.Coordinator.ID)
	if err != nil {
		return err
	}
	server, err := backlogadmin.NewRemoteServer(backlogadmin.RemoteServerConfig{
		CoordinatorID: cfg.BacklogV2.Coordinator.ID,
		Clients:       clients,
		Supervisors:   supervisors,
		Approvers:     approvers,
		Replay:        replay,
		Relay: backlogadmin.LocalClient{
			Path:               socketPath,
			CoordinatorID:      cfg.BacklogV2.Coordinator.ID,
			MaxResponseBytes:   cfg.BacklogV2.MessageLimits.MaxBytes,
			MaxArtifactBytes:   cfg.BacklogV2.MessageLimits.MaxArtifactBytes,
			MaxSubmissionBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
			RequestTimeout:     cfg.BacklogV2.Transport.RequestTimeout.D(),
		},
		MaxRequestBytes:    cfg.BacklogV2.MessageLimits.MaxBytes,
		MaxArtifactBytes:   cfg.BacklogV2.MessageLimits.MaxArtifactBytes,
		MaxSubmissionBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
	})
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return server.Serve(ctx, operation, os.Stdin, os.Stdout)
}
