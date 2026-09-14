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

var coordinatorExchangeUsage = `t3-steward coordinator-exchange <operation> --config PATH

The restricted SSH endpoint of the coordinator. It is invoked by a forced
command in authorized_keys, never by a person and never by an agent.

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
// built as a sibling of worker-exchange. One positional operation word is its
// only request-shaped argument; identity comes only from validated local
// configuration, and no part of the SSH invocation reaches a shell.
func cmdCoordinatorExchange(g globalFlags, operation string) error {
	if !backlogadmin.ValidOperation(operation) {
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
	for principal, client := range cfg.BacklogV2.Coordinator.AdminClients {
		credentials, err := adminCredentials.ResolveAdmin(client.Credential)
		if err != nil {
			return err
		}
		clients[principal] = credentials
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
