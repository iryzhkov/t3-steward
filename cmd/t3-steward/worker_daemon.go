package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

func cmdWorker(g globalFlags, args []string) error {
	if len(args) > 0 && args[0] == "enroll" {
		return cmdWorkerEnroll(g, args[1:])
	}
	if len(args) > 0 && args[0] == "list" {
		return cmdBacklog(g, append([]string{"workers"}, args[1:]...))
	}
	if len(args) != 1 {
		return errors.New("worker requires inspect-bootstrap, serve, or bridge")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	bootstrap, digest, err := workerruntime.LoadWorkerBootstrap(home)
	if err != nil {
		return err
	}
	credentials := workerruntime.ProtocolResolver{Home: home}
	if args[0] != "bridge" {
		if _, err = credentials.ResolveProtocol(context.Background(), bootstrap.CredentialRef); err != nil {
			return err
		}
	}
	root := filepath.Join(home, ".local/state/t3-steward/worker")
	socket := filepath.Join(root, "worker.sock")
	switch args[0] {
	case "inspect-bootstrap":
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"workerId": bootstrap.WorkerID, "coordinatorId": bootstrap.CoordinatorID, "bootstrapDigest": digest, "credentialRef": bootstrap.CredentialRef, "state": "configured", "enrolled": false})
	case "bridge":
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		dialer := net.Dialer{Timeout: 10 * time.Second}
		conn, err := dialer.DialContext(ctx, "unix", socket)
		if err != nil {
			return err
		}
		return workerruntime.BridgeWorkerStream(ctx, os.Stdin, os.Stdout, conn, (8<<20)+workerproto.StreamArtifactLimit, 2*time.Minute)
	case "serve":
	default:
		return errors.New("worker requires inspect-bootstrap, serve, or bridge")
	}
	cfg, err := config.LoadFile(g.configPath)
	if err != nil {
		return err
	}
	logger := newLogger(cfg.LogLevel)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err = os.MkdirAll(root, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.New("worker state directory must be private 0700")
	}
	ownership, err := workerruntime.LockWorkerSocket(socket)
	if err != nil {
		return err
	}
	defer ownership.Close()
	client, dataDir, err := connect(cfg, logger)
	if err != nil {
		return err
	}
	control := t3control.New(client, logger, cfg.Policy.DryRun)
	host := &workerruntime.CatalogHost{Home: home, Bootstrap: bootstrap, Options: workerruntime.WorkerServiceOptions{
		RuntimeIdentity:     &domain.WorkerRuntimeIdentity{Release: version, Commit: commit, BootstrapDigest: digest},
		ProtocolCredentials: credentials, ProjectCredentials: workerruntime.EnvironmentCredentialChecker{},
		ObserveInventory: observeHostInventory(control, dataDir),
		T3:               control, DryRun: cfg.Policy.DryRun, Logger: logger,
	}}
	if err = host.Load(ctx); err != nil {
		return err
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("worker socket: %w", err)
	}
	defer listener.Close()
	if err = os.Chmod(socket, 0600); err != nil {
		return err
	}
	reconcileDone := make(chan struct{})
	go func() {
		defer close(reconcileDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := host.Reconcile(ctx); err != nil && ctx.Err() == nil {
					logger.Warn("worker reconciliation", "error", err)
				}
			}
		}
	}()
	defer func() { cancel(); <-reconcileDone }()
	logger.Info("persistent worker listening", "worker", bootstrap.WorkerID, "bootstrap_digest", digest)
	err = workerruntime.ServeWorkerListener(ctx, listener, (8<<20)+workerproto.StreamArtifactLimit, 2*time.Minute, 4, host.HandleFrame)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
