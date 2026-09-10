package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

const (
	workerOperationControl         = "control"
	workerOperationArtifactReceive = "artifact-receive"
	workerOperationArtifactSend    = "artifact-send"
)

// cmdWorkerExchange is the fixed SSH-command boundary. Identity and epochs come
// only from validated local configuration; the SSH request may select no worker.
func cmdWorkerExchange(g globalFlags, operation string) error {
	switch operation {
	case workerOperationControl, workerOperationArtifactReceive, workerOperationArtifactSend:
	default:
		return errors.New("worker-exchange operation must be control, artifact-receive, or artifact-send")
	}
	if g.dryRun || g.noDryRun || g.logLevel != "" {
		return errors.New("worker-exchange permits only the local --config override")
	}
	if !g.configExplicit {
		return errors.New("worker-exchange requires an explicit operator-controlled --config path")
	}
	cfg, err := config.LoadFile(g.configPath)
	if err != nil {
		return err
	}
	if cfg.BacklogV2.Mode != "worker" {
		return errors.New("worker-exchange requires backlog_v2.mode=worker")
	}
	logger := newLogger(cfg.LogLevel)
	client, _, err := connect(cfg, logger)
	if err != nil {
		return err
	}
	local := cfg.BacklogV2.LocalWorker
	service, err := workerruntime.NewWorkerService(context.Background(), workerruntime.WorkerServiceOptions{
		Settings:            cfg.BacklogV2,
		WorkerID:            local.ID,
		WorkerEpoch:         local.Epoch,
		CoordinatorEpoch:    local.CoordinatorEpoch,
		ProtocolCredentials: workerruntime.EnvironmentProtocolCredentialResolver{},
		ProjectCredentials:  workerruntime.EnvironmentCredentialChecker{},
		T3:                  t3control.New(client, logger, cfg.Policy.DryRun),
		DryRun:              cfg.Policy.DryRun,
	})
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	switch operation {
	case workerOperationControl:
		return service.Serve(ctx, os.Stdin, os.Stdout)
	case workerOperationArtifactReceive:
		return service.ServeArtifactReceive(ctx, os.Stdin, os.Stdout)
	case workerOperationArtifactSend:
		return service.ServeArtifactSend(ctx, os.Stdin, os.Stdout)
	default:
		return errors.New("worker-exchange operation must be control, artifact-receive, or artifact-send")
	}
}
