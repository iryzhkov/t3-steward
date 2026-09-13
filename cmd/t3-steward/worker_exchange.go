package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
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
	// The worker runs as a short-lived SSH command whose stderr reaches the
	// coordinator only when the process fails, so it also appends to a durable
	// log beside its journal for later diagnosis.
	logger := newLogger(cfg.LogLevel)
	_, _, journalRoot := workerruntime.WorkerRoots(cfg.BacklogV2, cfg.BacklogV2.LocalWorker.ID)
	if err := os.MkdirAll(journalRoot, 0o700); err == nil {
		if logFile, err := os.OpenFile(filepath.Join(journalRoot, "worker.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); err == nil {
			defer logFile.Close()
			logger = slog.New(slog.NewTextHandler(io.MultiWriter(os.Stderr, logFile), &slog.HandlerOptions{Level: slog.LevelInfo}))
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	local := cfg.BacklogV2.LocalWorker
	options := workerruntime.WorkerServiceOptions{
		Settings:            cfg.BacklogV2,
		WorkerID:            local.ID,
		WorkerEpoch:         local.Epoch,
		CoordinatorEpoch:    local.CoordinatorEpoch,
		ProtocolCredentials: workerruntime.ProtocolResolver{},
		ProjectCredentials:  workerruntime.EnvironmentCredentialChecker{},
		DryRun:              cfg.Policy.DryRun,
		Logger:              logger,
	}
	// The request is read before the service exists so the worker can adopt
	// the coordinator epoch of an authenticated envelope. Only the envelope
	// header is consumed here; artifact payloads stay in the buffered stream.
	codec := workerproto.Codec{MaxBytes: cfg.BacklogV2.MessageLimits.MaxBytes}
	envelope, buffered, err := workerruntime.ReadStreamEnvelope(os.Stdin, codec)
	if err != nil {
		return err
	}
	epoch, err := workerruntime.AdoptCoordinatorEpoch(ctx, options, envelope)
	if err != nil {
		return err
	}
	options.CoordinatorEpoch = epoch
	client, _, err := connect(cfg, logger)
	if err != nil {
		return err
	}
	options.T3 = t3control.New(client, logger, cfg.Policy.DryRun)
	service, err := workerruntime.NewWorkerService(ctx, options)
	if err != nil {
		return err
	}
	switch operation {
	case workerOperationControl:
		return service.ServeEnvelope(ctx, envelope, os.Stdout)
	case workerOperationArtifactReceive:
		return service.ServeArtifactReceiveEnvelope(ctx, envelope, buffered, os.Stdout)
	case workerOperationArtifactSend:
		return service.ServeArtifactSendEnvelope(ctx, envelope, buffered, os.Stdout)
	default:
		return errors.New("worker-exchange operation must be control, artifact-receive, or artifact-send")
	}
}
