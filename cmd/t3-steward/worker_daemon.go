package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

const workerUsage = `Usage: t3-steward worker <command> [args]

Worker daemon (reads the worker bootstrap under $HOME):
  serve                Run the worker: enroll with the coordinator and execute
                       dispatched attempts. This is the service's ExecStart.
  bridge               Relay stdin/stdout to the running worker's socket; the
                       coordinator's SSH exchange uses it.
  inspect-bootstrap    Print the worker's identity and bootstrap digest (JSON).
  inspect-journal      Print the attempt journal summary: whether restarting the
                       worker would interrupt a dispatched execution (JSON).

On the coordinator host:
  enroll <worker>|--all [--current-catalog] --reason TEXT [--json]
                       Accept a worker's catalog; "worker enroll --help" for detail.
  list [--json]        Alias of "backlog workers".

Operator diagnostics and provider containment:
  inspect-directory --registration FILE [--expected FILE]
                       Read-only check of a directory resource on this host.
  contained-exec --spec FILE
                       Run one containment launch specification in the foreground.
  contained-start|contained-show|contained-stop --spec FILE --state-dir DIR --execution ID
                       Supervise a contained execution across worker restarts.
  contained-t3         Run the contained T3 server (internal; invoked by the
                       containment launcher).
  contained-child      Run the contained child process (internal).

help, --help, -h and no arguments print this text.
`

// workerVerbList names every verb cmdWorker dispatches, for the refusal of an
// unknown one. Keep it in step with workerUsage.
var workerVerbList = []string{
	"serve", "bridge", "inspect-bootstrap", "inspect-journal", "enroll", "list",
	"inspect-directory", "contained-exec", "contained-start", "contained-show",
	"contained-stop", "contained-t3", "contained-child",
}

// isWorkerDaemonVerb reports whether the verb needs the worker bootstrap.
func isWorkerDaemonVerb(verb string) bool {
	switch verb {
	case "serve", "bridge", "inspect-bootstrap", "inspect-journal":
		return true
	}
	return false
}

func cmdWorker(g globalFlags, args []string) error {
	if answered, err := admitFamilyHelp(os.Stdout, []string{"worker"}, args); answered || err != nil {
		return err
	}
	if len(args) > 0 {
		switch args[0] {
		case "contained-t3":
			return cmdContainedT3(args[1:])
		case "contained-start":
			return cmdContainedSupervisor("start", args[1:])
		case "contained-show":
			return cmdContainedSupervisor("show", args[1:])
		case "contained-stop":
			return cmdContainedSupervisor("stop", args[1:])
		}
	}
	if len(args) > 0 && args[0] == "contained-exec" {
		return cmdContainedExec(args[1:])
	}
	if len(args) > 0 && args[0] == "contained-child" {
		return cmdContainedChild(args[1:])
	}
	if len(args) > 0 && args[0] == "inspect-directory" {
		return cmdInspectDirectory(args[1:])
	}
	if len(args) > 0 && args[0] == "enroll" {
		return cmdWorkerEnroll(g, args[1:])
	}
	if len(args) > 0 && args[0] == "list" {
		return cmdBacklog(g, append([]string{"workers"}, args[1:]...))
	}
	// The daemon verbs read the bootstrap first, so an unknown verb is refused
	// here, before a missing bootstrap can hide the real mistake.
	if len(args) != 1 || !isWorkerDaemonVerb(args[0]) {
		return fmt.Errorf("unknown worker command %q; the commands are %s (try worker --help)", strings.Join(args, " "), strings.Join(workerVerbList, ", "))
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
	case "inspect-journal":
		// Read-only: an updater uses this to see whether restarting the worker
		// would interrupt a dispatched execution.
		summary, err := workerruntime.InspectWorkerJournal(home)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(summary)
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
		return fmt.Errorf("unknown worker command %q; the commands are %s (try worker --help)", args[0], strings.Join(workerVerbList, ", "))
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
	control.SendThreadEnvironment = cfg.T3.SendThreadEnvironment
	// The watchdog on this host leaves the worker's threads alone; the worker
	// pauses and resumes them itself from the same bucket state, read from
	// the watchdog's state database. Without that database (no watchdog on
	// this host) there are no local pauses, which is today's behaviour.
	quota := hostQuotaGuard(cfg, logger, control)
	if closer, ok := quota.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	usage := hostUsageStore(cfg, logger)
	if usage != nil {
		defer usage.Close()
	}
	host := &workerruntime.CatalogHost{Home: home, Bootstrap: bootstrap,
		Options: persistentWorkerOptions(ctx, cfg, logger, control, dataDir, digest, credentials, quota, usage)}
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
	defer func() {
		cancel()
		<-reconcileDone
		// Cancelling ctx cancels every running collection, which kills its
		// verification scope. Wait for them, so that none is still running
		// when the next daemon collects the same attempt again.
		drainCtx, stopDrain := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopDrain()
		if err := workerruntime.DrainCollections(drainCtx); err != nil {
			logger.Warn("worker shutdown", "error", err)
		}
	}()
	logger.Info("persistent worker listening", "worker", bootstrap.WorkerID, "bootstrap_digest", digest)
	err = workerruntime.ServeWorkerListener(ctx, listener, (8<<20)+workerproto.StreamArtifactLimit, 2*time.Minute, 4, host.HandleFrame)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// persistentWorkerOptions is the service configuration the persistent worker
// serves every exchange with. It is built apart from serve so that a test can
// see what the daemon actually hands its exchange: the usage source was once
// missing here, and the persistent worker every host runs forwarded no usage
// without anything failing (S8).
func persistentWorkerOptions(
	ctx context.Context,
	cfg config.Config,
	logger *slog.Logger,
	control *t3control.Control,
	dataDir, digest string,
	credentials workerruntime.ProtocolCredentialResolver,
	quota workerruntime.QuotaGuard,
	usage *sqlite.Store,
) workerruntime.WorkerServiceOptions {
	return workerruntime.WorkerServiceOptions{
		RuntimeIdentity:     &domain.WorkerRuntimeIdentity{Release: version, Commit: commit, BootstrapDigest: digest},
		Usage:               workerUsageSource(usage, logger),
		ProtocolCredentials: credentials, ProjectCredentials: workerruntime.EnvironmentCredentialChecker{},
		ObserveInventory: observeHostInventory(control, dataDir),
		Quota:            quota,
		// A local quota stop sends the drain notice first and escalates to the
		// stop after the window the watchdog itself gives a stop to take effect.
		PauseEscalation: cfg.Policy.StopVerifyTimeout.D(),
		// A collection outlives the exchange or reconcile tick that starts it,
		// because its verification commands may run for minutes; it ends with
		// the daemon, never with a request.
		Lifetime: ctx,
		T3:       control, DryRun: cfg.Policy.DryRun, Logger: logger,
	}
}

// storeQuotaGuard is a HostQuotaGuard over the watchdog's state database that
// closes the database with the worker.
type storeQuotaGuard struct {
	workerruntime.HostQuotaGuard
	store *sqlite.Store
}

func (g storeQuotaGuard) Close() error { return g.store.Close() }

// hostUsageStore opens the host watchdog's state database, where the provider
// token readings of this host's sessions are recorded, so the persistent worker
// can forward them on its exchanges. Only the one-shot worker-exchange command
// used to; the persistent worker every host runs forwarded nothing, and every
// run's usage read zero attributed samples (S8). Without the database there
// is nothing to forward, which is logged once.
func hostUsageStore(cfg config.Config, logger *slog.Logger) *sqlite.Store {
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		logger.Warn("watchdog state path unavailable; this worker forwards no usage", "err", err)
		return nil
	}
	store, err := sqlite.Open(statePath)
	if err != nil {
		logger.Warn("watchdog state unavailable; this worker forwards no usage", "path", statePath, "err", err)
		return nil
	}
	return store
}

// workerUsageSource keeps a nil store a nil interface, so the exchange sees no
// usage source rather than one that fails every call, and wraps a real one so
// that a usage read that fails (a busy database, an older schema) costs this
// exchange its usage rather than its snapshot.
func workerUsageSource(store *sqlite.Store, logger *slog.Logger) workerruntime.UsageDeliveryStore {
	if store == nil {
		return nil
	}
	return tolerantUsage{store: store, logger: logger}
}

// tolerantUsage reports a failed usage read and forwards nothing that time.
// The readings stay in the watchdog's database and go on the next exchange.
type tolerantUsage struct {
	store  workerruntime.UsageDeliveryStore
	logger *slog.Logger
}

func (u tolerantUsage) WorkerUsageBatch(ctx context.Context, acknowledged []string, limit int) ([]domain.UsageSample, error) {
	samples, err := u.store.WorkerUsageBatch(ctx, acknowledged, limit)
	if err != nil {
		u.logger.Warn("usage readings not forwarded on this exchange", "err", err)
		return nil, nil
	}
	return samples, nil
}

// hostQuotaGuard opens the watchdog's state database on this host for the
// worker's local quota pauses, or returns nil when there is none. threads is
// the worker's T3 control client, which the probe rule asks whether anything
// on the host is running that would produce a reading.
func hostQuotaGuard(cfg config.Config, logger *slog.Logger, threads workerruntime.ThreadLister) workerruntime.QuotaGuard {
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		logger.Warn("quota watchdog state path unavailable; owned attempts are not paused locally", "err", err)
		return nil
	}
	store, err := sqlite.Open(statePath)
	if err != nil {
		logger.Warn("quota watchdog state unavailable; owned attempts are not paused locally", "path", statePath, "err", err)
		return nil
	}
	logger.Info("local quota pauses enabled from the watchdog state", "path", statePath)
	return storeQuotaGuard{HostQuotaGuard: workerruntime.HostQuotaGuard{Config: cfg, Buckets: store, Threads: threads}, store: store}
}
