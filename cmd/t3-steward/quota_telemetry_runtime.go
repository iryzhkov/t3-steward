package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/quotatelemetry"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// quotaTelemetryStopWait bounds how long Stop waits for an in-flight tick.
// Cancelling the recorder's context interrupts its SQLite statements, so a
// tick normally ends within milliseconds.
const quotaTelemetryStopWait = 5 * time.Second

// quotaTelemetryFirstTickDelay keeps the recorder's first tick out of the
// coordinator's startup. A test that waits for a tick sets it to zero.
var quotaTelemetryFirstTickDelay = 10 * time.Second

// quotaTelemetryRecorder is the phase 0 quota telemetry recorder running
// beside the watchdog in the coordinator process.
type quotaTelemetryRecorder struct {
	recorder *quotatelemetry.Recorder
	cancel   context.CancelFunc
	done     chan struct{}
	logger   *slog.Logger
}

// startQuotaTelemetryRecorder starts the recorder in its own goroutine and
// returns at once. Nothing it meets reaches the caller: a state path that
// cannot hold the store, an unwritable directory, a failed read or a panic is
// logged, counted and recorded as a gap by the recorder itself, and the
// coordinator's scheduling never waits for it.
//
// It reads the coordinator database only through its own query-only
// connection, never through the coordinator's store.
func startQuotaTelemetryRecorder(ctx context.Context, cfg config.Config, logger *slog.Logger) *quotaTelemetryRecorder {
	logger = logger.With("component", "quota-telemetry")
	statePath, err := cfg.ResolveStatePath()
	storePath := ""
	if err == nil {
		storePath, err = quotatelemetry.StorePath(statePath)
	}
	if err != nil {
		logger.Warn("quota telemetry recording is disabled; the coordinator is unaffected", "error", err)
	}
	sources := &quotaTelemetrySources{
		statePath:     statePath,
		artifactsRoot: cfg.BacklogV2.Storage.Artifacts, submissionsRoot: cfg.BacklogV2.Storage.Bundles,
	}
	recorder := &quotatelemetry.Recorder{
		StorePath: storePath, OpenSource: sources.open, OpenArtifact: sources.openArtifact,
		Logger: logger, Interval: quotatelemetry.DefaultInterval, TickTimeout: quotatelemetry.DefaultTickTimeout,
		FirstTickDelay: quotaTelemetryFirstTickDelay,
	}
	recorderCtx, cancel := context.WithCancel(ctx)
	running := &quotaTelemetryRecorder{recorder: recorder, cancel: cancel, done: make(chan struct{}), logger: logger}
	go func() {
		defer close(running.done)
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Warn("quota telemetry recorder stopped after a panic; the coordinator is unaffected", "panic", recovered)
			}
		}()
		recorder.Run(recorderCtx)
	}()
	return running
}

// Stop ends the recorder and waits briefly for its in-flight tick.
func (r *quotaTelemetryRecorder) Stop() {
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(quotaTelemetryStopWait):
		r.logger.Warn("quota telemetry recorder did not stop in time; continuing shutdown")
	}
}

// Stats returns the recorder's counters.
func (r *quotaTelemetryRecorder) Stats() quotatelemetry.Stats {
	return r.recorder.Stats()
}

// quotaTelemetrySources opens the recorder's query-only view of the
// coordinator database and reads retained check reports through it. Both are
// called only from the recorder's goroutine.
type quotaTelemetrySources struct {
	statePath       string
	artifactsRoot   string
	submissionsRoot string
	current         *sqlite.QueryOnlyStore
}

func (s *quotaTelemetrySources) open(context.Context) (quotatelemetry.Source, error) {
	store, err := sqlite.OpenQueryOnly(s.statePath)
	if err != nil {
		return nil, err
	}
	s.current = store
	return store, nil
}

func (s *quotaTelemetrySources) openArtifact(ctx context.Context, artifactID string) (io.ReadCloser, error) {
	if s.current == nil {
		return nil, errors.New("quota telemetry has no coordinator source open")
	}
	store := backlog.CoordinatorArtifactStore{Root: s.artifactsRoot, SubmissionRoot: s.submissionsRoot,
		Catalog: readOnlyArtifactCatalog{store: s.current}}
	_, content, err := store.Open(ctx, artifactID)
	if err != nil {
		return nil, err
	}
	return content, nil
}

// readOnlyArtifactCatalog lets CoordinatorArtifactStore.Open read metadata
// through the query-only connection. Every method that would change custody
// is refused.
type readOnlyArtifactCatalog struct {
	store *sqlite.QueryOnlyStore
}

var errQuotaTelemetryReadOnly = errors.New("the quota telemetry recorder reads artifacts only")

func (c readOnlyArtifactCatalog) CommitArtifactPublication(context.Context, domain.ArtifactPublication) (domain.Artifact, error) {
	return domain.Artifact{}, errQuotaTelemetryReadOnly
}

func (c readOnlyArtifactCatalog) LoadArtifacts(ctx context.Context, ids []string) ([]domain.Artifact, error) {
	return c.store.LoadArtifacts(ctx, ids)
}

func (c readOnlyArtifactCatalog) PruneArtifacts(context.Context, time.Time, []string) ([]domain.Artifact, []domain.ArtifactRetentionSkip, error) {
	return nil, nil, errQuotaTelemetryReadOnly
}

func (c readOnlyArtifactCatalog) ArtifactStoragePathReferenced(context.Context, string) (bool, error) {
	return false, errQuotaTelemetryReadOnly
}
