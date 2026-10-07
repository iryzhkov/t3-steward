package quotatelemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Source is the coordinator database as the recorder reads it. The production
// source is a query-only connection; nothing here can write coordinator state.
//
// A row that exists and does not decode is reported with an error that has a
// Malformed() bool method returning true; the recorder skips and counts that
// row rather than failing every tick on it.
type Source interface {
	MaxAuditSequence(context.Context) (int64, error)
	AuditEventsAfter(ctx context.Context, after int64, limit int) ([]domain.AuditEvent, error)
	LoadAssignment(context.Context, string) (domain.Assignment, bool, error)
	// LoadAssignmentEpoch reads the assignment as it was dispatched at one
	// epoch: an assignment offered again keeps its id and moves to a later
	// epoch, possibly on another worker and route.
	LoadAssignmentEpoch(ctx context.Context, id string, epoch int64) (domain.Assignment, bool, error)
	LoadAttempt(context.Context, string) (domain.Attempt, bool, error)
	LoadTask(context.Context, string) (domain.Task, bool, error)
	LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error)
	ListBuckets(context.Context) ([]domain.BucketState, error)
	ListCheckArtifacts(ctx context.Context, taskID, attemptID string) ([]domain.Artifact, error)
	Close() error
}

// Recorder states.
const (
	RecorderStarted = "started"
	RecorderGap     = "gap"
)

// Recorder timing and bounds.
const (
	DefaultInterval    = 30 * time.Second
	DefaultTickTimeout = 10 * time.Second
	// auditBatch bounds the audit rows one tick reads.
	auditBatch = 5000
	// openBatch bounds the open assignments one tick re-reads.
	openBatch  = 200
	pruneEvery = time.Hour
	// logEvery throttles repeated warnings with the same text.
	logEvery = 10 * time.Minute
	// maxErrorBytes caps a stored error text.
	maxErrorBytes = 512
	// maxIdentityBytes bounds an id or key an event is named by; a longer one
	// is skipped and counted, so every record fits in MaxRecordBytes.
	maxIdentityBytes = 256
	// readingFutureSlack is how far ahead of the recorder's clock a reading's
	// observation time may be before it is treated as malformed.
	readingFutureSlack = 24 * time.Hour
)

// malformed reports a source error for one bad row.
func malformed(err error) bool {
	var row interface{ Malformed() bool }
	return errors.As(err, &row) && row.Malformed()
}

// Stats are the recorder's counters.
type Stats struct {
	Ticks           int64
	Failures        int64
	SkippedReadings int64
	SkippedChecks   int64
	// SkippedRecords counts coordinator rows that did not decode, and ids too
	// long to record.
	SkippedRecords int64
	LastError      string
	LastErrorAt    *time.Time
	LastSuccessAt  *time.Time
}

// Recorder appends telemetry events to its own store, one tick at a time.
// Tick is not safe for concurrent use; Stats is.
type Recorder struct {
	StorePath  string
	OpenSource func(context.Context) (Source, error)
	// OpenArtifact reads a retained artifact's content. Without it check
	// reports are skipped and counted.
	OpenArtifact func(ctx context.Context, artifactID string) (io.ReadCloser, error)
	Now          func() time.Time
	Logger       *slog.Logger
	Interval     time.Duration
	TickTimeout  time.Duration
	// FirstTickDelay postpones the first tick, so the recorder does not
	// compete with the coordinator's own startup.
	FirstTickDelay time.Duration

	mu    sync.Mutex
	stats Stats

	store       *Store
	source      Source
	merged      bool
	failedTicks int
	failedSince time.Time
	lastPrune   time.Time
	logged      map[string]time.Time
	// failCommit, set by a test, aborts the tick's write as a crash would.
	failCommit error
}

// Run ticks after FirstTickDelay and then every interval until ctx ends.
// Nothing it meets is returned: failures are logged, counted and recorded as
// gaps.
func (r *Recorder) Run(ctx context.Context) {
	defer r.Close()
	interval := r.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}
	if r.FirstTickDelay > 0 {
		delay := time.NewTimer(r.FirstTickDelay)
		select {
		case <-ctx.Done():
			delay.Stop()
			return
		case <-delay.C:
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		_ = r.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Close releases the store and the source.
func (r *Recorder) Close() {
	r.closeSource()
	r.closeStore()
}

func (r *Recorder) closeSource() {
	if r.source != nil {
		_ = r.source.Close()
		r.source = nil
	}
}

func (r *Recorder) closeStore() {
	if r.store != nil {
		_ = r.store.Close()
		r.store = nil
	}
}

// Stats returns a copy of the counters.
func (r *Recorder) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

func (r *Recorder) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func (r *Recorder) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Tick records one pass. It returns the tick's error for tests and logs; the
// error has already been counted, and a panic is recovered into one. When ctx
// has ended, the tick is abandoned without counting a failure.
func (r *Recorder) Tick(ctx context.Context) (err error) {
	now := r.now()
	tickTimeout := r.TickTimeout
	if tickTimeout <= 0 {
		tickTimeout = DefaultTickTimeout
	}
	tickCtx, cancel := context.WithTimeout(ctx, tickTimeout)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("quota telemetry recorder panic: %v", recovered)
		}
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			r.Close()
			return
		}
		r.recordFailure(now, err)
	}()
	return r.tick(tickCtx, now)
}

// recordFailure counts a failed tick, writes the counters best-effort and
// drops both connections so the next tick reopens them.
func (r *Recorder) recordFailure(now time.Time, failure error) {
	message := truncateUTF8(failure.Error(), maxErrorBytes)
	r.mu.Lock()
	r.stats.Ticks++
	r.stats.Failures++
	r.stats.LastError = message
	r.stats.LastErrorAt = &now
	stats := r.stats
	r.mu.Unlock()
	if r.failedTicks == 0 {
		r.failedSince = now
	}
	r.failedTicks++
	r.warn("quota telemetry recorder tick failed; the coordinator is unaffected", message)
	if r.store != nil {
		// The failed span is persisted too, so a restart before the next
		// successful tick still records its gap.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = r.store.commit(ctx, nil, nil, map[string]string{
			metaTicks: strconv.FormatInt(stats.Ticks, 10), metaFailures: strconv.FormatInt(stats.Failures, 10),
			metaLastError: message, metaLastErrorAt: formatInstant(now),
			metaFailedTicks: strconv.Itoa(r.failedTicks), metaFailedSince: formatInstant(r.failedSince),
		}, nil)
		cancel()
	}
	r.Close()
}

// warn logs a message the first time its error text is seen, then at most once
// per logEvery.
func (r *Recorder) warn(message, text string) {
	now := time.Now()
	if r.logged == nil || len(r.logged) > 100 {
		r.logged = map[string]time.Time{}
	}
	if last, seen := r.logged[text]; seen && now.Sub(last) < logEvery {
		return
	}
	r.logged[text] = now
	r.logger().Warn(message, "error", text)
}

func (r *Recorder) ensureStore() error {
	if r.store != nil {
		// A deleted file is recreated rather than written through a handle to
		// an unlinked inode.
		if _, err := os.Stat(r.StorePath); err != nil {
			r.closeStore()
		}
	}
	if r.store != nil {
		return nil
	}
	store, err := OpenStore(r.StorePath)
	if err != nil {
		return err
	}
	if !r.merged {
		// Counters persist across restarts: the first open of a process
		// continues from the stored values. The store is kept only once
		// they are merged, so a failure here cannot overwrite them.
		meta, err := store.Meta(context.Background())
		if err != nil {
			store.Close()
			return err
		}
		r.mu.Lock()
		r.stats.Ticks += meta.Ticks
		r.stats.Failures += meta.Failures
		r.stats.SkippedReadings += meta.SkippedReadings
		r.stats.SkippedChecks += meta.SkippedChecks
		r.stats.SkippedRecords += meta.SkippedRecords
		if r.stats.LastError == "" {
			r.stats.LastError, r.stats.LastErrorAt = meta.LastError, meta.LastErrorAt
		}
		if r.stats.LastSuccessAt == nil {
			r.stats.LastSuccessAt = meta.LastSuccessAt
		}
		r.mu.Unlock()
		if meta.FailedTicks > 0 && meta.FailedSince != nil {
			// A span that failed before this process started: its gap is
			// recorded by the next successful tick, like one of our own.
			r.failedTicks += int(meta.FailedTicks)
			if r.failedSince.IsZero() || meta.FailedSince.Before(r.failedSince) {
				r.failedSince = *meta.FailedSince
			}
		}
		r.merged = true
	}
	r.store = store
	return nil
}

func (r *Recorder) ensureSource(ctx context.Context) error {
	if r.source != nil {
		return nil
	}
	if r.OpenSource == nil {
		return errors.New("quota telemetry recorder has no coordinator source")
	}
	source, err := r.OpenSource(ctx)
	if err != nil {
		return err
	}
	r.source = source
	return nil
}

// tickResult is everything one tick commits.
type tickResult struct {
	events []Event
	// replacements overwrite stored events: a finish recorded before its
	// start was read, completed with that start.
	replacements    []Event
	meta            map[string]string
	skippedReadings int64
	skippedChecks   int64
	skippedRecords  int64
}

func (r *Recorder) tick(ctx context.Context, now time.Time) error {
	if err := r.ensureStore(); err != nil {
		return err
	}
	if err := r.ensureSource(ctx); err != nil {
		return err
	}
	if r.lastPrune.IsZero() || now.Sub(r.lastPrune) >= pruneEvery {
		if _, err := r.store.Prune(ctx, now); err != nil {
			return err
		}
		r.lastPrune = now
	}
	meta, err := r.store.Meta(ctx)
	if err != nil {
		return err
	}
	result := tickResult{meta: map[string]string{}}
	if err := r.collectReadings(ctx, now, &result); err != nil {
		return err
	}
	if err := r.collectLifecycle(ctx, now, meta, &result); err != nil {
		return err
	}
	if err := r.collectFinishes(ctx, now, meta, &result); err != nil {
		return err
	}
	if r.failedTicks > 0 {
		from, to := r.failedSince, now
		r.mu.Lock()
		lastError := r.stats.LastError
		r.mu.Unlock()
		result.events = append(result.events, Event{
			EventID: "recorder:gap:" + formatInstant(from) + ":" + formatInstant(to), Kind: KindRecorder, At: now,
			Recorder: &RecorderNote{State: RecorderGap, From: &from, To: &to, FailedTicks: r.failedTicks, LastError: lastError,
				Reason: "recorder ticks failed; readings in this span are lost, audit-driven events were recovered"},
		})
	}

	r.mu.Lock()
	stats := r.stats
	r.mu.Unlock()
	stats.Ticks++
	stats.SkippedReadings += result.skippedReadings
	stats.SkippedChecks += result.skippedChecks
	stats.SkippedRecords += result.skippedRecords
	stats.LastSuccessAt = &now
	result.meta[metaSkippedRecords] = strconv.FormatInt(stats.SkippedRecords, 10)
	result.meta[metaFailedTicks] = "0"
	result.meta[metaFailedSince] = ""
	result.meta[metaTicks] = strconv.FormatInt(stats.Ticks, 10)
	result.meta[metaFailures] = strconv.FormatInt(stats.Failures, 10)
	result.meta[metaSkippedReadings] = strconv.FormatInt(stats.SkippedReadings, 10)
	result.meta[metaSkippedChecks] = strconv.FormatInt(stats.SkippedChecks, 10)
	result.meta[metaLastSuccessAt] = formatInstant(now)
	// The stored counters are the in-memory ones as a whole: a failure whose
	// own best-effort write could not reach the store, because it could not
	// be opened or written, is persisted here.
	result.meta[metaLastError] = stats.LastError
	result.meta[metaLastErrorAt] = ""
	if stats.LastErrorAt != nil {
		result.meta[metaLastErrorAt] = formatInstant(*stats.LastErrorAt)
	}
	for index := range result.events {
		result.events[index].SchemaVersion = SchemaVersion
		result.events[index].RecordedAt = now
	}
	for index := range result.replacements {
		result.replacements[index].SchemaVersion = SchemaVersion
		result.replacements[index].RecordedAt = now
	}
	if err := r.store.commit(ctx, result.events, result.replacements, result.meta, r.failCommit); err != nil {
		return err
	}
	r.mu.Lock()
	r.stats = stats
	r.mu.Unlock()
	r.failedTicks, r.failedSince = 0, time.Time{}
	return nil
}

// collectReadings records every reading the coordinator holds: each worker
// snapshot's observations and the coordinator host's bucket states. The
// event id repeats for a repeated reading, so it is stored once.
func (r *Recorder) collectReadings(ctx context.Context, now time.Time, result *tickResult) error {
	snapshots, err := r.source.LoadWorkerSnapshots(ctx)
	if err != nil {
		return err
	}
	for _, snapshot := range snapshots {
		for _, observation := range snapshot.QuotaObservations {
			addReading(result, "worker:"+snapshot.WorkerID, observation.Key, now, Reading{
				UsedPercent: observation.UsedPercent, ResetsAt: utcPointer(observation.ResetsAt),
				ObservedAt: observation.ObservedAt.UTC(), Phase: string(observation.Phase), Healthy: observation.Healthy,
				Epoch: observation.Epoch, LimitName: observation.LimitName,
			})
		}
	}
	buckets, err := r.source.ListBuckets(ctx)
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		addReading(result, "coordinator-host", bucket.Key, now, Reading{
			UsedPercent: bucket.UsedPercent, ResetsAt: utcPointer(bucket.ResetsAt),
			ObservedAt: bucket.ObservedAt.UTC(), Phase: string(bucket.Phase), Healthy: bucket.Healthy,
			Epoch: bucket.Epoch, LimitName: bucket.LimitName,
		})
	}
	return nil
}

// addReading records one reading, or skips and counts it when it cannot be
// stored faithfully: no observation time, one implausibly far from the
// recorder's clock, a usage that is not a finite number, or a source or key
// too long to name it.
func addReading(result *tickResult, source string, key domain.BucketKey, now time.Time, reading Reading) {
	observed := reading.ObservedAt
	if observed.IsZero() || observed.Year() < 2000 || observed.After(now.Add(readingFutureSlack)) ||
		math.IsNaN(reading.UsedPercent) || math.IsInf(reading.UsedPercent, 0) ||
		len(source) > maxIdentityBytes || len(key.String()) > maxIdentityBytes {
		result.skippedReadings++
		return
	}
	if reading.ResetsAt != nil && (reading.ResetsAt.Year() < 2000 || reading.ResetsAt.Year() > 2200) {
		reading.ResetsAt = nil
	}
	result.events = append(result.events, readingEvent(source, key, now, reading))
}

func readingEvent(source string, key domain.BucketKey, now time.Time, reading Reading) Event {
	reading.Source = source
	reading.BucketKey = key.String()
	reading.ProviderInstanceID, reading.AccountID = key.ProviderInstanceID, key.AccountID
	reading.LimitID, reading.Window = key.LimitID, key.Window
	reading.AgeSeconds = now.Sub(reading.ObservedAt).Seconds()
	return Event{
		EventID: "reading:" + source + ":" + reading.BucketKey + ":" + formatInstant(reading.ObservedAt),
		Kind:    KindReading, At: reading.ObservedAt, Reading: &reading,
	}
}

func utcPointer(at *time.Time) *time.Time {
	if at == nil || at.IsZero() {
		return nil
	}
	value := at.UTC()
	return &value
}

// collectLifecycle tails the audit stream after the stored watermark and
// turns offers into dispatch events and claims into start events. The new
// watermark is committed with the events, so a restart replays without loss
// or duplication.
func (r *Recorder) collectLifecycle(ctx context.Context, now time.Time, meta Meta, result *tickResult) error {
	maximum, err := r.source.MaxAuditSequence(ctx)
	if err != nil {
		return err
	}
	if meta.AuditWatermark == nil {
		// First start of this store: coverage begins now, with no backfill.
		result.meta[metaAuditWatermark] = strconv.FormatInt(maximum, 10)
		result.meta[metaCoverageFrom] = formatInstant(now)
		coverage := now
		result.events = append(result.events, Event{
			EventID: "recorder:started:" + formatInstant(now), Kind: KindRecorder, At: now,
			Recorder: &RecorderNote{State: RecorderStarted, CoverageFrom: &coverage,
				Reason: "recording begins at the current audit sequence " + strconv.FormatInt(maximum, 10) + "; earlier work is not backfilled"},
		})
		return nil
	}
	watermark := *meta.AuditWatermark
	if watermark > maximum {
		result.events = append(result.events, Event{
			EventID: fmt.Sprintf("recorder:gap:watermark:%d:%d:%s", watermark, maximum, formatInstant(now)), Kind: KindRecorder, At: now,
			Recorder: &RecorderNote{State: RecorderGap, To: &now, Reason: fmt.Sprintf(
				"the coordinator audit stream ends at sequence %d, below the recorded watermark %d (a restored database); recording continues from %d",
				maximum, watermark, maximum)},
		})
		watermark = maximum
	}
	rows, err := r.source.AuditEventsAfter(ctx, watermark, auditBatch)
	if err != nil {
		return err
	}
	dispatched := map[string]time.Time{}
	for _, row := range rows {
		if row.Sequence > watermark {
			watermark = row.Sequence
		}
		var kind string
		switch row.Kind {
		case "assignment-offered":
			kind = KindDispatch
		case "assignment-claimed":
			kind = KindStart
		default:
			continue
		}
		event, ok, err := r.lifecycleEvent(ctx, kind, row, now, result)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		work := event.Work
		if kind == KindDispatch {
			if work.DispatchedAt != nil {
				dispatched[work.Key()] = *work.DispatchedAt
			}
		} else if work.StartedAt != nil {
			dispatchedAt, found := dispatched[work.Key()]
			if !found {
				stored, ok, err := r.store.event(ctx, KindDispatch+":"+work.Key())
				if err != nil {
					return err
				}
				if ok && stored.Work != nil && stored.Work.DispatchedAt != nil {
					dispatchedAt, found = *stored.Work.DispatchedAt, true
				}
			}
			if found {
				work.DispatchedAt = &dispatchedAt
				if queued := work.StartedAt.Sub(dispatchedAt).Milliseconds(); queued >= 0 {
					work.DispatchToStartMs = &queued
				}
			}
			if err := r.completeFinish(ctx, *work, result); err != nil {
				return err
			}
		}
		result.events = append(result.events, event)
	}
	result.meta[metaAuditWatermark] = strconv.FormatInt(watermark, 10)
	return nil
}

// completeFinish gives a finish recorded before its start was read the start
// it now has, with its duration. The finish was recorded from open work that
// had only a dispatch, because the claim lay beyond the tick's audit batch or
// was written after the batch was read; every other field stays as recorded.
func (r *Recorder) completeFinish(ctx context.Context, started Work, result *tickResult) error {
	stored, found, err := r.store.event(ctx, KindFinish+":"+started.Key())
	if err != nil || !found || stored.Work == nil || stored.Work.StartedAt != nil {
		return err
	}
	work := *stored.Work
	work.StartedAt = started.StartedAt
	setDuration(&work)
	stored.Work = &work
	result.replacements = append(result.replacements, stored)
	return nil
}

// setDuration derives a finished work's duration from its start. A finish
// recorded before its start, by clocks that disagree, has no duration rather
// than a negative one.
func setDuration(work *Work) {
	work.DurationMs = nil
	if work.StartedAt != nil && work.FinishedAt != nil && !work.FinishedAt.Before(*work.StartedAt) {
		duration := work.FinishedAt.Sub(*work.StartedAt).Milliseconds()
		work.DurationMs = &duration
	}
}

// lifecycleEvent builds a dispatch or start event from one audit row and the
// assignment, attempt and task it names, read by id.
//
// The current assignment record describes its latest epoch. A row for an
// earlier epoch takes its route and worker from the binding frozen with that
// epoch's dispatch, or records them as unknown, never as the later epoch's.
// A row naming an id too long to record is skipped and counted.
func (r *Recorder) lifecycleEvent(ctx context.Context, kind string, row domain.AuditEvent, now time.Time, result *tickResult) (Event, bool, error) {
	for _, id := range []string{row.TargetID, row.AttemptID, row.TaskID, row.WorkflowRunID} {
		if len(id) > maxIdentityBytes {
			result.skippedRecords++
			return Event{}, false, nil
		}
	}
	var detail struct {
		AssignmentEpoch int64 `json:"assignmentEpoch"`
	}
	if len(row.Detail) > 0 {
		_ = json.Unmarshal(row.Detail, &detail)
	}
	assignment, _, err := r.loadAssignment(ctx, row.TargetID, result)
	if err != nil {
		return Event{}, false, err
	}
	epoch := detail.AssignmentEpoch
	if epoch == 0 {
		epoch = assignment.Epoch
	}
	routeKnown := true
	if epoch > 0 && epoch < assignment.Epoch {
		frozen, found, err := r.source.LoadAssignmentEpoch(ctx, row.TargetID, epoch)
		if err != nil && !malformed(err) {
			return Event{}, false, err
		}
		if err != nil {
			result.skippedRecords++
		}
		if found {
			assignment = frozen
		} else {
			routeKnown = false
			assignment.Route, assignment.WorkerID, assignment.Project = domain.ProviderRoute{}, "", ""
		}
	}
	if assignment.ID == "" {
		assignment.ID = row.TargetID
	}
	if assignment.AttemptID == "" {
		assignment.AttemptID = row.AttemptID
	}
	work, err := r.describeWork(ctx, assignment, epoch, row.WorkflowRunID, row.TaskID, result)
	if err != nil {
		return Event{}, false, err
	}
	if !routeKnown {
		work.RouteUnknown, work.Route.EffortAbsent = true, false
	}
	at := now
	if !row.CreatedAt.IsZero() {
		at = row.CreatedAt.UTC()
		if kind == KindDispatch {
			work.DispatchedAt = &at
		} else {
			work.StartedAt = &at
		}
	}
	return Event{EventID: kind + ":" + work.Key(), Kind: kind, At: at, QuotaPoolID: work.Route.QuotaPoolID, Work: &work}, true, nil
}

// loadAssignment, loadAttempt and loadTask read one record by id; a record
// that does not decode is counted and treated as absent.
func (r *Recorder) loadAssignment(ctx context.Context, id string, result *tickResult) (domain.Assignment, bool, error) {
	assignment, found, err := r.source.LoadAssignment(ctx, id)
	if malformed(err) {
		result.skippedRecords++
		return domain.Assignment{}, false, nil
	}
	return assignment, found, err
}

func (r *Recorder) loadAttempt(ctx context.Context, id string, result *tickResult) (domain.Attempt, bool, error) {
	attempt, found, err := r.source.LoadAttempt(ctx, id)
	if malformed(err) {
		result.skippedRecords++
		return domain.Attempt{}, false, nil
	}
	return attempt, found, err
}

func (r *Recorder) loadTask(ctx context.Context, id string, result *tickResult) (domain.Task, bool, error) {
	task, found, err := r.source.LoadTask(ctx, id)
	if malformed(err) {
		result.skippedRecords++
		return domain.Task{}, false, nil
	}
	return task, found, err
}

// describeWork fills the identity, route and derived task type of one
// assignment epoch. Nothing is read from a prompt or any free text, and no
// token or failure text is copied.
func (r *Recorder) describeWork(ctx context.Context, assignment domain.Assignment, epoch int64, runID, taskID string, result *tickResult) (Work, error) {
	attempt, _, err := r.loadAttempt(ctx, assignment.AttemptID, result)
	if err != nil {
		return Work{}, err
	}
	if attempt.TaskID != "" {
		taskID = attempt.TaskID
	}
	if attempt.WorkflowRunID != "" {
		runID = attempt.WorkflowRunID
	}
	task, _, err := r.loadTask(ctx, taskID, result)
	if err != nil {
		return Work{}, err
	}
	route := Route{ProviderInstanceID: assignment.Route.ProviderInstanceID, Model: assignment.Route.Model,
		QuotaPoolID: assignment.Route.QuotaPoolID}
	if effort := assignment.Route.Options["effort"]; effort != "" {
		route.Effort = &effort
	} else {
		route.EffortAbsent = true
	}
	return Work{
		RunID: runID, TaskID: taskID, TaskName: task.Name, AttemptID: assignment.AttemptID, AttemptNumber: attempt.Number,
		AssignmentID: assignment.ID, AssignmentEpoch: epoch, WorkerID: assignment.WorkerID, Project: assignment.Project,
		Route: route, ExecutionRole: string(assignment.ExecutionRole),
		TaskType: DeriveTaskType(task, attempt, assignment.ExecutionRole),
	}, nil
}

// supersededAt is when an epoch that was offered again ended: the next
// epoch's dispatch, from this tick's events, the store or the binding frozen
// with it, and only failing those the assignment's last update.
func (r *Recorder) supersededAt(ctx context.Context, work Work, current domain.Assignment, result *tickResult) (time.Time, error) {
	nextID := KindDispatch + ":" + workKey(work.AssignmentID, work.AssignmentEpoch+1)
	for _, event := range result.events {
		if event.EventID == nextID {
			return event.At, nil
		}
	}
	stored, found, err := r.store.event(ctx, nextID)
	if err != nil {
		return time.Time{}, err
	}
	if found {
		return stored.At, nil
	}
	frozen, found, err := r.source.LoadAssignmentEpoch(ctx, work.AssignmentID, work.AssignmentEpoch+1)
	if err != nil && !malformed(err) {
		return time.Time{}, err
	}
	if found && !frozen.CreatedAt.IsZero() {
		return frozen.CreatedAt, nil
	}
	return current.UpdatedAt, nil
}

// withStagedWork completes the open work read from the store with the
// dispatch and start events this tick has staged, so open work is what the
// store will hold once the tick commits. A finish seen in the same tick as its
// start, or as its dispatch, is then built from them: it keeps its start time,
// duration and deltas. Committed events take precedence over staged ones.
func (r *Recorder) withStagedWork(ctx context.Context, open []openWork, result *tickResult) ([]openWork, error) {
	position := make(map[string]int, len(open))
	for index, item := range open {
		position[item.key] = index
	}
	for index := range result.events {
		event := result.events[index]
		if (event.Kind != KindDispatch && event.Kind != KindStart) || event.Work == nil {
			continue
		}
		key := event.Work.Key()
		at, listed := position[key]
		if !listed {
			// Not in this tick's batch of open work: it is new, or open and
			// outside the batch, or already finished.
			item := openWork{key: key}
			if _, finished, err := r.store.event(ctx, KindFinish+":"+key); err != nil {
				return nil, err
			} else if finished {
				continue
			}
			for _, kind := range []string{KindDispatch, KindStart} {
				stored, found, err := r.store.event(ctx, kind+":"+key)
				if err != nil {
					return nil, err
				}
				if !found || stored.Work == nil {
					continue
				}
				if kind == KindDispatch {
					item.dispatch = &stored
				} else {
					item.start = &stored
				}
			}
			open = append(open, item)
			at = len(open) - 1
			position[key] = at
		}
		if event.Kind == KindDispatch && open[at].dispatch == nil {
			open[at].dispatch = &event
		} else if event.Kind == KindStart && open[at].start == nil {
			open[at].start = &event
		}
	}
	return open, nil
}

func workKey(assignmentID string, epoch int64) string {
	return assignmentID + ":" + strconv.FormatInt(epoch, 10)
}

// collectFinishes re-reads up to openBatch open assignments by id, and every
// assignment this tick dispatched or started, and records a finish, with its
// check events, for each one that has ended.
func (r *Recorder) collectFinishes(ctx context.Context, now time.Time, meta Meta, result *tickResult) error {
	open, cursor, err := r.store.openWorkAfter(ctx, meta.OpenCursor, openBatch)
	if err != nil {
		return err
	}
	result.meta[metaOpenCursor] = cursor
	if open, err = r.withStagedWork(ctx, open, result); err != nil {
		return err
	}
	for _, item := range open {
		base := item.start
		if base == nil {
			base = item.dispatch
		}
		work := *base.Work
		assignment, found, err := r.loadAssignment(ctx, work.AssignmentID, result)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		attempt, attemptFound, err := r.loadAttempt(ctx, work.AttemptID, result)
		if err != nil {
			return err
		}
		var finishedAt time.Time
		// Checks belong to the epoch that ran the attempt to its end; an
		// epoch that was superseded or released never records them.
		withChecks := false
		switch {
		case assignment.Epoch > work.AssignmentEpoch:
			// The assignment was offered again at a later epoch, which is
			// recorded as its own dispatch; this epoch ended no later than that.
			work.Outcome = "superseded"
			if finishedAt, err = r.supersededAt(ctx, work, assignment, result); err != nil {
				return err
			}
		case attemptFound && attempt.Progress.Terminal():
			work.Outcome, finishedAt, withChecks = string(attempt.Progress), attempt.UpdatedAt, true
			if attempt.CompletedAt != nil {
				finishedAt = *attempt.CompletedAt
			}
		case assignment.State == domain.AssignmentReleased:
			work.Outcome, finishedAt = string(domain.AssignmentReleased), assignment.UpdatedAt
		case assignment.State == domain.AssignmentCompleted:
			work.Outcome, finishedAt, withChecks = string(domain.AssignmentCompleted), assignment.UpdatedAt, attemptFound
			if attemptFound {
				work.Outcome = string(attempt.Progress)
			}
		default:
			continue
		}
		if finishedAt.IsZero() {
			finishedAt = now
		}
		finishedAt = finishedAt.UTC()
		if item.dispatch != nil && item.dispatch.Work.DispatchedAt != nil {
			work.DispatchedAt = item.dispatch.Work.DispatchedAt
		}
		work.FinishedAt = &finishedAt
		work.DispatchToStartMs = nil
		setDuration(&work)
		result.events = append(result.events, Event{EventID: KindFinish + ":" + work.Key(), Kind: KindFinish,
			At: finishedAt, QuotaPoolID: work.Route.QuotaPoolID, Work: &work})
		if withChecks {
			if err := r.collectChecks(ctx, work, finishedAt, result); err != nil {
				return err
			}
		}
	}
	return nil
}
