// Package quotatelemetry is phase 0 of the quota controller: an observe-only
// recorder that joins the quota readings the coordinator holds to the work
// that consumed them, in a SQLite file of its own.
//
// It changes neither dispatch nor quota policy. It reads the coordinator
// database through a query-only connection, writes only its own file, and a
// failure of either is logged, counted and recorded as a gap; it never reaches
// the coordinator. docs/quota-telemetry.md is the operator's reference.
package quotatelemetry

import (
	"time"
)

// SchemaVersion is the version of the store and of every event record.
const SchemaVersion = 1

// DocumentKind names the JSON document "t3-steward quota telemetry --json"
// prints.
const DocumentKind = "t3-steward.quota-telemetry/v1"

// Event kinds.
const (
	KindReading  = "reading"
	KindDispatch = "dispatch"
	KindStart    = "start"
	KindFinish   = "finish"
	KindCheck    = "check"
	KindRecorder = "recorder"
)

// Kinds lists every event kind, in the order the read command documents them.
var Kinds = []string{KindReading, KindDispatch, KindStart, KindFinish, KindCheck, KindRecorder}

// Event is one immutable telemetry record. Exactly one of Reading, Work (with
// Check for a check event) or Recorder is set, by kind. A missing time, reset
// or usage is null, never zero.
type Event struct {
	SchemaVersion int    `json:"schemaVersion"`
	EventID       string `json:"eventId"`
	Kind          string `json:"kind"`
	// At is the event's own time: the reading's observation, the dispatch,
	// claim or finish, a check's completion, or the recorder's own clock.
	At time.Time `json:"at"`
	// RecordedAt is when the recorder wrote it.
	RecordedAt  time.Time     `json:"recordedAt"`
	QuotaPoolID string        `json:"quotaPoolId"`
	Reading     *Reading      `json:"reading,omitempty"`
	Work        *Work         `json:"work,omitempty"`
	Check       *Check        `json:"check,omitempty"`
	Recorder    *RecorderNote `json:"recorder,omitempty"`
	// Deltas are computed by the reader for a finish event from the retained
	// readings; they are never stored, so a later method can re-derive them.
	Deltas []Delta `json:"deltas,omitempty"`
	// Truncated says long strings were cut to keep the record under
	// MaxRecordBytes.
	Truncated bool `json:"truncated,omitempty"`
}

// Reading is one quota observation the coordinator held.
type Reading struct {
	// Source is "worker:<id>" for a worker snapshot's observation and
	// "coordinator-host" for the coordinator host's own watchdog.
	Source             string     `json:"source"`
	BucketKey          string     `json:"bucketKey"`
	ProviderInstanceID string     `json:"providerInstanceId"`
	AccountID          string     `json:"accountId"`
	LimitID            string     `json:"limitId"`
	Window             string     `json:"window"`
	UsedPercent        float64    `json:"usedPercent"`
	ResetsAt           *time.Time `json:"resetsAt"`
	ObservedAt         time.Time  `json:"observedAt"`
	Phase              string     `json:"phase"`
	Healthy            bool       `json:"healthy"`
	Epoch              string     `json:"epoch"`
	LimitName          string     `json:"limitName"`
	// AgeSeconds is how old the reading was when the recorder first saw it.
	AgeSeconds float64 `json:"ageSeconds"`
}

// Route is the provider route an assignment committed to.
type Route struct {
	ProviderInstanceID string `json:"providerInstanceId"`
	Model              string `json:"model"`
	// Effort is null, with EffortAbsent set, when the route names none.
	Effort       *string `json:"effort"`
	EffortAbsent bool    `json:"effortAbsent,omitempty"`
	QuotaPoolID  string  `json:"quotaPoolId"`
}

// Name is "instance/model", what --route matches.
func (r Route) Name() string {
	if r.ProviderInstanceID == "" && r.Model == "" {
		return ""
	}
	return r.ProviderInstanceID + "/" + r.Model
}

// Work identifies the assignment a dispatch, start, finish or check event
// belongs to.
type Work struct {
	RunID           string `json:"runId"`
	TaskID          string `json:"taskId"`
	TaskName        string `json:"taskName"`
	AttemptID       string `json:"attemptId"`
	AttemptNumber   int    `json:"attemptNumber"`
	AssignmentID    string `json:"assignmentId"`
	AssignmentEpoch int64  `json:"assignmentEpoch"`
	WorkerID        string `json:"workerId"`
	Project         string `json:"project"`
	Route           Route  `json:"route"`
	// RouteUnknown is set on an earlier epoch of an assignment offered again
	// when no binding was frozen for that epoch: its route, worker and project
	// are left empty rather than taken from the later epoch.
	RouteUnknown  bool     `json:"routeUnknown,omitempty"`
	ExecutionRole string   `json:"executionRole"`
	TaskType      TaskType `json:"taskType"`
	// DispatchedAt, StartedAt and FinishedAt are the times this record knows;
	// the rest are null.
	DispatchedAt *time.Time `json:"dispatchedAt"`
	StartedAt    *time.Time `json:"startedAt"`
	FinishedAt   *time.Time `json:"finishedAt"`
	// DispatchToStartMs is on a start event when its dispatch was recorded.
	DispatchToStartMs *int64 `json:"dispatchToStartMs"`
	// DurationMs is finish minus start, wall clock with parked time included,
	// on a finish event whose start was recorded.
	DurationMs *int64 `json:"durationMs"`
	// Outcome is the attempt's progress at finish, or the assignment's state
	// when it was released or superseded.
	Outcome string `json:"outcome,omitempty"`
}

// Key identifies one assignment epoch.
func (w Work) Key() string {
	return workKey(w.AssignmentID, w.AssignmentEpoch)
}

// Check is one worker-measured verification or gate command of a finished
// attempt. Its output and the gate log are never stored.
type Check struct {
	// Stage is "verification" or "gate".
	Stage string `json:"stage"`
	// Index is the verification report's NNN, or the gate command's position,
	// both from 1.
	Index       int        `json:"index"`
	ExitCode    int        `json:"exitCode"`
	StartedAt   *time.Time `json:"startedAt"`
	CompletedAt *time.Time `json:"completedAt"`
	DurationMs  *int64     `json:"durationMs"`
	// Command is cut to MaxCommandBytes.
	Command          string `json:"command"`
	CommandTruncated bool   `json:"commandTruncated,omitempty"`
}

// RecorderNote is the recorder's own account: "started" when a store begins
// coverage, "gap" for a span it could not record.
type RecorderNote struct {
	State        string     `json:"state"`
	CoverageFrom *time.Time `json:"coverageFrom,omitempty"`
	From         *time.Time `json:"from,omitempty"`
	To           *time.Time `json:"to,omitempty"`
	FailedTicks  int        `json:"failedTicks,omitempty"`
	LastError    string     `json:"lastError,omitempty"`
	Reason       string     `json:"reason,omitempty"`
}

// Limits on what is stored.
const (
	// MaxRecordBytes caps one stored event record.
	MaxRecordBytes = 4096
	// MaxCommandBytes caps a check event's command text.
	MaxCommandBytes = 256
	// MaxReportBytes caps a verification or gate report the recorder reads.
	MaxReportBytes = 4 << 20
)
