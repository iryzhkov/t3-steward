package backlogadmin

import (
	"encoding/json"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const Version = "backlog.admin/v1"

type QueryKind string

const (
	QueryStatus       QueryKind = "status"
	QueryWorkflows    QueryKind = "workflows"
	QueryWorkflow     QueryKind = "workflow"
	QueryGraph        QueryKind = "graph"
	QueryDiagnose     QueryKind = "diagnose"
	QueryTask         QueryKind = "task"
	QueryExplanation  QueryKind = "explanation"
	QueryEvents       QueryKind = "events"
	QueryArtifacts    QueryKind = "artifacts"
	QueryArtifact     QueryKind = "artifact"
	QuerySchedules    QueryKind = "schedules"
	QueryWorkers      QueryKind = "workers"
	QueryQuota        QueryKind = "quota"
	QueryReservations QueryKind = "reservations"
	QueryLocks        QueryKind = "locks"
	QueryCommands     QueryKind = "commands"
	QueryRecovery     QueryKind = "recovery"
	// QueryViability asks whether a projected campaign could run. It is
	// read-only and live: it consults the fleet as it is now and creates
	// nothing.
	QueryViability QueryKind = "viability"
	// QueryQuarantine lists the intake submissions the coordinator refused
	// permanently and is now silent about. Their audit events carry no workflow
	// run, so the run-scoped event view cannot show them and an operator had no
	// way to see them at all.
	QueryQuarantine QueryKind = "quarantine"
	// QueryProjects lists the projects this coordinator is configured with,
	// each with its repository, default ref and setup profile, and the workers
	// that could take its work. It exists so that a client can match a checkout
	// to a fleet project and choose a route the fleet actually offers without
	// reading every worker's inventory itself.
	QueryProjects QueryKind = "projects"
)

// QueryKinds is every declared query kind. A query kind is a read view by
// definition: it answers a question and creates nothing, and a mutation names
// its verb in Action.CommandKind instead.
//
// It exists so that authorization can ask "is this a read" without repeating
// the list somewhere that will fall behind. A kind that is added above and not
// added here is refused to remote clients, which is how the viability query
// became unreachable from a client host; the test beside this function is what
// catches the omission now.
func QueryKinds() []QueryKind {
	return []QueryKind{
		QueryStatus, QueryWorkflows, QueryWorkflow, QueryGraph, QueryDiagnose,
		QueryTask, QueryExplanation, QueryEvents, QueryArtifacts, QueryArtifact,
		QuerySchedules, QueryWorkers, QueryQuota, QueryReservations, QueryLocks,
		QueryCommands, QueryRecovery, QueryViability, QueryQuarantine, QueryProjects,
	}
}

// IsQueryKind reports whether kind is one of the declared read views.
func IsQueryKind(kind QueryKind) bool {
	for _, candidate := range QueryKinds() {
		if candidate == kind {
			return true
		}
	}
	return false
}

// QuarantineRetryAdvice is the one sentence a quarantine view has to say, in
// the same words everywhere: the marker is bound to the exact content that was
// refused, so changing the file is both the recovery and the retry.
const QuarantineRetryAdvice = "change the file: a different content digest releases this marker and the submission is tried again."

// QuarantinedIntake is one permanently refused intake submission. It names no
// workflow or run because nothing was accepted, which is exactly why it is
// invisible in every run-scoped view.
type QuarantinedIntake struct {
	// Key is the intake idempotency key the submission source used.
	Key string `json:"key"`
	// RecordKey is the namespaced key the durable record is stored under in
	// coordinator_submissions, for an operator reading the database directly.
	RecordKey string `json:"recordKey"`
	// Digest is the content the marker was recorded for. A different digest is
	// different content, and different content is tried again.
	Digest        string    `json:"digest"`
	QuarantinedAt time.Time `json:"quarantinedAt"`
	Reason        string    `json:"reason"`
	// Retry is QuarantineRetryAdvice, carried in the document so that a reader
	// of the JSON is told the same thing as a reader of the text.
	Retry string `json:"retry"`
}

type Principal struct {
	ID    string   `json:"id"`
	Roles []string `json:"roles,omitempty"`
}

type Filter struct {
	Project     string                 `json:"project,omitempty"`
	ScheduleID  string                 `json:"scheduleId,omitempty"`
	Progress    []domain.ProgressState `json:"progress,omitempty"`
	Class       domain.TaskClass       `json:"class,omitempty"`
	WorkerID    string                 `json:"workerId,omitempty"`
	QuotaPoolID string                 `json:"quotaPoolId,omitempty"`
}

type Query struct {
	IncludeSink   bool      `json:"includeSink,omitempty"`
	Version       string    `json:"version"`
	Kind          QueryKind `json:"kind"`
	Principal     Principal `json:"principal"`
	WorkflowRunID string    `json:"workflowRunId,omitempty"`
	TaskID        string    `json:"taskId,omitempty"`
	ArtifactID    string    `json:"artifactId,omitempty"`
	CommandID     string    `json:"commandId,omitempty"`
	Filter        Filter    `json:"filter,omitempty"`
	// Viability carries the projected requirements of a campaign that has not
	// been submitted. It is present only on a QueryViability query, and it
	// never carries the bundle.
	Viability *ViabilityRequest `json:"viability,omitempty"`
}

type Action struct {
	Kind          QueryKind
	CommandKind   domain.AdminCommandKind
	WorkflowRunID string
	TaskID        string
	ScheduleID    string
	ArtifactID    string
	CommandID     string
	OutcomeState  domain.AdminCommandState
	AssignmentID  string
	Filter        Filter
	// The fields below are the scope a run-and-epoch-bound capability is
	// checked against. An Authorizer sees nothing but an Action, so a scope
	// the Action cannot carry is a scope nobody can enforce.
	//
	// ActivationEpoch is the epoch the caller claims to act under. Zero means
	// the caller named none, which only an operator may do.
	ActivationEpoch int64
	GateID          string
	HoldID          string
	IncidentID      string
}

// QuarantineReleaseRequest asks the coordinator to clear one intake
// quarantine. It is mutating and deliberate: nothing about the file changed,
// the operator changed something around it.
type QuarantineReleaseRequest struct {
	// Key is the intake idempotency key as the quarantine view reports it,
	// without the namespace prefix of the record it is stored under.
	Key    string `json:"key"`
	Reason string `json:"reason"`
}

type UnknownRecoveryRequest struct {
	ID                      string                        `json:"id"`
	AssignmentID            string                        `json:"assignmentId"`
	CoordinatorEpoch        int64                         `json:"coordinatorEpoch"`
	ExpectedAssignmentEpoch int64                         `json:"expectedAssignmentEpoch"`
	ExpectedAttemptRevision int64                         `json:"expectedAttemptRevision"`
	Outcome                 domain.UnknownRecoveryOutcome `json:"outcome"`
	EvidenceID              string                        `json:"evidenceId"`
	EvidenceSHA256          string                        `json:"evidenceSha256"`
	Reason                  string                        `json:"reason"`
}

type Response struct {
	Diagnosis     *Diagnosis        `json:"diagnosis,omitempty"`
	Version       string            `json:"version"`
	Kind          QueryKind         `json:"kind"`
	GeneratedAt   time.Time         `json:"generatedAt"`
	Status        *Status           `json:"status,omitempty"`
	Workflows     []WorkflowSummary `json:"workflows,omitempty"`
	Workflow      *WorkflowDetail   `json:"workflow,omitempty"`
	Graph         *Graph            `json:"graph,omitempty"`
	Task          *TaskDetail       `json:"task,omitempty"`
	Explanation   *Explanation      `json:"explanation,omitempty"`
	Events        []Event           `json:"events,omitempty"`
	Artifacts     []Artifact        `json:"artifacts,omitempty"`
	Artifact      *Artifact         `json:"artifact,omitempty"`
	Schedules     []Schedule        `json:"schedules,omitempty"`
	Workers       []Worker          `json:"workers,omitempty"`
	Quotas        []Quota           `json:"quotas,omitempty"`
	Reservations  []Reservation     `json:"reservations,omitempty"`
	ResourceLocks []ResourceLock    `json:"resourceLocks,omitempty"`
	Commands      []Command         `json:"commands,omitempty"`
	Viability     *ViabilityMatrix  `json:"viability,omitempty"`
	// Quarantine is the whole list on a QueryQuarantine response. It is absent
	// when nothing is quarantined, which the text renderer states in words so
	// that an empty answer is never mistaken for a failed query.
	Quarantine []QuarantinedIntake `json:"quarantine,omitempty"`
	// Projects is the whole catalog on a QueryProjects response, or the one
	// project the filter named.
	Projects []Project `json:"projects,omitempty"`
}

// Project is one configured fleet project as the projects query reports it.
type Project struct {
	Name         string `json:"name"`
	Repository   string `json:"repository,omitempty"`
	DefaultRef   string `json:"defaultRef,omitempty"`
	Type         string `json:"type,omitempty"`
	SetupProfile string `json:"setupProfile,omitempty"`
	// Workers are the workers that could take this project's work: every
	// worker the configuration names for it and every worker whose inventory
	// advertises it. A worker on neither list is not eligible and is not
	// listed.
	Workers []ProjectWorker `json:"workers"`
}

// ProjectWorker is one eligible worker of a project with what a client needs
// to know before choosing it: whether it is enrolled and ready now, and which
// provider routes it advertises.
type ProjectWorker struct {
	Worker string `json:"worker"`
	// Configured reports that backlog_v2.projects.<name>.workers names it.
	Configured bool `json:"configured"`
	// Advertises reports that the worker's inventory lists the project as
	// available.
	Advertises bool `json:"advertises"`
	Enrolled   bool `json:"enrolled"`
	// Ready reports a fresh, connected, enrolled worker whose inventory is
	// healthy and accepting backlog: the same judgement the workers view makes.
	Ready  bool   `json:"ready"`
	State  string `json:"state"`
	Health string `json:"health,omitempty"`
	// Routes are the instance, model and quota pool triples the worker's
	// inventory advertises as available, one per model.
	Routes []ProjectRoute `json:"routes,omitempty"`
}

// ProjectRoute is one advertised provider route. QuotaPool is the pool the
// worker's inventory binds the instance to, and is empty when it binds none.
type ProjectRoute struct {
	Instance  string `json:"instance"`
	Model     string `json:"model"`
	QuotaPool string `json:"quotaPool,omitempty"`
}

type Status struct {
	WorkflowRuns map[domain.ProgressState]int  `json:"workflowRuns"`
	Tasks        map[domain.ProgressState]int  `json:"tasks"`
	Workers      map[string]int                `json:"workers"`
	QuotaPools   map[domain.AdmissionState]int `json:"quotaPools"`
	Reservations int                           `json:"reservations"`
	Locks        int                           `json:"locks"`
	Runtime      RuntimeStatus                 `json:"runtime"`
}

type RuntimeStatus struct {
	Release             string `json:"release,omitempty"`
	ConfigurationDigest string `json:"configurationDigest,omitempty"`
	// LastReload is when the effective configuration was activated: at
	// startup, or by the last accepted reload. It has been a time under this
	// key since v0.11.0-rc.56 and every deployed admin client decodes it as
	// one, so its type is part of the wire contract; the receipt travels under
	// LastReloadReceipt instead.
	LastReload time.Time `json:"lastReload,omitzero"`
	// LastReloadReceipt is the receipt of the last SIGHUP the coordinator
	// handled, absent until the first one. It is the same record the
	// coordinator writes to its state directory, so a remote admin host reads
	// the verdict without a shell on the coordinator host. A new key, so an
	// older client ignores it rather than failing on it.
	LastReloadReceipt    *ReloadReceipt `json:"lastReloadReceipt,omitempty"`
	Mode                 string         `json:"mode"`
	Owner                string         `json:"owner"`
	Epoch                int64          `json:"epoch"`
	Health               string         `json:"health"`
	Transport            string         `json:"transport"`
	FreshWorkers         int            `json:"freshWorkers"`
	StaleWorkers         int            `json:"staleWorkers"`
	FreshQuotaPools      int            `json:"freshQuotaPools"`
	StaleQuotaPools      int            `json:"staleQuotaPools"`
	ReconciliationIssues []string       `json:"reconciliationIssues,omitempty"`
	UnknownExecutionIDs  []string       `json:"unknownExecutionIds,omitempty"`
	CustodyIncidentIDs   []string       `json:"custodyIncidentIds,omitempty"`
}

// Reload outcomes. Accepted means the new configuration is active; unchanged
// means the file was re-read and its digest equals the effective one, so nothing
// was replaced; rejected means the effective configuration and its digest are
// exactly what they were before the signal.
const (
	ReloadAccepted  = "accepted"
	ReloadRejected  = "rejected"
	ReloadUnchanged = "unchanged"
)

// ReloadReceipt is the coordinator's verdict on one SIGHUP. The coordinator
// owns the verdict: it writes one receipt per signal, atomically, before the
// log line that reports the same outcome, and never one older than the
// previous. Whoever sent the signal reads the receipt instead of the journal.
type ReloadReceipt struct {
	RequestedAt time.Time `json:"requestedAt"`
	CompletedAt time.Time `json:"completedAt"`
	Outcome     string    `json:"outcome"`
	Error       string    `json:"error,omitempty"`
	// ConfigurationDigest is the digest effective after the request. On a
	// rejection it equals PreviousDigest, which is how the receipt says that
	// nothing changed.
	ConfigurationDigest string `json:"configurationDigest"`
	PreviousDigest      string `json:"previousDigest"`
	Release             string `json:"release,omitempty"`
	// Blockers names, on a rejection, every retained assignment that kept the
	// reload from being accepted, with the command that unblocks it.
	Blockers []ReloadBlocker `json:"blockers,omitempty"`
}

// ReloadBlocker is one assignment that blocks a catalog change on its worker.
type ReloadBlocker struct {
	WorkerID     string `json:"workerId"`
	AssignmentID string `json:"assignmentId"`
	AttemptID    string `json:"attemptId,omitempty"`
	// Progress and Control are the attempt's coordinator-side states, and
	// JournalPhase the phase the worker last reported for it, when a snapshot
	// carried one. They tell an operator whether the attempt is running,
	// paused or parked before deciding what to do with it.
	Progress     string `json:"progress,omitempty"`
	Control      string `json:"control,omitempty"`
	JournalPhase string `json:"journalPhase,omitempty"`
	// Unblock is the operator action that removes this blocker: a
	// "t3-steward backlog cancel" command line, or an instruction to wait.
	Unblock string `json:"unblock"`
}

type Progress struct {
	Total      int `json:"total"`
	Queued     int `json:"queued"`
	Blocked    int `json:"blocked"`
	Ready      int `json:"ready"`
	Active     int `json:"active"`
	NeedsInput int `json:"needsInput"`
	// WaitingExternal counts attempts parked on a task-bound wait. They are
	// reported apart from NeedsInput because an operator acts on the two
	// differently: needs-input is a question addressed to a human, waiting is a
	// machine condition nobody has to answer.
	WaitingExternal int `json:"waitingExternal"`
	Verifying       int `json:"verifying"`
	Succeeded       int `json:"succeeded"`
	Failed          int `json:"failed"`
	Cancelled       int `json:"cancelled"`
	Skipped         int `json:"skipped"`
}

type WorkflowSummary struct {
	Run      domain.WorkflowRun `json:"run"`
	Workflow domain.Workflow    `json:"workflow"`
	Progress Progress           `json:"progress"`
}

type WorkflowDetail struct {
	Summary       WorkflowSummary `json:"summary"`
	Tasks         []TaskDetail    `json:"tasks"`
	Artifacts     []Artifact      `json:"artifacts,omitempty"`
	ResourceLocks []ResourceLock  `json:"resourceLocks,omitempty"`
	Reservations  []Reservation   `json:"reservations,omitempty"`
	// TaskWaits are every task-bound wait of this run, live and settled, each
	// naming its task and, once it has one, its outcome. A settled wait is why
	// a task stopped waiting, so dropping it made the run document unable to
	// say what became of a wait it had just reported. It is never omitted, so
	// a reader branches on the array rather than on whether the key exists.
	TaskWaits []TaskWaitDetail `json:"taskWaits"`
	// Waits is TaskWaits restricted to the live ones, the meaning this key has
	// always had here, and to the fields the previous release declares.
	// Deprecated: kept for one release because a client of the previous release
	// reads it; read taskWaits, whose name means the same thing in this
	// document and in a diagnosis.
	Waits []TaskWaitDetail `json:"waits"`
	// Gates are the declared gates of a supervised run with their current
	// state. An unsupervised run has none.
	Gates []GateDetail `json:"gates,omitempty"`
}

// TaskWaitDetail is one live task-bound wait as "campaign show" reports it:
// what the task is waiting for, on which attempt, and until when.
type TaskWaitDetail struct {
	ID           string    `json:"id"`
	TaskID       string    `json:"taskId"`
	TaskName     string    `json:"taskName,omitempty"`
	AttemptID    string    `json:"attemptId"`
	Name         string    `json:"name,omitempty"`
	Condition    string    `json:"condition,omitempty"`
	RegisteredAt time.Time `json:"registeredAt"`
	Deadline     time.Time `json:"deadline"`
	// Kind is how the wait is settled: shell, time, github, node or quota.
	Kind string `json:"kind,omitempty"`
	// Outcome, ExitCode, Reason and SettledAt are the settlement, and are
	// present only once the wait has one. While the wait is live there is no
	// exit code to report: the condition is polled on the worker host, whose
	// local check row holds the last exit code and output, and the coordinator
	// records one only at settlement.
	Outcome   string     `json:"outcome,omitempty"`
	ExitCode  int        `json:"exitCode,omitempty"`
	Reason    string     `json:"reason,omitempty"`
	SettledAt *time.Time `json:"settledAt,omitempty"`
}

// GateDetail is one gate of a supervised run and, while it is pending, the
// observed tasks it is still missing evidence from.
type GateDetail struct {
	ID               string           `json:"id"`
	Name             string           `json:"name"`
	State            domain.GateState `json:"state"`
	Final            bool             `json:"final,omitempty"`
	ObservedTaskIDs  []string         `json:"observedTaskIds"`
	ProtectedTaskIDs []string         `json:"protectedTaskIds,omitempty"`
	// MissingEvidence names each observed task whose latest attempt has not
	// succeeded, in the gate's observed order. It is empty once every observed
	// task succeeded, whatever the gate's state. It is derived from the
	// attempt records the query already reads, not from the store's evidence
	// computation, so it can lag one boundary cycle behind the gate state.
	MissingEvidence []GateEvidenceGap `json:"missingEvidence,omitempty"`
}

// GateEvidenceGap is one observed task a gate has no evidence from yet.
type GateEvidenceGap struct {
	TaskID   string               `json:"taskId"`
	TaskName string               `json:"taskName,omitempty"`
	Progress domain.ProgressState `json:"progress"`
}

type TaskDetail struct {
	Sink       *domain.SinkTask `json:"sink,omitempty"`
	Task       domain.Task      `json:"task"`
	Attempt    *domain.Attempt  `json:"attempt,omitempty"`
	Assignment *Assignment      `json:"assignment,omitempty"`
	ThreadURL  string           `json:"threadUrl,omitempty"`
	// Evidence is what the assigned worker last reported about the attempt's
	// execution: the thread, the worker, the observed session state and any
	// quota pause in force. It is absent for an attempt no worker holds.
	Evidence      *AttemptEvidence `json:"evidence,omitempty"`
	Artifacts     []Artifact       `json:"artifacts,omitempty"`
	ResourceLocks []string         `json:"resourceLocks,omitempty"`
}

// AttemptEvidence is the worker's last word on an attempt, as carried by its
// snapshot: enough for an operator to find the thread and see why it is not
// running without reading the T3 database.
type AttemptEvidence struct {
	ThreadID string `json:"threadId,omitempty"`
	WorkerID string `json:"workerId,omitempty"`
	// Control is the control state the worker observed for the attempt.
	Control domain.ControlState `json:"control,omitempty"`
	// Phase is the worker journal phase.
	Phase string `json:"phase,omitempty"`
	// ThreadState is the worker's last observation of the T3 thread: active,
	// stopped or missing.
	ThreadState string `json:"threadState,omitempty"`
	// PauseReason names the bucket that paused the attempt on the worker
	// host while a quota pause is in force.
	PauseReason string `json:"pauseReason,omitempty"`
	Failure     string `json:"failure,omitempty"`
	// ObservedAt is when the worker reported this; zero when the worker has
	// not reported the assignment yet.
	ObservedAt time.Time `json:"observedAt,omitzero"`
}

type Assignment struct {
	ID                  string                 `json:"id"`
	AttemptID           string                 `json:"attemptId"`
	WorkerID            string                 `json:"workerId"`
	Route               domain.ProviderRoute   `json:"route"`
	State               domain.AssignmentState `json:"state"`
	Epoch               int64                  `json:"epoch"`
	LeaseExpiresAt      time.Time              `json:"leaseExpiresAt"`
	ThreadID            string                 `json:"threadId,omitempty"`
	DispatchState       domain.DispatchState   `json:"dispatchState,omitempty"`
	DispatchRevision    int64                  `json:"dispatchRevision,omitempty"`
	DispatchConfirmedAt *time.Time             `json:"dispatchConfirmedAt,omitempty"`
	DispatchError       string                 `json:"dispatchError,omitempty"`
	CreatedAt           time.Time              `json:"createdAt"`
	UpdatedAt           time.Time              `json:"updatedAt"`
}

type Graph struct {
	GraphRevision int64       `json:"graphRevision"`
	WorkflowRunID string      `json:"workflowRunId"`
	Nodes         []GraphNode `json:"nodes"`
	Edges         []GraphEdge `json:"edges"`
}

type GraphNode struct {
	Sink      *domain.SinkTask     `json:"sink,omitempty"`
	TaskID    string               `json:"taskId"`
	Name      string               `json:"name"`
	Progress  domain.ProgressState `json:"progress"`
	Control   domain.ControlState  `json:"control,omitempty"`
	AttemptID string               `json:"attemptId,omitempty"`
}

type GraphEdge struct {
	FromTaskID string `json:"fromTaskId"`
	ToTaskID   string `json:"toTaskId"`
}

type Blocker struct {
	Code        string     `json:"code"`
	Detail      string     `json:"detail"`
	DependsOn   string     `json:"dependsOn,omitempty"`
	WorkerID    string     `json:"workerId,omitempty"`
	QuotaPoolID string     `json:"quotaPoolId,omitempty"`
	Resource    string     `json:"resource,omitempty"`
	OwnerID     string     `json:"ownerId,omitempty"`
	EarliestAt  *time.Time `json:"earliestAt,omitempty"`
	// GateID and HoldID name the supervision record a supervision blocker is
	// about. An operator reading explain has to learn which gate is waiting, not
	// only that supervision said no, and the gate ID is what the decide command
	// takes.
	GateID string `json:"gateId,omitempty"`
	HoldID string `json:"holdId,omitempty"`
	// SupervisionCode is the stable domain code behind a supervision blocker,
	// kept beside the coarser planning code so that a caller can distinguish
	// supervision-route-unavailable from supervision-gate-awaiting-review
	// without parsing the detail sentence.
	SupervisionCode domain.SupervisionBlockerCode `json:"supervisionCode,omitempty"`
}

type Explanation struct {
	WorkflowRunID string     `json:"workflowRunId"`
	TaskID        string     `json:"taskId"`
	AttemptID     string     `json:"attemptId,omitempty"`
	Eligible      bool       `json:"eligible"`
	Summary       string     `json:"summary"`
	EarliestAt    *time.Time `json:"earliestAt,omitempty"`
	Blockers      []Blocker  `json:"blockers"`
	// Details are informational findings that block nothing, such as
	// project-binding-defaulted. They never influence Eligible; a detail that
	// changed eligibility would be a blocker wearing an informational label.
	Details []string `json:"details,omitempty"`
}

type Event struct {
	ID            string                 `json:"id"`
	Sequence      int64                  `json:"sequence,omitempty"`
	WorkflowRunID string                 `json:"workflowRunId"`
	TaskID        string                 `json:"taskId,omitempty"`
	AttemptID     string                 `json:"attemptId,omitempty"`
	Kind          string                 `json:"kind"`
	TargetType    domain.AdminTargetType `json:"targetType,omitempty"`
	TargetID      string                 `json:"targetId,omitempty"`
	Actor         string                 `json:"actor,omitempty"`
	Reason        string                 `json:"reason,omitempty"`
	At            time.Time              `json:"at"`
	Detail        json.RawMessage        `json:"detail,omitempty"`
}

type Artifact struct {
	Metadata ArtifactMetadata `json:"metadata"`
	Download string           `json:"download"`
}

type ArtifactMetadata struct {
	ID            string              `json:"id"`
	WorkflowRunID string              `json:"workflowRunId"`
	TaskID        string              `json:"taskId,omitempty"`
	AttemptID     string              `json:"attemptId,omitempty"`
	Kind          domain.ArtifactKind `json:"kind"`
	Name          string              `json:"name"`
	MediaType     string              `json:"mediaType"`
	Size          int64               `json:"size"`
	SHA256        string              `json:"sha256"`
	Producer      string              `json:"producer"`
	CreatedAt     time.Time           `json:"createdAt"`
}

type Schedule struct {
	Schedule domain.Schedule  `json:"schedule"`
	Triggers []domain.Trigger `json:"triggers,omitempty"`
}

// WorkerProviderAuthorization is one provider instance the coordinator's
// effective configuration authorizes for one worker, after the fleet
// projection was applied. It is authorization and never observation: it says
// what the coordinator may route to this worker, never that the instance is
// installed there, signed in, or offering a model. The worker's inventory is
// what says that, and the two fail separately.
type WorkerProviderAuthorization struct {
	Instance string `json:"instance"`
	// QuotaPool is the pool this instance's work is charged to, empty when the
	// load did not bind it.
	QuotaPool string `json:"quotaPool,omitempty"`
	// Models is the desired model allowlist, empty for an instance authorized
	// for no model.
	Models []string `json:"models,omitempty"`
	// Dropped is why the load did not install this instance in the worker's
	// catalog (config.DroppedProviderMissingBinding or
	// config.DroppedProviderNoModels), and is empty for a bound instance. An
	// instance the coordinator dropped is in no quota pool, so this is the one
	// place a reader can learn that the fleet authorized it at all.
	Dropped string `json:"dropped,omitempty"`
}

type Worker struct {
	PoolConcurrency map[string]int `json:"poolConcurrency,omitempty"`
	// Providers is the configured provider authorization for this worker,
	// sorted by instance, or nil on a coordinator that reports none.
	Providers          []WorkerProviderAuthorization `json:"providers,omitempty"`
	State              string                        `json:"state"`
	Enrolled           bool                          `json:"enrolled"`
	Requirement        *domain.WorkerRequirement     `json:"requirement,omitempty"`
	Enrollment         *domain.WorkerEnrollment      `json:"enrollment,omitempty"`
	SnapshotAgeSeconds float64                       `json:"snapshotAgeSeconds"`
	ConcurrencySource  string                        `json:"concurrencySource"`
	Snapshot           domain.WorkerSnapshot         `json:"snapshot"`
	Health             string                        `json:"health"`
	Stale              bool                          `json:"stale"`
}

type Quota struct {
	Pool      domain.QuotaPool             `json:"pool"`
	Admission *domain.QuotaAdmissionRecord `json:"admission,omitempty"`
}

type Reservation struct {
	WorkflowRunID string   `json:"workflowRunId"`
	TaskID        string   `json:"taskId"`
	AttemptID     string   `json:"attemptId"`
	AssignmentID  string   `json:"assignmentId,omitempty"`
	QuotaPoolID   string   `json:"quotaPoolId,omitempty"`
	EstimatedCost *float64 `json:"estimatedCost,omitempty"`
	HoldsSlot     bool     `json:"holdsSlot"`
}

type ResourceLock struct {
	Name             string   `json:"name"`
	OwnerAttemptID   string   `json:"ownerAttemptId,omitempty"`
	WaiterAttemptIDs []string `json:"waiterAttemptIds,omitempty"`
}

type Command struct {
	ID               string                   `json:"id"`
	Kind             domain.AdminCommandKind  `json:"kind"`
	TargetType       domain.AdminTargetType   `json:"targetType"`
	TargetID         string                   `json:"targetId"`
	ExpectedRevision int64                    `json:"expectedRevision"`
	Reason           string                   `json:"reason"`
	RequestedBy      string                   `json:"requestedBy"`
	Payload          json.RawMessage          `json:"payload,omitempty"`
	State            domain.AdminCommandState `json:"state"`
	Failure          string                   `json:"failure,omitempty"`
	CreatedAt        time.Time                `json:"createdAt"`
	AppliedAt        *time.Time               `json:"appliedAt,omitempty"`
}

type Mutation struct {
	Version          string                  `json:"version"`
	Principal        Principal               `json:"principal"`
	ID               string                  `json:"id"`
	Kind             domain.AdminCommandKind `json:"kind"`
	WorkflowRunID    string                  `json:"workflowRunId,omitempty"`
	TaskID           string                  `json:"taskId,omitempty"`
	ScheduleID       string                  `json:"scheduleId,omitempty"`
	ExpectedRevision int64                   `json:"expectedRevision"`
	Reason           string                  `json:"reason"`
	Payload          json.RawMessage         `json:"payload,omitempty"`
}

type MutationResponse struct {
	Version       string                      `json:"version"`
	Command       Command                     `json:"command"`
	Event         Event                       `json:"event"`
	CurrentTarget *domain.AdminTargetSnapshot `json:"currentTarget,omitempty"`
}

type CommandOutcome struct {
	Version       string                   `json:"version"`
	Principal     Principal                `json:"principal"`
	CommandID     string                   `json:"commandId"`
	ExpectedState domain.AdminCommandState `json:"expectedState"`
	State         domain.AdminCommandState `json:"state"`
	Failure       string                   `json:"failure,omitempty"`
}
