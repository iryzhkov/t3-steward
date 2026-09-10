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
)

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
	Version       string    `json:"version"`
	Kind          QueryKind `json:"kind"`
	Principal     Principal `json:"principal"`
	WorkflowRunID string    `json:"workflowRunId,omitempty"`
	TaskID        string    `json:"taskId,omitempty"`
	ArtifactID    string    `json:"artifactId,omitempty"`
	Filter        Filter    `json:"filter,omitempty"`
}

type Action struct {
	Kind          QueryKind
	WorkflowRunID string
	TaskID        string
	ArtifactID    string
	Filter        Filter
}

type Response struct {
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
}

type Status struct {
	WorkflowRuns map[domain.ProgressState]int  `json:"workflowRuns"`
	Tasks        map[domain.ProgressState]int  `json:"tasks"`
	Workers      map[string]int                `json:"workers"`
	QuotaPools   map[domain.AdmissionState]int `json:"quotaPools"`
	Reservations int                           `json:"reservations"`
	Locks        int                           `json:"locks"`
}

type Progress struct {
	Total      int `json:"total"`
	Queued     int `json:"queued"`
	Blocked    int `json:"blocked"`
	Ready      int `json:"ready"`
	Active     int `json:"active"`
	NeedsInput int `json:"needsInput"`
	Verifying  int `json:"verifying"`
	Succeeded  int `json:"succeeded"`
	Failed     int `json:"failed"`
	Cancelled  int `json:"cancelled"`
	Skipped    int `json:"skipped"`
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
}

type TaskDetail struct {
	Task          domain.Task     `json:"task"`
	Attempt       *domain.Attempt `json:"attempt,omitempty"`
	Assignment    *Assignment     `json:"assignment,omitempty"`
	ThreadURL     string          `json:"threadUrl,omitempty"`
	Artifacts     []Artifact      `json:"artifacts,omitempty"`
	ResourceLocks []string        `json:"resourceLocks,omitempty"`
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
	WorkflowRunID string      `json:"workflowRunId"`
	Nodes         []GraphNode `json:"nodes"`
	Edges         []GraphEdge `json:"edges"`
}

type GraphNode struct {
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
}

type Explanation struct {
	WorkflowRunID string     `json:"workflowRunId"`
	TaskID        string     `json:"taskId"`
	AttemptID     string     `json:"attemptId,omitempty"`
	Eligible      bool       `json:"eligible"`
	Summary       string     `json:"summary"`
	EarliestAt    *time.Time `json:"earliestAt,omitempty"`
	Blockers      []Blocker  `json:"blockers"`
}

type Event struct {
	ID            string          `json:"id"`
	WorkflowRunID string          `json:"workflowRunId"`
	TaskID        string          `json:"taskId,omitempty"`
	AttemptID     string          `json:"attemptId,omitempty"`
	Kind          string          `json:"kind"`
	At            time.Time       `json:"at"`
	Detail        json.RawMessage `json:"detail,omitempty"`
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

type Worker struct {
	Snapshot domain.WorkerSnapshot `json:"snapshot"`
	Health   string                `json:"health"`
	Stale    bool                  `json:"stale"`
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
