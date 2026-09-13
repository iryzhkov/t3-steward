package backlogadmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var (
	ErrUnsupportedVersion = errors.New("unsupported backlog admin version")
	ErrInvalidQuery       = errors.New("invalid backlog admin query")
	ErrNotFound           = errors.New("backlog admin record not found")
)

type Authorizer interface {
	Authorize(context.Context, Principal, Action) error
}

type Reader interface {
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error)
	LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error)
}

type UnknownRecoveryWriter interface {
	RecoverUnknownAssignment(context.Context, domain.UnknownAssignmentRecovery) (domain.UnknownAssignmentRecoveryDecision, error)
}

type Service struct {
	graphInputRoot string
	graphValidator func(domain.Workflow, domain.Task) error
	reader         Reader
	authorizer     Authorizer
	now            func() time.Time
	artifactOpen   ArtifactOpenFunc
	runtime        RuntimeInfo
	recovery       UnknownRecoveryWriter
}

type RuntimeInfo struct {
	Mode                   string
	Owner                  string
	Epoch                  int64
	Transport              string
	MaxWorkerSnapshotAge   time.Duration
	MaxQuotaObservationAge time.Duration
}

func New(reader Reader, authorizer Authorizer) (*Service, error) {
	if reader == nil {
		return nil, fmt.Errorf("%w: reader is required", ErrInvalidQuery)
	}
	if authorizer == nil {
		return nil, fmt.Errorf("%w: authorizer is required", ErrInvalidQuery)
	}
	service := &Service{reader: reader, authorizer: authorizer, now: time.Now}
	service.recovery, _ = reader.(UnknownRecoveryWriter)
	return service, nil
}

func (s *Service) SetClock(now func() time.Time) {
	if now != nil {
		s.now = now
	}
}

// SetRuntimeInfo supplies the immutable production composition identity used by
// the status projection. Dynamic freshness and incident state is derived from
// the same durable records as the rest of the response.
func (s *Service) SetRuntimeInfo(info RuntimeInfo) { s.runtime = info }

func (s *Service) RecoverUnknown(ctx context.Context, principal Principal, request UnknownRecoveryRequest) (domain.UnknownAssignmentRecoveryDecision, error) {
	if s.recovery == nil {
		return domain.UnknownAssignmentRecoveryDecision{}, errors.New("unknown recovery is unavailable")
	}
	action := Action{Kind: QueryRecovery, AssignmentID: request.AssignmentID}
	if err := s.authorizer.Authorize(ctx, principal, action); err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, fmt.Errorf("authorize unknown recovery: %w", err)
	}
	return s.recovery.RecoverUnknownAssignment(ctx, domain.UnknownAssignmentRecovery{
		ID: request.ID, AssignmentID: request.AssignmentID, CoordinatorEpoch: request.CoordinatorEpoch,
		ExpectedAssignmentEpoch: request.ExpectedAssignmentEpoch, ExpectedAttemptRevision: request.ExpectedAttemptRevision,
		Outcome: request.Outcome, EvidenceID: request.EvidenceID, EvidenceSHA256: request.EvidenceSHA256,
		Actor: principal.ID, Reason: request.Reason, RecoveredAt: s.now().UTC(),
	})
}

func (s *Service) Query(ctx context.Context, query Query) (Response, error) {
	if query.Version != Version {
		return Response{}, fmt.Errorf("%w: got %q, want %q", ErrUnsupportedVersion, query.Version, Version)
	}
	if !validQuery(query) {
		return Response{}, fmt.Errorf("%w: kind %q or target is invalid", ErrInvalidQuery, query.Kind)
	}
	action := Action{
		Kind: query.Kind, WorkflowRunID: query.WorkflowRunID,
		TaskID: query.TaskID, ArtifactID: query.ArtifactID,
		CommandID: query.CommandID, Filter: query.Filter,
	}
	if err := s.authorizer.Authorize(ctx, query.Principal, action); err != nil {
		return Response{}, fmt.Errorf("authorize %s: %w", query.Kind, err)
	}

	records, err := s.reader.LoadCoordinatorRecords(ctx)
	if err != nil {
		return Response{}, fmt.Errorf("load coordinator snapshot: %w", err)
	}
	workers, err := s.reader.LoadWorkerSnapshots(ctx)
	if err != nil {
		return Response{}, fmt.Errorf("load worker snapshots: %w", err)
	}
	admissions, err := s.reader.LoadQuotaAdmissions(ctx)
	if err != nil {
		return Response{}, fmt.Errorf("load quota admissions: %w", err)
	}
	view := newView(records, workers, admissions, s.runtime, s.now().UTC())
	view.includeSink = query.IncludeSink
	response := Response{Version: Version, Kind: query.Kind, GeneratedAt: view.now}

	switch query.Kind {
	case QueryDiagnose:
		diagnosis, err := s.diagnose(ctx, view, query.WorkflowRunID)
		if err != nil {
			return Response{}, err
		}
		response.Diagnosis = &diagnosis
	case QueryStatus:
		status := view.status()
		response.Status = &status
	case QueryWorkflows:
		response.Workflows = view.workflowSummaries(query.Filter)
	case QueryWorkflow:
		detail, ok := view.workflowDetail(query.WorkflowRunID)
		if !ok {
			return Response{}, notFound("workflow run", query.WorkflowRunID)
		}
		response.Workflow = &detail
	case QueryGraph:
		graph, ok := view.graph(query.WorkflowRunID)
		if !ok {
			return Response{}, notFound("workflow run", query.WorkflowRunID)
		}
		response.Graph = &graph
	case QueryTask:
		detail, ok := view.taskDetail(query.WorkflowRunID, query.TaskID)
		if !ok {
			return Response{}, notFound("task", query.WorkflowRunID+"/"+query.TaskID)
		}
		response.Task = &detail
	case QueryExplanation:
		explanation, ok := view.explanation(query.WorkflowRunID, query.TaskID)
		if !ok {
			return Response{}, notFound("task", query.WorkflowRunID+"/"+query.TaskID)
		}
		response.Explanation = &explanation
	case QueryEvents:
		if _, ok := view.runs[query.WorkflowRunID]; !ok {
			return Response{}, notFound("workflow run", query.WorkflowRunID)
		}
		response.Events = view.events(query.WorkflowRunID)
	case QueryArtifacts:
		response.Artifacts = view.artifacts(query)
	case QueryArtifact:
		artifact, ok := view.artifact(query.ArtifactID)
		if !ok {
			return Response{}, notFound("artifact", query.ArtifactID)
		}
		response.Artifact = &artifact
	case QuerySchedules:
		response.Schedules = view.schedules()
	case QueryWorkers:
		response.Workers = view.workersResponse(query.Filter)
	case QueryQuota:
		response.Quotas = view.quotas(query.Filter)
	case QueryReservations:
		response.Reservations = view.reservations(query.Filter)
	case QueryLocks:
		response.ResourceLocks = view.locks(query.Filter)
	case QueryCommands:
		response.Commands = view.commands(query)
	}
	return response, nil
}

func validQuery(query Query) bool {
	switch query.Kind {
	case QueryStatus, QueryWorkflows, QuerySchedules, QueryWorkers, QueryQuota,
		QueryReservations, QueryLocks:
		return true
	case QueryCommands:
		return query.TaskID == "" || query.WorkflowRunID != ""
	case QueryWorkflow, QueryGraph, QueryEvents, QueryDiagnose:
		return query.WorkflowRunID != ""
	case QueryTask, QueryExplanation:
		return query.WorkflowRunID != "" && query.TaskID != ""
	case QueryArtifacts:
		return true
	case QueryArtifact:
		return query.ArtifactID != ""
	default:
		return false
	}
}

func notFound(kind, id string) error {
	return fmt.Errorf("%w: %s %q", ErrNotFound, kind, id)
}

type view struct {
	includeSink bool
	records     sqlite.CoordinatorRecords
	workers     []domain.WorkerSnapshot
	admissions  []domain.QuotaAdmissionRecord
	now         time.Time
	workflows   map[string]domain.Workflow
	runs        map[string]domain.WorkflowRun
	tasks       map[string]domain.Task
	attempts    map[string][]domain.Attempt
	assignments map[string]domain.Assignment
	runtime     RuntimeInfo
}

func newView(records sqlite.CoordinatorRecords, workers []domain.WorkerSnapshot, admissions []domain.QuotaAdmissionRecord, runtime RuntimeInfo, now time.Time) view {
	v := view{
		records: records, workers: workers, admissions: admissions, runtime: runtime, now: now,
		workflows: make(map[string]domain.Workflow), runs: make(map[string]domain.WorkflowRun),
		tasks: make(map[string]domain.Task), attempts: make(map[string][]domain.Attempt),
		assignments: make(map[string]domain.Assignment),
	}
	for _, item := range records.Workflows {
		v.workflows[item.ID] = item
	}
	for _, item := range records.WorkflowRuns {
		v.runs[item.ID] = item
	}
	for _, item := range records.Tasks {
		v.tasks[item.ID] = item
	}
	for _, item := range records.Attempts {
		v.attempts[item.WorkflowRunID+"\x00"+item.TaskID] = append(v.attempts[item.WorkflowRunID+"\x00"+item.TaskID], item)
	}
	for _, item := range records.Assignments {
		v.assignments[item.ID] = item
	}
	sort.Slice(v.workers, func(i, j int) bool { return v.workers[i].WorkerID < v.workers[j].WorkerID })
	sort.Slice(v.admissions, func(i, j int) bool { return v.admissions[i].QuotaPoolID < v.admissions[j].QuotaPoolID })
	return v
}

func (v view) status() Status {
	status := Status{
		WorkflowRuns: make(map[domain.ProgressState]int),
		Tasks:        make(map[domain.ProgressState]int),
		Workers:      make(map[string]int),
		QuotaPools:   make(map[domain.AdmissionState]int),
	}
	for _, run := range v.records.WorkflowRuns {
		status.WorkflowRuns[run.Progress]++
		if v.includeSink && run.Sink != nil {
			status.Tasks[run.Sink.Progress]++
		}
	}
	for _, attempts := range v.attempts {
		if attempt := latestAttempt(attempts); attempt != nil {
			status.Tasks[attempt.Progress]++
		}
	}
	for _, worker := range v.workersResponse(Filter{}) {
		status.Workers[worker.Health]++
	}
	for _, quota := range v.quotas(Filter{}) {
		state := quota.Pool.Admission
		if quota.Admission != nil {
			state = quota.Admission.Admission
		}
		status.QuotaPools[state]++
	}
	status.Reservations = len(v.reservations(Filter{}))
	status.Locks = len(v.locks(Filter{}))
	status.Runtime = v.runtimeStatus()
	return status
}

func (v view) runtimeStatus() RuntimeStatus {
	status := RuntimeStatus{
		Mode: v.runtime.Mode, Owner: v.runtime.Owner, Epoch: v.runtime.Epoch,
		Transport: v.runtime.Transport, Health: "healthy",
	}
	for _, worker := range v.workers {
		stale := !worker.Connected || worker.ObservedAt.After(v.now) || !worker.ValidUntil.After(v.now)
		if v.runtime.MaxWorkerSnapshotAge > 0 && v.now.Sub(worker.ObservedAt) > v.runtime.MaxWorkerSnapshotAge {
			stale = true
		}
		if stale {
			status.StaleWorkers++
			status.ReconciliationIssues = append(status.ReconciliationIssues, "worker:"+worker.WorkerID+":stale")
		} else {
			status.FreshWorkers++
		}
	}
	for _, admission := range v.admissions {
		stale := admission.ObservedAt.IsZero() || admission.ObservedAt.After(v.now)
		if v.runtime.MaxQuotaObservationAge > 0 && v.now.Sub(admission.ObservedAt) > v.runtime.MaxQuotaObservationAge {
			stale = true
		}
		if stale {
			status.StaleQuotaPools++
			status.ReconciliationIssues = append(status.ReconciliationIssues, "quota:"+admission.QuotaPoolID+":stale")
		} else {
			status.FreshQuotaPools++
		}
	}
	for _, assignment := range v.records.Assignments {
		if assignment.State == domain.AssignmentUnknown || assignment.DispatchState == domain.DispatchUnknown {
			status.UnknownExecutionIDs = append(status.UnknownExecutionIDs, assignment.ID)
		}
	}
	for _, artifact := range v.records.Artifacts {
		if artifact.StoragePath == "" || artifact.Size < 0 || len(artifact.SHA256) != 64 {
			status.CustodyIncidentIDs = append(status.CustodyIncidentIDs, artifact.ID)
		}
	}
	sort.Strings(status.ReconciliationIssues)
	sort.Strings(status.UnknownExecutionIDs)
	sort.Strings(status.CustodyIncidentIDs)
	if len(status.ReconciliationIssues) != 0 || len(status.UnknownExecutionIDs) != 0 || len(status.CustodyIncidentIDs) != 0 {
		status.Health = "degraded"
	}
	return status
}

func (v view) workflowSummaries(filter Filter) []WorkflowSummary {
	result := make([]WorkflowSummary, 0)
	for _, run := range v.records.WorkflowRuns {
		workflow, ok := v.workflows[run.WorkflowID]
		if !ok || !matchesWorkflowFilter(v, run, workflow, filter) {
			continue
		}
		result = append(result, WorkflowSummary{Run: run, Workflow: workflow, Progress: v.progress(run.ID, workflow.ID)})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Run.CreatedAt.Equal(result[j].Run.CreatedAt) {
			return result[i].Run.ID < result[j].Run.ID
		}
		return result[i].Run.CreatedAt.After(result[j].Run.CreatedAt)
	})
	return result
}

func matchesWorkflowFilter(v view, run domain.WorkflowRun, workflow domain.Workflow, filter Filter) bool {
	if filter.Project != "" && workflow.Project != filter.Project {
		return false
	}
	if filter.ScheduleID != "" && run.ScheduleID != filter.ScheduleID {
		return false
	}
	if filter.Class != "" && workflow.Class != filter.Class {
		return false
	}
	if len(filter.Progress) != 0 {
		found := false
		for _, state := range filter.Progress {
			found = found || state == run.Progress
		}
		if !found {
			return false
		}
	}
	if filter.WorkerID != "" || filter.QuotaPoolID != "" {
		found := false
		for _, task := range v.runTasks(run.ID) {
			attempt := latestAttempt(v.attempts[run.ID+"\x00"+task.ID])
			if attempt == nil {
				continue
			}
			assignment, ok := v.assignments[attempt.AssignmentID]
			if ok && (filter.WorkerID == "" || assignment.WorkerID == filter.WorkerID) &&
				(filter.QuotaPoolID == "" || assignment.Route.QuotaPoolID == filter.QuotaPoolID) {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (v view) progress(runID, workflowID string) Progress {
	var progress Progress
	for _, task := range v.runTasks(runID) {
		state := domain.ProgressQueued
		if attempt := latestAttempt(v.attempts[runID+"\x00"+task.ID]); attempt != nil {
			state = attempt.Progress
		}
		progress.add(state)
	}
	if sink := v.runs[runID].Sink; v.includeSink && sink != nil {
		progress.add(sink.Progress)
	}
	return progress
}
func (progress *Progress) add(state domain.ProgressState) {
	progress.Total++
	switch state {
	case domain.ProgressQueued:
		progress.Queued++
	case domain.ProgressBlocked:
		progress.Blocked++
	case domain.ProgressReady:
		progress.Ready++
	case domain.ProgressActive:
		progress.Active++
	case domain.ProgressNeedsInput:
		progress.NeedsInput++
	case domain.ProgressVerifying:
		progress.Verifying++
	case domain.ProgressSucceeded:
		progress.Succeeded++
	case domain.ProgressFailed:
		progress.Failed++
	case domain.ProgressCancelled:
		progress.Cancelled++
	case domain.ProgressSkipped:
		progress.Skipped++
	}
}

func (v view) workflowDetail(runID string) (WorkflowDetail, bool) {
	run, ok := v.runs[runID]
	if !ok {
		return WorkflowDetail{}, false
	}
	workflow, ok := v.workflows[run.WorkflowID]
	if !ok {
		return WorkflowDetail{}, false
	}
	detail := WorkflowDetail{
		Summary:       WorkflowSummary{Run: run, Workflow: workflow, Progress: v.progress(run.ID, workflow.ID)},
		Artifacts:     v.artifacts(Query{WorkflowRunID: runID}),
		ResourceLocks: v.locks(Filter{}),
		Reservations:  v.reservations(Filter{}),
	}
	for _, task := range v.runTasks(runID) {
		taskDetail, _ := v.taskDetail(runID, task.ID)
		detail.Tasks = append(detail.Tasks, taskDetail)
	}
	if run.Sink != nil {
		sink, _ := v.taskDetail(runID, run.Sink.ID)
		detail.Tasks = append(detail.Tasks, sink)
	}
	detail.ResourceLocks = filterLocksForRun(detail.ResourceLocks, detail.Tasks)
	detail.Reservations = filterReservationsForRun(detail.Reservations, runID)
	return detail, true
}

func (v view) runTasks(runID string) []domain.Task {
	tasks := append([]domain.Task(nil), domain.TasksForRun(v.runs[runID], v.records.Tasks)...)
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].Name < tasks[j].Name })
	return tasks
}

func (v view) resolveTask(runID, taskID string) (domain.Task, bool) {
	run, ok := v.runs[runID]
	if !ok {
		return domain.Task{}, false
	}
	if run.Sink != nil && (taskID == run.Sink.ID || taskID == domain.SinkTaskName) {
		return domain.Task{ID: run.Sink.ID, Name: domain.SinkTaskName, WorkflowID: run.WorkflowID, Needs: append([]string(nil), run.Sink.Needs...)}, true
	}
	for _, task := range v.runTasks(runID) {
		if task.Name == taskID || task.ID == taskID {
			return task, true
		}
	}
	return domain.Task{}, false
}

func (v view) taskDetail(runID, taskID string) (TaskDetail, bool) {
	task, ok := v.resolveTask(runID, taskID)
	if !ok {
		return TaskDetail{}, false
	}
	detail := TaskDetail{Task: task, ResourceLocks: append([]string(nil), task.ResourceLocks...)}
	if sink := v.runs[runID].Sink; sink != nil && task.ID == sink.ID {
		detail.Sink = domain.CloneSink(sink)
		return detail, true
	}
	if attempt := latestAttempt(v.attempts[runID+"\x00"+task.ID]); attempt != nil {
		detail.Attempt = attempt
		if assignment, ok := v.assignments[attempt.AssignmentID]; ok {
			projected := assignmentDTO(assignment)
			detail.Assignment = &projected
			detail.ThreadURL = v.threadURL(assignment.WorkerID, firstNonEmpty(assignment.ThreadID, attempt.ThreadID))
		} else {
			detail.ThreadURL = v.threadURL("", attempt.ThreadID)
		}
	}
	detail.Artifacts = v.artifacts(Query{WorkflowRunID: runID, TaskID: task.ID})
	return detail, true
}

func (v view) graph(runID string) (Graph, bool) {
	run, ok := v.runs[runID]
	if !ok {
		return Graph{}, false
	}
	graph := Graph{GraphRevision: run.GraphRevision, WorkflowRunID: runID, Nodes: make([]GraphNode, 0), Edges: make([]GraphEdge, 0)}
	byName := make(map[string]string)
	tasks := v.runTasks(runID)
	for _, task := range tasks {
		byName[task.Name] = task.ID
	}
	for _, task := range tasks {
		node := GraphNode{TaskID: task.ID, Name: task.Name, Progress: domain.ProgressQueued}
		if attempt := latestAttempt(v.attempts[runID+"\x00"+task.ID]); attempt != nil {
			node.Progress, node.Control, node.AttemptID = attempt.Progress, attempt.Control, attempt.ID
		}
		graph.Nodes = append(graph.Nodes, node)
		for _, ref := range task.ExternalNeeds {
			graph.Edges = append(graph.Edges, GraphEdge{FromTaskID: ref.String(), ToTaskID: task.ID})
		}
		for _, dependency := range task.Needs {
			from := byName[dependency]
			if from == "" {
				from = dependency
			}
			graph.Edges = append(graph.Edges, GraphEdge{FromTaskID: from, ToTaskID: task.ID})
		}
	}
	if run.Sink != nil {
		graph.Nodes = append(graph.Nodes, GraphNode{TaskID: run.Sink.ID, Name: run.Sink.Name, Progress: run.Sink.Progress, Sink: domain.CloneSink(run.Sink)})
		for _, id := range run.Sink.Needs {
			graph.Edges = append(graph.Edges, GraphEdge{FromTaskID: id, ToTaskID: run.Sink.ID})
		}
	}
	sort.Slice(graph.Edges, func(i, j int) bool {
		if graph.Edges[i].FromTaskID == graph.Edges[j].FromTaskID {
			return graph.Edges[i].ToTaskID < graph.Edges[j].ToTaskID
		}
		return graph.Edges[i].FromTaskID < graph.Edges[j].FromTaskID
	})
	return graph, true
}

func (v view) explanation(runID, taskID string) (Explanation, bool) {
	task, ok := v.resolveTask(runID, taskID)
	if !ok {
		return Explanation{}, false
	}
	explanation := Explanation{WorkflowRunID: runID, TaskID: task.ID, Blockers: make([]Blocker, 0)}
	if sink := v.runs[runID].Sink; sink != nil && task.ID == sink.ID {
		explanation.Summary = "coordinator sink waits for all predecessors to be terminal and execution to be quiescent"
		if sink.Progress.Terminal() {
			explanation.Summary = "coordinator sink is terminal: " + string(sink.Progress)
		}
		return explanation, true
	}
	attempt := latestAttempt(v.attempts[runID+"\x00"+task.ID])
	if attempt != nil {
		explanation.AttemptID = attempt.ID
		if attempt.Progress.Terminal() {
			explanation.Summary = "task is terminal"
			return explanation, true
		}
		if attempt.Progress == domain.ProgressNeedsInput {
			explanation.Blockers = append(explanation.Blockers, Blocker{Code: "needs-input", Detail: "task requires operator input"})
		}
		if attempt.Control == domain.ControlPaused || attempt.Control == domain.ControlPausedUncheckpointed || attempt.Control == domain.ControlDraining {
			explanation.Blockers = append(explanation.Blockers, Blocker{Code: "control", Detail: "attempt control state is " + string(attempt.Control)})
		}
	}
	for _, dependency := range task.Needs {
		dependencyTask, found := v.resolveTask(runID, dependency)
		if !found {
			explanation.Blockers = append(explanation.Blockers, Blocker{Code: "dependency", Detail: "dependency is not available", DependsOn: dependency})
			continue
		}
		dependencyAttempt := latestAttempt(v.attempts[runID+"\x00"+dependencyTask.ID])
		if dependencyAttempt == nil || dependencyAttempt.Progress != domain.ProgressSucceeded {
			explanation.Blockers = append(explanation.Blockers, Blocker{Code: "dependency", Detail: "dependency has not succeeded", DependsOn: dependencyTask.ID})
		}
	}
	if task.NotBefore != nil && task.NotBefore.After(v.now) {
		at := task.NotBefore.UTC()
		explanation.EarliestAt = &at
		explanation.Blockers = append(explanation.Blockers, Blocker{Code: "not-before", Detail: "task start window has not opened", EarliestAt: &at})
	}
	if task.ExpiresAt != nil && !task.ExpiresAt.After(v.now) {
		explanation.Blockers = append(explanation.Blockers, Blocker{Code: "expired", Detail: "task has expired"})
	}
	for _, ref := range task.ExternalNeeds {
		obs, err := domain.ResolveNode(ref, v.records.WorkflowRuns, v.records.Tasks, v.records.Attempts, v.records.Assignments)
		if err != nil || obs.ExitCode != 0 {
			explanation.Blockers = append(explanation.Blockers, Blocker{Code: "cross-run-dependency", Detail: "source has not succeeded", DependsOn: ref.String()})
		}
	}
	v.addWorkerBlocker(&explanation, task)
	v.addQuotaBlocker(&explanation, task, attempt)
	for _, lock := range v.locks(Filter{}) {
		if contains(task.ResourceLocks, lock.Name) && lock.OwnerAttemptID != "" && (attempt == nil || lock.OwnerAttemptID != attempt.ID) {
			explanation.Blockers = append(explanation.Blockers, Blocker{
				Code: "resource-lock", Detail: "resource is held by another attempt",
				Resource: lock.Name, OwnerID: lock.OwnerAttemptID,
			})
		}
	}
	sort.Slice(explanation.Blockers, func(i, j int) bool {
		left, right := explanation.Blockers[i], explanation.Blockers[j]
		return left.Code+"\x00"+left.Detail+"\x00"+left.DependsOn+"\x00"+left.Resource <
			right.Code+"\x00"+right.Detail+"\x00"+right.DependsOn+"\x00"+right.Resource
	})
	explanation.Eligible = len(explanation.Blockers) == 0 && (attempt == nil || attempt.Control == domain.ControlUnassigned)
	if explanation.Eligible {
		explanation.Summary = "task is eligible to start"
	} else if explanation.Summary == "" {
		explanation.Summary = fmt.Sprintf("task has %d blocker(s)", len(explanation.Blockers))
	}
	return explanation, true
}

func (v view) addWorkerBlocker(explanation *Explanation, task domain.Task) {
	eligible := false
	for _, snapshot := range v.workers {
		if len(task.Placement.Hosts) != 0 && !contains(task.Placement.Hosts, snapshot.WorkerID) {
			continue
		}
		if snapshot.Connected && !v.now.After(snapshot.ValidUntil) && snapshot.Inventory.AcceptBacklog &&
			snapshot.Inventory.Health == domain.WorkerHealthReady && capabilitiesInclude(snapshot.Inventory.Capabilities, task.Placement.Capabilities) {
			eligible = true
			break
		}
	}
	if !eligible {
		explanation.Blockers = append(explanation.Blockers, Blocker{Code: "worker", Detail: "no fresh ready worker satisfies placement"})
	}
}

func (v view) addQuotaBlocker(explanation *Explanation, task domain.Task, attempt *domain.Attempt) {
	poolID := ""
	if attempt != nil {
		if assignment, ok := v.assignments[attempt.AssignmentID]; ok {
			poolID = assignment.Route.QuotaPoolID
		}
	}
	if poolID == "" && len(task.Routes) != 0 {
		poolID = task.Routes[0].QuotaPoolID
	}
	if poolID == "" {
		return
	}
	state := domain.AdmissionState("")
	for _, admission := range v.admissions {
		if admission.QuotaPoolID == poolID {
			state = admission.Admission
			break
		}
	}
	if state == "" {
		for _, pool := range v.records.QuotaPools {
			if pool.ID == poolID {
				state = pool.Admission
				break
			}
		}
	}
	blocked := state == domain.AdmissionClosed || state == domain.AdmissionDraining
	blocked = blocked || task.Class == domain.TaskClassSurplus &&
		(state == domain.AdmissionConstrained || state == domain.AdmissionRecovering)
	if blocked {
		explanation.Blockers = append(explanation.Blockers, Blocker{
			Code: "quota-admission", Detail: "quota admission is " + string(state), QuotaPoolID: poolID,
		})
	}
}

func (v view) artifacts(query Query) []Artifact {
	result := make([]Artifact, 0)
	for _, metadata := range v.records.Artifacts {
		if query.WorkflowRunID != "" && metadata.WorkflowRunID != query.WorkflowRunID {
			continue
		}
		if query.TaskID != "" {
			taskID := query.TaskID
			if query.WorkflowRunID != "" {
				if task, ok := v.resolveTask(query.WorkflowRunID, taskID); ok {
					taskID = task.ID
				}
			}
			if metadata.TaskID != taskID {
				continue
			}
		}
		result = append(result, artifactDTO(metadata))
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Metadata.CreatedAt.Equal(result[j].Metadata.CreatedAt) {
			return result[i].Metadata.ID < result[j].Metadata.ID
		}
		return result[i].Metadata.CreatedAt.Before(result[j].Metadata.CreatedAt)
	})
	return result
}

func (v view) artifact(id string) (Artifact, bool) {
	for _, item := range v.records.Artifacts {
		if item.ID == id {
			return artifactDTO(item), true
		}
	}
	return Artifact{}, false
}

func artifactDTO(metadata domain.Artifact) Artifact {
	return Artifact{
		Metadata: ArtifactMetadata{
			ID: metadata.ID, WorkflowRunID: metadata.WorkflowRunID, TaskID: metadata.TaskID,
			AttemptID: metadata.AttemptID, Kind: metadata.Kind, Name: metadata.Name,
			MediaType: metadata.MediaType, Size: metadata.Size, SHA256: metadata.SHA256,
			Producer: metadata.Producer, CreatedAt: metadata.CreatedAt,
		},
		Download: "/backlog/artifacts/" + url.PathEscape(metadata.ID),
	}
}

func assignmentDTO(assignment domain.Assignment) Assignment {
	return Assignment{
		ID: assignment.ID, AttemptID: assignment.AttemptID, WorkerID: assignment.WorkerID,
		Route: assignment.Route, State: assignment.State, Epoch: assignment.Epoch,
		LeaseExpiresAt: assignment.LeaseExpiresAt, ThreadID: assignment.ThreadID,
		DispatchState: assignment.DispatchState, DispatchRevision: assignment.DispatchRevision,
		DispatchConfirmedAt: assignment.DispatchConfirmedAt, DispatchError: assignment.DispatchError,
		CreatedAt: assignment.CreatedAt, UpdatedAt: assignment.UpdatedAt,
	}
}

func (v view) schedules() []Schedule {
	result := make([]Schedule, 0, len(v.records.Schedules))
	for _, item := range v.records.Schedules {
		dto := Schedule{Schedule: item}
		for _, trigger := range v.records.Triggers {
			if trigger.ScheduleID == item.ID {
				dto.Triggers = append(dto.Triggers, trigger)
			}
		}
		sort.Slice(dto.Triggers, func(i, j int) bool {
			if dto.Triggers[i].NominalAt.Equal(dto.Triggers[j].NominalAt) {
				return dto.Triggers[i].ID < dto.Triggers[j].ID
			}
			return dto.Triggers[i].NominalAt.After(dto.Triggers[j].NominalAt)
		})
		result = append(result, dto)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Schedule.ID < result[j].Schedule.ID })
	return result
}

func (v view) workersResponse(filter Filter) []Worker {
	result := make([]Worker, 0)
	for _, snapshot := range v.workers {
		if filter.WorkerID != "" && snapshot.WorkerID != filter.WorkerID {
			continue
		}
		stale := v.now.After(snapshot.ValidUntil)
		health := string(snapshot.Inventory.Health)
		if !snapshot.Connected || stale {
			health = string(domain.WorkerHealthOffline)
		}
		result = append(result, Worker{Snapshot: snapshot, Health: health, Stale: stale})
	}
	return result
}

func (v view) quotas(filter Filter) []Quota {
	admissions := make(map[string]domain.QuotaAdmissionRecord)
	for _, item := range v.admissions {
		admissions[item.QuotaPoolID] = item
	}
	result := make([]Quota, 0)
	for _, pool := range v.records.QuotaPools {
		if filter.QuotaPoolID != "" && pool.ID != filter.QuotaPoolID {
			continue
		}
		dto := Quota{Pool: pool}
		if item, ok := admissions[pool.ID]; ok {
			copy := item
			dto.Admission = &copy
		}
		result = append(result, dto)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Pool.ID < result[j].Pool.ID })
	return result
}

func (v view) reservations(filter Filter) []Reservation {
	result := make([]Reservation, 0)
	for key, attempts := range v.attempts {
		attempt := latestAttempt(attempts)
		if attempt == nil || attempt.Progress.Terminal() {
			continue
		}
		task, ok := v.resolveTask(attempt.WorkflowRunID, attempt.TaskID)
		if !ok {
			continue
		}
		assignment, hasAssignment := v.assignments[attempt.AssignmentID]
		if filter.WorkerID != "" && (!hasAssignment || assignment.WorkerID != filter.WorkerID) {
			continue
		}
		poolID, assignmentID := "", ""
		if hasAssignment {
			poolID, assignmentID = assignment.Route.QuotaPoolID, assignment.ID
		} else if len(task.Routes) != 0 {
			poolID = task.Routes[0].QuotaPoolID
		}
		if filter.QuotaPoolID != "" && poolID != filter.QuotaPoolID {
			continue
		}
		runID := strings.SplitN(key, "\x00", 2)[0]
		result = append(result, Reservation{
			WorkflowRunID: runID, TaskID: task.ID, AttemptID: attempt.ID,
			AssignmentID: assignmentID, QuotaPoolID: poolID,
			EstimatedCost: task.EstimatedCost, HoldsSlot: attempt.Control.HoldsProviderSlot(),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].AttemptID < result[j].AttemptID })
	return result
}

func (v view) locks(filter Filter) []ResourceLock {
	type lockState struct {
		owners  []string
		waiters []string
	}
	states := make(map[string]*lockState)
	for _, attempts := range v.attempts {
		attempt := latestAttempt(attempts)
		if attempt == nil || attempt.Progress.Terminal() {
			continue
		}
		task, ok := v.resolveTask(attempt.WorkflowRunID, attempt.TaskID)
		if !ok {
			continue
		}
		assignment, assigned := v.assignments[attempt.AssignmentID]
		if filter.WorkerID != "" && (!assigned || assignment.WorkerID != filter.WorkerID) {
			continue
		}
		if filter.QuotaPoolID != "" && (!assigned || assignment.Route.QuotaPoolID != filter.QuotaPoolID) {
			continue
		}
		for _, name := range task.ResourceLocks {
			state := states[name]
			if state == nil {
				state = &lockState{}
				states[name] = state
			}
			if assigned && assignment.State != domain.AssignmentReleased && assignment.State != domain.AssignmentCompleted {
				state.owners = append(state.owners, attempt.ID)
			} else {
				state.waiters = append(state.waiters, attempt.ID)
			}
		}
	}
	names := make([]string, 0, len(states))
	for name := range states {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]ResourceLock, 0, len(names))
	for _, name := range names {
		state := states[name]
		sort.Strings(state.owners)
		sort.Strings(state.waiters)
		lock := ResourceLock{Name: name}
		if len(state.owners) != 0 {
			lock.OwnerAttemptID = state.owners[0]
			lock.WaiterAttemptIDs = append(lock.WaiterAttemptIDs, state.owners[1:]...)
		}
		lock.WaiterAttemptIDs = append(lock.WaiterAttemptIDs, state.waiters...)
		sort.Strings(lock.WaiterAttemptIDs)
		result = append(result, lock)
	}
	return result
}

func (v view) threadURL(workerID, threadID string) string {
	if threadID == "" {
		return ""
	}
	for _, worker := range v.workers {
		if worker.WorkerID == workerID && worker.Inventory.WebBaseURL != "" {
			return strings.TrimRight(worker.Inventory.WebBaseURL, "/") + "/thread/" + url.PathEscape(threadID)
		}
	}
	return ""
}

func (v view) events(runID string) []Event {
	taskIDs := make(map[string]struct{})
	attemptIDs := make(map[string]struct{})
	for _, task := range v.runTasks(runID) {
		taskIDs[task.ID] = struct{}{}
	}
	result := make([]Event, 0)
	if run, ok := v.runs[runID]; ok {
		result = append(result, event("run:"+run.ID, runID, "", "", "workflow-run-created", run.CreatedAt, map[string]any{"progress": run.Progress, "revision": run.Revision}))
	}
	for _, trigger := range v.records.Triggers {
		if trigger.WorkflowRunID == runID {
			result = append(result, event("trigger:"+trigger.ID, runID, "", "", "schedule-trigger-"+string(trigger.State), trigger.ObservedAt, map[string]any{"scheduleId": trigger.ScheduleID, "reason": trigger.Reason}))
		}
	}
	for _, attempt := range v.records.Attempts {
		if attempt.WorkflowRunID == runID {
			attemptIDs[attempt.ID] = struct{}{}
			result = append(result, event("attempt:"+attempt.ID, runID, attempt.TaskID, attempt.ID, "attempt-"+string(attempt.Progress), attempt.UpdatedAt, map[string]any{"control": attempt.Control, "revision": attempt.Revision}))
		}
	}
	for _, assignment := range v.records.Assignments {
		if _, ok := attemptIDs[assignment.AttemptID]; ok {
			result = append(result, event("assignment:"+assignment.ID, runID, "", assignment.AttemptID, "assignment-"+string(assignment.State), assignment.UpdatedAt, map[string]any{"workerId": assignment.WorkerID, "quotaPoolId": assignment.Route.QuotaPoolID}))
		}
	}
	for _, artifact := range v.records.Artifacts {
		if artifact.WorkflowRunID == runID {
			result = append(result, event("artifact:"+artifact.ID, runID, artifact.TaskID, artifact.AttemptID, "artifact-"+string(artifact.Kind), artifact.CreatedAt, map[string]any{"artifactId": artifact.ID, "name": artifact.Name}))
		}
	}

	durableCommands := make(map[string]struct{})
	for _, audit := range v.records.AuditEvents {
		if audit.WorkflowRunID != runID {
			continue
		}
		result = append(result, auditEventDTO(audit))
		for _, command := range v.records.AdminCommands {
			if strings.HasPrefix(audit.ID, "admin-command:"+command.ID+":") {
				durableCommands[command.ID] = struct{}{}
			}
		}
	}
	for _, command := range v.records.AdminCommands {
		if _, durable := durableCommands[command.ID]; durable {
			continue
		}
		_, taskTarget := taskIDs[command.TargetID]
		_, attemptTarget := attemptIDs[command.TargetID]
		if (command.TargetType == domain.AdminTargetWorkflowRun && command.TargetID == runID) ||
			(command.TargetType == domain.AdminTargetAttempt && attemptTarget) ||
			(command.TargetType == "task" && taskTarget) {
			result = append(result, event("admin-command:"+command.ID, runID, command.TargetID, "", "admin-command-"+string(command.State), command.CreatedAt, map[string]any{"kind": command.Kind, "requestedBy": command.RequestedBy, "reason": command.Reason}))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].At.Equal(result[j].At) {
			if result[i].Sequence != result[j].Sequence {
				return result[i].Sequence < result[j].Sequence
			}
			return result[i].ID < result[j].ID
		}
		return result[i].At.Before(result[j].At)
	})
	return result
}

func event(id, runID, taskID, attemptID, kind string, at time.Time, detail any) Event {
	raw, _ := json.Marshal(detail)
	return Event{ID: id, WorkflowRunID: runID, TaskID: taskID, AttemptID: attemptID, Kind: kind, At: at, Detail: raw}
}

func latestAttempt(attempts []domain.Attempt) *domain.Attempt {
	if len(attempts) == 0 {
		return nil
	}
	latest := attempts[0]
	for _, item := range attempts[1:] {
		if item.Number > latest.Number || (item.Number == latest.Number && item.Revision > latest.Revision) {
			latest = item
		}
	}
	return &latest
}

func filterReservationsForRun(items []Reservation, runID string) []Reservation {
	result := make([]Reservation, 0)
	for _, item := range items {
		if item.WorkflowRunID == runID {
			result = append(result, item)
		}
	}
	return result
}

func filterLocksForRun(items []ResourceLock, tasks []TaskDetail) []ResourceLock {
	attempts := make(map[string]struct{})
	for _, task := range tasks {
		if task.Attempt != nil {
			attempts[task.Attempt.ID] = struct{}{}
		}
	}
	result := make([]ResourceLock, 0)
	for _, item := range items {
		_, owner := attempts[item.OwnerAttemptID]
		waiters := make([]string, 0)
		for _, waiter := range item.WaiterAttemptIDs {
			if _, ok := attempts[waiter]; ok {
				waiters = append(waiters, waiter)
			}
		}
		if owner || len(waiters) != 0 {
			item.WaiterAttemptIDs = waiters
			result = append(result, item)
		}
	}
	return result
}

func capabilitiesInclude(have, required []string) bool {
	for _, wanted := range required {
		if !contains(have, wanted) {
			return false
		}
	}
	return true
}

func contains(items []string, wanted string) bool {
	for _, item := range items {
		if item == wanted {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
