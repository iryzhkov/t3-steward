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

	"github.com/iryzhkov/t3-steward/internal/backlog"
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

// QuarantineWriter clears one intake quarantine deliberately. It is a
// capability of the reader for the same reason QuarantineReader is: a store
// that predates the marker answers that it does not record one.
type QuarantineWriter interface {
	ReleaseQuarantinedSubmission(ctx context.Context, key, actor, reason string, at time.Time) (domain.QuarantineRelease, error)
}

// QuarantineReader lists the intake submissions that were refused permanently.
// It is a capability of the reader rather than part of Reader, so a store that
// predates the quarantine marker still satisfies the service and answers the
// query with "unavailable" instead of failing to compile.
type QuarantineReader interface {
	ListQuarantinedSubmissions(context.Context) ([]domain.SubmissionRecord, error)
}

type Service struct {
	enrollWorker   WorkerEnrollmentHandler
	graphInputRoot string
	graphValidator func(domain.Workflow, domain.Task) error
	reader         Reader
	authorizer     Authorizer
	now            func() time.Time
	artifactOpen   ArtifactOpenFunc
	runtime        RuntimeInfo
	recovery       UnknownRecoveryWriter
	quarantine     QuarantineReader
	quarantineOps  QuarantineWriter
	// supervision is the durable half of the supervision family. It is set
	// explicitly rather than asserted from the reader because the binding is
	// phrased in this package's types and the store cannot name them.
	supervision SupervisionStore
	// supervisorClientConfigured reports that this coordinator has a supervisor
	// admin client and can therefore dispatch an overseer at all. It is
	// configuration, not state, so it is supplied rather than read.
	supervisorClientConfigured bool

	viabilitySettings ViabilitySettings
}

type RuntimeInfo struct {
	Release                string
	ConfigurationDigest    string
	LastReload             time.Time
	Mode                   string
	Owner                  string
	Epoch                  int64
	Transport              string
	MaxWorkerSnapshotAge   time.Duration
	MaxQuotaObservationAge time.Duration
	// CatalogIssues names the configuration this coordinator could not use, one
	// line per problem. A misconfigured project is isolated so that it disables
	// itself rather than the fleet, which means nothing else goes wrong to make
	// the operator look; the coordinator has to say so instead.
	CatalogIssues []string
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
	service.quarantine, _ = reader.(QuarantineReader)
	service.quarantineOps, _ = reader.(QuarantineWriter)
	return service, nil
}

// SetSupervisionStore binds the durable supervision operations. A service with
// no supervision store answers the operation as unavailable rather than
// failing, which is what a coordinator store that predates supervision gets.
func (s *Service) SetSupervisionStore(store SupervisionStore) { s.supervision = store }

// SetSupervisorClientConfigured records whether this coordinator has exactly
// one backlog_v2.coordinator.admin_clients entry with supervisor: true.
//
// Without one no activation is ever dispatched, so every gate of a supervised
// run waits for an operator. Explain, the readiness matrix and "campaign
// supervision show" all have to say so, and none of them can discover it: the
// admin clients are the coordinator process's own configuration and appear in
// no record a query reads.
func (s *Service) SetSupervisorClientConfigured(configured bool) {
	s.supervisorClientConfigured = configured
}

// SupervisionSnapshotSource is the optional readiness snapshot of one run.
//
// It is separate from SupervisionStore because it answers a different question
// with a different cost: LoadSupervision reads the whole reviewable picture for
// one run an operator asked about, while this reads the compact predicate input
// for every supervised run an explanation may touch.
type SupervisionSnapshotSource interface {
	LoadSupervisionSnapshot(ctx context.Context, runID string) (domain.SupervisionSnapshot, error)
}

// supervisionSnapshots resolves the readiness snapshot of every supervised run,
// with RouteAvailable answered honestly.
//
// The store reports RouteAvailable true because provider admission is not
// observable from a database. It is observable here, from the same worker
// snapshots and the same rule the planner's own pass uses, so explain reports
// the supervision blocker the planner applied instead of reporting a
// gate-protected task as eligible to start.
func (s *Service) supervisionSnapshots(
	ctx context.Context,
	records sqlite.CoordinatorRecords,
	workers []domain.WorkerSnapshot,
) (map[string]domain.SupervisionSnapshot, error) {
	source, ok := s.supervisionSnapshotSource()
	if !ok {
		return nil, nil
	}
	var snapshots map[string]domain.SupervisionSnapshot
	for _, run := range records.WorkflowRuns {
		if run.Supervision == nil {
			continue
		}
		snapshot, err := source.LoadSupervisionSnapshot(ctx, run.ID)
		if err != nil {
			return nil, fmt.Errorf("load supervision snapshot of run %q: %w", run.ID, err)
		}
		if !snapshot.Supervised {
			continue
		}
		snapshot.RouteAvailable = backlog.SupervisionRouteAvailable(backlog.SupervisionRouteRequest{
			Route: run.Supervision.Config.Route, Workers: workers,
			SupervisorClientConfigured: s.supervisorClientConfigured,
		})
		if snapshots == nil {
			snapshots = make(map[string]domain.SupervisionSnapshot)
		}
		snapshots[run.ID] = snapshot
	}
	return snapshots, nil
}

// supervisionSnapshotSource prefers the explicitly bound supervision store and
// falls back to the reader, which is how every other optional capability of
// this service is reached.
func (s *Service) supervisionSnapshotSource() (SupervisionSnapshotSource, bool) {
	if source, ok := s.supervision.(SupervisionSnapshotSource); ok {
		return source, true
	}
	source, ok := s.reader.(SupervisionSnapshotSource)
	return source, ok
}

// supervisionStore returns the bound store, or the reader when it happens to
// satisfy the interface itself, which is what an in-process test fixture does.
func (s *Service) supervisionStore() (SupervisionStore, bool) {
	if s.supervision != nil {
		return s.supervision, true
	}
	store, ok := s.reader.(SupervisionStore)
	return store, ok
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

	view, err := s.loadView(ctx)
	if err != nil {
		return Response{}, err
	}
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
	case QueryQuarantine:
		quarantined, err := s.quarantinedIntake(ctx)
		if err != nil {
			return Response{}, err
		}
		response.Quarantine = quarantined
	case QueryViability:
		if !s.viabilitySettings.configured() {
			return Response{}, fmt.Errorf("%w: this coordinator has no project catalog to check against", ErrInvalidQuery)
		}
		matrix := view.viability(ctx, s.viabilitySettings, *query.Viability)
		response.Viability = &matrix
	}
	return response, nil
}

// loadView reads the one consistent snapshot every query is answered from.
func (s *Service) loadView(ctx context.Context) (view, error) {
	records, err := s.reader.LoadCoordinatorRecords(ctx)
	if err != nil {
		return view{}, fmt.Errorf("load coordinator snapshot: %w", err)
	}
	workers, err := s.reader.LoadWorkerSnapshots(ctx)
	if err != nil {
		return view{}, fmt.Errorf("load worker snapshots: %w", err)
	}
	admissions, err := s.reader.LoadQuotaAdmissions(ctx)
	if err != nil {
		return view{}, fmt.Errorf("load quota admissions: %w", err)
	}
	loaded := newView(records, workers, admissions, s.runtime, s.now().UTC())
	loaded.supervisorClientConfigured = s.supervisorClientConfigured
	if loaded.supervision, err = s.supervisionSnapshots(ctx, records, workers); err != nil {
		return view{}, err
	}
	if waits, ok := s.reader.(interface {
		ListTaskWaits(context.Context) ([]domain.TaskWait, error)
	}); ok {
		if loaded.taskWaits, err = waits.ListTaskWaits(ctx); err != nil {
			return view{}, fmt.Errorf("load task-bound waits: %w", err)
		}
	}
	if reader, ok := s.reader.(interface {
		LoadWorkerRequirements(context.Context) ([]domain.WorkerRequirement, error)
		LoadWorkerEnrollments(context.Context) ([]domain.WorkerEnrollment, error)
	}); ok {
		loaded.requirements, err = reader.LoadWorkerRequirements(ctx)
		if err != nil {
			return view{}, err
		}
		loaded.enrollments, err = reader.LoadWorkerEnrollments(ctx)
		if err != nil {
			return view{}, err
		}
	}
	return loaded, nil
}

func validQuery(query Query) bool {
	switch query.Kind {
	case QueryStatus, QueryWorkflows, QuerySchedules, QueryWorkers, QueryQuota,
		QueryReservations, QueryLocks, QueryQuarantine:
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
	case QueryViability:
		// A viability query answers about tasks it was given. An empty request
		// would otherwise report a ready campaign with nothing in it.
		return query.Viability != nil && len(query.Viability.Tasks) != 0
	default:
		return false
	}
}

func notFound(kind, id string) error {
	return fmt.Errorf("%w: %s %q", ErrNotFound, kind, id)
}

type view struct {
	requirements []domain.WorkerRequirement
	enrollments  []domain.WorkerEnrollment
	includeSink  bool
	records      sqlite.CoordinatorRecords
	workers      []domain.WorkerSnapshot
	admissions   []domain.QuotaAdmissionRecord
	now          time.Time
	workflows    map[string]domain.Workflow
	runs         map[string]domain.WorkflowRun
	tasks        map[string]domain.Task
	attempts     map[string][]domain.Attempt
	assignments  map[string]domain.Assignment
	runtime      RuntimeInfo
	// supervision is the readiness snapshot of every supervised run in records,
	// keyed by run ID. An absent run is unsupervised, which is every run on a
	// coordinator that has never accepted a supervised campaign.
	supervision map[string]domain.SupervisionSnapshot
	// supervisorClientConfigured is this coordinator's own configuration; see
	// Service.SetSupervisorClientConfigured.
	supervisorClientConfigured bool
	// taskWaits are every task-bound wait the reader owns, live or settled.
	// A reader that owns none, which is every reader that is not the
	// coordinator store, leaves it nil.
	taskWaits []domain.TaskWait
}

func newView(records sqlite.CoordinatorRecords, workers []domain.WorkerSnapshot, admissions []domain.QuotaAdmissionRecord, runtime RuntimeInfo, now time.Time) view {
	// Retained quota history is not current admission when checks are disabled.
	disabled := map[string]bool{}
	for _, pool := range records.QuotaPools {
		disabled[pool.ID] = pool.ChecksDisabled
	}
	currentAdmissions := make([]domain.QuotaAdmissionRecord, 0, len(admissions))
	for _, item := range admissions {
		if !disabled[item.QuotaPoolID] {
			currentAdmissions = append(currentAdmissions, item)
		}
	}
	admissions = currentAdmissions
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
		Release: v.runtime.Release, ConfigurationDigest: v.runtime.ConfigurationDigest, LastReload: v.runtime.LastReload,
		Transport: v.runtime.Transport, Health: "healthy",
	}
	for _, worker := range v.workers {
		stale := !worker.Connected || worker.ObservedAt.After(v.now) || !worker.ValidUntil.After(v.now) || (v.runtime.Epoch > 0 && worker.CoordinatorEpoch != v.runtime.Epoch)
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
	status.ReconciliationIssues = append(status.ReconciliationIssues, v.runtime.CatalogIssues...)
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
	case domain.ProgressWaitingExternal:
		progress.WaitingExternal++
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
	detail.Waits = v.runWaits(runID)
	detail.Gates = v.runGates(runID)
	return detail, true
}

// runWaits lists the live task-bound waits of one run, oldest registration
// first. A settled wait has an outcome and no longer parks its attempt, so it
// is not a reason the run is waiting and is left out.
func (v view) runWaits(runID string) []TaskWaitDetail {
	var waits []TaskWaitDetail
	for _, wait := range v.taskWaits {
		if wait.WorkflowRunID != runID || !wait.Live() {
			continue
		}
		detail := TaskWaitDetail{
			ID: wait.ID, TaskID: wait.TaskID, TaskName: v.tasks[wait.TaskID].Name, AttemptID: wait.AttemptID,
			Name: wait.Name, Condition: wait.Condition,
			RegisteredAt: wait.RegisteredAt, Deadline: wait.Deadline,
		}
		if wait.Result != nil {
			code := wait.Result.ExitCode
			detail.LastExitCode = &code
		}
		waits = append(waits, detail)
	}
	sort.SliceStable(waits, func(i, j int) bool {
		if !waits[i].RegisteredAt.Equal(waits[j].RegisteredAt) {
			return waits[i].RegisteredAt.Before(waits[j].RegisteredAt)
		}
		return waits[i].ID < waits[j].ID
	})
	return waits
}

// runGates lists the gates of a supervised run by name, each with the
// observed tasks it is still missing evidence from. The gate state comes from
// the same readiness snapshot explain and the planner use; the evidence gaps
// are read off the attempt records, where a task counts as having produced
// evidence once its latest attempt succeeded.
func (v view) runGates(runID string) []GateDetail {
	snapshot, ok := v.supervision[runID]
	if !ok || !snapshot.Supervised {
		return nil
	}
	gates := make([]GateDetail, 0, len(snapshot.Gates))
	for _, gate := range snapshot.Gates {
		detail := GateDetail{
			ID: gate.Definition.ID, Name: gate.Definition.Name, State: gate.State, Final: gate.Definition.Final,
			ObservedTaskIDs:  append([]string(nil), gate.Definition.ObservedTaskIDs...),
			ProtectedTaskIDs: append([]string(nil), gate.Definition.ProtectedTaskIDs...),
		}
		if detail.State == "" {
			detail.State = domain.GatePendingEvidence
		}
		for _, taskID := range gate.Definition.ObservedTaskIDs {
			progress := domain.ProgressQueued
			if attempt := latestAttempt(v.attempts[runID+"\x00"+taskID]); attempt != nil {
				progress = attempt.Progress
			}
			if progress == domain.ProgressSucceeded {
				continue
			}
			detail.MissingEvidence = append(detail.MissingEvidence, GateEvidenceGap{
				TaskID: taskID, TaskName: v.tasks[taskID].Name, Progress: progress,
			})
		}
		gates = append(gates, detail)
	}
	sort.SliceStable(gates, func(i, j int) bool { return gates[i].Name < gates[j].Name })
	return gates
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
	v.addSupervisionBlocker(&explanation, runID, task)
	v.addWorkerBlocker(&explanation, task)
	v.addRouteBlocker(&explanation, task, attempt)
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
	switch {
	case explanation.Eligible:
		explanation.Summary = "task is eligible to start"
	case explanation.Summary != "":
	case len(explanation.Blockers) == 0:
		// Ineligible with nothing blocking it reads as a contradiction, so
		// name the actual reason: the attempt is already under a control
		// decision and is therefore not waiting on anything. Only an
		// existing attempt can reach this case, because a task with no
		// attempt and no blockers is eligible.
		explanation.Summary = fmt.Sprintf("task is not waiting: its current attempt is %s", attempt.Control)
	default:
		explanation.Summary = fmt.Sprintf("task has %d blocker(s)", len(explanation.Blockers))
	}
	return explanation, true
}

// addSupervisionBlocker reports the supervision verdict the planner applied to
// this task, through the planner's own function rather than through a second
// implementation of the gate and hold rules.
//
// A second implementation is exactly what was missing. The planner withheld a
// gate-protected task while explain, which consulted no supervision state at
// all, reported the same task as "eligible to start" with no blockers, so the
// one command an operator runs to find out why nothing is happening said the
// opposite of what was happening.
func (v view) addSupervisionBlocker(explanation *Explanation, runID string, task domain.Task) {
	for _, blocker := range backlog.SupervisionPlanningBlockers(v.supervision, runID, task) {
		detail := blocker.Detail
		if blocker.SupervisionCode == domain.SupervisionBlockerRouteUnavailable {
			// The route-unavailable blocker is the one whose remedy is not on the
			// run at all, so it names the cause an operator can act on.
			detail = detail + ": " +
				backlog.SupervisionRouteBlockCause(v.supervisorClientConfigured)
		}
		explanation.Blockers = append(explanation.Blockers, Blocker{
			Code: blocker.Code, Detail: detail, SupervisionCode: blocker.SupervisionCode,
			GateID: blocker.GateID, HoldID: blocker.HoldID,
		})
	}
}

// addWorkerBlocker explains placement through the same matcher the planner uses,
// so a blocked task reports why each worker was excluded rather than only that
// none was suitable.
//
// This deliberately does not reimplement eligibility. A second copy of the rules
// drifts from the planner's, and it cannot report a reason the planner knows
// about: an operator whose task requires a device no host provides was told
// "no fresh ready worker satisfies placement", which names neither the
// requirement nor the host that failed it.
func (v view) addWorkerBlocker(explanation *Explanation, task domain.Task) {
	// Staleness stays the view's own decision, which already accounts for the
	// snapshot's validity window and the coordinator epoch. A stale worker is
	// dropped here rather than re-judged by the matcher, so this change adds
	// reasons without moving the freshness boundary.
	inventories := make([]domain.WorkerInventory, 0)
	var considered, stale int
	for _, worker := range v.workersResponse(Filter{}) {
		if worker.Snapshot.Inventory.ID == "" || (worker.Requirement != nil && !worker.Enrolled) {
			continue
		}
		considered++
		if worker.State != "observed" || worker.Stale || !worker.Snapshot.Connected {
			stale++
			continue
		}
		// Freshness has already been judged, so the inventory is presented as
		// observed now. Leaving the original timestamp would let the matcher
		// apply a second, different staleness rule and report a worker as stale
		// that this view just accepted as fresh.
		inventory := worker.Snapshot.Inventory
		inventory.ObservedAt = v.now
		inventories = append(inventories, inventory)
	}
	if len(inventories) == 0 {
		detail := "no enrolled worker has reported an inventory"
		if considered != 0 {
			detail = fmt.Sprintf("all %d enrolled worker(s) are stale or disconnected", stale)
		}
		explanation.Blockers = append(explanation.Blockers, Blocker{Code: "worker", Detail: detail})
		return
	}
	// MatchWorkers requires a positive bound; freshness was applied above, so
	// this one is deliberately not binding.
	placement, err := backlog.MatchWorkers(backlog.WorkerPlacementRequest{
		Task: task, Now: v.now, MaxSnapshotAge: time.Duration(1 << 62),
	}, inventories)
	if err != nil {
		explanation.Blockers = append(explanation.Blockers, Blocker{
			Code: "worker", Detail: "placement could not be evaluated: " + err.Error(),
		})
		return
	}
	if len(placement.EligibleWorkerIDs) != 0 {
		return
	}
	for _, evaluation := range placement.Evaluations {
		for _, exclusion := range evaluation.Exclusions {
			explanation.Blockers = append(explanation.Blockers, Blocker{
				Code: exclusion.Code, Detail: exclusion.Detail, WorkerID: evaluation.WorkerID,
			})
		}
	}
}

// addRouteBlocker explains a task that no configured provider route can serve.
//
// Placement and routing are separate decisions: a worker can satisfy every
// capability, class and capacity requirement and still be unable to run a task
// whose declared model it does not offer. Reporting only placement therefore
// called such a task eligible, which is worse than saying nothing, because a
// task that will never be assigned looked ready to start.
func (v view) addRouteBlocker(explanation *Explanation, task domain.Task, attempt *domain.Attempt) {
	if len(task.Routes) == 0 {
		return
	}
	inventories := make([]domain.WorkerInventory, 0)
	for _, worker := range v.workersResponse(Filter{}) {
		if worker.Snapshot.Inventory.ID == "" || (worker.Requirement != nil && !worker.Enrolled) {
			continue
		}
		if worker.State != "observed" || worker.Stale || !worker.Snapshot.Connected {
			continue
		}
		inventories = append(inventories, worker.Snapshot.Inventory)
	}
	if len(inventories) == 0 {
		return
	}
	current := domain.Attempt{}
	if attempt != nil {
		current = *attempt
	}
	resolved, err := backlog.ResolveProviderRoutePools(task, current, inventories, v.records.QuotaPools)
	if err != nil || len(resolved) != 0 {
		return
	}
	models := make([]string, 0, len(task.Routes))
	for _, route := range task.Routes {
		models = append(models, route.ProviderInstanceID+"/"+route.Model)
	}
	explanation.Blockers = append(explanation.Blockers, Blocker{
		Code: "provider-route-unavailable",
		Detail: fmt.Sprintf("no enrolled worker offers any declared route (%s)",
			strings.Join(models, ", ")),
	})
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
	snapshots := append([]domain.WorkerSnapshot(nil), v.workers...)
	seen := make(map[string]bool)
	for _, snapshot := range snapshots {
		seen[snapshot.WorkerID] = true
	}
	for _, requirement := range v.requirements {
		if !seen[requirement.WorkerID] {
			snapshots = append(snapshots, domain.WorkerSnapshot{WorkerID: requirement.WorkerID})
		}
	}
	for _, snapshot := range snapshots {
		if filter.WorkerID != "" && snapshot.WorkerID != filter.WorkerID {
			continue
		}
		stale := !snapshot.ValidUntil.After(v.now) || snapshot.ObservedAt.After(v.now) || (v.runtime.Epoch > 0 && snapshot.CoordinatorEpoch != v.runtime.Epoch) || (v.runtime.MaxWorkerSnapshotAge > 0 && v.now.Sub(snapshot.ObservedAt) > v.runtime.MaxWorkerSnapshotAge)
		health := string(snapshot.Inventory.Health)
		if !snapshot.Connected || stale {
			health = string(domain.WorkerHealthOffline)
		}
		dto := Worker{Snapshot: snapshot, Health: health, Stale: stale, State: "observed", ConcurrencySource: "coordinator-quota-pools"}
		for _, provider := range snapshot.Inventory.Providers {
			for _, pool := range v.records.QuotaPools {
				if pool.ID == provider.QuotaPoolID {
					if dto.PoolConcurrency == nil {
						dto.PoolConcurrency = map[string]int{}
					}
					dto.PoolConcurrency[pool.ID] = pool.MaxConcurrent
				}
			}
		}
		if !snapshot.ObservedAt.IsZero() {
			dto.SnapshotAgeSeconds = max(0, v.now.Sub(snapshot.ObservedAt).Seconds())
		}
		if stale || !snapshot.Connected {
			dto.State = "stale"
		}
		for _, requirement := range v.requirements {
			if requirement.WorkerID != snapshot.WorkerID {
				continue
			}
			copy := requirement
			dto.Requirement = &copy
			dto.State = "configured"
			for _, enrollment := range v.enrollments {
				if enrollment.Request.WorkerID != snapshot.WorkerID {
					continue
				}
				copy := enrollment
				dto.Enrollment = &copy
				dto.Enrolled = enrollment.Request.CatalogRevision == requirement.CatalogRevision && enrollment.WorkerEpoch == requirement.WorkerEpoch && enrollment.CredentialRef == requirement.CredentialRef && enrollment.Connection == requirement.Connection
				if !dto.Enrolled {
					dto.State = "draining"
				}
			}
			if dto.Enrolled {
				dto.State = "enrolled"
				if stale || !snapshot.Connected {
					dto.State = "stale"
				} else if snapshot.WorkerEpoch != requirement.WorkerEpoch {
					dto.State = "recovery-required"
				} else if snapshot.Inventory.CatalogRevision != requirement.CatalogRevision || !snapshot.Inventory.AcceptBacklog {
					dto.State = "draining"
				} else {
					dto.State = "observed"
				}
			}
			if requirement.Draining || requirement.Connection == "removed" {
				dto.State = "draining"
			}
		}
		result = append(result, dto)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Snapshot.WorkerID < result[j].Snapshot.WorkerID })
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
