package backlogadmin

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// RepositoryObserver answers whether one worker can read one repository and
// ref under the credential references the real task would use. It is a seam
// because the observation belongs on the worker: the coordinator's credentials
// and network position are not the worker's, and a coordinator-side answer
// would report success for a repository the worker cannot reach.
//
// A coordinator with no observer reports nothing about reachability rather than
// reporting that it is fine, because an unasked question must never read as a
// passed one.
type RepositoryObserver interface {
	ObserveRepository(context.Context, backlog.RepositoryProbeKey) (backlog.RepositoryProbeObservation, error)
}

// CredentialResolver reports which of the credential references a project
// requires are unavailable to one worker. It answers with references, never
// with values.
type CredentialResolver interface {
	MissingCredentials(context.Context, string, []string) ([]string, error)
}

// ViabilitySettings is what a viability answer needs beyond the coordinator's
// own records.
type ViabilitySettings struct {
	// Catalog is the project catalog this coordinator would resolve the
	// campaign against. Without it the unknown-catalog-entry findings cannot be
	// made, and a viability query is refused rather than answered incompletely.
	Catalog *backlog.ProjectCatalog
	// MaxBundleBytes and MaxBundleFiles are the coordinator's message limits.
	MaxBundleBytes int64
	MaxBundleFiles int
	Repository     RepositoryObserver
	Credentials    CredentialResolver
}

// SetViability supplies the catalog, limits and observers a viability query
// needs. A coordinator that never sets them refuses the query rather than
// answering it from an empty catalog.
func (s *Service) SetViability(settings ViabilitySettings) { s.viabilitySettings = settings }

// viability composes the per-task, per-worker matrix.
func (v view) viability(ctx context.Context, settings ViabilitySettings, request ViabilityRequest) ViabilityMatrix {
	matrix := ViabilityMatrix{SchemaVersion: ViabilityMatrixSchemaVersion}
	if settings.MaxBundleBytes > 0 && request.BundleBytes > settings.MaxBundleBytes {
		matrix.Reasons = append(matrix.Reasons, newViabilityReason(ReasonMessageLimitExceeded,
			fmt.Sprintf("the bundle is %d bytes and this coordinator accepts at most %d",
				request.BundleBytes, settings.MaxBundleBytes)))
	}
	if settings.MaxBundleFiles > 0 && request.BundleFiles > settings.MaxBundleFiles {
		matrix.Reasons = append(matrix.Reasons, newViabilityReason(ReasonMessageLimitExceeded,
			fmt.Sprintf("the bundle has %d files and this coordinator accepts at most %d",
				request.BundleFiles, settings.MaxBundleFiles)))
	}
	workers := v.viabilityWorkers()
	for _, task := range request.Tasks {
		matrix.Tasks = append(matrix.Tasks, v.viabilityTask(ctx, settings, task, workers))
	}
	matrix.Outcome = matrixOutcome(matrix)
	return matrix
}

func matrixOutcome(matrix ViabilityMatrix) ViabilityOutcome {
	outcome := outcomeFor(matrix.Reasons)
	if outcome == ViabilityImpossible {
		return ViabilityImpossible
	}
	for _, task := range matrix.Tasks {
		switch task.Outcome {
		case ViabilityImpossible:
			return ViabilityImpossible
		case ViabilityAcceptedWaiting:
			outcome = ViabilityAcceptedWaiting
		}
	}
	return outcome
}

// viabilityWorker is one worker as the matrix sees it, with its requirement and
// enrollment kept alongside the observed inventory rather than used to drop it.
type viabilityWorker struct {
	id          string
	dto         Worker
	inventory   domain.WorkerInventory
	hasSnapshot bool
}

// viabilityWorkers lists every worker the fleet knows about.
//
// It deliberately keeps a worker whose requirement no longer matches its
// enrollment. The explanation path drops such a worker before routing is
// evaluated, which is exactly the blind spot this query exists to close: the
// operator was told "no eligible worker" when the truth was a catalog digest
// mismatch that re-enrolment fixes.
func (v view) viabilityWorkers() []viabilityWorker {
	workers := make([]viabilityWorker, 0)
	for _, dto := range v.workersResponse(Filter{}) {
		id := dto.Snapshot.WorkerID
		if id == "" && dto.Requirement != nil {
			id = dto.Requirement.WorkerID
		}
		if id == "" {
			continue
		}
		candidate := viabilityWorker{id: id, dto: dto}
		if dto.Snapshot.Inventory.ID != "" {
			candidate.hasSnapshot = true
			candidate.inventory = dto.Snapshot.Inventory
			// Freshness has already been judged by the view, which accounts for
			// the validity window and the coordinator epoch. Presenting the
			// inventory as observed now keeps the matcher from applying a
			// second, different staleness rule to the same snapshot.
			candidate.inventory.ObservedAt = v.now
		}
		workers = append(workers, candidate)
	}
	return workers
}

func (v view) viabilityTask(ctx context.Context, settings ViabilitySettings, task ViabilityTask, workers []viabilityWorker) ViabilityTaskResult {
	result := ViabilityTaskResult{Task: task.Name, Candidates: make([]ViabilityCandidate, 0, len(workers))}
	project, known := settings.Catalog.Project(task.Project)
	if !known {
		result.Reasons = append(result.Reasons, newViabilityReason(ReasonUnknownProject,
			fmt.Sprintf("this coordinator has no project %q", task.Project)))
		result.Outcome = ViabilityImpossible
		return result
	}
	if _, known := settings.Catalog.Profile(project.SetupProfile); !known {
		result.Reasons = append(result.Reasons, newViabilityReason(ReasonUnknownSetupProfile,
			fmt.Sprintf("project %q names setup profile %q, which this coordinator does not have",
				task.Project, project.SetupProfile)))
	}
	ref := task.Ref
	if ref == "" {
		ref = project.DefaultRef
	}
	repositoryUsable := project.Type != backlog.EnvironmentFresh
	if repositoryUsable {
		if err := backlog.ValidateRepositorySyntax(project.Repository); err != nil {
			repositoryUsable = false
			result.Reasons = append(result.Reasons, newViabilityReason(ReasonRepositorySyntaxInvalid,
				fmt.Sprintf("project %q repository: %v", task.Project, err)))
		}
		if err := backlog.ValidateRefSyntax(ref); err != nil {
			repositoryUsable = false
			result.Reasons = append(result.Reasons, newViabilityReason(ReasonRepositorySyntaxInvalid,
				fmt.Sprintf("ref %q: %v", ref, err)))
		}
	}
	if len(task.Directories) != 0 {
		if _, err := directoryresource.Resolve(project.DirectoryBindings, task.Directories); err != nil {
			result.Reasons = append(result.Reasons, newViabilityReason(ReasonDirectoryImpossible,
				fmt.Sprintf("project %q cannot bind the requested directories: %v", task.Project, err)))
		}
	}
	result.Reasons = append(result.Reasons, v.timingReasons(task)...)
	result.Reasons = append(result.Reasons, v.lockReasons(task, project)...)

	domainTask := domain.Task{
		ID: task.Name, Name: task.Name, Class: task.Class,
		Placement:      domain.Placement{Hosts: task.Hosts, Capabilities: task.Capabilities},
		ResourceDemand: task.Resources,
		Routes:         task.Routes,
		ResourceLocks:  task.ResourceLocks,
	}
	for _, worker := range workers {
		candidate := v.viabilityCandidate(ctx, settings, task, project, ref, repositoryUsable, domainTask, worker)
		result.Candidates = append(result.Candidates, candidate)
	}
	result.Outcome = taskOutcome(result)
	return result
}

func taskOutcome(result ViabilityTaskResult) ViabilityOutcome {
	if outcomeFor(result.Reasons) == ViabilityImpossible {
		return ViabilityImpossible
	}
	best := ViabilityImpossible
	for _, candidate := range result.Candidates {
		switch candidate.Outcome {
		case ViabilityReady:
			best = ViabilityReady
		case ViabilityAcceptedWaiting:
			if best != ViabilityReady {
				best = ViabilityAcceptedWaiting
			}
		}
	}
	if best == ViabilityReady && outcomeFor(result.Reasons) == ViabilityAcceptedWaiting {
		return ViabilityAcceptedWaiting
	}
	return best
}

// timingReasons reports a declared window that is already closed or not yet
// open. A closed window is permanent: waiting cannot reopen it.
func (v view) timingReasons(task ViabilityTask) []ViabilityReason {
	var reasons []ViabilityReason
	if task.ExpiresAt != nil && !task.ExpiresAt.After(v.now) {
		reasons = append(reasons, newViabilityReason(ReasonTimingWindowClosed,
			fmt.Sprintf("task %q expires at %s, which has passed", task.Name, task.ExpiresAt.UTC().Format(time.RFC3339))))
	}
	if task.NotBefore != nil && task.NotBefore.After(v.now) {
		reasons = append(reasons, newViabilityReason(ReasonTimingWindowNotOpen,
			fmt.Sprintf("task %q may not start before %s", task.Name, task.NotBefore.UTC().Format(time.RFC3339))))
	}
	return reasons
}

// lockReasons reports a resource lock another attempt already holds. It is
// temporary: the holder finishes and the lock is released.
func (v view) lockReasons(task ViabilityTask, project backlog.ProjectDefinition) []ViabilityReason {
	required := make(map[string]bool, len(task.ResourceLocks)+len(project.ResourceLocks))
	for _, name := range append(append([]string(nil), project.ResourceLocks...), task.ResourceLocks...) {
		required[name] = true
	}
	var reasons []ViabilityReason
	for _, lock := range v.locks(Filter{}) {
		if required[lock.Name] && lock.OwnerAttemptID != "" {
			reasons = append(reasons, newViabilityReason(ReasonLockHeld,
				fmt.Sprintf("resource lock %q is held by attempt %s", lock.Name, lock.OwnerAttemptID)))
		}
	}
	return reasons
}

func (v view) viabilityCandidate(
	ctx context.Context,
	settings ViabilitySettings,
	task ViabilityTask,
	project backlog.ProjectDefinition,
	ref string,
	repositoryUsable bool,
	domainTask domain.Task,
	worker viabilityWorker,
) ViabilityCandidate {
	candidate := ViabilityCandidate{Worker: worker.id}
	candidate.Reasons = append(candidate.Reasons, enrollmentReasons(worker)...)
	drifted := false
	for _, reason := range candidate.Reasons {
		drifted = drifted || reason.Code == ReasonCatalogDigestMismatch
	}
	if !worker.hasSnapshot {
		candidate.Reasons = append(candidate.Reasons, newViabilityReason(ReasonWorkerOffline,
			fmt.Sprintf("worker %q has reported no inventory", worker.id)))
		candidate.Outcome = outcomeFor(candidate.Reasons)
		return candidate
	}
	if worker.dto.Stale || !worker.dto.Snapshot.Connected {
		candidate.Reasons = append(candidate.Reasons, newViabilityReason(ReasonWorkerStale,
			fmt.Sprintf("worker %q last reported %.0fs ago and is %s",
				worker.id, worker.dto.SnapshotAgeSeconds, worker.dto.State)))
	}

	inventories := []domain.WorkerInventory{worker.inventory}
	placement, err := backlog.MatchWorkers(backlog.WorkerPlacementRequest{
		Task: domainTask, Project: task.Project, Now: v.now,
		// Freshness was judged above; this bound is deliberately not binding.
		MaxSnapshotAge: time.Duration(1 << 62),
	}, inventories)
	if err != nil {
		candidate.Reasons = append(candidate.Reasons, newViabilityReason(ReasonWorkerNotEligible,
			"placement could not be evaluated: "+err.Error()))
		candidate.Outcome = outcomeFor(candidate.Reasons)
		return candidate
	}
	for _, evaluation := range placement.Evaluations {
		for _, exclusion := range evaluation.Exclusions {
			candidate.Reasons = append(candidate.Reasons,
				newViabilityReason(placementReasonCode(exclusion.Code), exclusion.Detail))
		}
	}

	candidate.Reasons = append(candidate.Reasons, v.routeReasons(domainTask, worker)...)
	candidate.Reasons = append(candidate.Reasons, v.quotaReasons(domainTask, worker)...)
	candidate.Reasons = append(candidate.Reasons, v.credentialReasons(ctx, settings, project, worker)...)
	if repositoryUsable {
		candidate.Reasons = append(candidate.Reasons,
			v.repositoryReasons(ctx, settings, project, ref, worker, drifted)...)
	}
	candidate.Outcome = outcomeFor(candidate.Reasons)
	return candidate
}

// enrollmentReasons reports drift as drift.
//
// The requirement's catalog revision is the digest the coordinator wants the
// worker to be running; the enrollment request's is the digest the worker
// actually accepted. Comparing them here, before anything else looks at the
// worker, is what keeps a digest mismatch from being reported as an absent
// candidate.
func enrollmentReasons(worker viabilityWorker) []ViabilityReason {
	requirement := worker.dto.Requirement
	if requirement == nil {
		return nil
	}
	enrollment := worker.dto.Enrollment
	if enrollment == nil {
		return []ViabilityReason{newViabilityReason(ReasonWorkerNotEligible,
			fmt.Sprintf("worker %q is configured but has never enrolled", requirement.WorkerID))}
	}
	var reasons []ViabilityReason
	if enrollment.Request.CatalogRevision != requirement.CatalogRevision {
		reason := newViabilityReason(ReasonCatalogDigestMismatch, fmt.Sprintf(
			"worker %q accepted catalog %s and the coordinator requires %s; re-enrol it",
			requirement.WorkerID, shortDigest(enrollment.Request.CatalogRevision), shortDigest(requirement.CatalogRevision)))
		reason.Desired = requirement.CatalogRevision
		reason.Observed = enrollment.Request.CatalogRevision
		if enrollment.Revision > 0 {
			reason.Revision = uint64(enrollment.Revision)
		}
		reasons = append(reasons, reason)
	}
	if requirement.Draining || requirement.Connection == "removed" {
		reasons = append(reasons, newViabilityReason(ReasonWorkerOffline,
			fmt.Sprintf("worker %q is draining", requirement.WorkerID)))
	}
	if !worker.dto.Enrolled && enrollment.Request.CatalogRevision == requirement.CatalogRevision {
		// The digests agree, so the mismatch is in the worker epoch, the
		// credential reference or the connection. Naming drift here would be a
		// lie, and naming nothing would lose the finding.
		reasons = append(reasons, newViabilityReason(ReasonWorkerNotEligible,
			fmt.Sprintf("worker %q enrollment does not match its requirement", requirement.WorkerID)))
	}
	return reasons
}

func shortDigest(digest string) string {
	if len(digest) > 12 {
		return digest[:12]
	}
	return digest
}

// placementReasonCode maps one placement exclusion onto a viability code. The
// eligibility rules stay in the matcher; only the naming happens here.
func placementReasonCode(exclusion string) string {
	switch exclusion {
	case backlog.ExclusionMissingCapability:
		return ReasonCapabilityMissing
	case backlog.ExclusionCPUClassBelowMinimum, backlog.ExclusionCPUClassUnknown:
		return ReasonCPUClassImpossible
	case backlog.ExclusionCapacityExhausted:
		return ReasonWorkerAtCapacity
	case backlog.ExclusionWorkerHealth:
		return ReasonWorkerOffline
	case backlog.ExclusionWorkerStale, backlog.ExclusionWorkerEpochSuperseded:
		return ReasonWorkerStale
	default:
		// host-not-allowed, backlog-disabled and project-unavailable are all
		// statements that this worker may not serve this task as written.
		return ReasonWorkerNotEligible
	}
}

// routeReasons explains a task no configured provider route can serve on this
// worker. It reads the router's own blockers rather than re-deriving them, so
// the answer cannot disagree with the planner's.
func (v view) routeReasons(task domain.Task, worker viabilityWorker) []ViabilityReason {
	if len(task.Routes) == 0 {
		// An unconstrained task takes whatever route the planner finds later.
		return nil
	}
	resolved, blockers, err := backlog.ExplainProviderRoutePools(
		task, domain.Attempt{}, []domain.WorkerInventory{worker.inventory}, v.records.QuotaPools)
	if err != nil {
		return []ViabilityReason{newViabilityReason(ReasonNoConfiguredRoute,
			"provider routing could not be evaluated: "+err.Error())}
	}
	if len(resolved) != 0 {
		return nil
	}
	var reasons []ViabilityReason
	for _, blocker := range blockers {
		reasons = append(reasons, newViabilityReason(routeReasonCode(blocker.Code), blocker.Detail))
	}
	if len(reasons) == 0 {
		reasons = append(reasons, newViabilityReason(ReasonNoConfiguredRoute,
			fmt.Sprintf("worker %q offers none of the declared routes", worker.id)))
	}
	return reasons
}

func routeReasonCode(blocker string) string {
	switch blocker {
	case backlog.PlanningBlockerProviderUnavailable:
		return ReasonUnknownProviderInstance
	case backlog.PlanningBlockerModelUnavailable:
		return ReasonUnknownModel
	case backlog.PlanningBlockerQuotaPoolUnavailable:
		return ReasonUnknownQuotaPool
	case backlog.PlanningBlockerPoolConcurrency:
		return ReasonWorkerAtCapacity
	default:
		return ReasonNoConfiguredRoute
	}
}

// quotaReasons reports a closed quota pool and a quota observation this
// coordinator no longer trusts. Both are temporary.
func (v view) quotaReasons(task domain.Task, worker viabilityWorker) []ViabilityReason {
	pools := make(map[string]bool)
	resolved, _, err := backlog.ExplainProviderRoutePools(
		task, domain.Attempt{}, []domain.WorkerInventory{worker.inventory}, v.records.QuotaPools)
	if err == nil {
		for _, route := range resolved {
			if route.QuotaPoolID != "" {
				pools[route.QuotaPoolID] = true
			}
		}
	}
	for _, route := range task.Routes {
		if route.QuotaPoolID != "" {
			pools[route.QuotaPoolID] = true
		}
	}
	names := make([]string, 0, len(pools))
	for name := range pools {
		names = append(names, name)
	}
	sort.Strings(names)
	var reasons []ViabilityReason
	for _, name := range names {
		state, observedAt, known := v.admissionState(name)
		if !known {
			continue
		}
		blocked := state == domain.AdmissionClosed || state == domain.AdmissionDraining
		blocked = blocked || task.Class == domain.TaskClassSurplus &&
			(state == domain.AdmissionConstrained || state == domain.AdmissionRecovering)
		if blocked {
			reasons = append(reasons, newViabilityReason(ReasonQuotaClosed,
				fmt.Sprintf("quota pool %q admission is %s", name, state)))
		}
		maxAge := v.runtime.MaxQuotaObservationAge
		if maxAge > 0 && (observedAt.IsZero() || v.now.Sub(observedAt) > maxAge) {
			reasons = append(reasons, newViabilityReason(ReasonSnapshotStale,
				fmt.Sprintf("quota pool %q was last observed more than %s ago", name, maxAge)))
		}
	}
	return reasons
}

func (v view) admissionState(poolID string) (domain.AdmissionState, time.Time, bool) {
	for _, admission := range v.admissions {
		if admission.QuotaPoolID == poolID {
			return admission.Admission, admission.ObservedAt, true
		}
	}
	for _, pool := range v.records.QuotaPools {
		if pool.ID == poolID {
			return pool.Admission, time.Time{}, true
		}
	}
	return "", time.Time{}, false
}

// credentialReasons reports required credential references the worker cannot
// present. It names references and never values.
func (v view) credentialReasons(ctx context.Context, settings ViabilitySettings, project backlog.ProjectDefinition, worker viabilityWorker) []ViabilityReason {
	if len(project.RequiredCredentials) == 0 {
		return nil
	}
	if worker.dto.Enrollment != nil && worker.dto.Enrollment.CredentialRef == "" {
		return []ViabilityReason{newViabilityReason(ReasonCredentialMissing, fmt.Sprintf(
			"worker %q is enrolled with no credential reference and project %q requires %d",
			worker.id, project.Name, len(project.RequiredCredentials)))}
	}
	if settings.Credentials == nil {
		return nil
	}
	missing, err := settings.Credentials.MissingCredentials(ctx, worker.id, project.RequiredCredentials)
	if err != nil {
		// An unanswered question is not a passed one, but it is also not proof
		// that the credential is absent, so it is reported as temporary.
		return []ViabilityReason{newViabilityReason(ReasonSnapshotStale,
			fmt.Sprintf("credential availability for worker %q could not be read: %v", worker.id, err))}
	}
	var reasons []ViabilityReason
	for _, reference := range missing {
		reasons = append(reasons, newViabilityReason(ReasonCredentialMissing,
			fmt.Sprintf("worker %q cannot present credential reference %q", worker.id, reference)))
	}
	return reasons
}

// repositoryReasons observes whether the worker can read the project's
// repository and ref. A worker whose catalog has drifted is not probed: the
// answer would describe an execution identity the coordinator is already
// replacing.
func (v view) repositoryReasons(ctx context.Context, settings ViabilitySettings, project backlog.ProjectDefinition, ref string, worker viabilityWorker, drifted bool) []ViabilityReason {
	if settings.Repository == nil || drifted || !worker.hasSnapshot {
		return nil
	}
	key := backlog.RepositoryProbeKey{
		WorkerID:       worker.id,
		CatalogDigest:  worker.inventory.CatalogRevision,
		Repository:     project.Repository,
		Ref:            ref,
		CredentialRefs: append([]string(nil), project.RequiredCredentials...),
	}
	observation, err := settings.Repository.ObserveRepository(ctx, key)
	if err != nil {
		return []ViabilityReason{newViabilityReason(ReasonNetworkUnavailable,
			fmt.Sprintf("repository reachability on worker %q could not be observed: %v", worker.id, err))}
	}
	code := repositoryReasonCode(observation.Class)
	if code == "" {
		return nil
	}
	reason := newViabilityReason(code, fmt.Sprintf(
		"worker %q reported %s for repository %s at %s",
		worker.id, observation.Class, project.Repository, ref))
	reason.Desired = ref
	return []ViabilityReason{reason}
}

func repositoryReasonCode(class backlog.RepositoryReachability) string {
	switch class {
	case backlog.RepositoryAuthenticatedOK:
		return ""
	case backlog.RepositoryAuthenticationFailed:
		return ReasonRepositoryAuthFailed
	case backlog.RepositoryNotFound:
		return ReasonRepositoryNotFound
	case backlog.RepositoryRefNotFound:
		return ReasonRefNotFound
	case backlog.RepositoryProbeTimeout:
		return ReasonProbeTimeout
	case backlog.RepositoryDNSFailure:
		return ReasonDNSFailure
	default:
		return ReasonNetworkUnavailable
	}
}
