package backlogadmin

import (
	"context"
	"fmt"
	"sort"
	"strings"
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
	// Projects and SetupProfiles are the catalog entries this coordinator is
	// configured with. They are the definitions rather than a constructed
	// ProjectCatalog on purpose: the catalog constructor refuses to hold a
	// project whose repository syntax is wrong, and a project that exists and
	// is misconfigured must be reported as misconfigured, never as unknown.
	Projects      []backlog.ProjectDefinition
	SetupProfiles []backlog.SetupProfile
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

// project returns one configured project by name.
func (v ViabilitySettings) project(name string) (backlog.ProjectDefinition, bool) {
	for _, project := range v.Projects {
		if project.Name == name {
			return project, true
		}
	}
	return backlog.ProjectDefinition{}, false
}

// profile returns one configured setup profile by name.
func (v ViabilitySettings) profile(name string) (backlog.SetupProfile, bool) {
	for _, profile := range v.SetupProfiles {
		if profile.Name == name {
			return profile, true
		}
	}
	return backlog.SetupProfile{}, false
}

// configured reports whether this coordinator can answer a viability query at
// all. A coordinator with no configured project would report every campaign as
// impossible, which is a fault in the coordinator, not in the campaign.
func (v ViabilitySettings) configured() bool { return len(v.Projects) != 0 }

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
	project, known := settings.project(task.Project)
	if !known {
		result.Reasons = append(result.Reasons, newViabilityReason(ReasonUnknownProject,
			fmt.Sprintf("this coordinator has no project %q", task.Project)))
		result.Outcome = ViabilityImpossible
		return result
	}
	if _, known := settings.profile(project.SetupProfile); !known {
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
	result.Reasons = append(result.Reasons, repositoryVerdict(project.Repository, ref, result.Candidates)...)
	result.Outcome = taskOutcome(result)
	return result
}

// repositoryVerdict decides the task-level repository answer from the
// candidates' observations.
//
// The rule is that a permanent repository verdict from at least one observed
// candidate, with no candidate observing success, is permanent for the task.
//
// The reason this is not "permanent on every candidate" is that the second
// condition is unsatisfiable whenever any candidate is unobserved, and on a
// real fleet at least one candidate usually is: a worker with no snapshot, a
// worker whose catalog has drifted, a worker that cannot be dialled, a worker
// on an older build. Requiring unanimity let one non-answering worker mask a
// confirmed permanent failure, and the run was created and died hours later
// preparing its workspace, which is the failure this whole check exists to
// prevent.
//
// An unobserved candidate is not contradicting evidence. It is named in the
// reason so the basis of the verdict is visible, and "submit
// --allow-unverified" remains the operator's escape for the rare case where an
// unobserved worker would have succeeded.
func repositoryVerdict(repository, ref string, candidates []ViabilityCandidate) []ViabilityReason {
	var observing []string
	var unobserved []string
	code, class := "", ""
	for _, candidate := range candidates {
		observation := candidate.Repository
		if observation == nil {
			continue
		}
		if !observation.Observed {
			unobserved = append(unobserved,
				fmt.Sprintf("%s (%s)", candidate.Worker, observation.Unobserved))
			continue
		}
		if backlog.RepositoryReachability(observation.Class) == backlog.RepositoryAuthenticatedOK {
			// One candidate that can read the repository settles it: the work
			// can be placed there, whatever the others reported.
			return nil
		}
		if observed := repositoryReasonCode(backlog.RepositoryReachability(observation.Class)); PermanentViabilityReason(observed) {
			observing = append(observing, candidate.Worker)
			if code == "" {
				code, class = observed, observation.Class
			}
		}
	}
	if code == "" {
		return nil
	}
	basis := "every candidate was observed"
	if len(unobserved) != 0 {
		basis = "not observed: " + strings.Join(unobserved, ", ")
	}
	reason := newViabilityReason(code, fmt.Sprintf(
		"%s observed %s for repository %s at %s, and no candidate observed success; %s",
		strings.Join(observing, ", "), class, repository, ref, basis))
	reason.Observed = class
	reason.Desired = ref
	return []ViabilityReason{reason}
}

func taskOutcome(result ViabilityTaskResult) ViabilityOutcome {
	if outcomeFor(result.Reasons) == ViabilityImpossible {
		return ViabilityImpossible
	}
	if len(result.Candidates) == 0 {
		// An empty fleet is not an impossible request. Nothing about the
		// campaign is wrong; there is simply nobody to run it yet, and a worker
		// enrolling fixes that.
		return ViabilityAcceptedWaiting
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
	candidate.Unchecked = append(candidate.Unchecked, enrollmentUnchecked(worker)...)
	drifted := false
	for _, reason := range candidate.Reasons {
		drifted = drifted || reason.Code == ReasonCatalogDigestMismatch
	}
	// Every exit from here on records a repository observation, including the
	// early ones. A candidate with no observation at all would be invisible to
	// the task-level verdict, which is exactly how one non-answering worker used
	// to mask a confirmed permanent failure.
	if !worker.hasSnapshot {
		candidate.Reasons = append(candidate.Reasons, newViabilityReason(ReasonWorkerOffline,
			fmt.Sprintf("worker %q has reported no inventory", worker.id)))
		observation := unobservedRepository("worker %q has reported no inventory", worker.id)
		candidate.Repository = &observation
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
		observation := unobservedRepository(
			"placement for worker %q could not be evaluated, so nothing was dialled", worker.id)
		candidate.Repository = &observation
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
	credentialReasons, credentialUnchecked := v.credentialReasons(ctx, settings, project, worker)
	candidate.Reasons = append(candidate.Reasons, credentialReasons...)
	candidate.Unchecked = append(candidate.Unchecked, credentialUnchecked...)
	observation, repositoryReasons := v.repositoryReasons(
		ctx, settings, project, ref, worker, drifted, repositoryUsable)
	candidate.Repository = &observation
	candidate.Reasons = append(candidate.Reasons, repositoryReasons...)
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
	// The digest the worker is observably running is a third value, and it is
	// the one the repository probe key is built from. A worker that enrolled
	// against the right catalog and is running a different one would otherwise
	// be probed under an identity nobody compared to anything.
	if observed := worker.inventory.CatalogRevision; worker.hasSnapshot && observed != "" &&
		observed != requirement.CatalogRevision && observed != enrollment.Request.CatalogRevision {
		reason := newViabilityReason(ReasonCatalogDigestMismatch, fmt.Sprintf(
			"worker %q is running catalog %s and the coordinator requires %s; re-enrol it",
			requirement.WorkerID, shortDigest(observed), shortDigest(requirement.CatalogRevision)))
		reason.Desired = requirement.CatalogRevision
		reason.Observed = observed
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

// enrollmentUnchecked names the comparisons that could not be made. A worker
// with a snapshot and no requirement row is not managed by this coordinator's
// fleet configuration, so there is no desired digest to compare its catalog
// against; saying nothing would present an unchecked worker as a checked one.
func enrollmentUnchecked(worker viabilityWorker) []string {
	if worker.dto.Requirement != nil || !worker.hasSnapshot {
		return nil
	}
	return []string{fmt.Sprintf(
		"worker %q has no requirement row, so the catalog %s it is running was not compared "+
			"against a desired digest",
		worker.id, shortDigest(worker.inventory.CatalogRevision))}
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
// present, and says plainly when it could not tell. It names references and
// never values.
//
// Per-reference availability is not something the coordinator knows. A worker
// inventory reports capabilities, projects and providers; it does not report
// which references the worker's own secret store can resolve. Without a
// resolver this answer therefore states that availability was not observed
// rather than staying silent, because silence here reads as "checked and
// fine".
//
// The case that is observable is worth reporting on its own: a worker enrolled
// with no credential reference at all cannot present one, whatever its store
// holds. When a worker does hold the wrong credential, the repository probe
// observes authentication-failed on the worker itself, which is permanent and
// is where that failure is actually caught.
func (v view) credentialReasons(ctx context.Context, settings ViabilitySettings, project backlog.ProjectDefinition, worker viabilityWorker) ([]ViabilityReason, []string) {
	if len(project.RequiredCredentials) == 0 {
		return nil, nil
	}
	if worker.dto.Enrollment != nil && worker.dto.Enrollment.CredentialRef == "" {
		return []ViabilityReason{newViabilityReason(ReasonCredentialMissing, fmt.Sprintf(
			"worker %q is enrolled with no credential reference and project %q requires %d",
			worker.id, project.Name, len(project.RequiredCredentials)))}, nil
	}
	if settings.Credentials == nil {
		references := append([]string(nil), project.RequiredCredentials...)
		sort.Strings(references)
		return nil, []string{fmt.Sprintf(
			"whether worker %q can present the credential references project %q requires (%s) "+
				"was not observed: this coordinator has no credential resolver, and a worker "+
				"inventory does not report its secret store",
			worker.id, project.Name, strings.Join(references, ", "))}
	}
	missing, err := settings.Credentials.MissingCredentials(ctx, worker.id, project.RequiredCredentials)
	if err != nil {
		// An unanswered question is not a passed one, but it is also not proof
		// that the credential is absent, so it is reported as temporary.
		return []ViabilityReason{newViabilityReason(ReasonSnapshotStale,
				fmt.Sprintf("credential availability for worker %q could not be read: %v", worker.id, err))},
			[]string{fmt.Sprintf("credential availability for worker %q was not observed", worker.id)}
	}
	var reasons []ViabilityReason
	for _, reference := range missing {
		reasons = append(reasons, newViabilityReason(ReasonCredentialMissing,
			fmt.Sprintf("worker %q cannot present credential reference %q", worker.id, reference)))
	}
	return reasons, nil
}

// repositoryReasons observes whether the worker can read the project's
// repository and ref.
//
// It always says what it did, including when it did nothing. Returning silence
// for an unobserved candidate read exactly like a candidate that had been
// observed and was fine: illegible to an operator, and the wrong input for the
// task-level verdict, which has to distinguish absence of evidence from
// evidence of success.
//
// A worker whose catalog has drifted is not probed, because the answer would
// describe an execution identity the coordinator is already replacing. That is
// recorded as unobserved rather than passed.
func (v view) repositoryReasons(
	ctx context.Context,
	settings ViabilitySettings,
	project backlog.ProjectDefinition,
	ref string,
	worker viabilityWorker,
	drifted bool,
	repositoryUsable bool,
) (ViabilityRepositoryObservation, []ViabilityReason) {
	switch {
	case !repositoryUsable && project.Type == backlog.EnvironmentFresh:
		return unobservedRepository("project %q prepares a fresh workspace and has no repository to reach", project.Name), nil
	case !repositoryUsable:
		return unobservedRepository("the repository or ref syntax is invalid, so nothing was dialled"), nil
	case settings.Repository == nil:
		return unobservedRepository("this coordinator has no repository observer configured"), nil
	case drifted:
		return unobservedRepository("worker %q is running a catalog the coordinator has replaced, so its answer would describe an execution identity that is being retired", worker.id), nil
	case !worker.hasSnapshot:
		return unobservedRepository("worker %q has reported no inventory", worker.id), nil
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
		// The worker did not answer. That is a temporary finding for this
		// candidate and no evidence at all about the repository, so it must not
		// count as a candidate that observed success.
		return unobservedRepository("worker %q could not be reached: %v", worker.id, err),
			[]ViabilityReason{newViabilityReason(ReasonNetworkUnavailable,
				fmt.Sprintf("repository reachability on worker %q could not be observed: %v", worker.id, err))}
	}
	seen := ViabilityRepositoryObservation{Observed: true, Class: string(observation.Class)}
	code := repositoryReasonCode(observation.Class)
	if code == "" {
		return seen, nil
	}
	reason := newViabilityReason(code, fmt.Sprintf(
		"worker %q reported %s for repository %s at %s",
		worker.id, observation.Class, project.Repository, ref))
	reason.Desired = ref
	reason.Observed = string(observation.Class)
	return seen, []ViabilityReason{reason}
}

func unobservedRepository(format string, args ...any) ViabilityRepositoryObservation {
	return ViabilityRepositoryObservation{Unobserved: fmt.Sprintf(format, args...)}
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
