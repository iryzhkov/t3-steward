package backlogadmin

import (
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ViabilityOutcome says whether a request can run now, later, or at all.
type ViabilityOutcome string

const (
	// ViabilityReady means at least one candidate can take the work now.
	ViabilityReady ViabilityOutcome = "ready"
	// ViabilityAcceptedWaiting means no candidate can take the work now and the
	// obstruction is temporary, so submission proceeds and the reasons are
	// reported.
	ViabilityAcceptedWaiting ViabilityOutcome = "accepted_waiting"
	// ViabilityImpossible means no candidate can ever satisfy the request as
	// written, so submission is refused and no workflow run is created.
	ViabilityImpossible ViabilityOutcome = "impossible"
)

// Permanent reason codes. Every one of them refuses a submission before a
// workflow exists, because waiting cannot change the answer.
const (
	ReasonUnknownProject          = "unknown-project"
	ReasonUnknownSetupProfile     = "unknown-setup-profile"
	ReasonUnknownProviderInstance = "unknown-provider-instance"
	ReasonUnknownModel            = "unknown-model"
	ReasonUnknownQuotaPool        = "unknown-quota-pool"
	ReasonWorkerNotEligible       = "worker-not-eligible"
	ReasonCapabilityMissing       = "capability-missing"
	ReasonCPUClassImpossible      = "cpu-class-impossible"
	ReasonResourcesImpossible     = "resources-impossible"
	ReasonDirectoryImpossible     = "directory-impossible"
	ReasonCredentialMissing       = "credential-missing"
	ReasonRepositorySyntaxInvalid = "repository-syntax-invalid"
	ReasonRepositoryAuthFailed    = "repository-authentication-failed"
	ReasonRepositoryNotFound      = "repository-not-found"
	ReasonRefNotFound             = "ref-not-found"
	ReasonNoConfiguredRoute       = "no-configured-route"
	// ReasonNoRoute means the task declares no provider route at all. The
	// coordinator never chooses a route on a task's behalf: a task with none
	// is not dispatchable, and before this code existed it was accepted and
	// then stalled planning for every run on every tick. The detail names the
	// instance/model pairs the project's eligible workers advertise.
	ReasonNoRoute = "no-route"
	// ReasonSupervisorClientMissing means the coordinator has no admin client
	// with supervisor: true, so it would dispatch no overseer for this campaign
	// and every gate the manifest declares would wait for an operator. It is
	// permanent for the same reason no-configured-route is: waiting does not
	// change the answer, a configuration change does.
	ReasonSupervisorClientMissing = "supervisor-client-missing"
)

// Temporary reason codes. Every one of them is compatible with asynchronous
// execution, because waiting is what fixes it.
const (
	ReasonQuotaClosed        = "quota-closed"
	ReasonWorkerAtCapacity   = "worker-at-capacity"
	ReasonWorkerOffline      = "worker-offline"
	ReasonWorkerStale        = "worker-stale"
	ReasonNetworkUnavailable = "network-unavailable"
	ReasonDNSFailure         = "dns-failure"
	ReasonProbeTimeout       = "probe-timeout"
	ReasonSnapshotStale      = "snapshot-stale"
	ReasonLockHeld           = "lock-held"

	// ReasonCatalogDigestMismatch is drift, and it is its own code on purpose.
	// A worker whose requirement no longer matches its enrollment used to be
	// dropped from the inventory before routing was evaluated, so an operator
	// was told "no eligible worker" when the truth was a digest mismatch that
	// re-enrolment fixes. It is temporary and always carries both digests and
	// the expected revision, and it is never reported as worker-not-eligible.
	ReasonCatalogDigestMismatch = "catalog-digest-mismatch"
)

// ReasonProjectBindingDefaulted is an informational detail, not a reason. It
// is carried on a candidate's Unchecked list when the coordinator loaded the
// task's project with a default local binding (no backlog_v2.projects entry:
// no credentials, resource locks or directory resources). The project runs; a
// reader is told that nothing host-local was checked for it because nothing
// host-local is bound. It belongs to neither reason set on purpose, so it can
// never turn a ready fleet into accepted_waiting or refuse a submission.
const ReasonProjectBindingDefaulted = "project-binding-defaulted"

// Reason codes the frozen contract does not name.
//
// The H3 ADR puts timing constraints and artifact and message limits in the
// matrix, and the freeze's two code lists have nothing for either. Reporting
// them under a code that means something else would be worse than naming them,
// so they are named here and flagged as a gap in the freeze rather than folded
// into a neighbouring code.
const (
	// ReasonTimingWindowClosed is permanent: a task whose expiry has already
	// passed cannot become runnable by waiting.
	ReasonTimingWindowClosed = "timing-window-closed"
	// ReasonTimingWindowNotOpen is temporary: the start window opens later.
	ReasonTimingWindowNotOpen = "timing-window-not-open"
	// ReasonMessageLimitExceeded is permanent: a bundle larger than the
	// coordinator accepts is refused however long the caller waits.
	ReasonMessageLimitExceeded = "message-limit-exceeded"
)

// permanentReasons is the closed set of codes that refuse a submission.
var permanentReasons = map[string]bool{
	ReasonUnknownProject:          true,
	ReasonUnknownSetupProfile:     true,
	ReasonUnknownProviderInstance: true,
	ReasonUnknownModel:            true,
	ReasonUnknownQuotaPool:        true,
	ReasonWorkerNotEligible:       true,
	ReasonCapabilityMissing:       true,
	ReasonCPUClassImpossible:      true,
	ReasonResourcesImpossible:     true,
	ReasonDirectoryImpossible:     true,
	ReasonCredentialMissing:       true,
	ReasonRepositorySyntaxInvalid: true,
	ReasonRepositoryAuthFailed:    true,
	ReasonRepositoryNotFound:      true,
	ReasonRefNotFound:             true,
	ReasonNoConfiguredRoute:       true,
	ReasonNoRoute:                 true,
	ReasonSupervisorClientMissing: true,
	ReasonTimingWindowClosed:      true,
	ReasonMessageLimitExceeded:    true,
}

// PermanentViabilityReason reports whether a code refuses the request as
// written. An unknown code is temporary, so a code this build does not know
// can never refuse a submission.
func PermanentViabilityReason(code string) bool { return permanentReasons[code] }

// ViabilityRequest is one campaign's projected requirements. It never carries
// the bundle: the coordinator is asked whether the work could run, not asked to
// take delivery of it.
type ViabilityRequest struct {
	SchemaVersion int             `json:"schemaVersion"`
	Tasks         []ViabilityTask `json:"tasks"`
	// BundleBytes and BundleFiles are what submission would send. They are
	// checked against the coordinator's own message limits, so a bundle this
	// coordinator would refuse on arrival is refused before a run exists.
	BundleBytes int64 `json:"bundleBytes,omitempty"`
	BundleFiles int   `json:"bundleFiles,omitempty"`
	// Supervision is the overseer a supervised campaign declares, and is absent
	// for every unsupervised one. It is a property of the request rather than of
	// any task: the overseer decides the run's gates and runs no task at all.
	Supervision *ViabilitySupervision `json:"supervision,omitempty"`
}

// ViabilitySupervision is the declared overseer's own requirement.
//
// It exists because a supervised campaign asks for something no task of it asks
// for: a worker that hosts the overseer route and advertises the campaign
// supervision capability. Without one, every gate the manifest declares is a
// gate nobody can decide, so the campaign is refused before a run exists rather
// than submitted and held forever.
type ViabilitySupervision struct {
	Route domain.ProviderRoute `json:"route"`
	// RequiredCapability is the worker inventory capability an activation needs.
	// An empty value means the campaign supervision capability.
	RequiredCapability string `json:"requiredCapability,omitempty"`
}

// ViabilityTask is one projected task's requirements.
type ViabilityTask struct {
	Name          string                      `json:"name"`
	Project       string                      `json:"project"`
	Ref           string                      `json:"ref,omitempty"`
	Class         domain.TaskClass            `json:"class,omitempty"`
	Hosts         []string                    `json:"hosts,omitempty"`
	Capabilities  []string                    `json:"capabilities,omitempty"`
	Resources     domain.ResourceDemand       `json:"resources,omitzero"`
	Routes        []domain.ProviderRoute      `json:"routes,omitempty"`
	Directories   []directoryresource.Request `json:"directories,omitempty"`
	ResourceLocks []string                    `json:"resourceLocks,omitempty"`
	NotBefore     *time.Time                  `json:"notBefore,omitempty"`
	ExpiresAt     *time.Time                  `json:"expiresAt,omitempty"`
	Outputs       int                         `json:"outputs,omitempty"`
}

// ViabilityMatrix is the per-task, per-worker answer.
type ViabilityMatrix struct {
	SchemaVersion int              `json:"schemaVersion"`
	Outcome       ViabilityOutcome `json:"outcome"`
	// Reasons carries the findings that belong to the request as a whole rather
	// than to any task, such as a bundle larger than this coordinator accepts.
	// The frozen shape has no such field; attaching a request-level finding to
	// an arbitrary task would have implied it was that task's fault.
	Reasons []ViabilityReason     `json:"reasons,omitempty"`
	Tasks   []ViabilityTaskResult `json:"tasks"`
}

// ViabilityMatrixSchemaVersion versions the matrix document.
const ViabilityMatrixSchemaVersion = 1

// ViabilityTaskResult is one task's answer.
//
// Reasons carries the findings that belong to the task rather than to any one
// worker: an unknown project, an invalid repository, a closed timing window or
// a bundle over the coordinator's limits. The frozen shape has no such field;
// copying a project-level finding onto every candidate would have said the same
// thing N times and implied it was a property of the worker.
type ViabilityTaskResult struct {
	Task       string               `json:"task"`
	Outcome    ViabilityOutcome     `json:"outcome"`
	Reasons    []ViabilityReason    `json:"reasons,omitempty"`
	Candidates []ViabilityCandidate `json:"candidates"`
}

// ViabilityCandidate is one worker's answer for one task.
type ViabilityCandidate struct {
	Worker  string            `json:"worker"`
	Outcome ViabilityOutcome  `json:"outcome"`
	Reasons []ViabilityReason `json:"reasons,omitempty"`
	// Repository says whether this candidate's repository reachability was
	// actually observed, and what it observed. It is a separate field from
	// Reasons because the task-level verdict depends on the difference between
	// "observed and fine" and "never asked", and a reader of an outcome must be
	// able to tell those apart too.
	Repository *ViabilityRepositoryObservation `json:"repository,omitempty"`
	// Unchecked names what this answer did not look at for this candidate.
	// Silence about a question nobody asked reads exactly like a passed check,
	// which is the failure this field exists to prevent.
	Unchecked []string `json:"unchecked,omitempty"`
}

// ViabilityRepositoryObservation is one candidate's repository reachability.
//
// Observed distinguishes evidence from absence. An unobserved candidate is not
// contradicting evidence: it neither confirms that the repository can be read
// nor argues against a permanent verdict another candidate did observe.
type ViabilityRepositoryObservation struct {
	Observed bool `json:"observed"`
	// Class is the reachability classification, present only when Observed.
	Class string `json:"class,omitempty"`
	// Unobserved says why no observation was made, present only when the
	// candidate was not observed.
	Unobserved string `json:"unobserved,omitempty"`
}

// ViabilityReason is one independent finding.
type ViabilityReason struct {
	Code      string `json:"code"`
	Permanent bool   `json:"permanent"`
	Detail    string `json:"detail"`
	// Desired and Observed carry the two sides of a mismatch, such as the
	// catalog digest a worker must accept and the one it did accept.
	Desired  string `json:"desired,omitempty"`
	Observed string `json:"observed,omitempty"`
	// Revision is the expected revision where one is relevant, such as the
	// enrollment revision a re-enrolment must fence against.
	Revision uint64 `json:"revision,omitempty"`
}

// newViabilityReason builds one finding with its permanence derived from the
// code, so a caller can never disagree with the table about what a code means.
func newViabilityReason(code, detail string) ViabilityReason {
	return ViabilityReason{Code: code, Permanent: PermanentViabilityReason(code), Detail: detail}
}

// outcomeFor reduces a set of findings to one outcome.
func outcomeFor(reasons []ViabilityReason) ViabilityOutcome {
	if len(reasons) == 0 {
		return ViabilityReady
	}
	for _, reason := range reasons {
		if reason.Permanent {
			return ViabilityImpossible
		}
	}
	return ViabilityAcceptedWaiting
}

// PermanentReasons returns every permanent finding in a matrix, which is what a
// caller needs to refuse a request and say why.
func (m ViabilityMatrix) PermanentReasons() []ViabilityReason {
	var result []ViabilityReason
	for _, reason := range m.Reasons {
		if reason.Permanent {
			result = append(result, reason)
		}
	}
	for _, task := range m.Tasks {
		for _, reason := range task.Reasons {
			if reason.Permanent {
				result = append(result, reason)
			}
		}
		if task.Outcome != ViabilityImpossible {
			continue
		}
		// A task is impossible when no candidate could ever take it, so the
		// candidates' permanent findings are the explanation.
		for _, candidate := range task.Candidates {
			for _, reason := range candidate.Reasons {
				if reason.Permanent {
					result = append(result, reason)
				}
			}
		}
	}
	return result
}
