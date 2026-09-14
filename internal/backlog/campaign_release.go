package backlog

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// CampaignRefReleaser is the store a reconciler converges: what it holds, and
// how to let one run go. It is the CampaignRefStore in production and a
// recorder in tests, so that the decision about which runs may be released can
// be exercised without a Git repository.
type CampaignRefReleaser interface {
	// Runs reports the workflow runs the store currently holds commits for.
	Runs() ([]string, error)
	ReleaseRun(ctx context.Context, workflowRunID string, log io.Writer) error
}

// CampaignRefReleaseReport is what one reconciliation pass did. A failed
// release is an operational error: it is reported, the pass continues, and the
// run it belongs to is not affected, because the run has already finished and
// nothing about its outcome depends on whether a ref was deleted.
type CampaignRefReleaseReport struct {
	Released []string
	Errors   []error
}

// CampaignRefLifetime divides the runs the coordinator knows into the ones
// whose declared commits must stay reachable and the ones whose campaign refs
// may be released. A run that declares no commit appears in neither list:
// there is nothing to keep and nothing to delete.
//
// The lifetime of a declared commit is the lifetime of the provenance record
// that names it. That record is the run's retained artifact, so while it is
// retained the commit it points at must resolve, and once retention has removed
// it nothing can ask for the commit again. Settlement is the wrong boundary and
// was the first attempt at this: a rerun may only be created from a run that
// has already finished, so releasing at settlement released exactly the commits
// a rerun was about to carry.
//
// Tying the release to retention needs no special case for a rerun. A rerun
// pins its source run against retention, and a pinned run's artifacts cannot be
// pruned, so the provenance record survives for as long as the new run does and
// this function keeps the source's refs for the same span. When the pin is gone
// and retention removes the record, the same rule releases them. A rerun
// authored after the record has been pruned is refused by the rerun itself,
// which reads the artifact before it creates anything.
//
// A settled sink is still required before anything is released. It closes the
// window between a worker publishing a commit and the coordinator recording the
// artifact that names it: during that window the artifact is legitimately
// missing, and only an unsettled run can be in it.
func CampaignRefLifetime(records sqlite.CoordinatorRecords) (retained, releasable []string) {
	for _, run := range records.WorkflowRuns {
		declared := declaredCommitNames(run, records.Tasks)
		if len(declared) == 0 {
			continue
		}
		if runSettled(run) && !commitRecordRetained(run.ID, declared, records.Artifacts) {
			releasable = append(releasable, run.ID)
			continue
		}
		retained = append(retained, run.ID)
	}
	sort.Strings(retained)
	sort.Strings(releasable)
	return retained, releasable
}

// declaredCommitNames indexes the (task, name) pairs of one run that name a
// declared commit rather than a file the task wrote.
func declaredCommitNames(run domain.WorkflowRun, templates []domain.Task) map[string]bool {
	declared := map[string]bool{}
	for _, task := range domain.TasksForRun(run, templates) {
		for _, output := range task.Outputs {
			if output.Commit != nil {
				declared[task.ID+"\x00"+output.Name] = true
			}
		}
	}
	return declared
}

// commitRecordRetained reports whether any provenance record of a run's
// declared commits is still retained. Only an output artifact of the run itself
// counts: a reference carried into another run is that run's artifact and is
// held by the pin on this one, not by this test.
func commitRecordRetained(runID string, declared map[string]bool, artifacts []domain.Artifact) bool {
	for _, artifact := range artifacts {
		if artifact.WorkflowRunID == runID && artifact.Kind == domain.ArtifactOutput &&
			declared[artifact.TaskID+"\x00"+artifact.Name] {
			return true
		}
	}
	return false
}

// runSettled reports whether the coordinator-owned sink has reached a terminal
// state, which is the one durable statement that the run is over.
func runSettled(run domain.WorkflowRun) bool {
	return run.Sink != nil && run.Sink.Progress.Terminal()
}

// stateCampaignRefs adds the coordinator's statement of which campaign runs a
// worker must keep commits for.
//
// The list is the retained half of CampaignRefLifetime, so a worker releases
// exactly what the coordinator released locally, and a run the coordinator has
// never heard of is released too: it cannot be needed by a campaign this
// coordinator owns. The statement is only made from a snapshot that loaded,
// because an empty list built from a failed read would tell every worker to
// release everything.
func stateCampaignRefs(request *workerproto.SnapshotRequest, records sqlite.CoordinatorRecords) error {
	retained, _ := CampaignRefLifetime(records)
	if len(retained) > workerproto.MaxRetainedCampaignRuns {
		return fmt.Errorf("%d campaigns hold declared commits, above the protocol limit of %d",
			len(retained), workerproto.MaxRetainedCampaignRuns)
	}
	request.CampaignRefsReported = true
	request.RetainedCampaignRuns = retained
	return nil
}

// CampaignRefReleaseReconciler releases the campaign refs whose provenance
// records retention has removed, on every coordinator boundary. It is a
// reconciler rather than a hook on the prune that removed them because a
// release can fail, and a durable state change must not depend on a Git command
// succeeding; retrying on the next boundary is both simpler and more honest
// than rolling the prune back. Reconciling also converges on a prune performed
// by a process that died before it could release anything.
type CampaignRefReleaseReconciler struct {
	// Records loads the coordinator snapshot the decision is read from.
	Records func(context.Context) (sqlite.CoordinatorRecords, error)
	// Refs is the store whose refs are released.
	Refs CampaignRefReleaser
	// Log receives the Git output of a release, and may be nil.
	Log io.Writer
}

// Tick releases every run the store holds that the coordinator does not
// retain.
//
// It converges on what the store holds rather than walking the runs the
// coordinator still has records for, which is the same rule the worker applies
// to the coordinator's keep list. A run whose records are gone — pruned,
// restored from a backup taken before it existed, or never known to this
// coordinator — is in neither half of the lifetime, and walking the records
// would leave its refs pinned forever with nothing left to name them.
func (r *CampaignRefReleaseReconciler) Tick(ctx context.Context) CampaignRefReleaseReport {
	var report CampaignRefReleaseReport
	if r == nil || r.Records == nil || r.Refs == nil {
		return report
	}
	records, err := r.Records(ctx)
	if err != nil {
		report.Errors = append(report.Errors, fmt.Errorf("load records for campaign commit release: %w", err))
		return report
	}
	held, err := r.Refs.Runs()
	if err != nil {
		report.Errors = append(report.Errors, fmt.Errorf("list campaign commit runs: %w", err))
		return report
	}
	retainedRuns, _ := CampaignRefLifetime(records)
	retained := make(map[string]struct{}, len(retainedRuns))
	for _, runID := range retainedRuns {
		retained[runID] = struct{}{}
	}
	for _, runID := range held {
		if _, keep := retained[runID]; keep {
			continue
		}
		if err := r.Refs.ReleaseRun(ctx, runID, r.Log); err != nil {
			report.Errors = append(report.Errors, fmt.Errorf("release campaign commits of run %s: %w", runID, err))
			continue
		}
		report.Released = append(report.Released, runID)
	}
	return report
}
