package backlog

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// CampaignRefReleaser ends the declared campaign lifetime of one run's commits.
// It is the CampaignRefStore in production and a recorder in tests, so that the
// decision about which runs may be released can be exercised without a Git
// repository.
type CampaignRefReleaser interface {
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

// PlanCampaignRefRelease names the runs whose declared commits may stop being
// pinned. A run qualifies when its coordinator-owned sink is terminal, which is
// the point at which the run is over and its outcome can no longer change.
//
// A run is held back while another run that has not settled carries one of its
// artifacts by reference. That is how a rerun consumes an ancestor's declared
// commit: the carried artifact is the commit's provenance record, and the
// record names the source run's campaign ref, so releasing the source would
// take the commit away from a run that still needs it. Any carried input holds
// the producing run, not only one whose declaration says commit, because the
// cost of holding a handful of refs for the life of a rerun is nothing and the
// cost of being wrong is a rerun that cannot start.
//
// Known gap, deliberately not papered over: a rerun may only be created from a
// run that has already finished, so a rerun authored long after its source
// settled finds the source's refs already released. Closing that needs either a
// republication of the carried commit into the new run's own namespace or a
// campaign-ref lifetime tied to artifact retention rather than to settlement.
// Both are larger decisions than this function, and neither is made here.
func PlanCampaignRefRelease(records sqlite.CoordinatorRecords) []string {
	taskRun := make(map[string]string)
	for _, run := range records.WorkflowRuns {
		for _, task := range domain.TasksForRun(run, records.Tasks) {
			taskRun[task.ID] = run.ID
		}
	}
	held := make(map[string]bool)
	for _, run := range records.WorkflowRuns {
		if runSettled(run) {
			continue
		}
		for _, task := range domain.TasksForRun(run, records.Tasks) {
			for _, carried := range task.CarriedInputs {
				if producer := taskRun[carried.ProducerTaskID]; producer != "" {
					held[producer] = true
				}
			}
		}
	}
	var release []string
	for _, run := range records.WorkflowRuns {
		if runSettled(run) && !held[run.ID] {
			release = append(release, run.ID)
		}
	}
	sort.Strings(release)
	return release
}

// runSettled reports whether the coordinator-owned sink has reached a terminal
// state, which is the one durable statement that the run is over.
func runSettled(run domain.WorkflowRun) bool {
	return run.Sink != nil && run.Sink.Progress.Terminal()
}

// CampaignRefReleaseReconciler releases the campaign refs of settled runs on
// every coordinator boundary. It is a reconciler rather than a hook on the
// settlement transition because a release can fail, and a durable state change
// must not depend on a Git command succeeding; retrying on the next boundary is
// both simpler and more honest than rolling settlement back.
type CampaignRefReleaseReconciler struct {
	// Records loads the coordinator snapshot the decision is read from.
	Records func(context.Context) (sqlite.CoordinatorRecords, error)
	// Refs is the store whose refs are released.
	Refs CampaignRefReleaser
	// Log receives the Git output of a release, and may be nil.
	Log io.Writer

	// released remembers what this process already released, so a settled run
	// is not walked again on every boundary. It is an optimisation and not the
	// idempotency guarantee: ReleaseRun is itself idempotent, so a restart that
	// loses this map releases nothing twice.
	released map[string]struct{}
}

// Tick releases every settled run that nothing still needs.
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
	if r.released == nil {
		r.released = make(map[string]struct{})
	}
	for _, runID := range PlanCampaignRefRelease(records) {
		if _, done := r.released[runID]; done {
			continue
		}
		if err := r.Refs.ReleaseRun(ctx, runID, r.Log); err != nil {
			report.Errors = append(report.Errors, fmt.Errorf("release campaign commits of run %s: %w", runID, err))
			continue
		}
		r.released[runID] = struct{}{}
		report.Released = append(report.Released, runID)
	}
	return report
}
