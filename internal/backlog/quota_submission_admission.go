package backlog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// SubmissionQuotaAdmission is shared by both submission entry points. It reads
// built attempts, not manifest dependency glosses, and uses planner arithmetic.
type SubmissionQuotaAdmission struct {
	Bridge QuotaBridge
	Now    func() time.Time
}

type QuotaSubmissionRefusal struct {
	Receipt *domain.QuotaAdmissionReceipt
	Detail  string
}

func (e *QuotaSubmissionRefusal) Error() string {
	return "quota admission refused: " + e.Detail + "; no run was created"
}

type admittedCoordinatorStore interface {
	CheckQuotaAdmission(context.Context, sqlite.QuotaAdmissionGate) (*domain.QuotaAdmissionReceipt, error)
	SaveAdmittedCoordinatorRecords(context.Context, sqlite.CoordinatorRecords, sqlite.QuotaAdmissionGate) error
}

// TaskQuotaEstimate is the original task estimate used at admission and by the planner.
func TaskQuotaEstimate(task domain.Task) float64 {
	if task.EstimatedCost != nil {
		return *task.EstimatedCost
	}
	return SeedCost(task.Difficulty)
}

// dispatchableQuotaRoot is the one eligibility rule for new reservations and
// durable ready demand. Future work does not consume current admission headroom.
func dispatchableQuotaRoot(attempt domain.Attempt, task domain.Task, now time.Time) bool {
	return attempt.Progress == domain.ProgressReady && (task.NotBefore == nil || !now.Before(*task.NotBefore))
}

func (a SubmissionQuotaAdmission) evaluate(records sqlite.CoordinatorRecords, snapshot sqlite.QuotaAdmissionSnapshot) (*domain.QuotaAdmissionReceipt, error) {
	now := time.Now().UTC()
	if a.Now != nil {
		now = a.Now().UTC()
	}
	age := snapshot.StaleAfter
	if age <= 0 {
		age = domain.DefaultQuotaStaleAfter
	}
	bridge := a.Bridge
	// The planner forecast horizon is independent of telemetry freshness.
	// Keep its configured budget horizon; only default it for direct callers.
	if bridge.MaxObservationAge <= 0 {
		bridge.MaxObservationAge = age
	}
	receipt := &domain.QuotaAdmissionReceipt{AdmittedAt: now}
	pools := map[string]domain.QuotaPool{}
	owners := map[string]string{}
	for _, pool := range snapshot.Records.QuotaPools {
		pools[pool.ID] = pool
		for _, instance := range pool.ProviderInstanceIDs {
			if owner, exists := owners[instance]; exists && owner != pool.ID {
				return receipt, fmt.Errorf("quota admission: provider instance %s belongs to multiple quota pools", instance)
			}
			owners[instance] = pool.ID
		}
	}
	poolID := func(route domain.ProviderRoute) string {
		if route.QuotaPoolID != "" {
			return route.QuotaPoolID
		}
		return owners[route.ProviderInstanceID]
	}
	// Receipts pin the chosen route for existing admitted roots. Older records
	// have no receipt, so reserve conservatively on each distinct candidate pool.
	chosen := map[string]domain.ProviderRoute{}
	for _, run := range snapshot.Records.WorkflowRuns {
		if run.QuotaAdmission != nil {
			for _, root := range run.QuotaAdmission.Roots {
				chosen[run.ID+"\x00"+root.TaskID] = root.Route
			}
		}
	}
	assigned := map[string]bool{}
	for _, assignment := range snapshot.Records.Assignments {
		if assignment.State != domain.AssignmentReleased && assignment.State != domain.AssignmentCompleted {
			assigned[assignment.AttemptID] = true
		}
	}
	demand := map[string]float64{}
	for _, attempt := range snapshot.Records.Attempts {
		if attempt.Progress != domain.ProgressReady || attempt.Control != domain.ControlUnassigned || assigned[attempt.ID] {
			continue
		}
		task, found := domain.TaskForAttempt(attempt, snapshot.Records.WorkflowRuns, snapshot.Records.Tasks)
		if !found {
			return receipt, fmt.Errorf("quota admission: pending attempt %s has no task", attempt.ID)
		}
		if !dispatchableQuotaRoot(attempt, task, now) {
			continue
		}
		routes := task.Routes
		if route, ok := chosen[attempt.WorkflowRunID+"\x00"+task.ID]; ok {
			routes = []domain.ProviderRoute{route}
		}
		seen := map[string]bool{}
		cost := TaskQuotaEstimate(task)
		if !nonnegativeFinite(cost) {
			return receipt, fmt.Errorf("quota admission: task %s estimate must be nonnegative and finite", task.Name)
		}
		for _, route := range routes {
			id := poolID(route)
			if id != "" && !seen[id] {
				demand[id] += cost
				if !nonnegativeFinite(demand[id]) {
					return receipt, fmt.Errorf("quota admission: pool %s pending demand overflow", id)
				}
				seen[id] = true
			}
		}
	}
	ceiling := map[string]float64{}
	for _, task := range records.Tasks {
		cost := TaskQuotaEstimate(task)
		if !nonnegativeFinite(cost) {
			return receipt, fmt.Errorf("quota admission: task %s estimate must be nonnegative and finite", task.Name)
		}
		seen := map[string]bool{}
		for _, route := range task.Routes {
			id := poolID(route)
			if !seen[id] {
				ceiling[id] += cost
				if !nonnegativeFinite(ceiling[id]) {
					return receipt, fmt.Errorf("quota admission: pool %s campaign ceiling overflow", id)
				}
				seen[id] = true
			}
		}
	}
	poolReceipts := map[string]*domain.QuotaAdmissionPoolReceipt{}
	poolErrors := map[string]string{}
	for id, cost := range ceiling {
		entry := &domain.QuotaAdmissionPoolReceipt{PoolID: id, AlreadyAdmittedDemand: demand[id], CampaignBudgetCeiling: cost, Decision: "admitted"}
		poolReceipts[id] = entry
		pool, known := pools[id]
		switch {
		case bridge.Disabled || pool.ChecksDisabled:
			entry.Decision = "not checked: quota checks disabled"
			continue
		case id == "" || unmeteredQuotaPoolID(id):
			entry.Decision = "not checked: unmetered route"
			continue
		case !known:
			entry.Decision = "refused"
			poolErrors[id] = "quota-unknown"
			continue
		}
		entry.WindowSet = domain.ReadQuotaWindows(pool, snapshot.States, now, age)
		if problem, bad := entry.WindowSet.Problem(); bad {
			detail := problem.Code + " " + problem.Window
			if problem.ObservedAt != nil {
				detail += fmt.Sprintf(" (observed %s, older than %s)", problem.ObservedAt.UTC().Format(time.RFC3339), age)
			}
			entry.Decision = "refused"
			poolErrors[id] = detail
			continue
		}
		// ReadQuotaWindows selected each governing reading. Derive budgets only
		// from those exact newest keys, so irrelevant or older readings cannot drift.
		var states []domain.BucketState
		for _, reading := range entry.WindowSet.Windows {
			var newest domain.BucketState
			for _, state := range snapshot.States {
				if state.Key == reading.Key && (newest.ObservedAt.IsZero() || state.ObservedAt.After(newest.ObservedAt)) {
					newest = state
				}
			}
			states = append(states, newest)
		}
		windowOwners := map[string]string{}
		for _, state := range states {
			windowOwners[state.Key.ProviderInstanceID] = id
		}
		windows, err := deriveQuotaPlanningWindows(now, []domain.QuotaPool{pool}, states, windowOwners, bridge)
		if err != nil {
			entry.Decision = "refused"
			poolErrors[id] = err.Error()
			continue
		}
		for _, window := range windows {
			freshness := quotaAdmissionSession{now: now, maxObservationAge: age}
			if detail, stale := freshness.quotaObservationStaleness(window); stale {
				entry.Decision = "refused"
				poolErrors[id] = detail
				break
			}
			var key domain.BucketKey
			for _, state := range states {
				if state.Key.String() == window.WindowID {
					key = state.Key
					break
				}
			}
			entry.Windows = append(entry.Windows, domain.QuotaAdmissionWindowReceipt{Key: key, UsedPercent: window.CurrentUsage, AvailableHeadroom: quotaAvailable(window) - demand[id], AlreadyAdmittedDemand: demand[id], SafetyMarginPercent: window.SafetyMargin})
		}
	}
	finish := func() {
		receipt.Pools = nil
		ids := make([]string, 0, len(poolReceipts))
		for id := range poolReceipts {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			receipt.Pools = append(receipt.Pools, *poolReceipts[id])
		}
	}
	tasks := map[string]domain.Task{}
	for _, task := range records.Tasks {
		tasks[task.ID] = task
	}
	for _, attempt := range records.Attempts {
		task, ok := tasks[attempt.TaskID]
		if !ok {
			return receipt, fmt.Errorf("quota admission: attempt %s has no built task", attempt.ID)
		}
		if !dispatchableQuotaRoot(attempt, task, now) {
			continue
		}
		cost := TaskQuotaEstimate(task)
		var lastDetail string
		var rejected *domain.QuotaAdmissionPoolReceipt
		selected := false
		for _, route := range task.Routes {
			id := poolID(route)
			entry := poolReceipts[id]
			if entry == nil {
				return receipt, fmt.Errorf("quota admission: route pool %s missing ceiling", id)
			}
			rejected = entry
			if detail := poolErrors[id]; detail != "" {
				lastDetail = fmt.Sprintf("pool %s: %s; campaign ceiling %g", id, detail, entry.CampaignBudgetCeiling)
				continue
			}
			if entry.Decision == "admitted" {
				if quotaAdmissionBlocked(pools[id].Admission, task.Class) {
					lastDetail = fmt.Sprintf("pool %s: admission %s; campaign ceiling %g", id, pools[id].Admission, entry.CampaignBudgetCeiling)
					continue
				}
				fits := true
				for _, window := range entry.Windows {
					if entry.RootCost+cost > window.AvailableHeadroom {
						names := []string{}
						for _, root := range receipt.Roots {
							if poolID(root.Route) == id {
								names = append(names, root.TaskName)
							}
						}
						names = append(names, task.Name)
						lastDetail = fmt.Sprintf("pool %s window %s needs %g for %d root task(s) (%s), %g available (used %g%%, admitted %g, margin %g); campaign ceiling %g", id, window.Key.String(), entry.RootCost+cost, len(names), strings.Join(names, ", "), window.AvailableHeadroom, window.UsedPercent, window.AlreadyAdmittedDemand, window.SafetyMarginPercent, entry.CampaignBudgetCeiling)
						fits = false
						break
					}
				}
				if !fits {
					continue
				}
			}
			route.QuotaPoolID = id
			entry.RootCost += cost
			receipt.Roots = append(receipt.Roots, domain.QuotaAdmissionRootReceipt{TaskID: task.ID, TaskName: task.Name, Route: route, Cost: cost, Decision: entry.Decision})
			selected = true
			break
		}
		if !selected {
			if lastDetail == "" {
				lastDetail = fmt.Sprintf("task %s has no admissible route", task.Name)
			}
			if rejected != nil {
				rejected.RootCost += cost
				rejected.Decision = "refused"
			}
			finish()
			return receipt, &QuotaSubmissionRefusal{Receipt: receipt, Detail: lastDetail}
		}
	}
	finish()
	for index := range receipt.Pools {
		entry := &receipt.Pools[index]
		hasRoot := false
		for _, root := range receipt.Roots {
			if root.Route.QuotaPoolID == entry.PoolID {
				hasRoot = true
				break
			}
		}
		if !hasRoot {
			entry.Decision = "not checked: no dispatchable roots"
		}
	}
	return receipt, nil
}

func (i BundleIngester) precheckQuota(ctx context.Context, manifest Manifest, root string, source *os.Root, relativePaths, inputPaths []string, bindings map[string][]directoryresource.Binding) error {
	if i.QuotaAdmission == nil || i.RegisterOnly {
		return nil
	}
	store, ok := i.Store.(admittedCoordinatorStore)
	if !ok {
		return fmt.Errorf("quota admission: coordinator store lacks transactional admission")
	}
	files := map[string]ingestedFile{}
	for _, relative := range relativePaths {
		resolved, err := safeBundleFile(root, relative)
		if err != nil {
			return err
		}
		name, err := filepath.Rel(root, resolved)
		if err != nil {
			return err
		}
		f, err := source.Open(name)
		if err != nil {
			return err
		}
		hash := sha256.New()
		size, err := io.Copy(hash, f)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		files[relative] = ingestedFile{relative: relative, size: size, sha256: fmt.Sprintf("%x", hash.Sum(nil)), storagePath: filepath.ToSlash(filepath.Join("workflows", "quota-preview", "files", relative))}
	}
	// Preview IDs are private, deterministic local labels. No public generator
	// runs, no record is written and no bundle directory exists before this check.
	preview := i
	next := 0
	preview.NewTypedID = func(kind string) string { next++; return fmt.Sprintf("quota-preview-%s-%d", kind, next) }
	records, _, err := preview.buildRecords(manifest, "quota-preview-workflow", "quota-preview-run", inputPaths, files, bindings, i.now())
	if err != nil {
		return err
	}
	_, err = store.CheckQuotaAdmission(ctx, func(snapshot sqlite.QuotaAdmissionSnapshot) (*domain.QuotaAdmissionReceipt, error) {
		return i.QuotaAdmission.evaluate(records, snapshot)
	})
	return err
}

func (i BundleIngester) saveQuotaRecords(ctx context.Context, records *sqlite.CoordinatorRecords) error {
	if i.QuotaAdmission == nil || i.RegisterOnly {
		return i.Store.SaveCoordinatorRecords(ctx, *records)
	}
	store, ok := i.Store.(admittedCoordinatorStore)
	if !ok {
		return fmt.Errorf("quota admission: coordinator store lacks transactional admission")
	}
	var receipt *domain.QuotaAdmissionReceipt
	err := store.SaveAdmittedCoordinatorRecords(ctx, *records, func(snapshot sqlite.QuotaAdmissionSnapshot) (*domain.QuotaAdmissionReceipt, error) {
		var err error
		receipt, err = i.QuotaAdmission.evaluate(*records, snapshot)
		return receipt, err
	})
	if err == nil {
		for n := range records.WorkflowRuns {
			records.WorkflowRuns[n].QuotaAdmission = receipt
		}
	}
	return err
}
