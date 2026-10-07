package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/jocasta"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// Ledger boundaries. Every boundary is written once per run: the document is
// created at "open", one record is appended for each terminal attempt of a
// declared task, and "close" is appended once the run's sink is terminal.
const (
	ledgerBoundaryOpen    = "open"
	ledgerBoundaryClose   = "close"
	ledgerAttemptBoundary = "attempt:"

	ledgerHandoffName              = "handoff.md"
	defaultLedgerConflictRetries   = 3
	defaultLedgerMaxHandoffBytes   = 8 << 10
	ledgerBaseBackoff              = time.Minute
	ledgerMaxBackoff               = 30 * time.Minute
	ledgerMaxInlineBytes           = 300
	ledgerMaxStateErrorBytes       = 512
	ledgerRunMarkerPrefix          = "<!-- steward-ledger:run "
	ledgerBoundaryMarkerPrefix     = "<!-- steward-ledger:boundary "
	ledgerMarkerSuffix             = " -->"
	ledgerExecutorProvidedHeadline = "Executor-provided"
)

// LedgerPath is where a run's ledger lives in Jocasta.
func LedgerPath(project, runID string) string {
	return project + "/handoffs/" + runID + ".md"
}

func ledgerMarker(key string) string {
	return ledgerBoundaryMarkerPrefix + key + ledgerMarkerSuffix
}

func ledgerRunMarker(runID string) string {
	return ledgerRunMarkerPrefix + runID + ledgerMarkerSuffix
}

// ledgerHasLine reports whether a whole line of the document is exactly line.
// Markers are matched as whole lines, and executor text is only ever written
// as quoted lines, so no quoted handoff can forge a boundary.
func ledgerHasLine(content []byte, line string) bool {
	return strings.Contains("\n"+string(content)+"\n", "\n"+line+"\n")
}

// ledgerBackoff is the wait after the n-th consecutive failure: one minute,
// doubling, at most thirty.
func ledgerBackoff(failures int) time.Duration {
	delay := ledgerBaseBackoff
	for n := 1; n < failures; n++ {
		delay *= 2
		if delay >= ledgerMaxBackoff {
			return ledgerMaxBackoff
		}
	}
	return delay
}

// LedgerStateStore holds the coordinator's progress on each run's ledger.
type LedgerStateStore interface {
	LoadLedgerStates(context.Context) ([]domain.LedgerState, error)
	SaveLedgerState(context.Context, domain.LedgerState) error
}

// LedgerReconciler writes the Jocasta milestone ledger of every run whose
// workflow opted in, on the coordinator host.
//
// It is a reconciler rather than a hook on the transitions it records, for the
// same reason the campaign commit release is: a ledger write can fail, and no
// durable state change may depend on Jocasta answering. Each pass derives the
// boundaries a run has reached from the coordinator records, compares them with
// the boundaries already applied, and writes the difference in order. A pass
// that cannot reach Jocasta marks the ledger behind and leaves it for a later
// pass after a backoff; the run itself never waits for it.
type LedgerReconciler struct {
	// Records loads the coordinator snapshot boundaries are derived from.
	Records func(context.Context) (sqlite.CoordinatorRecords, error)
	// ReviewRounds loads the review rounds the coordinator recorded for one
	// run. The coordinator snapshot does not carry them, so the verdicts come
	// from here. Nil means verdicts are reported as unavailable; a load that
	// fails leaves the boundary pending, because a record is never revisited.
	ReviewRounds func(context.Context, string) ([]review.Round, error)
	// States persists the applied boundaries and the Jocasta revision.
	States LedgerStateStore
	// Client is the Jocasta CLI configured on the coordinator host.
	Client jocasta.Client
	// Open reads a retained artifact; it supplies handoff.md, which is quoted
	// as executor-provided text. Nil leaves handoffs out.
	Open func(context.Context, string) (domain.Artifact, io.ReadCloser, error)
	// Usage reports a run's attributed provider usage. Nil means usage is
	// reported as unavailable.
	Usage func(context.Context, string) (domain.UsageReport, error)
	// Waits lists task-bound waits, whose ask answers the records name. Nil
	// means ask answers are reported as unavailable.
	Waits func(context.Context) ([]domain.TaskWait, error)
	Now   func() time.Time
	// MaxConflictRetries bounds the re-reads after a conflicting write within
	// one pass; zero means 3.
	MaxConflictRetries int
	// MaxHandoffBytes bounds the quoted handoff; zero means 8 KiB.
	MaxHandoffBytes int

	busy sync.Mutex
}

// LedgerReport says what one pass did.
type LedgerReport struct {
	// Applied names each boundary written, as "<run> <boundary>".
	Applied []string
	// Behind names the runs whose ledger this pass could not bring up to date.
	Behind []string
	// Errors are failures to read coordinator records or record progress.
	Errors []error
	// Busy is set when another pass was still running and this one did nothing.
	Busy bool
}

// errLedgerStateNotSaved marks a write Jocasta accepted whose progress the
// coordinator could not record. The next pass finds the record by its marker.
var errLedgerStateNotSaved = errors.New("ledger progress not recorded")

// Tick runs one reconciliation pass. Concurrent calls do not overlap: a call
// made while a pass is running returns at once with Busy set.
func (r *LedgerReconciler) Tick(ctx context.Context) LedgerReport {
	var report LedgerReport
	if r == nil || r.Records == nil || r.States == nil || r.Client == nil {
		return report
	}
	if !r.busy.TryLock() {
		report.Busy = true
		return report
	}
	defer r.busy.Unlock()
	records, err := r.Records(ctx)
	if err != nil {
		report.Errors = append(report.Errors, fmt.Errorf("load records for the milestone ledger: %w", err))
		return report
	}
	workflows := make(map[string]domain.Workflow)
	for _, workflow := range records.Workflows {
		if workflow.Ledger != nil {
			workflows[workflow.ID] = workflow
		}
	}
	// A coordinator with no ledgered campaign reads no ledger state and runs
	// no command: the opt-out is today's behaviour exactly.
	if len(workflows) == 0 {
		return report
	}
	var runs []domain.WorkflowRun
	for _, run := range records.WorkflowRuns {
		if _, ok := workflows[run.WorkflowID]; ok {
			runs = append(runs, run)
		}
	}
	if len(runs) == 0 {
		return report
	}
	sort.Slice(runs, func(i, j int) bool {
		if !runs[i].CreatedAt.Equal(runs[j].CreatedAt) {
			return runs[i].CreatedAt.Before(runs[j].CreatedAt)
		}
		return runs[i].ID < runs[j].ID
	})
	loaded, err := r.States.LoadLedgerStates(ctx)
	if err != nil {
		report.Errors = append(report.Errors, fmt.Errorf("load milestone ledger state: %w", err))
		return report
	}
	states := make(map[string]domain.LedgerState, len(loaded))
	for _, state := range loaded {
		states[state.RunID] = state
	}
	facts := &ledgerFacts{reconciler: r, records: records}
	for _, run := range runs {
		if ctx.Err() != nil {
			report.Errors = append(report.Errors, ctx.Err())
			return report
		}
		workflow := workflows[run.WorkflowID]
		state, known := states[run.ID]
		if !known {
			state = domain.LedgerState{
				RunID: run.ID, Project: workflow.Ledger.JocastaProject,
				Path: LedgerPath(workflow.Ledger.JocastaProject, run.ID),
			}
		}
		if state.Closed {
			continue
		}
		now := r.now()
		if state.NextAttemptAt != nil && now.Before(*state.NextAttemptAt) {
			continue
		}
		view := newLedgerRunView(records, workflow, run)
		pending := view.pending(state.Applied)
		if len(pending) == 0 {
			continue
		}
		err := r.advance(ctx, &state, view, facts, pending, &report)
		if err == nil {
			continue
		}
		if errors.Is(err, errLedgerStateNotSaved) {
			report.Errors = append(report.Errors, err)
			continue
		}
		state.Behind = true
		state.Failures++
		next := now.Add(ledgerBackoff(state.Failures))
		state.NextAttemptAt = &next
		state.LastError = ledgerInline(err.Error(), ledgerMaxStateErrorBytes)
		state.UpdatedAt = now
		report.Behind = append(report.Behind, run.ID)
		if saveErr := r.States.SaveLedgerState(ctx, state); saveErr != nil {
			report.Errors = append(report.Errors, fmt.Errorf("record that the ledger of run %s is behind: %w", run.ID, saveErr))
		}
	}
	return report
}

// advance writes the pending boundaries in order, recording each one as soon
// as Jocasta accepted it. It stops at the first failure.
func (r *LedgerReconciler) advance(
	ctx context.Context,
	state *domain.LedgerState,
	view ledgerRunView,
	facts *ledgerFacts,
	pending []string,
	report *LedgerReport,
) error {
	for _, key := range pending {
		var (
			revision int64
			err      error
		)
		if key == ledgerBoundaryOpen {
			revision, err = r.open(ctx, state, view)
		} else {
			var record []byte
			if record, err = view.record(ctx, facts, key, r.maxHandoffBytes()); err == nil {
				revision, err = r.append(ctx, state, key, record)
			}
		}
		if err != nil {
			return fmt.Errorf("ledger %s boundary %s: %w", state.Path, key, err)
		}
		state.Applied = append(state.Applied, key)
		state.LastBoundary = key
		state.Revision = revision
		state.Closed = key == ledgerBoundaryClose
		state.Behind, state.Failures, state.NextAttemptAt, state.LastError = false, 0, nil, ""
		state.UpdatedAt = r.now()
		report.Applied = append(report.Applied, state.RunID+" "+key)
		if err := r.States.SaveLedgerState(ctx, *state); err != nil {
			return fmt.Errorf("%w: run %s boundary %s at Jocasta revision %d: %v",
				errLedgerStateNotSaved, state.RunID, key, revision, err)
		}
	}
	return nil
}

// open creates the ledger document. A path that is already taken is adopted
// only if it is this run's ledger, which is what a create whose recorded
// progress was lost leaves behind; anything else at the path is left alone.
func (r *LedgerReconciler) open(ctx context.Context, state *domain.LedgerState, view ledgerRunView) (int64, error) {
	revision, err := r.Client.Create(ctx, state.Path, view.header(), ledgerRequestID(state.RunID, ledgerBoundaryOpen, 0))
	if err == nil {
		return revision, nil
	}
	if !jocasta.IsConflict(err) {
		return 0, err
	}
	document, getErr := r.Client.Get(ctx, state.Path)
	if getErr != nil {
		return 0, getErr
	}
	if ledgerHasLine(document.Content, ledgerRunMarker(state.RunID)) && ledgerHasLine(document.Content, ledgerMarker(ledgerBoundaryOpen)) {
		return document.Revision, nil
	}
	return 0, errors.New("the path holds a document this run's ledger did not create; it is left untouched")
}

// append adds one record with a full fetch, append and fenced put. A conflict
// means someone else wrote first: the document is read again and the record
// appended to the newer content, so nothing anyone wrote is ever overwritten.
func (r *LedgerReconciler) append(ctx context.Context, state *domain.LedgerState, key string, record []byte) (int64, error) {
	retries := r.MaxConflictRetries
	if retries <= 0 {
		retries = defaultLedgerConflictRetries
	}
	for attempt := 0; attempt <= retries; attempt++ {
		document, err := r.Client.Get(ctx, state.Path)
		if err != nil {
			return 0, err
		}
		if !ledgerHasLine(document.Content, ledgerRunMarker(state.RunID)) {
			return 0, errors.New("the document at the ledger path is not this run's ledger; it is left untouched")
		}
		// Written before, but its progress was never recorded.
		if ledgerHasLine(document.Content, ledgerMarker(key)) {
			return document.Revision, nil
		}
		content := append([]byte(nil), document.Content...)
		if len(content) != 0 && content[len(content)-1] != '\n' {
			content = append(content, '\n')
		}
		content = append(append(content, '\n'), record...)
		revision, err := r.Client.Update(ctx, state.Path, content, document.Revision,
			ledgerRequestID(state.RunID, key, document.Revision))
		if err == nil {
			return revision, nil
		}
		if !jocasta.IsConflict(err) {
			return 0, err
		}
	}
	return 0, fmt.Errorf("conflict: the document changed under %d consecutive writes; retrying later", retries+1)
}

func (r *LedgerReconciler) maxHandoffBytes() int {
	if r.MaxHandoffBytes > 0 {
		return r.MaxHandoffBytes
	}
	return defaultLedgerMaxHandoffBytes
}

func (r *LedgerReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

// ledgerRequestID is the Jocasta idempotency key of one write. It names the
// run, the boundary and the revision the write replaces, so repeating a write
// whose receipt was lost commits it at most once.
func ledgerRequestID(runID, key string, base int64) string {
	sum := sha256.Sum256([]byte(runID + "\x00" + key + "\x00" + strconv.FormatInt(base, 10)))
	return "steward-ledger-" + hex.EncodeToString(sum[:])[:40]
}

// ledgerFacts loads, at most once per pass, the facts that do not come with
// the coordinator records.
type ledgerFacts struct {
	reconciler  *LedgerReconciler
	records     sqlite.CoordinatorRecords
	waits       []domain.TaskWait
	waitsLoaded bool
	waitsErr    error
	usage       map[string]ledgerUsage
	rounds      map[string][]review.Round
}

type ledgerUsage struct {
	report domain.UsageReport
	err    error
}

func (f *ledgerFacts) taskWaits(ctx context.Context) ([]domain.TaskWait, error) {
	if f.reconciler.Waits == nil {
		return nil, errors.New("not reported on this coordinator")
	}
	if !f.waitsLoaded {
		f.waits, f.waitsErr = f.reconciler.Waits(ctx)
		f.waitsLoaded = true
	}
	return f.waits, f.waitsErr
}

func (f *ledgerFacts) runUsage(ctx context.Context, runID string) (domain.UsageReport, error) {
	if f.reconciler.Usage == nil {
		return domain.UsageReport{}, errors.New("not reported on this coordinator")
	}
	if f.usage == nil {
		f.usage = make(map[string]ledgerUsage)
	}
	if cached, ok := f.usage[runID]; ok {
		return cached.report, cached.err
	}
	report, err := f.reconciler.Usage(ctx, runID)
	f.usage[runID] = ledgerUsage{report: report, err: err}
	return report, err
}

// reviewRounds loads one run's review rounds at most once per pass. reported
// is false when this coordinator does not report them at all.
func (f *ledgerFacts) reviewRounds(ctx context.Context, runID string) ([]review.Round, bool, error) {
	if f.reconciler.ReviewRounds == nil {
		return nil, false, nil
	}
	if f.rounds == nil {
		f.rounds = make(map[string][]review.Round)
	}
	if cached, ok := f.rounds[runID]; ok {
		return cached, true, nil
	}
	rounds, err := f.reconciler.ReviewRounds(ctx, runID)
	if err != nil {
		return nil, false, fmt.Errorf("load the review rounds of run %s: %w", runID, err)
	}
	var own []review.Round
	for _, round := range rounds {
		if round.WorkflowRunID == runID {
			own = append(own, round)
		}
	}
	if own == nil {
		own = []review.Round{}
	}
	f.rounds[runID] = own
	return own, true, nil
}

// ledgerRunView is one run as the coordinator records describe it.
type ledgerRunView struct {
	workflow    domain.Workflow
	run         domain.WorkflowRun
	tasks       []domain.Task
	taskByID    map[string]domain.Task
	attempts    []domain.Attempt
	assignments map[string]domain.Assignment
	artifacts   map[string][]domain.Artifact
}

func newLedgerRunView(records sqlite.CoordinatorRecords, workflow domain.Workflow, run domain.WorkflowRun) ledgerRunView {
	view := ledgerRunView{
		workflow: workflow, run: run,
		taskByID:    make(map[string]domain.Task),
		assignments: make(map[string]domain.Assignment),
		artifacts:   make(map[string][]domain.Artifact),
	}
	// The run's graph, when it has one, is the authoritative definition: an
	// amendment changes run.Graph and leaves the task templates as they were.
	for _, task := range domain.TasksForRun(run, records.Tasks) {
		view.tasks = append(view.tasks, task)
		view.taskByID[task.ID] = task
	}
	sort.Slice(view.tasks, func(i, j int) bool { return view.tasks[i].Name < view.tasks[j].Name })
	var runAttempts []domain.Attempt
	for _, attempt := range records.Attempts {
		if attempt.WorkflowRunID == run.ID {
			runAttempts = append(runAttempts, attempt)
		}
	}
	view.attempts = domain.DeclaredTaskAttempts(runAttempts)
	for _, assignment := range records.Assignments {
		view.assignments[assignment.ID] = assignment
	}
	for _, artifact := range records.Artifacts {
		if artifact.WorkflowRunID == run.ID && artifact.AttemptID != "" && artifact.Kind == domain.ArtifactOutput {
			view.artifacts[artifact.AttemptID] = append(view.artifacts[artifact.AttemptID], artifact)
		}
	}
	for attemptID := range view.artifacts {
		outputs := view.artifacts[attemptID]
		sort.Slice(outputs, func(i, j int) bool {
			if outputs[i].Name != outputs[j].Name {
				return outputs[i].Name < outputs[j].Name
			}
			return outputs[i].ID < outputs[j].ID
		})
	}
	return view
}

// pending lists the boundaries the run has reached and the ledger has not
// applied, in the order they are written.
func (v ledgerRunView) pending(applied []string) []string {
	done := make(map[string]bool, len(applied))
	for _, key := range applied {
		done[key] = true
	}
	var reached []string
	if !done[ledgerBoundaryOpen] {
		reached = append(reached, ledgerBoundaryOpen)
	}
	var terminal []domain.Attempt
	for _, attempt := range v.attempts {
		if attempt.Progress.Terminal() && !done[ledgerAttemptBoundary+attempt.ID] {
			terminal = append(terminal, attempt)
		}
	}
	sort.Slice(terminal, func(i, j int) bool {
		left, right := ledgerAttemptTime(terminal[i]), ledgerAttemptTime(terminal[j])
		if !left.Equal(right) {
			return left.Before(right)
		}
		return terminal[i].ID < terminal[j].ID
	})
	for _, attempt := range terminal {
		reached = append(reached, ledgerAttemptBoundary+attempt.ID)
	}
	// The closing record comes last, after every attempt record it summarises.
	if runSettled(v.run) && !done[ledgerBoundaryClose] {
		reached = append(reached, ledgerBoundaryClose)
	}
	return reached
}

func ledgerAttemptTime(attempt domain.Attempt) time.Time {
	if attempt.CompletedAt != nil {
		return *attempt.CompletedAt
	}
	return attempt.UpdatedAt
}

// header is the document created at submission. It is a pure function of the
// run's records, so a create repeated with the same idempotency key is the
// same request.
func (v ledgerRunView) header() []byte {
	ledger := v.workflow.Ledger
	var b strings.Builder
	b.WriteString(ledgerRunMarker(v.run.ID) + "\n")
	b.WriteString(ledgerMarker(ledgerBoundaryOpen) + "\n")
	fmt.Fprintf(&b, "# Milestone ledger: %s\n\n", ledgerInline(v.workflow.Name, ledgerMaxInlineBytes))
	b.WriteString("Written by Steward on the coordinator. Each record has a \"Steward record\" part, " +
		"which holds facts the coordinator itself holds, and an \"" + ledgerExecutorProvidedHeadline + "\" part, " +
		"which quotes the task's own handoff.md and is not verified by Steward.\n\n")
	fmt.Fprintf(&b, "- Run: %s\n", v.run.ID)
	fmt.Fprintf(&b, "- Workflow: %s (%s), project %s\n", ledgerInline(v.workflow.Name, ledgerMaxInlineBytes), v.workflow.ID,
		ledgerOr(ledgerInline(v.workflow.Project, ledgerMaxInlineBytes), "not recorded"))
	fmt.Fprintf(&b, "- Plan: %s\n", ledgerOr(ledgerInline(ledger.Plan, 512), "not declared"))
	fmt.Fprintf(&b, "- Risk: %s\n", ledgerOr(ledger.Risk, "not declared"))
	fmt.Fprintf(&b, "- Submitted: %s\n", v.run.CreatedAt.UTC().Format(time.RFC3339))
	b.WriteString("\n## Acceptance criteria\n\n")
	if len(ledger.Acceptance) == 0 {
		b.WriteString("None declared.\n")
	}
	for n, criterion := range ledger.Acceptance {
		fmt.Fprintf(&b, "%d. %s\n", n+1, ledgerInline(criterion, 1024))
	}
	b.WriteString("\n## Milestones\n\nRun status when this ledger was created: " + string(v.run.Progress) + ".\n\n")
	b.WriteString("| Task | Needs | Declared outputs |\n| --- | --- | --- |\n")
	for _, task := range v.tasks {
		var outputs []string
		for _, output := range task.Outputs {
			outputs = append(outputs, output.Name)
		}
		fmt.Fprintf(&b, "| %s | %s | %s |\n", ledgerCell(task.Name), ledgerCell(strings.Join(task.Needs, ", ")),
			ledgerCell(strings.Join(outputs, ", ")))
	}
	b.WriteString("\n## Records\n")
	return []byte(b.String())
}

// record renders the record of one boundary after open. It fails only when a
// fact the record must carry could not be read, so the boundary stays pending.
func (v ledgerRunView) record(ctx context.Context, facts *ledgerFacts, key string, maxHandoff int) ([]byte, error) {
	rounds, reported, err := facts.reviewRounds(ctx, v.run.ID)
	if err != nil {
		return nil, err
	}
	if key == ledgerBoundaryClose {
		return v.closing(ctx, facts, rounds, reported), nil
	}
	attemptID := strings.TrimPrefix(key, ledgerAttemptBoundary)
	var attempt domain.Attempt
	for _, candidate := range v.attempts {
		if candidate.ID == attemptID {
			attempt = candidate
		}
	}
	task := v.taskByID[attempt.TaskID]
	var b strings.Builder
	b.WriteString(ledgerMarker(key) + "\n")
	fmt.Fprintf(&b, "### %s attempt %d: %s (%s)\n\n", ledgerOr(task.Name, attempt.TaskID), attempt.Number, attempt.Progress,
		ledgerAttemptTime(attempt).UTC().Format(time.RFC3339))
	b.WriteString("Steward record (facts the coordinator holds):\n\n")
	taskFacts := v.taskProgress(task, attempt, rounds, reported)
	stateLine := fmt.Sprintf("progress %s, control %s", taskFacts.State, taskFacts.Control)
	if attempt.Failure != "" {
		stateLine += "; failure: " + ledgerInline(attempt.Failure, ledgerMaxInlineBytes)
	}
	fmt.Fprintf(&b, "- State: %s\n", stateLine)
	fmt.Fprintf(&b, "- Route: %s\n", v.route(attempt))
	fmt.Fprintf(&b, "- Review verdicts: %s\n", taskFacts.ReviewVerdicts)
	fmt.Fprintf(&b, "- Retained outputs: %s\n", v.outputs(attempt.ID))
	fmt.Fprintf(&b, "- Usage: %s\n", v.attemptUsage(ctx, facts, attempt.ID))
	fmt.Fprintf(&b, "- Ask answers: %s\n", v.answers(ctx, facts, attempt.ID))
	b.WriteString("\n")
	b.WriteString(v.handoff(ctx, facts, attempt.ID, maxHandoff))
	return []byte(b.String()), nil
}

func (v ledgerRunView) closing(ctx context.Context, facts *ledgerFacts, rounds []review.Round, reported bool) []byte {
	var b strings.Builder
	b.WriteString(ledgerMarker(ledgerBoundaryClose) + "\n")
	completed := v.run.UpdatedAt
	if v.run.Sink != nil && v.run.Sink.CompletedAt != nil {
		completed = *v.run.Sink.CompletedAt
	} else if v.run.CompletedAt != nil {
		completed = *v.run.CompletedAt
	}
	outcome := v.run.Progress
	if v.run.Sink != nil {
		outcome = v.run.Sink.Progress
	}
	fmt.Fprintf(&b, "### Run closed: %s (%s)\n\n", outcome, completed.UTC().Format(time.RFC3339))
	b.WriteString("Steward record (facts the coordinator holds):\n\n")
	fmt.Fprintf(&b, "- Final state: run %s, sink %s\n", v.run.Progress, outcome)
	latest := make(map[string]domain.Attempt)
	for _, attempt := range v.attempts {
		if current, ok := latest[attempt.TaskID]; !ok || attempt.Number > current.Number {
			latest[attempt.TaskID] = attempt
		}
	}
	var tasks []string
	for _, task := range v.tasks {
		if attempt, ok := latest[task.ID]; ok {
			tasks = append(tasks, fmt.Sprintf("%s %s (attempt %d)", task.Name, attempt.Progress, attempt.Number))
		} else {
			tasks = append(tasks, task.Name+" never attempted")
		}
	}
	fmt.Fprintf(&b, "- Tasks: %s\n", ledgerOr(strings.Join(tasks, "; "), "none"))
	if reported {
		var combined []string
		for _, round := range rounds {
			combined = append(combined, fmt.Sprintf("%s: %s", round.ID, ledgerOr(round.Combined, "no combined verdict")))
		}
		sort.Strings(combined)
		fmt.Fprintf(&b, "- Review rounds: %s\n", ledgerOr(strings.Join(combined, "; "), "none recorded"))
	} else {
		b.WriteString("- Review rounds: " + ledgerReviewsUnavailable + "\n")
	}
	usage, err := facts.runUsage(ctx, v.run.ID)
	if err != nil {
		fmt.Fprintf(&b, "- Usage: unavailable (%s)\n", ledgerInline(err.Error(), ledgerMaxInlineBytes))
	} else {
		fmt.Fprintf(&b, "- Usage: %s, coverage %s\n", ledgerTotals(usage.Totals), ledgerOr(string(usage.Coverage.State), "unknown"))
	}
	return []byte(b.String())
}

func (v ledgerRunView) route(attempt domain.Attempt) string {
	assignment, ok := v.assignments[attempt.AssignmentID]
	if attempt.AssignmentID == "" || !ok {
		return "no assignment was recorded"
	}
	facts := v.taskProgress(domain.Task{}, attempt, nil, false)
	route := assignment.Route
	effort := ledgerOr(facts.Effort, "not set")
	return fmt.Sprintf("%s/%s, effort %s, worker %s", ledgerInline(route.ProviderInstanceID, 128), ledgerInline(route.Model, 128),
		ledgerInline(effort, 64), ledgerOr(ledgerInline(assignment.WorkerID, 128), "unknown"))
}

const ledgerReviewsUnavailable = "unavailable (not reported on this coordinator)"

// ledgerVerdicts lists the verdicts of the reviewers whose task is taskID,
// from the rounds of the record's own run.
func ledgerVerdicts(rounds []review.Round, reported bool, taskID string) string {
	if !reported {
		return ledgerReviewsUnavailable
	}
	var verdicts []string
	for _, round := range rounds {
		for _, reviewer := range round.Reviewers {
			if taskID == "" || reviewer.TaskID != taskID {
				continue
			}
			verdict := "no verdict, state " + ledgerOr(reviewer.State, "unknown")
			if reviewer.Verdict != nil {
				verdict = reviewer.Verdict.Verdict
			}
			verdicts = append(verdicts, fmt.Sprintf("%s reviewer %s (%s, %s): %s", round.ID, reviewer.ID, reviewer.Role,
				ledgerInline(reviewer.Route, 256), ledgerInline(verdict, 64)))
		}
	}
	sort.Strings(verdicts)
	return ledgerOr(strings.Join(verdicts, "; "), "none recorded")
}

func (v ledgerRunView) outputs(attemptID string) string {
	var outputs []string
	for _, artifact := range v.artifacts[attemptID] {
		outputs = append(outputs, fmt.Sprintf("%s sha256:%s (%d bytes, artifact %s)",
			ledgerInline(artifact.Name, 256), artifact.SHA256, artifact.Size, artifact.ID))
	}
	return ledgerOr(strings.Join(outputs, "; "), "none")
}

func (v ledgerRunView) attemptUsage(ctx context.Context, facts *ledgerFacts, attemptID string) string {
	report, err := facts.runUsage(ctx, v.run.ID)
	if err != nil {
		return "unavailable (" + ledgerInline(err.Error(), ledgerMaxInlineBytes) + ")"
	}
	for _, aggregate := range report.ByAttempt {
		if aggregate.AttemptID == attemptID {
			return ledgerTotals(aggregate.Totals) + ", as attributed at this boundary"
		}
	}
	return "none attributed at this boundary"
}

func ledgerTotals(totals domain.UsageTotals) string {
	text := fmt.Sprintf("%d calls, %d turns, %d input, %d cache read, %d cache write, %d output tokens",
		totals.Calls, totals.Turns, totals.UncachedInputTokens, totals.CacheReadTokens, totals.CacheWriteTokens, totals.OutputTokens)
	if totals.ProviderCostReported {
		text += fmt.Sprintf(", provider cost $%.4f", totals.ProviderCostUSD)
	}
	return text
}

func (v ledgerRunView) answers(ctx context.Context, facts *ledgerFacts, attemptID string) string {
	waits, err := facts.taskWaits(ctx)
	if err != nil {
		return "unavailable (" + ledgerInline(err.Error(), ledgerMaxInlineBytes) + ")"
	}
	var answers []string
	for _, wait := range waits {
		if wait.WorkflowRunID != v.run.ID || wait.AttemptID != attemptID || wait.AskAnswer == nil {
			continue
		}
		answer := wait.AskAnswer
		chosen := strings.Join(answer.Options, ", ")
		if answer.FreeText != "" {
			chosen = strings.TrimPrefix(chosen+"; free text: "+answer.FreeText, "; ")
		}
		answers = append(answers, fmt.Sprintf("%q answered %q by %s via %s at %s",
			ledgerInline(answer.Question, ledgerMaxInlineBytes), ledgerInline(chosen, ledgerMaxInlineBytes),
			ledgerOr(ledgerInline(answer.AnsweredBy, 128), "nobody"), ledgerOr(string(answer.Source), "unknown"),
			answer.AnsweredAt.UTC().Format(time.RFC3339)))
	}
	return ledgerOr(strings.Join(answers, "; "), "none")
}

// handoff quotes the attempt's retained handoff.md, bounded and labelled as
// executor-provided. Every line is quoted, so nothing in it is read as a
// marker or as Steward's own text.
func (v ledgerRunView) handoff(ctx context.Context, facts *ledgerFacts, attemptID string, limit int) string {
	var handoff *domain.Artifact
	for _, artifact := range v.artifacts[attemptID] {
		if artifact.Name == ledgerHandoffName {
			artifact := artifact
			handoff = &artifact
		}
	}
	if handoff == nil {
		return ledgerExecutorProvidedHeadline + ": No handoff.md was retained for this attempt.\n"
	}
	pointer := fmt.Sprintf("artifact %s, sha256:%s", handoff.ID, handoff.SHA256)
	if facts.reconciler.Open == nil {
		return ledgerExecutorProvidedHeadline + ": handoff.md is retained as " + pointer + "; this coordinator does not quote it.\n"
	}
	_, content, err := facts.reconciler.Open(ctx, handoff.ID)
	if err != nil {
		return ledgerExecutorProvidedHeadline + ": handoff.md is retained as " + pointer + " but could not be read (" +
			ledgerInline(err.Error(), ledgerMaxInlineBytes) + ").\n"
	}
	defer content.Close()
	raw, err := io.ReadAll(io.LimitReader(content, int64(limit)+utf8.UTFMax))
	if err != nil {
		return ledgerExecutorProvidedHeadline + ": handoff.md is retained as " + pointer + " but could not be read (" +
			ledgerInline(err.Error(), ledgerMaxInlineBytes) + ").\n"
	}
	truncated := len(raw) > limit || handoff.Size > int64(limit)
	if len(raw) > limit {
		cut := limit
		for cut > 0 && !utf8.RuneStart(raw[cut]) {
			cut--
		}
		raw = raw[:cut]
	}
	text := strings.TrimRight(ledgerSanitize(string(raw)), "\n")
	var b strings.Builder
	fmt.Fprintf(&b, "%s (quoted from handoff.md, %s; not verified by Steward):\n\n", ledgerExecutorProvidedHeadline, pointer)
	for _, line := range strings.Split(text, "\n") {
		if line == "" {
			b.WriteString(">\n")
			continue
		}
		b.WriteString("> " + line + "\n")
	}
	if truncated {
		fmt.Fprintf(&b, "\nTruncated at %d of %d bytes; the full text is retained as %s.\n", len(raw), handoff.Size, pointer)
	}
	return b.String()
}

// ledgerSanitize keeps executor text valid UTF-8 without control characters
// other than tabs and line breaks.
func ledgerSanitize(text string) string {
	text = strings.ToValidUTF8(text, "�")
	text = strings.ReplaceAll(text, "\r\n", "\n")
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, text)
}

// ledgerInline renders text as one bounded line.
func ledgerInline(text string, limit int) string {
	text = strings.Join(strings.Fields(ledgerSanitize(text)), " ")
	if len(text) <= limit {
		return text
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + "..."
}

func ledgerCell(text string) string {
	text = strings.ReplaceAll(ledgerInline(text, ledgerMaxInlineBytes), "|", "\\|")
	return ledgerOr(text, "-")
}

func ledgerOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
