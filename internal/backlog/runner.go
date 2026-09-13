package backlog

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/report"
)

// Control is what the runner needs from the T3 adapter.
type Control interface {
	ListProjects(ctx context.Context) ([]t3control.Project, error)
	CreateAndStartThread(ctx context.Context, in t3control.NewThreadInput) (string, error)
	ResumeThread(ctx context.Context, thread domain.Thread, prompt string) error
	LastAssistantMessage(ctx context.Context, threadID string) (string, error)
}

// Store is what the runner needs from persistence.
type Store interface {
	SaveTaskState(ctx context.Context, id, status string, state any) error
	LoadTaskStates(ctx context.Context) (map[string]json.RawMessage, error)
	RegisterDispatchedThread(ctx context.Context, threadID, taskID, project string, at time.Time) error
	DispatchedThreads(ctx context.Context) (map[string]string, error)
	Observations(ctx context.Context, from, to time.Time) ([]domain.Observation, error)
	RecordAction(ctx context.Context, a domain.ActionRecord) error
}

// Options configure the runner.
type Options struct {
	DisableQuotaChecks       bool
	Dir                      string
	Preamble                 string
	QuietFor                 time.Duration
	SafetyMargin             float64
	FallbackPerHour          float64
	Quantile                 float64
	MinSamples               int
	LongWindowCap            float64
	HistoryDays              int
	DryRun                   bool
	MaxConcurrentPerProvider int
	Logger                   *slog.Logger
	// LocalHost is this machine's name as tasks refer to it. Tasks whose
	// host is empty use DefaultHost; tasks for any other host are handed
	// to Forward.
	LocalHost   string
	DefaultHost string
	Forward     func(ctx context.Context, host string, task Task) error
	// DataDir is T3's base directory, for validating provider instances
	// and models against T3's caches before a dispatch.
	DataDir string
}

// Runner drives the backlog.
type Runner struct {
	opts    Options
	store   Store
	control Control
	log     *slog.Logger
	now     func() time.Time

	states          map[string]*State
	lastInteractive time.Time
	demand          map[domain.BucketKey]report.Demand
	demandBuiltAt   time.Time
	dispatched      map[string]string
}

// DefaultPreamble precedes every backlog prompt.
const DefaultPreamble = `You are running unattended from a task backlog while the user is away. Work autonomously: do not ask questions and do not wait for confirmation. If a decision genuinely needs the user, do everything that does not depend on it first, then write a short handoff at the end: what was done, what is blocked, and the exact question. Leave the work in a state the user can pick up (commit on a branch, or save files and describe them). Normal turn completion needs no status marker. If unfinished, end with BACKLOG STATUS: continue (another turn can help) or BACKLOG STATUS: needs-input (requires the user).`

var statusLine = regexp.MustCompile(`(?im)^\s*BACKLOG STATUS:\s*(done|continue|needs-input)\s*$`)

// New builds a runner.
func New(opts Options, store Store, control Control) *Runner {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Preamble == "" {
		opts.Preamble = DefaultPreamble
	}
	if opts.QuietFor <= 0 {
		opts.QuietFor = 30 * time.Minute
	}
	if opts.MaxConcurrentPerProvider <= 0 {
		opts.MaxConcurrentPerProvider = 1
	}
	if opts.HistoryDays <= 0 {
		opts.HistoryDays = 56
	}
	if opts.LongWindowCap <= 0 {
		opts.LongWindowCap = 80
	}
	return &Runner{
		opts: opts, store: store, control: control, log: opts.Logger.With("component", "backlog"),
		now: time.Now, states: map[string]*State{}, demand: map[domain.BucketKey]report.Demand{},
	}
}

// SetClock replaces the clock, for tests.
func (r *Runner) SetClock(now func() time.Time) { r.now = now }

// States returns the current task states.
func (r *Runner) States() map[string]*State { return r.states }

// load reads task files and persisted states.
func (r *Runner) load(ctx context.Context) ([]Task, error) {
	tasks, errs := LoadDir(r.opts.Dir)
	for _, err := range errs {
		r.log.Warn("backlog task skipped", "err", err)
	}
	// The database is the source of truth for the legacy runner's mutable
	// projection, so reload it every tick to recover daemon restarts.
	raw, err := r.store.LoadTaskStates(ctx)
	if err != nil {
		return nil, err
	}
	r.states = map[string]*State{}
	for id, js := range raw {
		var st State
		if json.Unmarshal(js, &st) == nil {
			r.states[id] = &st
		}
	}
	now := r.now()
	present := map[string]bool{}
	for _, t := range tasks {
		present[t.ID] = true
		st, ok := r.states[t.ID]
		if !ok {
			st = &State{ID: t.ID, Status: StatusPending, EstimatedCost: SeedCost(t.Difficulty), EstimatedMins: SeedMinutes(t.Difficulty), FileModTime: t.ModTime, UpdatedAt: now}
			if t.EstimatedCost != nil {
				st.EstimatedCost = *t.EstimatedCost
			}
			r.states[t.ID] = st
			r.save(ctx, st)
			continue
		}
		if !t.IsEnabled() && st.Status == StatusPending {
			st.Status, st.Reason = StatusDisabled, "enabled: false"
			r.save(ctx, st)
		}
		if t.IsEnabled() && st.Status == StatusDisabled {
			st.Status, st.Reason = StatusPending, ""
			r.save(ctx, st)
		}
		// An edited file re-queues a finished or failed task.
		if t.ModTime.After(st.FileModTime) && (st.Status == StatusDone || st.Status == StatusFailed || st.Status == StatusNeedsInput || st.Status == StatusCancelled || st.Status == StatusForwarded) {
			st.Status, st.Reason, st.ThreadID, st.Turns = StatusPending, "re-queued after edit", "", 0
			st.FileModTime = t.ModTime
			r.save(ctx, st)
		}
	}
	for id, st := range r.states {
		if !present[id] && st.Status != StatusCancelled && st.Status != StatusDone {
			st.Status, st.Reason = StatusCancelled, "task file removed"
			r.save(ctx, st)
		}
	}
	return tasks, nil
}

func (r *Runner) save(ctx context.Context, st *State) {
	st.UpdatedAt = r.now()
	if err := r.store.SaveTaskState(ctx, st.ID, string(st.Status), st); err != nil {
		r.log.Error("save task state", "task", st.ID, "err", err)
	}
}

// Tick advances the backlog: updates running tasks from the thread list
// and dispatches the next task when the gate allows.
func (r *Runner) Tick(ctx context.Context, threads []domain.Thread, buckets []domain.BucketState) {
	tasks, err := r.load(ctx)
	if err != nil {
		r.log.Error("load backlog", "err", err)
		return
	}
	now := r.now()
	dispatched, err := r.store.DispatchedThreads(ctx)
	if err != nil {
		r.log.Error("list dispatched threads", "err", err)
		return
	}
	r.dispatched = dispatched
	byID := map[string]domain.Thread{}
	for _, t := range threads {
		byID[t.ID] = t
		if t.Running {
			if _, ok := dispatched[t.ID]; !ok {
				r.lastInteractive = now
			}
		}
	}
	taskByID := map[string]Task{}
	for _, t := range tasks {
		taskByID[t.ID] = t
	}
	r.updateRunning(ctx, taskByID, byID, now)
	r.dispatchNext(ctx, tasks, byID, buckets, now)
}

// updateRunning settles tasks whose thread finished its turn.
func (r *Runner) updateRunning(ctx context.Context, tasks map[string]Task, threads map[string]domain.Thread, now time.Time) {
	for id, st := range r.states {
		if st.Status != StatusRunning {
			continue
		}
		log := r.log.With("task", id, "thread", st.ThreadID)
		thread, ok := threads[st.ThreadID]
		if !ok {
			st.Status, st.Reason = StatusFailed, "thread no longer exists"
			log.Warn("backlog task failed", "reason", st.Reason)
			r.save(ctx, st)
			continue
		}
		if thread.HasPendingUserInput || thread.HasPendingApprovals {
			st.Status, st.Reason = StatusNeedsInput, "the agent asked a question or requested approval"
			log.Info("backlog task needs input")
			r.record(ctx, id, st.ThreadID, "needs input")
			r.save(ctx, st)
			continue
		}
		if thread.Running {
			continue
		}
		switch thread.TurnState {
		case "completed":
			if thread.TurnID == st.LastTurnID {
				continue // already settled this turn
			}
			st.LastTurnID = thread.TurnID
			st.Turns++
			r.measure(ctx, st, now)
			text, err := r.control.LastAssistantMessage(ctx, st.ThreadID)
			if err != nil {
				log.Warn("cannot read the final message", "err", err)
			}
			marker := ""
			if m := statusLine.FindStringSubmatch(text); m != nil {
				marker = strings.ToLower(m[1])
			}
			task := tasks[id]
			switch {
			case marker == "needs-input":
				st.Status, st.Reason = StatusNeedsInput, "the agent reported it needs input"
			case marker == "continue" && st.Turns < task.MaxTurns:
				st.Status, st.Reason = StatusPending, fmt.Sprintf("continuing after turn %d", st.Turns)
			case marker == "continue":
				st.Status, st.Reason = StatusDone, fmt.Sprintf("turn cap of %d reached with work remaining", task.MaxTurns)
				t := now
				st.CompletedAt = &t
			default:
				st.Status, st.Reason = StatusDone, "completed"
				if marker == "" {
					st.Reason = "completed (no status line in the final message)"
				}
				t := now
				st.CompletedAt = &t
			}
			log.Info("backlog turn finished", "turns", st.Turns, "status", string(st.Status), "measured_cost", st.MeasuredCost)
			r.record(ctx, id, st.ThreadID, fmt.Sprintf("turn %d finished: %s (cost %.0f%%)", st.Turns, st.Reason, st.MeasuredCost))
			r.save(ctx, st)
		case "interrupted":
			// The quota watchdog interrupts and resumes threads itself; a
			// resume intent carries the task on. Anything else is a person.
			if thread.LatestUserMessageAt != nil && st.TurnStartedAt != nil && thread.LatestUserMessageAt.After(st.TurnStartedAt.Add(time.Minute)) {
				st.Status, st.Reason = StatusNeedsInput, "interrupted by a user message"
				r.save(ctx, st)
			}
		case "error":
			st.Status, st.Reason = StatusFailed, "the turn ended with an error"
			log.Warn("backlog task failed", "reason", st.Reason)
			r.record(ctx, id, st.ThreadID, "failed: turn error")
			r.save(ctx, st)
		}
	}
}

// measure records the quota the last turn consumed on the short windows.
func (r *Runner) measure(ctx context.Context, st *State, now time.Time) {
	if st.TurnStartedAt == nil {
		return
	}
	obs, err := r.store.Observations(ctx, *st.TurnStartedAt, now.Add(time.Minute))
	if err != nil {
		return
	}
	rises := report.Rises(obs, nil, 5*time.Minute)
	perBucket := map[domain.BucketKey]float64{}
	for _, rise := range rises {
		if rise.ThreadID == st.ThreadID && shortWindow(rise.Key) {
			perBucket[rise.Key] += rise.Percent
		}
	}
	cost := 0.0
	for _, v := range perBucket {
		if v > cost {
			cost = v
		}
	}
	mins := now.Sub(*st.TurnStartedAt).Minutes()
	if st.Turns <= 1 {
		st.MeasuredCost, st.MeasuredMins = cost, mins
	} else {
		st.MeasuredCost = (st.MeasuredCost*float64(st.Turns-1) + cost) / float64(st.Turns)
		st.MeasuredMins = (st.MeasuredMins*float64(st.Turns-1) + mins) / float64(st.Turns)
	}
	if cost > 0 {
		st.EstimatedCost = st.MeasuredCost
	}
	if mins > 0 {
		st.EstimatedMins = st.MeasuredMins
	}
}

func shortWindow(key domain.BucketKey) bool {
	w := strings.ToLower(key.Window)
	return w == "five_hour" || w == domain.WindowPrimary
}

// dispatchNext starts the best pending task whose gate is open.
func (r *Runner) dispatchNext(ctx context.Context, tasks []Task, threads map[string]domain.Thread, buckets []domain.BucketState, now time.Time) {
	var pending []Task
	runningPerProvider := map[string]int{}
	for _, t := range tasks {
		st := r.states[t.ID]
		if st == nil {
			continue
		}
		if st.Status == StatusRunning {
			if th, ok := threads[st.ThreadID]; ok {
				runningPerProvider[th.ProviderInstanceID]++
			}
			continue
		}
		if st.Status == StatusPending && t.IsEnabled() {
			pending = append(pending, t)
		}
	}
	if len(pending) == 0 {
		return
	}
	projects, err := r.control.ListProjects(ctx)
	if err != nil {
		r.log.Warn("cannot list projects", "err", err)
		return
	}
	seen := map[string]bool{}
	for _, th := range threads {
		if th.ProviderInstanceID != "" {
			seen[th.ProviderInstanceID] = true
		}
	}
	Order(pending, r.states, now)
	r.refreshDemand(ctx, now)
	for _, t := range pending {
		st := r.states[t.ID]
		if host := r.targetHost(t); host != "" {
			r.forward(ctx, t, st, host)
			continue
		}
		if t.NotBefore != nil && now.Before(*t.NotBefore) {
			continue
		}
		val := Validator{Control: r.control, DataDir: r.opts.DataDir, SeenInstances: seen, Projects: projects, Now: now}
		result := val.Validate(ctx, t)
		if !result.OK() {
			var fails []string
			for _, f := range result.Findings {
				if f.Level == "fail" {
					fails = append(fails, f.Message)
				}
			}
			// A task that cannot run here is parked as failed with the
			// reasons, so backlog list shows what to fix; editing the file
			// re-queues it.
			st.Status, st.Reason = StatusFailed, "invalid: "+strings.Join(fails, "; ")
			r.log.Warn("backlog task invalid", "task", t.ID, "reason", st.Reason)
			r.record(ctx, t.ID, "", st.Reason)
			r.save(ctx, st)
			continue
		}
		project := *result.Project
		selection := result.Selection
		instance, _ := selection["instanceId"].(string)
		if runningPerProvider[instance] >= r.opts.MaxConcurrentPerProvider {
			continue
		}
		if ok, why := r.gateOpen(t, st, instance, buckets, now); !ok {
			r.setReason(ctx, st, "waiting: "+why)
			continue
		}
		r.dispatch(ctx, t, st, project, selection, threads, now)
		runningPerProvider[instance]++
	}
}

func (r *Runner) setReason(ctx context.Context, st *State, reason string) {
	if st.Reason != reason {
		st.Reason = reason
		r.save(ctx, st)
	}
}

// targetHost returns the remote host a task must be forwarded to, or ""
// when it runs here.
func (r *Runner) targetHost(t Task) string {
	host := strings.TrimSpace(t.Host)
	if host == "" {
		host = strings.TrimSpace(r.opts.DefaultHost)
	}
	if host == "" || IsLocalHost(host, r.opts.LocalHost) {
		return ""
	}
	return host
}

// IsLocalHost reports whether a task host name means this machine.
func IsLocalHost(host, localName string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "" || h == "local" || h == "localhost" {
		return true
	}
	l := strings.ToLower(strings.TrimSpace(localName))
	if l == "" {
		return false
	}
	// "normandy" matches "omarchy-normandy" and vice versa is not wanted;
	// accept the exact name or the first label of the host name.
	return h == l || h == strings.SplitN(l, ".", 2)[0]
}

// forward hands a task to another host's backlog.
func (r *Runner) forward(ctx context.Context, t Task, st *State, host string) {
	if r.opts.Forward == nil {
		r.setReason(ctx, st, "task is for host "+host+" but forwarding is not configured")
		return
	}
	if r.opts.DryRun {
		r.setReason(ctx, st, "dry-run: would forward to "+host)
		return
	}
	if err := r.opts.Forward(ctx, host, t); err != nil {
		r.log.Warn("forward backlog task", "task", t.ID, "host", host, "err", err)
		r.setReason(ctx, st, "forward to "+host+" failed: "+err.Error())
		return
	}
	st.Status = StatusForwarded
	st.Reason = "forwarded to " + host + " at " + r.now().Format(time.RFC3339)
	r.log.Info("backlog task forwarded", "task", t.ID, "host", host)
	r.record(ctx, t.ID, "", "forwarded to "+host)
	r.save(ctx, st)
}

// gateOpen decides whether a task may start now.
func (r *Runner) gateOpen(t Task, st *State, instance string, buckets []domain.BucketState, now time.Time) (bool, string) {
	urgent := t.Deadline != nil && t.Deadline.Sub(now) < 24*time.Hour
	gated := t.Gated() && !urgent
	if gated && r.lastInteractive.Add(r.opts.QuietFor).After(now) {
		return false, fmt.Sprintf("interactive session active within the last %s", r.opts.QuietFor)
	}
	if r.opts.DisableQuotaChecks {
		return true, ""
	}
	cost := st.EstimatedCost
	mins := st.EstimatedMins
	for _, b := range buckets {
		if b.Key.ProviderInstanceID != instance {
			continue
		}
		if !b.Healthy {
			return false, fmt.Sprintf("%s is %s at %.0f%%", b.Key, b.Phase, b.UsedPercent)
		}
		if b.ResetsAt == nil {
			continue
		}
		untilReset := b.ResetsAt.Sub(now)
		if untilReset <= 0 {
			continue
		}
		// Part of the task's cost that lands before this window resets.
		share := 1.0
		if mins > 0 && untilReset.Minutes() < mins {
			share = untilReset.Minutes() / mins
		}
		if untilReset > 24*time.Hour {
			if b.UsedPercent+cost*share > r.opts.LongWindowCap {
				return false, fmt.Sprintf("%s at %.0f%% plus %.0f%% would exceed the long-window cap of %.0f%%", b.Key, b.UsedPercent, cost*share, r.opts.LongWindowCap)
			}
			continue
		}
		demand := 0.0
		if gated {
			if d, ok := r.demand[b.Key]; ok {
				demand, _ = d.Expected(now, *b.ResetsAt, time.Local, r.opts.FallbackPerHour)
			} else {
				demand = r.opts.FallbackPerHour * untilReset.Hours()
			}
		}
		if b.UsedPercent+cost*share+demand > 100-r.opts.SafetyMargin {
			return false, fmt.Sprintf("%s at %.0f%% + task %.0f%% + forecast %.0f%% until %s exceeds %.0f%%",
				b.Key, b.UsedPercent, cost*share, demand, b.ResetsAt.Local().Format("15:04"), 100-r.opts.SafetyMargin)
		}
	}
	return true, ""
}

func (r *Runner) refreshDemand(ctx context.Context, now time.Time) {
	if now.Sub(r.demandBuiltAt) < time.Hour {
		return
	}
	obs, err := r.store.Observations(ctx, now.Add(-time.Duration(r.opts.HistoryDays)*24*time.Hour), now.Add(time.Hour))
	if err != nil {
		r.log.Warn("cannot load history for the forecast", "err", err)
		return
	}
	rises := report.Rises(obs, r.dispatched, 5*time.Minute)
	keys := map[domain.BucketKey]bool{}
	for _, o := range obs {
		keys[o.Key] = true
	}
	r.demand = map[domain.BucketKey]report.Demand{}
	for key := range keys {
		d := report.BuildDemand(key, rises, obs, time.Local)
		if r.opts.Quantile > 0 {
			d.Quantile = r.opts.Quantile
		}
		if r.opts.MinSamples > 0 {
			d.MinSamples = r.opts.MinSamples
		}
		r.demand[key] = d
	}
	r.demandBuiltAt = now
}

func findProject(projects []t3control.Project, ref string) (t3control.Project, bool) {
	needle := strings.ToLower(strings.TrimSpace(ref))
	for _, p := range projects {
		if p.ID == ref {
			return p, true
		}
	}
	var match *t3control.Project
	for i := range projects {
		if strings.Contains(strings.ToLower(projects[i].Title), needle) {
			if match != nil {
				return t3control.Project{}, false // ambiguous
			}
			match = &projects[i]
		}
	}
	if match == nil {
		return t3control.Project{}, false
	}
	return *match, true
}

func modelSelection(t Task, p t3control.Project) map[string]any {
	if t.Model != "" && t.Instance != "" {
		sel := map[string]any{"instanceId": t.Instance, "model": t.Model}
		if len(t.Options) > 0 {
			var opts []any
			for k, v := range t.Options {
				opts = append(opts, map[string]any{"id": k, "value": v})
			}
			sel["options"] = opts
		}
		return withCodexMinimumEffort(sel)
	}
	if p.DefaultModelSelection != nil {
		sel := map[string]any{}
		for k, v := range p.DefaultModelSelection {
			sel[k] = v
		}
		if t.Model != "" {
			sel["model"] = t.Model
		}
		if t.Instance != "" {
			sel["instanceId"] = t.Instance
		}
		return withCodexMinimumEffort(sel)
	}
	return nil
}

func withCodexMinimumEffort(sel map[string]any) map[string]any {
	if sel == nil || sel["instanceId"] != "codex" {
		return sel
	}

	rawOptions, _ := sel["options"].([]any)
	options := make([]any, 0, len(rawOptions)+1)
	found := false
	for _, raw := range rawOptions {
		option, ok := raw.(map[string]any)
		if !ok {
			options = append(options, raw)
			continue
		}
		copied := make(map[string]any, len(option))
		for key, value := range option {
			copied[key] = value
		}
		if copied["id"] == "effort" {
			found = true
			if value, _ := copied["value"].(string); value == "" || value == "low" {
				copied["value"] = "medium"
			}
		}
		options = append(options, copied)
	}
	if !found {
		options = append(options, map[string]any{"id": "effort", "value": "medium"})
	}
	sel["options"] = options
	return sel
}

func (r *Runner) dispatch(ctx context.Context, t Task, st *State, project t3control.Project, selection map[string]any, threads map[string]domain.Thread, now time.Time) {
	log := r.log.With("task", t.ID, "project", project.Title)
	if r.opts.DryRun {
		log.Info("dry-run: would dispatch backlog task", "estimated_cost", st.EstimatedCost)
		r.setReason(ctx, st, "dry-run: would dispatch now")
		return
	}
	prompt := r.opts.Preamble + "\n\n" + t.Prompt
	var threadID string
	var err error
	if st.ThreadID != "" {
		// Continuation of an earlier turn on the same thread.
		thread, ok := threads[st.ThreadID]
		if !ok {
			st.Status, st.Reason = StatusFailed, "thread no longer exists"
			r.save(ctx, st)
			return
		}
		err = r.control.ResumeThread(ctx, thread, "Continue the backlog task from where you left off; inspect the current state first. "+
			"End your final message with the same BACKLOG STATUS line as before.")
		threadID = st.ThreadID
	} else {
		threadID, err = r.control.CreateAndStartThread(ctx, t3control.NewThreadInput{
			ProjectID: project.ID, Title: t.Title, ModelSelection: selection, RuntimeMode: "full-access", Prompt: prompt,
		})
		if threadID != "" {
			_ = r.store.RegisterDispatchedThread(ctx, threadID, t.ID, project.ID, now)
		}
	}
	if err != nil {
		log.Error("dispatch backlog task", "err", err)
		st.Status, st.Reason = StatusFailed, err.Error()
		r.record(ctx, t.ID, threadID, "dispatch failed: "+err.Error())
		r.save(ctx, st)
		return
	}
	st.Status, st.Reason, st.ThreadID = StatusRunning, "", threadID
	st.DispatchedAt, st.TurnStartedAt = &now, &now
	log.Info("backlog task dispatched", "thread", threadID, "turn", st.Turns+1, "estimated_cost", st.EstimatedCost)
	r.record(ctx, t.ID, threadID, fmt.Sprintf("dispatched turn %d (%s, est. %.0f%%)", st.Turns+1, t.Title, st.EstimatedCost))
	r.save(ctx, st)
}

func (r *Runner) record(ctx context.Context, taskID, threadID, detail string) {
	_ = r.store.RecordAction(ctx, domain.ActionRecord{At: r.now(), Kind: "backlog", Bucket: taskID, ThreadID: threadID, Detail: detail, DryRun: r.opts.DryRun})
}
