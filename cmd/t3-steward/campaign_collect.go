package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// uncollectedKind names the document "campaign uncollected --json" prints.
const uncollectedKind = "t3-steward.uncollected/v1"

// runCollectionUpgrade is what a client says about a coordinator that does not
// know the run-collection actions.
const runCollectionUpgrade = "the coordinator does not record collected runs; upgrade it"

// runCollection dispatches the two run-collection verbs: "collect", which
// records that the calling thread has acted on finished runs, and
// "uncollected", which lists what that thread still has to act on.
func (c campaignCLI) runCollection(ctx context.Context, args []string) error {
	if args[0] == "collect" {
		return c.runCollect(ctx, args[1:])
	}
	return c.runUncollected(ctx, args[1:])
}

type collectionArgs struct {
	thread string
	runs   []string
	asJSON bool
}

// parseCampaignCollectArgs parses both verbs. Every run ID is checked before
// anything is sent, so a typo in the last argument records nothing.
func parseCampaignCollectArgs(verb string, args []string) (collectionArgs, error) {
	parsed := collectionArgs{thread: "current"}
	threadSeen := false
	seen := map[string]bool{}
	setThread := func(value string) error {
		if threadSeen {
			return fmt.Errorf("campaign %s takes --thread once", verb)
		}
		if value == "" {
			return fmt.Errorf("campaign %s --thread needs current or a T3 thread ID", verb)
		}
		if value != "current" {
			if err := domain.ValidateRunCollectionID("thread", value); err != nil {
				return fmt.Errorf("campaign %s --thread: %w", verb, err)
			}
		}
		threadSeen, parsed.thread = true, value
		return nil
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--json":
			parsed.asJSON = true
		case arg == "--thread":
			if i+1 >= len(args) {
				return parsed, fmt.Errorf("campaign %s --thread needs current or a T3 thread ID", verb)
			}
			i++
			if err := setThread(args[i]); err != nil {
				return parsed, err
			}
		case strings.HasPrefix(arg, "--thread="):
			if err := setThread(strings.TrimPrefix(arg, "--thread=")); err != nil {
				return parsed, err
			}
		case strings.HasPrefix(arg, "-"):
			return parsed, fmt.Errorf("campaign %s does not accept %s; it takes --thread current|ID and --json", verb, arg)
		case verb != "collect":
			return parsed, fmt.Errorf("campaign %s takes no run IDs; it lists every run the thread owns (got %q)", verb, arg)
		default:
			if err := domain.ValidateRunCollectionID("run", arg); err != nil {
				return parsed, fmt.Errorf("campaign collect: %w; nothing was collected", err)
			}
			if seen[arg] {
				return parsed, fmt.Errorf("campaign collect names run %s twice; nothing was collected", arg)
			}
			seen[arg] = true
			parsed.runs = append(parsed.runs, arg)
		}
	}
	if verb == "collect" && len(parsed.runs) == 0 {
		return parsed, errors.New("campaign collect needs at least one run ID: t3-steward campaign collect RUN... [--thread current|ID] [--json]")
	}
	return parsed, nil
}

// collectedEntry is one run "campaign collect" recorded.
type collectedEntry struct {
	Run         string               `json:"run"`
	Progress    domain.ProgressState `json:"progress"`
	CompletedAt *time.Time           `json:"completedAt"`
	CollectedAt time.Time            `json:"collectedAt"`
	Changed     bool                 `json:"changed"`
}

type collectDocument struct {
	SchemaVersion int              `json:"schemaVersion"`
	Thread        string           `json:"thread"`
	Collected     []collectedEntry `json:"collected"`
}

// runCollect records each named run for the thread, one coordinator call per
// run in argument order, and stops at the first refusal. The coordinator reads
// the run's state and checks that the thread owns it; the client sends only
// the two IDs.
func (c campaignCLI) runCollect(ctx context.Context, args []string) error {
	parsed, err := parseCampaignCollectArgs("collect", args)
	if err != nil {
		return err
	}
	if c.notify == nil {
		return errors.New("coordinator node-wait transport is unavailable")
	}
	thread, err := resolveThreadFlag(c.resolveThread, parsed.thread, "campaign collect", "nothing was collected")
	if err != nil {
		return err
	}
	document := collectDocument{SchemaVersion: 1, Thread: thread, Collected: []collectedEntry{}}
	for i, run := range parsed.runs {
		response, err := c.notify(ctx, backlogadmin.NodeWaitOperation{
			Action:  backlogadmin.RunCollectAction,
			Request: domain.NodeWaitRequest{ThreadID: thread, Target: domain.NodeRef{RunID: run}},
		})
		if err == nil && (len(response.Collections) != 1 || response.Collections[0].RunID != run) {
			err = &backlogadmin.TransportError{Class: backlogadmin.ClassProtocol, Operation: "node-wait",
				Err: fmt.Errorf("the coordinator answered %s with no record of run %s", backlogadmin.RunCollectAction, run)}
		}
		if err != nil {
			return collectStopped(run, c.collectRefusal(ctx, run, err), document.Collected, parsed.runs[i+1:])
		}
		record := response.Collections[0]
		entry := collectedEntry{Run: run, Progress: record.Progress, CompletedAt: utcPointer(record.CompletedAt), CollectedAt: record.CollectedAt.UTC(), Changed: response.Changed}
		document.Collected = append(document.Collected, entry)
		if !parsed.asJSON {
			fmt.Fprintf(c.stdout, "collected %s (%s, finished %s) for thread %s\n", run, record.Progress, finishedText(record.CompletedAt), thread)
		}
	}
	if parsed.asJSON {
		encoder := json.NewEncoder(c.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(document)
	}
	return nil
}

// collectRefusal turns one failed collect-run into what the caller acts on.
// An older coordinator's answer becomes the upgrade instruction, as a protocol
// failure. A refusal of a run that has not finished says what its attention is
// and what clears it, because collecting is never how attention on a running
// run goes away. Every other failure is returned unchanged.
func (c campaignCLI) collectRefusal(ctx context.Context, run string, err error) error {
	if backlogadmin.RunCollectionUnsupported(err) {
		upgrade := &backlogadmin.TransportError{Class: backlogadmin.ClassProtocol, Operation: "node-wait", Err: errors.New(runCollectionUpgrade)}
		var transport *backlogadmin.TransportError
		if errors.As(err, &transport) {
			upgrade.Operation, upgrade.Coordinator = transport.Operation, transport.Coordinator
		}
		return upgrade
	}
	if backlogadmin.ClassOf(err) != backlogadmin.ClassRejected || !strings.Contains(err.Error(), "cannot be collected") {
		return err
	}
	hint := fmt.Sprintf("It can be collected once it finishes: t3-steward campaign show %s --wait", run)
	// The hint is advice read after the refusal; failing to read it must not
	// replace the refusal itself.
	if tasks, listErr := c.notify(ctx, backlogadmin.NodeWaitOperation{Action: "list-task"}); listErr == nil {
		// The oldest open question is the one to answer first.
		if open := openWaitAttention(run, tasks.TaskWaits); len(open) > 0 {
			if open[0].Kind == "ask" {
				hint = "Its attention clears when the ask is answered: t3-steward ask answer " + open[0].Subject + " --option ..."
			} else {
				hint = "Its attention clears when the request is decided: t3-steward wait inspect " + open[0].Subject
			}
		}
	}
	return fmt.Errorf("%w. %s", err, hint)
}

// collectStopped reports a refusal together with what the command had already
// recorded and what it never sent.
func collectStopped(run string, err error, done []collectedEntry, rest []string) error {
	if len(done) == 0 && len(rest) == 0 {
		return err
	}
	var progress []string
	if len(done) > 0 {
		runs := make([]string, 0, len(done))
		for _, entry := range done {
			runs = append(runs, entry.Run)
		}
		progress = append(progress, strings.Join(runs, ", ")+pluralVerb(len(runs), " was", " were")+" collected before it")
	}
	if len(rest) > 0 {
		progress = append(progress, strings.Join(rest, ", ")+pluralVerb(len(rest), " was", " were")+" not sent")
	}
	return fmt.Errorf("collect %s: %w; %s", run, err, strings.Join(progress, "; "))
}

func pluralVerb(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// uncollectedView is the document "campaign uncollected --json" prints.
type uncollectedView struct {
	SchemaVersion       int                `json:"schemaVersion"`
	Kind                string             `json:"kind"`
	Thread              string             `json:"thread"`
	GeneratedAt         time.Time          `json:"generatedAt"`
	CollectionSupported bool               `json:"collectionSupported"`
	Note                string             `json:"note"`
	Runs                []uncollectedRun   `json:"runs"`
	Omitted             uncollectedOmitted `json:"omitted"`
}

type uncollectedRun struct {
	Run        string               `json:"run"`
	Name       string               `json:"name"`
	Project    string               `json:"project"`
	State      domain.ProgressState `json:"state"`
	FinishedAt *time.Time           `json:"finishedAt"`
	Attention  []runAttention       `json:"attention"`
	Wake       *runWake             `json:"wake"`
	Result     string               `json:"result"`
	Collect    string               `json:"collect"`

	terminal bool
}

// runAttention is one reason a run needs the thread's attention.
type runAttention struct {
	Kind    string     `json:"kind"`
	Subject string     `json:"subject"`
	Task    string     `json:"task"`
	Detail  string     `json:"detail"`
	Since   *time.Time `json:"since"`
}

// runWake is the thread's newest node wait on the run and how its wake went.
type runWake struct {
	WaitID      string     `json:"waitId"`
	Delivery    string     `json:"delivery"`
	DeliveredAt *time.Time `json:"deliveredAt"`
}

// uncollectedOmitted counts the owned runs the view does not show: collected
// ones, active ones that need nothing, and IDs the coordinator no longer lists.
type uncollectedOmitted struct {
	Collected int `json:"collected"`
	Active    int `json:"active"`
	Unknown   int `json:"unknown"`
}

// runUncollected lists the runs the thread owns that it still has to act on.
// It is read-only and asks the coordinator only what every release answers,
// plus the thread's collections, which an older coordinator does not know: it
// then lists every finished run as uncollected and says so.
func (c campaignCLI) runUncollected(ctx context.Context, args []string) error {
	parsed, err := parseCampaignCollectArgs("uncollected", args)
	if err != nil {
		return err
	}
	if c.query == nil {
		return errors.New("coordinator admin transport is unavailable")
	}
	ownership, err := readThreadOwnership(ctx, c.notify, c.resolveThread, parsed.thread, "campaign uncollected", "nothing was listed")
	if err != nil {
		return err
	}
	workflows, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkflows})
	if err != nil {
		return err
	}
	tasks, err := c.notify(ctx, backlogadmin.NodeWaitOperation{Action: "list-task"})
	if err != nil {
		return err
	}
	supported := true
	collections, err := c.notify(ctx, backlogadmin.NodeWaitOperation{
		Action: backlogadmin.RunCollectionListAction, Request: domain.NodeWaitRequest{ThreadID: ownership.thread},
	})
	if backlogadmin.RunCollectionUnsupported(err) {
		supported, collections = false, backlogadmin.NodeWaitResponse{}
	} else if err != nil {
		return err
	}
	generated := workflows.GeneratedAt
	if generated.IsZero() {
		generated = time.Now()
	}
	view := buildUncollectedView(ownership, workflows.Workflows, tasks.TaskWaits, collections.Collections, supported, generated.UTC())
	if parsed.asJSON {
		encoder := json.NewEncoder(c.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(view)
	}
	return renderUncollected(c.stdout, view, parsed.thread)
}

// buildUncollectedView computes the view from the four coordinator answers.
func buildUncollectedView(ownership threadOwnership, workflows []backlogadmin.WorkflowSummary, tasks []domain.TaskWait,
	collections []domain.RunCollection, supported bool, generated time.Time) uncollectedView {
	view := uncollectedView{
		SchemaVersion: 1, Kind: uncollectedKind, Thread: ownership.thread, GeneratedAt: generated,
		CollectionSupported: supported, Runs: []uncollectedRun{},
	}
	if !supported {
		view.Note = runCollectionUpgrade + "; until then every finished run of the thread is listed as uncollected"
	}
	collected := map[string]domain.RunCollection{}
	for _, collection := range collections {
		if collection.ThreadID == ownership.thread {
			collected[collection.RunID] = collection
		}
	}
	listed := map[string]bool{}
	for _, workflow := range workflows {
		run := workflow.Run
		if !ownership.runs[run.ID] || listed[run.ID] {
			continue
		}
		listed[run.ID] = true
		terminal := run.Progress.Terminal()
		attention := runAttentionOf(workflow, tasks)
		switch {
		case terminal && supported && coveredBy(collected, run):
			view.Omitted.Collected++
			continue
		case !terminal && len(attention) == 0:
			view.Omitted.Active++
			continue
		}
		view.Runs = append(view.Runs, uncollectedRun{
			Run: run.ID, Name: workflow.Workflow.Name, Project: workflow.Workflow.Project, State: run.Progress,
			FinishedAt: utcPointer(run.CompletedAt), Attention: attention, Wake: newestWake(ownership.waits, run.ID),
			Result:   "t3-steward task result " + run.ID,
			Collect:  "t3-steward campaign collect " + run.ID + " --thread " + ownership.thread,
			terminal: terminal,
		})
	}
	for run := range ownership.runs {
		if !listed[run] {
			view.Omitted.Unknown++
		}
	}
	sort.SliceStable(view.Runs, func(i, j int) bool {
		a, b := view.Runs[i], view.Runs[j]
		if a.terminal != b.terminal {
			return !a.terminal
		}
		if a.terminal && !domain.SameCompletion(a.FinishedAt, b.FinishedAt) {
			switch {
			case a.FinishedAt == nil:
				return false
			case b.FinishedAt == nil:
				return true
			}
			return a.FinishedAt.After(*b.FinishedAt)
		}
		return a.Run < b.Run
	})
	return view
}

func coveredBy(collected map[string]domain.RunCollection, run domain.WorkflowRun) bool {
	collection, ok := collected[run.ID]
	return ok && collection.Covers(run)
}

// runAttentionOf lists every reason a run needs the thread's attention. A
// finished run's only reason is how it ended. A running one has its open asks
// and attention requests, a needs-input state no open wait explains, and
// failed branches whose dependents cannot run.
func runAttentionOf(workflow backlogadmin.WorkflowSummary, tasks []domain.TaskWait) []runAttention {
	run := workflow.Run
	attention := []runAttention{}
	if run.Progress.Terminal() {
		switch run.Progress {
		case domain.ProgressFailed:
			attention = append(attention, runAttention{Kind: "failed", Subject: run.ID, Detail: "the run failed", Since: utcPointer(run.CompletedAt)})
		case domain.ProgressCancelled:
			attention = append(attention, runAttention{Kind: "cancelled", Subject: run.ID, Detail: "the run was cancelled", Since: utcPointer(run.CompletedAt)})
		}
		return attention
	}
	attention = append(attention, openWaitAttention(run.ID, tasks)...)
	if run.Progress == domain.ProgressNeedsInput && len(attention) == 0 {
		attention = append(attention, runAttention{Kind: "needs-input", Subject: run.ID,
			Detail: "the run needs input and no open ask or attention request was found"})
	}
	if failed := workflow.Progress.Failed + workflow.Progress.Cancelled; failed > 0 {
		attention = append(attention, runAttention{Kind: "task-failed", Subject: run.ID,
			Detail: fmt.Sprintf("%d tasks failed; dependents are blocked", failed)})
	}
	return attention
}

// openWaitAttention is the run's unsettled asks and attention requests, oldest
// first: the questions someone has to answer before the run can continue.
func openWaitAttention(runID string, tasks []domain.TaskWait) []runAttention {
	var open []domain.TaskWait
	for _, w := range tasks {
		if w.WorkflowRunID != runID || w.SettledAt != nil {
			continue
		}
		if (w.Kind == domain.WaitKindAsk && w.Ask != nil) || w.Kind == domain.WaitKindAttention {
			open = append(open, w)
		}
	}
	sort.SliceStable(open, func(i, j int) bool {
		if !open[i].RegisteredAt.Equal(open[j].RegisteredAt) {
			return open[i].RegisteredAt.Before(open[j].RegisteredAt)
		}
		return open[i].ID < open[j].ID
	})
	attention := []runAttention{}
	for _, w := range open {
		since := w.RegisteredAt.UTC()
		entry := runAttention{Kind: "attention", Subject: w.ID, Task: w.TaskID, Since: &since}
		if w.Kind == domain.WaitKindAsk {
			entry.Kind, entry.Detail = "ask", w.Ask.Question
		} else if w.Attention != nil {
			entry.Detail = w.Attention.Prompt
		}
		attention = append(attention, entry)
	}
	return attention
}

// newestWake is the thread's most recently registered node wait on the run.
func newestWake(waits []domain.NodeWait, runID string) *runWake {
	var newest *domain.NodeWait
	for i := range waits {
		w := &waits[i]
		if w.Request.Target.RunID != runID {
			continue
		}
		if newest == nil || w.CreatedAt.After(newest.CreatedAt) ||
			(w.CreatedAt.Equal(newest.CreatedAt) && w.Request.ID > newest.Request.ID) {
			newest = w
		}
	}
	if newest == nil {
		return nil
	}
	return &runWake{WaitID: newest.Request.ID, Delivery: newest.Delivery, DeliveredAt: utcPointer(newest.DeliveredAt)}
}

// renderUncollected prints the text view: a summary line, one row per run and
// the next step. requested is --thread as the caller typed it, so the command
// it suggests is the one the caller would type.
func renderUncollected(out io.Writer, view uncollectedView, requested string) error {
	if !view.CollectionSupported {
		fmt.Fprintf(out, "note: %s\n", view.Note)
	}
	omitted := omittedClause(view.Omitted)
	if len(view.Runs) == 0 {
		_, err := fmt.Fprintf(out, "nothing to collect for thread %s%s\n", view.Thread, omitted)
		return err
	}
	fmt.Fprintf(out, "thread %s: %d runs to collect%s\n", view.Thread, len(view.Runs), omitted)
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "RUN\tNAME\tSTATE\tFINISHED\tWAKE\tATTENTION")
	for _, run := range view.Runs {
		wake := "-"
		if run.Wake != nil && run.Wake.Delivery != "" {
			wake = run.Wake.Delivery
		}
		parts := make([]string, 0, len(run.Attention))
		for _, a := range run.Attention {
			parts = append(parts, attentionText(a))
		}
		attention := "-"
		if len(parts) > 0 {
			attention = strings.Join(parts, "; ")
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n", run.Run, textCell(run.Name), run.State, finishedText(run.FinishedAt), wake, attention)
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if !view.CollectionSupported {
		_, err := fmt.Fprintln(out, `Read a result with "t3-steward task result RUN"; recording it needs an upgraded coordinator.`)
		return err
	}
	_, err := fmt.Fprintf(out, "Read a result with \"t3-steward task result RUN\", then record it with \"t3-steward campaign collect RUN... --thread %s\".\n", requested)
	return err
}

func attentionText(a runAttention) string {
	since := ""
	if a.Since != nil {
		since = " (since " + a.Since.UTC().Format(time.RFC3339) + ")"
	}
	switch a.Kind {
	case "ask", "attention":
		return fmt.Sprintf("%s %s on %s: %s%s", a.Kind, textCell(a.Subject), textCell(a.Task), strconv.Quote(a.Detail), since)
	case "task-failed":
		return "task-failed: " + a.Detail
	default:
		return a.Kind
	}
}

// textCell keeps author text (a workflow name, a wait or task ID) from
// breaking the table: anything with a space or a character that needs
// escaping is printed quoted.
func textCell(value string) string {
	if value == "" {
		return "-"
	}
	quoted := strconv.Quote(value)
	if quoted[1:len(quoted)-1] != value || strings.ContainsAny(value, " \t") {
		return quoted
	}
	return value
}

func omittedClause(omitted uncollectedOmitted) string {
	var parts []string
	if omitted.Collected > 0 {
		parts = append(parts, fmt.Sprintf("%d collected", omitted.Collected))
	}
	if omitted.Active > 0 {
		parts = append(parts, fmt.Sprintf("%d active without attention", omitted.Active))
	}
	if omitted.Unknown > 0 {
		parts = append(parts, fmt.Sprintf("%d no longer listed by the coordinator", omitted.Unknown))
	}
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return " (" + parts[0] + " not shown)"
	default:
		return " (" + strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1] + " not shown)"
	}
}

func finishedText(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func utcPointer(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
