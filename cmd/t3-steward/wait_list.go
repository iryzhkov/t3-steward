package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// "wait list" answers one question: what is this thread waiting for. It used to
// answer it from the local SQLite store alone, so a coordinator-held node wait
// registered seconds earlier printed "No waits." -- a zero that means "nothing
// pending" and "I did not look there" at the same time. The other half of the
// answer was behind --native, which returned every wait of every thread on
// every host, most of them long delivered, and refused to be scoped.
//
// The two halves are joined here into one row shape, the sources are named in
// the output, and a source that could not be read is a labelled failure and a
// non-zero exit rather than a smaller list.

// waitListRow is one wait, whichever source it came from.
type waitListRow struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Thread string `json:"threadId,omitempty"`
	// Subject is what the wait is waiting for, in the source's own words.
	Subject string `json:"subject"`
	// State is waiting, or the settled outcome.
	State string `json:"state"`
	// Delivery is the wake's delivery state for a coordinator-held wait, and is
	// empty for a local check, which has no separate delivery.
	Delivery string `json:"delivery,omitempty"`
	// Host is where the wake would be delivered from. A wait recorded for
	// another host is not a wait this host will deliver, and the mismatch is
	// only visible if the host is printed.
	Host       string    `json:"host,omitempty"`
	Registered time.Time `json:"registeredAt"`
	Deadline   time.Time `json:"deadline"`
	// TaskWait names the coordinator-owned task-bound wait a local check
	// settles. The two are one park held in two places, and a worker reading
	// this list has to be able to get from its half to the other.
	TaskWait string `json:"taskWaitId,omitempty"`
	// Source is local or coordinator, so that a short list is readable as
	// "nothing is pending" rather than as "one source answered".
	Source string `json:"source"`
	// Settled reports whether this wait has an outcome. Settled waits are
	// hidden unless --all asks for them.
	Settled bool `json:"settled"`
}

// waitListOptions is one parsed "wait list" command line.
type waitListOptions struct {
	thread string
	host   string
	all    bool
	asJSON bool
	// threadResolutionErr is why no thread could be resolved, when none was
	// named. It is not fatal: --all still answers, and an empty answer says
	// which question could not be asked.
	threadResolutionErr error
}

// waitListAnswer is the joined answer with its provenance.
type waitListAnswer struct {
	Rows []waitListRow `json:"waits"`
	// Sources are the sources that answered, and Unavailable the ones that did
	// not, each with why. No ambiguous zero: an empty Rows with a non-empty
	// Unavailable is not "nothing is pending".
	Sources     []string `json:"sources"`
	Unavailable []string `json:"unavailable,omitempty"`
	// Hidden counts the settled waits --all would have shown.
	Hidden int `json:"hidden,omitempty"`
}

// waitListSources are the two places a wait can live.
type waitListSources struct {
	// local reads this host's check rows for one thread, or for every thread
	// when the thread is empty.
	local func(ctx context.Context, thread string) ([]wait.Wait, error)
	// coordinator reads the node waits and the task-bound waits the
	// coordinator holds. It is nil on a host with no coordinator configured,
	// which is a source that does not exist rather than one that failed.
	coordinator func(ctx context.Context) ([]domain.NodeWait, []domain.TaskWait, error)
	// host is this host's name, used to label local rows and to default the
	// host scope of the joined view.
	host string
}

func parseWaitListArgs(args []string) (waitListOptions, error) {
	fs := flag.NewFlagSet("wait list", flag.ContinueOnError)
	thread := fs.String("thread", "", "thread id")
	host := fs.String("host", "", "delivery host")
	all := fs.Bool("all", false, "every thread and every state, including settled and delivered waits")
	asJSON := fs.Bool("json", false, "print the list as JSON")
	if err := fs.Parse(args); err != nil {
		return waitListOptions{}, err
	}
	if fs.NArg() != 0 {
		return waitListOptions{}, fmt.Errorf("wait list takes no arguments (got %q)", strings.Join(fs.Args(), " "))
	}
	return waitListOptions{thread: *thread, host: *host, all: *all, asJSON: *asJSON}, nil
}

// collectWaitList joins the sources, scopes the result, and reports what could
// not be read. Scope is applied before anything is hidden or capped, so a
// narrow question is never answered from a truncated wide one.
func collectWaitList(ctx context.Context, sources waitListSources, options waitListOptions) waitListAnswer {
	answer := waitListAnswer{Rows: []waitListRow{}}
	var rows []waitListRow
	// A local check bound to a task-bound wait is one half of a park; the
	// coordinator holds the other. Listing both would report one park twice.
	boundTaskWaits := map[string]bool{}
	if sources.local != nil {
		local, err := sources.local(ctx, options.thread)
		if err != nil {
			answer.Unavailable = append(answer.Unavailable, "local checks on this host: "+err.Error())
		} else {
			answer.Sources = append(answer.Sources, "local checks on this host")
			for _, w := range local {
				if w.TaskWaitID != "" {
					boundTaskWaits[w.TaskWaitID] = true
				}
				rows = append(rows, localWaitRow(w, sources.host))
			}
		}
	}
	if sources.coordinator != nil {
		nodes, tasks, err := sources.coordinator(ctx)
		if err != nil {
			answer.Unavailable = append(answer.Unavailable, "coordinator-held waits: "+err.Error())
		} else {
			answer.Sources = append(answer.Sources, "coordinator-held waits")
			for _, w := range nodes {
				rows = append(rows, nodeWaitRow(w))
			}
			for _, w := range tasks {
				if boundTaskWaits[w.ID] && !options.all {
					continue
				}
				rows = append(rows, taskWaitRow(w))
			}
		}
	}
	for _, row := range scopeWaitRows(rows, options) {
		if row.Settled && !options.all {
			answer.Hidden++
			continue
		}
		answer.Rows = append(answer.Rows, row)
	}
	sort.SliceStable(answer.Rows, func(i, j int) bool {
		if answer.Rows[i].Registered.Equal(answer.Rows[j].Registered) {
			return answer.Rows[i].ID < answer.Rows[j].ID
		}
		return answer.Rows[i].Registered.Before(answer.Rows[j].Registered)
	})
	return answer
}

// scopeWaitRows applies --thread and --host. It runs before settled waits are
// hidden and before anything is printed, because a scope applied afterwards
// answers a different question than the one that was asked.
func scopeWaitRows(rows []waitListRow, options waitListOptions) []waitListRow {
	var kept []waitListRow
	for _, row := range rows {
		if options.thread != "" && row.Thread != "" && row.Thread != options.thread {
			continue
		}
		if options.host != "" && row.Host != "" && row.Host != options.host {
			continue
		}
		kept = append(kept, row)
	}
	return kept
}

func localWaitRow(w wait.Wait, host string) waitListRow {
	deadline := time.Time{}
	if w.Timeout > 0 {
		deadline = w.CreatedAt.Add(w.Timeout)
	}
	state := string(w.Status)
	if w.Settled() && w.Outcome != "" {
		state += ": " + w.Outcome
	}
	subject := w.Name
	if condition := w.Condition(); condition != "" {
		subject = strings.TrimPrefix(subject+": "+condition, ": ")
	}
	return waitListRow{
		ID: w.ID, Kind: string(w.Kind.OrShell()), Thread: w.ThreadID, Subject: subject,
		State: state, Host: host, Registered: w.CreatedAt, Deadline: deadline, TaskWait: w.TaskWaitID,
		Source: "local", Settled: w.Settled() || w.Status == wait.StatusCancelled || w.Status == wait.StatusWoken,
	}
}

func nodeWaitRow(w domain.NodeWait) waitListRow {
	state := "waiting"
	if w.SettledAt != nil {
		state = "settled"
		if w.Observation != nil && w.Observation.Outcome != "" {
			state += ": " + string(w.Observation.Outcome)
		}
	}
	subject := w.Request.Name
	if subject == "" {
		subject = w.Request.Target.String()
	}
	// A wake that has been delivered or cancelled is over, whatever the wait's
	// own settlement says: nothing more will reach the thread through it.
	settled := w.SettledAt != nil || w.Delivery == "delivered" || w.Delivery == "cancelled"
	return waitListRow{
		ID: w.Request.ID, Kind: string(w.Request.Kind()), Thread: w.Request.ThreadID, Subject: subject,
		State: state, Delivery: w.Delivery, Host: w.Host, Registered: w.CreatedAt, Deadline: w.Deadline,
		Source: "coordinator", Settled: settled,
	}
}

// taskWaitRow is one task-bound wait: the mutating kind, which parks an
// attempt. It carries its thread, so it scopes like every other row.
func taskWaitRow(w domain.TaskWait) waitListRow {
	state := "waiting"
	if w.SettledAt != nil {
		state = "settled"
		if w.Result != nil && w.Result.Outcome != "" {
			state += ": " + string(w.Result.Outcome)
		}
	}
	subject := "task " + w.TaskID + " " + w.Name
	if w.Condition != "" {
		subject += ": " + w.Condition
	}
	return waitListRow{
		ID: w.ID, Kind: string(w.Kind.OrShell()) + " (task-bound)", Thread: w.ThreadID,
		Subject: strings.TrimSpace(subject), State: state, Delivery: w.Delivery,
		Registered: w.RegisteredAt, Deadline: w.Deadline,
		Source: "coordinator", Settled: w.SettledAt != nil,
	}
}

// runWaitList prints the answer and reports an unreadable source as a failure.
func runWaitList(ctx context.Context, sources waitListSources, options waitListOptions, out io.Writer) error {
	answer := collectWaitList(ctx, sources, options)
	if options.asJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(answer); err != nil {
			return err
		}
	} else {
		renderWaitList(out, answer, options)
	}
	if len(answer.Unavailable) != 0 {
		// The list is incomplete and the caller has to know that from the exit
		// code as well as from the text, because a script reads one of the two.
		return errors.New("this list is incomplete: " + strings.Join(answer.Unavailable, "; "))
	}
	return nil
}

func renderWaitList(out io.Writer, answer waitListAnswer, options waitListOptions) {
	for _, row := range answer.Rows {
		line := fmt.Sprintf("%s %s %q state=%s", row.ID, row.Kind, row.Subject, row.State)
		if row.Delivery != "" {
			line += " delivery=" + row.Delivery
		}
		if row.Host != "" {
			line += " host=" + row.Host
		}
		if row.TaskWait != "" {
			line += " task-wait=" + row.TaskWait
		}
		if options.all || options.thread == "" {
			line += " thread=" + orDash(row.Thread)
		}
		fmt.Fprintf(out, "%s registered=%s deadline=%s\n", line, formatTime(row.Registered), formatTime(row.Deadline))
	}
	if len(answer.Rows) == 0 {
		fmt.Fprintln(out, emptyWaitListLine(answer, options))
	}
	if answer.Hidden != 0 {
		fmt.Fprintf(out, "%d settled or delivered wait(s) hidden; --all shows them.\n", answer.Hidden)
	}
	if len(answer.Sources) != 0 {
		fmt.Fprintf(out, "sources read: %s\n", strings.Join(answer.Sources, ", "))
	}
	for _, unavailable := range answer.Unavailable {
		fmt.Fprintf(out, "NOT READ: %s\n", unavailable)
	}
}

// emptyWaitListLine distinguishes the two zeros. "No waits." said both, which
// is why a thread that had just registered one read it as having none.
func emptyWaitListLine(answer waitListAnswer, options waitListOptions) string {
	if len(answer.Unavailable) != 0 {
		return "No waits could be listed from the sources that answered; one or more sources could not be read."
	}
	scope := "this thread"
	switch {
	case options.all:
		scope = "any thread on any host"
	case options.thread != "":
		scope = "thread " + options.thread
	case options.threadResolutionErr != nil:
		return fmt.Sprintf("No thread could be resolved for the caller (%v), so nothing was scoped to it; "+
			"pass --thread with a T3 thread id, or --all for every thread.", options.threadResolutionErr)
	}
	line := "Nothing is pending for " + scope + "."
	if answer.Hidden == 0 && !options.all {
		line += " Settled and delivered waits are hidden; --all shows them."
	}
	return line
}

// renderNativeWaitResult prints what the coordinator answered about its own
// waits, in the same row shape the joined list uses.
//
// Under --json it prints that row shape too, rather than the coordinator's own
// document: that document carried the registration and request blocks twice,
// differing only in the sink spelling, and every duration as a nanosecond
// count. The wire format between client and coordinator is untouched; what
// changes is what this command prints.
func renderNativeWaitResult(out io.Writer, result backlogadmin.NodeWaitResponse, options waitListOptions) error {
	answer := waitListAnswer{Rows: []waitListRow{}, Sources: []string{"coordinator-held waits"}}
	var rows []waitListRow
	for _, w := range result.Waits {
		rows = append(rows, nodeWaitRow(w))
	}
	for _, w := range result.TaskWaits {
		rows = append(rows, taskWaitRow(w))
	}
	answer.Rows = append(answer.Rows, scopeWaitRows(rows, options)...)
	if options.asJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(answer)
	}
	renderWaitList(out, answer, options)
	return nil
}

func orDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
