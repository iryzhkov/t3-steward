package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/quotatelemetry"
)

const quotaUsage = `Usage: t3-steward quota telemetry [--since DUR|RFC3339] [--route TEXT] [--pool ID] [--kind K[,K]] [--limit N] [--json]

Quota telemetry (phase 0 of the quota controller, observe-only). The
coordinator process runs a recorder beside the watchdog that every 30 seconds
appends, to its own SQLite file beside the state database
(quota-telemetry/recorder.sqlite):

  reading   each distinct quota reading the coordinator holds: worker
            snapshot observations and the coordinator host's buckets
  dispatch  an assignment offered to a worker
  start     the worker claimed it
  finish    the attempt ended, or the assignment was released
  check     one worker-measured verification or gate command of a finished
            attempt: exit code and duration, never its output or the gate log
  recorder  the recorder started a store, or could not record for a span

The recorder changes neither dispatch nor quota policy, and a failure of the
recorder never reaches the coordinator. The store is read on the coordinator
host; on another host this command says where to run it.

Commands:
  telemetry   list recorded events, with quota deltas for finished work

See docs/quota-telemetry.md for the event schema, the derived task type, the
delta method and what is not captured.
`

// cmdQuota is the quota family: one verb, telemetry.
func cmdQuota(g globalFlags, args []string) error {
	if answered, err := admitFamilyHelp(os.Stdout, []string{"quota"}, args); answered || err != nil {
		return err
	}
	switch args[0] {
	case "telemetry":
		return cmdQuotaTelemetry(g, args[1:])
	default:
		return fmt.Errorf("unknown quota command %q; the quota commands are telemetry (try \"t3-steward quota --help\")", args[0])
	}
}

func cmdQuotaTelemetry(g globalFlags, args []string) error {
	now := time.Now().UTC()
	options, err := parseQuotaTelemetryArgs(args, now)
	if err != nil {
		return err
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	path, err := quotatelemetry.StorePath(statePath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return runQuotaTelemetry(ctx, options, path, quotaTelemetryPoolInstances(cfg), os.Stdout, now)
}

// quotaTelemetryPoolInstances maps each configured quota pool to the provider
// instances bound to it, so --pool can select a pool's readings.
func quotaTelemetryPoolInstances(cfg config.Config) map[string][]string {
	pools := map[string][]string{}
	for _, binding := range coordinatorQuotaPoolBindings(cfg) {
		pools[binding.ID] = append([]string(nil), binding.ProviderInstanceIDs...)
	}
	return pools
}

type quotaTelemetryOptions struct {
	since  time.Time
	route  string
	pool   string
	kinds  []string
	limit  int
	asJSON bool
}

const quotaTelemetryUsage = "quota telemetry usage: t3-steward quota telemetry [--since DUR|RFC3339] [--route TEXT] [--pool ID] [--kind K[,K]] [--limit N] [--json]"

// parseQuotaTelemetryArgs takes the filters and refuses anything else by name.
func parseQuotaTelemetryArgs(args []string, now time.Time) (quotaTelemetryOptions, error) {
	clean, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return quotaTelemetryOptions{}, err
	}
	options := quotaTelemetryOptions{since: now.Add(-24 * time.Hour), kinds: quotatelemetry.Kinds,
		limit: quotatelemetry.DefaultQueryLimit, asJSON: asJSON}
	for i := 0; i < len(clean); i++ {
		flag := clean[i]
		switch flag {
		case "--since", "--route", "--pool", "--kind", "--limit":
		default:
			return quotaTelemetryOptions{}, fmt.Errorf("%s (got %q)", quotaTelemetryUsage, flag)
		}
		if i+1 >= len(clean) || strings.HasPrefix(clean[i+1], "--") {
			return quotaTelemetryOptions{}, fmt.Errorf("%s needs a value; %s", flag, quotaTelemetryUsage)
		}
		value := clean[i+1]
		i++
		switch flag {
		case "--since":
			since, err := parseQuotaTelemetrySince(value, now)
			if err != nil {
				return quotaTelemetryOptions{}, err
			}
			options.since = since
		case "--route":
			options.route = value
		case "--pool":
			options.pool = value
		case "--kind":
			kinds, err := parseQuotaTelemetryKinds(value)
			if err != nil {
				return quotaTelemetryOptions{}, err
			}
			options.kinds = kinds
		case "--limit":
			limit, err := strconv.Atoi(value)
			if err != nil || limit < 1 || limit > quotatelemetry.MaxQueryLimit {
				return quotaTelemetryOptions{}, fmt.Errorf("--limit needs a whole number from 1 to %d, not %q", quotatelemetry.MaxQueryLimit, value)
			}
			options.limit = limit
		}
	}
	return options, nil
}

// parseQuotaTelemetrySince reads an RFC3339 instant or a positive age: a Go
// duration such as 6h or 90m, or a number of days such as 7d.
func parseQuotaTelemetrySince(value string, now time.Time) (time.Time, error) {
	if at, err := time.Parse(time.RFC3339, value); err == nil {
		return at.UTC(), nil
	}
	var age time.Duration
	if days, found := strings.CutSuffix(value, "d"); found {
		count, err := strconv.Atoi(days)
		if err != nil || count < 1 {
			return time.Time{}, fmt.Errorf("--since needs a positive age such as 6h or 7d, or an RFC3339 time, not %q", value)
		}
		age = time.Duration(count) * 24 * time.Hour
	} else {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			return time.Time{}, fmt.Errorf("--since needs a positive age such as 6h or 7d, or an RFC3339 time, not %q", value)
		}
		age = parsed
	}
	return now.Add(-age), nil
}

func parseQuotaTelemetryKinds(value string) ([]string, error) {
	var kinds []string
	for _, kind := range strings.Split(value, ",") {
		kind = strings.TrimSpace(kind)
		if !slices.Contains(quotatelemetry.Kinds, kind) {
			return nil, fmt.Errorf("--kind %q is not a telemetry event kind; the kinds are %s", kind, strings.Join(quotatelemetry.Kinds, ", "))
		}
		if !slices.Contains(kinds, kind) {
			kinds = append(kinds, kind)
		}
	}
	return kinds, nil
}

// quotaTelemetryDocument is the --json answer.
type quotaTelemetryDocument struct {
	SchemaVersion int                        `json:"schemaVersion"`
	Kind          string                     `json:"kind"`
	GeneratedAt   time.Time                  `json:"generatedAt"`
	Store         quotatelemetry.StoreInfo   `json:"store"`
	Recorder      quotaTelemetryRecorderInfo `json:"recorder"`
	Filters       quotaTelemetryFilters      `json:"filters"`
	Events        []quotatelemetry.Event     `json:"events"`
}

type quotaTelemetryRecorderInfo struct {
	Ticks           int64                  `json:"ticks"`
	Failures        int64                  `json:"failures"`
	LastError       string                 `json:"lastError"`
	LastErrorAt     *time.Time             `json:"lastErrorAt"`
	LastSuccessAt   *time.Time             `json:"lastSuccessAt"`
	CoverageFrom    *time.Time             `json:"coverageFrom"`
	AuditWatermark  *int64                 `json:"auditWatermark"`
	SkippedReadings int64                  `json:"skippedReadings"`
	SkippedChecks   int64                  `json:"skippedChecks"`
	Gaps            []quotatelemetry.Event `json:"gaps"`
}

type quotaTelemetryFilters struct {
	Since         time.Time `json:"since"`
	Route         string    `json:"route"`
	Pool          string    `json:"pool"`
	PoolInstances []string  `json:"poolInstances"`
	Kinds         []string  `json:"kinds"`
	Limit         int       `json:"limit"`
}

// runQuotaTelemetry reads the store read-only and prints the matching events.
// It never creates the store: a missing file is exit 1 naming where to run.
func runQuotaTelemetry(ctx context.Context, options quotaTelemetryOptions, path string, pools map[string][]string, out io.Writer, now time.Time) error {
	reader, err := quotatelemetry.OpenReader(path)
	if errors.Is(err, quotatelemetry.ErrNoStore) {
		return fmt.Errorf("no quota telemetry store at %s; the recorder runs inside the coordinator, so run this on the coordinator host (for example: ssh <coordinator-host> t3-steward quota telemetry)", path)
	}
	if err != nil {
		return err
	}
	defer reader.Close()
	poolInstances := []string{}
	if options.pool != "" {
		poolInstances = append(poolInstances, pools[options.pool]...)
	}
	result, err := reader.Query(ctx, quotatelemetry.Filter{
		Since: options.since, Route: options.route, Pool: options.pool, PoolInstances: poolInstances,
		Kinds: options.kinds, Limit: options.limit,
	}, now)
	if err != nil {
		return err
	}
	if options.asJSON {
		document := quotaTelemetryDocument{
			SchemaVersion: quotatelemetry.SchemaVersion, Kind: quotatelemetry.DocumentKind, GeneratedAt: now,
			Store: result.Store,
			Recorder: quotaTelemetryRecorderInfo{
				Ticks: result.Meta.Ticks, Failures: result.Meta.Failures, LastError: result.Meta.LastError,
				LastErrorAt: result.Meta.LastErrorAt, LastSuccessAt: result.Meta.LastSuccessAt,
				CoverageFrom: result.Meta.CoverageFrom, AuditWatermark: result.Meta.AuditWatermark,
				SkippedReadings: result.Meta.SkippedReadings, SkippedChecks: result.Meta.SkippedChecks, Gaps: result.Gaps,
			},
			Filters: quotaTelemetryFilters{Since: options.since, Route: options.route, Pool: options.pool,
				PoolInstances: poolInstances, Kinds: options.kinds, Limit: options.limit},
			Events: result.Events,
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(document)
	}
	return renderQuotaTelemetry(out, options, result)
}

func renderQuotaTelemetry(out io.Writer, options quotaTelemetryOptions, result quotatelemetry.QueryResult) error {
	retained := "nothing retained"
	if result.Store.OldestRetained != nil {
		retained = "retained from " + result.Store.OldestRetained.UTC().Format(time.RFC3339)
	}
	if _, err := fmt.Fprintf(out, "quota telemetry: %d events since %s (store %s, schema %d, %s; recorder failures %d)\n",
		len(result.Events), options.since.UTC().Format(time.RFC3339), result.Store.Path, result.Store.SchemaVersion,
		retained, result.Meta.Failures); err != nil {
		return err
	}
	if len(result.Events) == 0 {
		fmt.Fprintln(out, "no telemetry events match")
	} else {
		table := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
		fmt.Fprintln(table, "TIME\tKIND\tROUTE\tEFFORT\tTYPE(derived)\tTASK\tDURATION\tDETAIL")
		for _, event := range result.Events {
			fmt.Fprintln(table, strings.Join(quotaTelemetryRow(event), "\t"))
		}
		if err := table.Flush(); err != nil {
			return err
		}
	}
	if len(result.Gaps) > 0 {
		fmt.Fprintf(out, "recorder gaps in this span: %d (see --kind recorder)\n", len(result.Gaps))
	}
	if result.Meta.Failures > 0 && result.Meta.LastError != "" {
		fmt.Fprintf(out, "last recorder error: %s\n", terminalText(result.Meta.LastError))
	}
	return nil
}

func terminalText(value string) string {
	return string(safeTerminalText([]byte(value)))
}

// quotaTelemetryRow is one event's columns: TIME KIND ROUTE EFFORT
// TYPE(derived) TASK DURATION DETAIL.
func quotaTelemetryRow(event quotatelemetry.Event) []string {
	row := []string{event.At.UTC().Format(time.RFC3339), event.Kind, "-", "-", "-", "-", "-", ""}
	if work := event.Work; work != nil {
		row[2] = terminalText(work.Route.Name())
		if work.Route.Effort != nil {
			row[3] = terminalText(*work.Route.Effort)
		}
		row[4] = terminalText(work.TaskType.Category + ":" + work.TaskType.Rule)
		runID := work.RunID
		if len(runID) > 11 {
			runID = runID[:8] + "..."
		}
		row[5] = terminalText(runID + "/" + work.TaskName)
	}
	switch {
	case event.Reading != nil:
		reading := event.Reading
		row[2] = terminalText(reading.BucketKey)
		row[5] = terminalText(reading.Source)
		resets := "resets unknown"
		if reading.ResetsAt != nil {
			resets = "resets " + reading.ResetsAt.UTC().Format(time.RFC3339)
		}
		row[7] = fmt.Sprintf("used %s%%, phase %s, %s", strconv.FormatFloat(reading.UsedPercent, 'f', 1, 64), terminalText(reading.Phase), resets)
	case event.Check != nil:
		check := event.Check
		if check.DurationMs != nil {
			row[6] = (time.Duration(*check.DurationMs) * time.Millisecond).String()
		}
		row[7] = fmt.Sprintf("%s #%d exit %d: %s", check.Stage, check.Index, check.ExitCode, terminalText(check.Command))
	case event.Work != nil:
		work := event.Work
		switch event.Kind {
		case quotatelemetry.KindDispatch:
			row[7] = fmt.Sprintf("offered to %s, attempt #%d", terminalText(work.WorkerID), work.AttemptNumber)
		case quotatelemetry.KindStart:
			row[7] = "claimed by " + terminalText(work.WorkerID)
			if work.DispatchToStartMs != nil {
				row[7] = fmt.Sprintf("queued %s after dispatch", (time.Duration(*work.DispatchToStartMs) * time.Millisecond).Round(time.Second))
			}
		case quotatelemetry.KindFinish:
			if work.DurationMs != nil {
				row[6] = (time.Duration(*work.DurationMs) * time.Millisecond).Round(time.Second).String()
			}
			parts := make([]string, 0, len(event.Deltas))
			for _, delta := range event.Deltas {
				parts = append(parts, quotaTelemetryDeltaText(delta))
			}
			row[7] = terminalText(work.Outcome)
			if len(parts) > 0 {
				row[7] += "; " + strings.Join(parts, ", ")
			}
		}
	case event.Recorder != nil:
		note := event.Recorder
		switch note.State {
		case quotatelemetry.RecorderStarted:
			row[7] = "started"
			if note.CoverageFrom != nil {
				row[7] += "; coverage from " + note.CoverageFrom.UTC().Format(time.RFC3339)
			}
		case quotatelemetry.RecorderGap:
			row[7] = "gap"
			if note.From != nil && note.To != nil {
				row[7] += fmt.Sprintf(" from %s to %s, %d failed ticks", note.From.UTC().Format(time.RFC3339),
					note.To.UTC().Format(time.RFC3339), note.FailedTicks)
			}
			if note.LastError != "" {
				row[7] += "; last error: " + terminalText(note.LastError)
			} else if note.Reason != "" {
				row[7] += "; " + terminalText(note.Reason)
			}
		default:
			row[7] = terminalText(note.State)
		}
	}
	// A tab inside a value would shift every later column.
	for index := range row {
		row[index] = strings.ReplaceAll(row[index], "\t", " ")
	}
	return row
}

// quotaTelemetryDeltaText is "five_hour +9pp shared(2)", or the absence.
func quotaTelemetryDeltaText(delta quotatelemetry.Delta) string {
	window := terminalText(delta.Window)
	if delta.DeltaPp == nil {
		return window + " n/a (" + delta.Absence + ")"
	}
	sign := ""
	if *delta.DeltaPp >= 0 {
		sign = "+"
	}
	text := window + " " + sign + strconv.FormatFloat(*delta.DeltaPp, 'f', -1, 64) + "pp " + delta.Attribution
	if delta.Attribution == quotatelemetry.AttributionShared {
		text += fmt.Sprintf("(%d)", delta.ConcurrentCount)
	}
	return text
}
