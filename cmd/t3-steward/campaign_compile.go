package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/diagnostics"
)

// campaignCompileSchemaVersion versions the compile document.
const campaignCompileSchemaVersion = 1

// campaignCompileReport is what compile prints under --json: one document,
// holding one object per compiled unit in plan order.
type campaignCompileReport struct {
	SchemaVersion int                   `json:"schemaVersion"`
	Out           string                `json:"out"`
	Units         []campaignCompileUnit `json:"units"`
}

type campaignCompileUnit struct {
	ID        string `json:"id"`
	Directory string `json:"directory"`
	// Valid and Errors are what "campaign validate" said about the written
	// directory.
	Valid       bool                        `json:"valid"`
	Errors      []string                    `json:"errors"`
	Diagnostics *diagnostics.Document       `json:"diagnostics,omitempty"`
	Plan        *campaignCompilePlanSummary `json:"plan,omitempty"`
	// Check is present only with --check.
	Check *campaignCompileCheck `json:"check,omitempty"`
	Size  *campaignCompileSize  `json:"size,omitempty"`
}

type campaignCompileSize struct {
	Raw           int64                             `json:"raw"`
	Estimate      int64                             `json:"estimate"`
	Multiplier    float64                           `json:"multiplier"`
	SampleCount   int                               `json:"sampleCount"`
	Label         string                            `json:"label"`
	Threshold     int64                             `json:"threshold"`
	Contributions []campaign.CompileSizeContributor `json:"contributions"`
}

type campaignCompilePlanSummary struct {
	Name   string `json:"name"`
	Tasks  int    `json:"tasks"`
	Edges  int    `json:"edges"`
	Digest string `json:"digest"`
}

type campaignCompileCheck struct {
	Outcome backlogadmin.ViabilityOutcome `json:"outcome"`
	// Reasons are the permanent reasons of an impossible unit, and the
	// temporary reasons it waits for otherwise, as "code: detail".
	Reasons []string `json:"reasons"`
}

// campaignCompileHelpPage is the reference of "campaign compile". The plan
// format is spelled out here because the plan is the one input of this verb
// that no other page describes.
func campaignCompileHelpPage() helpPage {
	return helpPage{
		Path:    "campaign compile",
		Purpose: "turn a structured plan into one ready-to-validate campaign directory per unit; never submits.",
		Usage:   []string{"t3-steward campaign compile PLAN --out DIR [--unit ID]... [--force] [--check] [--max-changed-lines N] [--size-calibration FILE] [--json]"},
		Flags: []helpFlag{
			{Name: "--out", Value: "DIR", Required: true, Text: "The directory the unit directories are written under, as DIR/<unit id>. It is created when missing."},
			{Name: "--unit", Value: "ID", Default: "every unit", Text: "Compile only this unit; repeat it for several. A unit the plan does not declare is refused."},
			{Name: "--force", Default: "off", Text: "Replace the directories of the units being compiled. Without it an existing unit directory is refused and nothing is written; other directories under DIR are never touched."},
			{Name: "--check", Default: "off", Text: "After writing, run the same live readiness check as \"campaign check\" on every valid unit. Without it compile reaches no coordinator."},
			{Name: "--max-changed-lines", Value: "N", Default: "300", Text: "Positive additions PLUS deletions estimate threshold; only --check gates annotated units above it, before writes or readiness."},
			{Name: "--size-calibration", Value: "FILE", Default: "none (uncalibrated)", Text: "Regular JSON file, at most 1 MiB; invalid explicit calibration refuses even legacy plans before writes or readiness."},
			jsonFlag("one document with one object per unit"),
		},
		Exits: append(append([]helpExit{
			{0, "every unit was written and is valid and, with --check, ready or accepted_waiting"},
			{1, "a malformed plan or command line (nothing is written), an existing unit directory without --force, a failed write, or a written unit campaign validate refuses. This is the campaign usage code; there is no separate code 2"},
		}, coordinatorExits()[2:7]...),
			helpExit{8, "with --check, a unit can never run as written, as \"campaign check\" exits"}),
		JSONKeys: []string{"schemaVersion", "out", "units", "id", "directory", "valid", "errors", "diagnostics", "plan", "check", "outcome", "reasons", "size", "raw", "estimate", "multiplier", "sampleCount", "label", "threshold", "contributions"},
		JSONNote: "Read schemaVersion first. units holds one object per compiled unit in plan order: id, directory, valid, errors (always an array), plan (name, tasks, edges, digest) for a valid unit, and check (outcome and reasons) with --check. Failed units add a diagnostics/v1 diagnostics document; malformed plans add it to the existing error envelope. Text refusals append diagnostic locations and hints. Available size estimates add size (raw, estimate, multiplier, sampleCount, label, threshold, contributions); unavailable legacy estimates omit size. " + jsonErrorNote,
		Notes: "Size annotations: each bullet in Code map and Tests to add ends with [changed-lines: N], counting additions PLUS deletions including tests. Put literal examples in fenced code. " +
			"Partial or malformed annotations refuse before any write or readiness; legacy units without annotations remain size unavailable/advisory, including with --check. Ordinary compile reports estimates advisory above threshold; --check allows equality. " +
			"Uncalibrated fallback is 1.25; calibration uses nearest-rank p90 with floor 1. Supply JSON {samples: [{raw, observed, base, head, accept_ref, gate_ref}]}, or an empty samples array for fallback. " +
			"Collect samples with the SAME raw estimator, recorded base and accepted head: sum Git numstat additions and deletions across committed files, excluding binary/unestimable samples. " +
			"Retain independent ACCEPT and passing gate references; references remain operator assertions. Refresh after accepted units and inspect ratios/underestimates; fallback tuning requires a separate reviewed change.\n\n" +
			"Plan format compile/v1 is a Markdown file whose YAML front matter, between leading --- lines, holds these keys and no others: " +
			"compile: v1; project, the catalog project; ref, a full 40-character commit every unit is pinned to (a branch name is refused); " +
			"class, required or surplus (default surplus); template: implement-review (default) or plan-review; " +
			"acceptance_tests: required|off (absent means off). With required, each brief needs, outside fenced code, a line exactly Acceptance tests: followed by a Markdown list. " +
			"Each item has a backticked Go test name or short case name, a colon and expected behavior; at least one item must contain (edge). " +
			"Unordered and ordered lists and wrapped/indented continuations are supported, including an edge tag on a continuation. Blank lines may separate items; a new top-level paragraph or heading ends the list. " +
			"type: git (default) requires ref; type: fresh requires plan-review and forbids even an empty ref. " +
			"plan-review units require outputs: [plan.md, risks.md], clean relative file names validated as manifest outputs; artifacts/continuation.md, artifacts/handoff.md and review.md are reserved. " +
			"The plan task declares these outputs and task state under artifacts/; review consumes every plan output and artifacts/handoff.md through inputs_from, writes review.md and task state, and declares the same verdict contract. Neither task declares commits. " +
			"routes.execute and roles.execute route plan; placement.implement and resources.implement configure it. Unspecified planning role effort is high. Planning prompts prohibit source edits and executing the produced plan. " +
			"roles.execute and roles.review, each an optional {effort: low|medium|high}; " +
			"routes.execute and routes.review, each {instance, model, quota_pool, effort}; verify, an optional list of implement verify commands; " +
			"ledger, the workflow ledger block {jocasta_project, plan, risk, acceptance}; placement, either shared {hosts, requires} for every task or {implement: {hosts, requires}, review: {hosts, requires}}; mixing forms is refused; " +
			"resources, a resources block per template task, as {implement: {preset: build}, review: {preset: review}}; review resources default to preset review (1 CPU unit, 1500 MB memory, 1024 MB scratch, minimum CPU class low), which fits 3-CPU/2000-MB/8-GB workers including the 2048-MB disk reserve; review has no host pin unless placement declares one; " +
			"max_turns, a positive turn bound for every task; inputs, a list of extra files relative to the plan's directory; " +
			"and units, a list of {id, title, section, outputs}, with outputs required for plan-review, such as {id: c1, title: Campaign compile skeleton, section: \"C-1\"}.\n\n" +
			"Routing: each template task is routed by role unless routes pins it. The implement task takes role execute and the review task role review, " +
			"with the effort roles names; the coordinator's route policy chooses the concrete route at check and submission, and prefers a review route of a provider family other than the implementation's. " +
			"Omitting both roles and routes routes both tasks by role. routes.<role> pins that task to exactly the route written. " +
			"roles.<role> and routes.<role> for the same task are refused as a conflict, and a plan that declares routes must pin every task roles does not route. " +
			"\"campaign check DIR/<unit id>\" shows the role selection, its effort and its diversity reason for each task.\n\n" +
			"An extra input must be a clean relative path inside the plan's directory, without glob characters, that is a regular file once symbolic links are resolved; " +
			"it is bundled into every unit at inputs/<path>, mounted at .t3/inputs/inputs/<path>, and named in both prompts. plan.md and unit.md are refused as input names because compile writes them.\n\n" +
			"A unit id is lower-case letters, digits and single hyphens starting with a letter, at most 64 bytes; it names the unit directory and the workflow. " +
			"section names the Markdown heading of the plan body whose section is the unit's brief. Only ATX headings (lines starting with #) count, and not inside fenced code; the heading's text is the section, or begins with it followed by a space or a colon, and the section runs to the next heading of the same or a higher level. " +
			"An unknown key, a missing or duplicate unit id, a section that matches no heading or several, and a git ref that is not a full commit are refused with the plan line and the reason, before anything is written.\n\n" +
			"For implement-review, each unit directory holds workflow.yaml (version 2, writing only the fields that differ from the workflow defaults, in the order: version, name, class, environment, placement, ledger, inputs, then the tasks in the order they run: an implement task that declares the commit implementation at HEAD and the outputs continuation.md and handoff.md, then a review task that needs it, takes the commit and handoff.md through inputs_from, and declares review_output: {verdict_line: review.md}), " +
			"inputs/plan.md (the whole plan, byte for byte), inputs/unit.md (the unit's section, byte for byte), prompts/implement.md and prompts/review.md. " +
			"The prompts name the pinned ref, the unit id and the mounted paths .t3/inputs/inputs/plan.md, .t3/inputs/inputs/unit.md and .t3/dependencies/.\n\n" +
			"The review prompt asks for review.md to start with VERDICT: ACCEPT or VERDICT: CHANGES_REQUESTED, and the coordinator records that line as the run's verdict, shown by task result and campaign show. " +
			"Only the coordinator parses it, so worker versions do not matter, but the coordinator must have this release: rc.116 and rc.117 refuse the VERDICT: label and fail the review task with review_output verification failed, and an older coordinator refuses the workflow at submission. Upgrade the coordinator first.\n\n" +
			"A unit larger than a campaign may carry, the whole plan plus its section, is refused before anything is written. " +
			"A unit is written into a temporary sibling (.compile-<id>-N) and renamed into place, so a failure leaves no half-written unit. " +
			"A compile that was killed part-way can leave those siblings, including .compile-<id>-retired-N/<id>, the unit a --force was replacing; the next compile of that unit is refused, naming them, until they are moved back or removed. " +
			"When some units are invalid and --check finds others impossible, the exit is 1. " +
			"Each written unit then goes through the campaign validate and campaign plan paths, offline. Compile never submits; submit each directory with \"t3-steward campaign submit DIR/<unit id> --idempotency-key KEY\".",
		Parsers: []parserSite{{Func: "parseCampaignCompileArgs"}},
	}
}

type campaignCompileArgs struct {
	plan   string
	out    string
	units  []string
	force  bool
	check  bool
	asJSON bool

	maxChangedLines int64
	sizeCalibration string
}

func parseCampaignCompileArgs(args []string) (campaignCompileArgs, error) {
	parsed := campaignCompileArgs{maxChangedLines: 300}
	seenSize := map[string]bool{}
	value := func(index int, flag string) (string, error) {
		if index+1 >= len(args) || args[index+1] == "" || strings.HasPrefix(args[index+1], "-") {
			return "", fmt.Errorf("%s needs a value; next: t3-steward campaign compile --help", flag)
		}
		return args[index+1], nil
	}
	for index := 0; index < len(args); index++ {
		switch argument := args[index]; argument {
		case "--out":
			if parsed.out != "" {
				return campaignCompileArgs{}, errors.New("--out may be supplied only once")
			}
			out, err := value(index, argument)
			if err != nil {
				return campaignCompileArgs{}, err
			}
			parsed.out = out
			index++
		case "--max-changed-lines", "--size-calibration":
			if seenSize[argument] {
				return campaignCompileArgs{}, fmt.Errorf("%s may be supplied only once", argument)
			}
			seenSize[argument] = true
			text, err := value(index, argument)
			if err != nil {
				return campaignCompileArgs{}, err
			}
			if argument == "--size-calibration" {
				parsed.sizeCalibration = text
			} else {
				parsed.maxChangedLines, err = strconv.ParseInt(text, 10, 64)
				if err != nil || parsed.maxChangedLines <= 0 {
					return campaignCompileArgs{}, errors.New("--max-changed-lines requires a positive integer")
				}
			}
			index++
		case "--unit":
			unit, err := value(index, argument)
			if err != nil {
				return campaignCompileArgs{}, err
			}
			parsed.units = append(parsed.units, unit)
			index++
		case "--force":
			parsed.force = true
		case "--check":
			parsed.check = true
		case "--json":
			if parsed.asJSON {
				return campaignCompileArgs{}, errors.New("--json may be supplied only once")
			}
			parsed.asJSON = true
		default:
			if strings.HasPrefix(argument, "-") {
				return campaignCompileArgs{}, fmt.Errorf("unknown campaign compile option %q; next: t3-steward campaign compile --help", argument)
			}
			if parsed.plan != "" {
				return campaignCompileArgs{}, errors.New("campaign compile accepts exactly one plan file")
			}
			parsed.plan = argument
		}
	}
	if parsed.plan == "" {
		return campaignCompileArgs{}, errors.New("campaign compile needs a plan file; next: t3-steward campaign compile --help")
	}
	if parsed.out == "" {
		return campaignCompileArgs{}, errors.New("campaign compile requires --out DIR, the directory the unit directories are written under")
	}
	return parsed, nil
}

// runCompile turns a compile/v1 plan into one campaign directory per unit and
// runs the validate and plan paths on each, plus the check path with --check.
// It never submits: the directories it writes are the input to "campaign
// submit", which a lead runs on purpose.
func (c campaignCLI) runCompile(ctx context.Context, args []string) (err error) {
	var source string
	defer func() { err = c.authoringFailure(err, source, requestsJSON(args)) }()
	parsed, err := parseCampaignCompileArgs(args)
	if err != nil {
		return err
	}
	source = parsed.plan
	raw, err := readCompilePlan(parsed.plan, c.limits.MaxBytes)
	if err != nil {
		return err
	}
	plan, err := campaign.ParseCompilePlanWith(parsed.plan, raw, campaign.CompileOptions{ReadInput: compileInputReader(parsed.plan, c.limits.MaxBytes)})
	if err != nil {
		return err
	}
	units, err := selectCompileUnits(plan.Units, parsed.units)
	if err != nil {
		return err
	}
	var calibration campaign.SizeCalibration
	if parsed.sizeCalibration != "" {
		data, err := readCompilePlan(parsed.sizeCalibration, 1<<20)
		if err != nil {
			return err
		}
		calibration, err = campaign.ParseSizeCalibration(data)
		if err != nil {
			return fmt.Errorf("campaign compile calibration: %w", err)
		}
	}
	sizes := make([]*campaignCompileSize, len(units))
	var sizeErrors []string
	for index, unit := range units {
		var section []byte
		for _, file := range unit.Files {
			if file.Path == "inputs/unit.md" {
				section = file.Content
			}
		}
		estimate := campaign.EstimateCompileSize(section, calibration)
		for _, diagnostic := range estimate.Diagnostics {
			sizeErrors = append(sizeErrors, unit.ID+": "+diagnostic)
		}
		if len(estimate.Diagnostics) > 0 || !estimate.Available {
			continue
		}
		sizes[index] = &campaignCompileSize{estimate.Raw, estimate.Estimate, estimate.Multiplier, estimate.SampleCount, estimate.Label, parsed.maxChangedLines, estimate.Contributors}
		if parsed.check && estimate.Estimate > parsed.maxChangedLines {
			message := fmt.Sprintf("unit %s estimate %d exceeds threshold %d", unit.ID, estimate.Estimate, parsed.maxChangedLines)
			for _, contribution := range estimate.Contributors {
				message += fmt.Sprintf("\n  %s unit line %d: %s (%d)", unit.ID, contribution.Line, contribution.Text, contribution.Raw)
			}
			sizeErrors = append(sizeErrors, message+"\nSeparate the largest owner and its tests into prerequisite parts.")
		}
	}
	if len(sizeErrors) > 0 {
		return fmt.Errorf("campaign compile: nothing was written:\n%s", strings.Join(sizeErrors, "\n"))
	}
	// Every refusal below is made before any unit is written, so it leaves the
	// output directory exactly as it was.
	for _, unit := range units {
		if size := compiledUnitBytes(unit); c.limits.MaxBytes > 0 && size > c.limits.MaxBytes {
			return &campaign.CompilePlanError{Source: parsed.plan, Line: unit.Line, Reason: fmt.Sprintf(
				"unit %q would be %d bytes, the whole plan plus its section, and a campaign may carry at most %d", unit.ID, size, c.limits.MaxBytes)}
		}
		leftovers, err := campaign.CompileLeftovers(parsed.out, unit.ID)
		if err != nil {
			return err
		}
		if len(leftovers) > 0 {
			return fmt.Errorf("campaign compile: nothing was written: %w", campaign.CompileLeftoverError(unit.ID, leftovers))
		}
	}
	if !parsed.force {
		var existing []string
		for _, unit := range units {
			path := filepath.Join(parsed.out, unit.ID)
			if _, statErr := os.Lstat(path); statErr == nil {
				existing = append(existing, path)
			} else if !os.IsNotExist(statErr) {
				return statErr
			}
		}
		if len(existing) > 0 {
			return fmt.Errorf("campaign compile refuses to overwrite %s; nothing was written. Pass --force to replace the units being compiled",
				strings.Join(existing, ", "))
		}
	}
	report := campaignCompileReport{SchemaVersion: campaignCompileSchemaVersion, Out: parsed.out, Units: []campaignCompileUnit{}}
	prepared := make([]struct {
		bundle campaign.Bundle
		plan   campaign.Plan
	}, len(units))
	for index, unit := range units {
		dir, err := campaign.WriteCompiledUnit(parsed.out, unit, campaign.WriteOptions{Force: parsed.force})
		if err != nil {
			written := make([]string, 0, index)
			for _, done := range report.Units {
				written = append(written, done.Directory)
			}
			return fmt.Errorf("campaign compile: unit %s was not written: %w (already written: %s)", unit.ID, err, campaignList(written))
		}
		result := campaignCompileUnit{ID: unit.ID, Directory: dir, Errors: []string{}, Size: sizes[index]}
		// The same path "campaign validate" and "campaign plan" take, so a unit
		// compile calls valid is one validate accepts.
		bundle, projected, err := c.prepare(dir)
		if err != nil {
			result.Errors = append(result.Errors, err.Error())
			document := diagnostics.FromError(err, filepath.Join(dir, "workflow.yaml"))
			result.Diagnostics = &document
		} else {
			result.Valid = true
			result.Plan = &campaignCompilePlanSummary{
				Name: projected.Name, Tasks: projected.Totals.Tasks, Edges: projected.Totals.Edges, Digest: bundle.ContentDigest,
			}
			prepared[index].bundle, prepared[index].plan = bundle, projected
		}
		report.Units = append(report.Units, result)
	}
	if parsed.check {
		for index := range report.Units {
			if !report.Units[index].Valid {
				continue
			}
			matrix, err := c.checkViability(ctx, prepared[index].plan, prepared[index].bundle, "")
			if err != nil {
				return fmt.Errorf("campaign compile wrote %d units, and the readiness check of %s failed: %w", len(report.Units), report.Units[index].ID, err)
			}
			report.Units[index].Check = compileCheckSummary(matrix)
		}
	}
	if parsed.asJSON {
		err = encodeCampaignJSON(c.stdout, report)
	} else {
		err = renderCampaignCompile(c.stdout, report)
	}
	if err != nil {
		return err
	}
	return afterDocument(campaignCompileVerdict(report))
}

// compiledUnitBytes is the size of a unit's files, which is what the
// coordinator's message limit is measured against.
func compiledUnitBytes(unit campaign.CompiledUnit) int64 {
	var total int64
	for _, file := range unit.Files {
		total += int64(len(file.Content))
	}
	return total
}

// readCompilePlan reads the plan, bounded by the largest campaign the
// coordinator accepts: every unit carries the whole plan, so a larger plan
// could only compile into units nothing would accept.
func readCompilePlan(path string, limit int64) ([]byte, error) {
	// The mode is checked before the open: opening a FIFO for reading blocks
	// until a writer appears, which would hang an unattended compile.
	if info, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("campaign compile: %w", err)
	} else if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("campaign compile: %s is not a regular file", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("campaign compile: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("campaign compile: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("campaign compile: %s is not a regular file", path)
	}
	reader := io.Reader(file)
	if limit > 0 {
		reader = io.LimitReader(file, limit+1)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("campaign compile: %w", err)
	}
	if limit > 0 && int64(len(raw)) > limit {
		return nil, fmt.Errorf("campaign compile: %s is larger than the %d bytes a campaign may carry, and every unit carries the whole plan", path, limit)
	}
	return raw, nil
}

// selectCompileUnits keeps the units --unit names, in plan order, or every
// unit when none is named.
func selectCompileUnits(units []campaign.CompiledUnit, wanted []string) ([]campaign.CompiledUnit, error) {
	if len(wanted) == 0 {
		return units, nil
	}
	known := map[string]bool{}
	ids := make([]string, 0, len(units))
	for _, unit := range units {
		known[unit.ID] = true
		ids = append(ids, unit.ID)
	}
	selected := map[string]bool{}
	for _, id := range wanted {
		if !known[id] {
			return nil, fmt.Errorf("campaign compile: the plan declares no unit %q; its units are %s", id, strings.Join(ids, ", "))
		}
		selected[id] = true
	}
	var kept []campaign.CompiledUnit
	for _, unit := range units {
		if selected[unit.ID] {
			kept = append(kept, unit)
		}
	}
	return kept, nil
}

// compileCheckSummary is the part of a readiness matrix compile reports: the
// outcome, and the reasons that explain it.
func compileCheckSummary(matrix backlogadmin.ViabilityMatrix) *campaignCompileCheck {
	summary := &campaignCompileCheck{Outcome: matrix.Outcome, Reasons: []string{}}
	seen := map[string]bool{}
	add := func(reason backlogadmin.ViabilityReason) {
		line := reason.Code + ": " + reason.Detail
		if !seen[line] {
			seen[line] = true
			summary.Reasons = append(summary.Reasons, line)
		}
	}
	if matrix.Outcome == backlogadmin.ViabilityImpossible {
		for _, reason := range matrix.PermanentReasons() {
			add(reason)
		}
	} else {
		temporary := func(reasons []backlogadmin.ViabilityReason) {
			for _, reason := range reasons {
				if !reason.Permanent {
					add(reason)
				}
			}
		}
		temporary(matrix.Reasons)
		for _, task := range matrix.Tasks {
			temporary(task.Reasons)
			for _, candidate := range task.Candidates {
				temporary(candidate.Reasons)
			}
		}
	}
	sort.Strings(summary.Reasons)
	return summary
}

// campaignCompileVerdict is the exit code of a compile whose document has been
// printed: 1 when a written unit is invalid, 8 when --check found a unit that
// can never run, as "campaign check" exits, and 0 otherwise.
func campaignCompileVerdict(report campaignCompileReport) error {
	var invalid, impossible []string
	var reasons []string
	for _, unit := range report.Units {
		if !unit.Valid {
			invalid = append(invalid, unit.ID)
			continue
		}
		if unit.Check != nil && unit.Check.Outcome == backlogadmin.ViabilityImpossible {
			impossible = append(impossible, unit.ID)
			for _, reason := range unit.Check.Reasons {
				reasons = append(reasons, unit.ID+": "+reason)
			}
		}
	}
	if len(invalid) > 0 {
		return fmt.Errorf("campaign compile wrote units that campaign validate refuses: %s", strings.Join(invalid, ", "))
	}
	if len(impossible) > 0 {
		return &backlogadmin.TransportError{
			Class:     backlogadmin.ClassRejected,
			Operation: "campaign compile",
			Err: fmt.Errorf("units %s can never run as written:\n  %s",
				strings.Join(impossible, ", "), strings.Join(reasons, "\n  ")),
		}
	}
	return nil
}

func renderCampaignCompile(out io.Writer, report campaignCompileReport) error {
	noun := "units"
	if len(report.Units) == 1 {
		noun = "unit"
	}
	if _, err := fmt.Fprintf(out, "compiled %d %s into %s\n", len(report.Units), noun, report.Out); err != nil {
		return err
	}
	table := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
	for _, unit := range report.Units {
		valid, planned := "invalid", "plan -"
		if unit.Valid {
			valid, planned = "valid", "plan ok"
		}
		line := fmt.Sprintf("  %s\t%s\t%s\t%s", unit.ID, unit.Directory, valid, planned)
		if unit.Size == nil {
			line += "\tsize unavailable/advisory"
		} else {
			line += fmt.Sprintf("\tsize raw %d estimate %d multiplier %g samples %d %s threshold %d", unit.Size.Raw, unit.Size.Estimate, unit.Size.Multiplier, unit.Size.SampleCount, unit.Size.Label, unit.Size.Threshold)
		}
		if unit.Check != nil {
			line += "\tcheck " + string(unit.Check.Outcome)
		}
		if _, err := fmt.Fprintln(table, line); err != nil {
			return err
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	for _, unit := range report.Units {
		if unit.Size != nil {
			for _, contribution := range unit.Size.Contributions {
				if _, err := fmt.Fprintf(out, "  %s size %s unit line %d: %s (%d)\n", unit.ID, contribution.Section, contribution.Line, contribution.Text, contribution.Raw); err != nil {
					return err
				}
			}
		}
		for _, message := range unit.Errors {
			if unit.Diagnostics != nil {
				message += diagnostics.Text(diagnostics.Attach(errors.New(message), *unit.Diagnostics), "")
			}
			if _, err := fmt.Fprintf(out, "  %s error: %s\n", unit.ID, message); err != nil {
				return err
			}
		}
		if unit.Check != nil && unit.Check.Outcome != backlogadmin.ViabilityReady {
			for _, reason := range unit.Check.Reasons {
				if _, err := fmt.Fprintf(out, "  %s %s: %s\n", unit.ID, unit.Check.Outcome, reason); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
