package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The submission carrier also exposes authorized artifact retrieval. Both real
// carriers authenticate their own identity; the Principal argument is ignored
// by LocalClient and SSHClient, just as on task result.
type campaignFixArtifactTransport interface {
	OpenArtifact(context.Context, backlogadmin.Principal, string) (backlogadmin.ArtifactContent, error)
}
type campaignFixCLI struct {
	campaignCLI
	open func(context.Context, string) (backlogadmin.ArtifactContent, error)
}

func (c campaignCLI) runFix(ctx context.Context, args []string) error {
	f := campaignFixCLI{campaignCLI: c}
	f.open = func(ctx context.Context, id string) (backlogadmin.ArtifactContent, error) {
		if c.submissions == nil {
			return backlogadmin.ArtifactContent{}, errors.New("coordinator artifact transport unavailable")
		}
		client, err := c.submissions()
		if err != nil {
			return backlogadmin.ArtifactContent{}, err
		}
		carrier, ok := client.(campaignFixArtifactTransport)
		if !ok {
			return backlogadmin.ArtifactContent{}, errors.New("coordinator artifact transport unavailable; upgrade the coordinator")
		}
		return carrier.OpenArtifact(ctx, backlogadmin.Principal{}, id)
	}
	return f.run(ctx, args)
}

type campaignFixArgs struct {
	node                             domain.NodeRef
	key, commit, out, notify         string
	roundLimit                       int
	gate                             []string
	gateTimeout                      time.Duration
	contexts                         []string
	noGate, noNotify, dryRun, asJSON bool
}

func parseCampaignFixArgs(args []string) (campaignFixArgs, error) {
	var a campaignFixArgs
	seen := map[string]bool{}
	var selector string
	for i := 0; i < len(args); i++ {
		flag := args[i]
		switch flag {
		case "--json", "--dry-run", "--no-gate", "--no-notify":
			if seen[flag] {
				return a, fmt.Errorf("%s may be supplied only once", flag)
			}
			seen[flag] = true
			switch flag {
			case "--json":
				a.asJSON = true
			case "--dry-run":
				a.dryRun = true
			case "--no-gate":
				a.noGate = true
			case "--no-notify":
				a.noNotify = true
			}
		case "--idempotency-key", "--round-limit", "--gate", "--gate-timeout", "--commit", "--context", "--out", "--notify-thread":
			if i+1 >= len(args) || strings.TrimSpace(args[i+1]) == "" {
				return a, fmt.Errorf("%s needs one nonempty value", flag)
			}
			if seen[flag] && flag != "--gate" && flag != "--context" {
				return a, fmt.Errorf("%s may be supplied only once", flag)
			}
			seen[flag] = true
			i++
			v := args[i]
			switch flag {
			case "--idempotency-key":
				a.key = v
			case "--round-limit":
				n, e := strconv.Atoi(v)
				if e != nil || n < 1 || n > 8 {
					return a, errors.New("--round-limit must be an integer in 1..8")
				}
				a.roundLimit = n
			case "--gate":
				a.gate = append(a.gate, v)
			case "--gate-timeout":
				d, e := time.ParseDuration(v)
				if e != nil || d <= 0 || d > 6*time.Hour {
					return a, errors.New("--gate-timeout must be a duration in (0, 6h]")
				}
				a.gateTimeout = d
			case "--commit":
				if _, _, _, e := parseFixCommitRef(v); e != nil {
					return a, e
				}
				a.commit = v
			case "--context":
				a.contexts = append(a.contexts, v)
			case "--out":
				a.out = v
			case "--notify-thread":
				a.notify = v
			}
		default:
			if strings.HasPrefix(flag, "-") {
				return a, fmt.Errorf("unknown campaign fix option %q", flag)
			}
			if selector != "" {
				return a, errors.New("campaign fix accepts exactly one <run>/<review-task>")
			}
			selector = flag
		}
	}
	var err error
	a.node, err = domain.ParseNodeRef(selector)
	if err != nil {
		return a, err
	}
	if !safeFixComponent(a.node.RunID) || !safeFixComponent(a.node.TaskID) {
		return a, errors.New("campaign fix requires path-safe run and task names")
	}
	if a.key == "" {
		return a, errors.New("campaign fix requires --idempotency-key KEY")
	}
	if a.dryRun && a.out == "" {
		return a, errors.New("--dry-run requires --out DIR")
	}
	if a.noNotify && a.notify != "" {
		return a, errors.New("--no-notify conflicts with --notify-thread")
	}
	if a.out != "" {
		if _, e := os.Lstat(a.out); e == nil {
			return a, fmt.Errorf("--out %s already exists", a.out)
		} else if !os.IsNotExist(e) {
			return a, e
		}
	}
	if len(a.gate) > 0 {
		timeout := a.gateTimeout
		if timeout == 0 {
			timeout = 30 * time.Minute
		}
		if err := (domain.TaskGate{Commands: a.gate, Timeout: timeout}).Validate(); err != nil {
			return a, err
		}
	}
	if err := campaign.ValidateFixContextNames(a.contexts); err != nil {
		return a, err
	}
	return a, nil
}
func safeFixComponent(s string) bool {
	if s == "" || s == "." || s == ".." {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}
func parseFixCommitRef(s string) (string, string, string, error) {
	p := strings.Split(s, "/")
	if len(p) != 3 || !safeFixComponent(p[0]) || !safeFixComponent(p[1]) || !safeFixComponent(p[2]) {
		return "", "", "", errors.New("--commit must be RUN/TASK/NAME with path-safe components")
	}
	return p[0], p[1], p[2], nil
}

type fixReviewedCommit struct {
	Run    string `json:"run"`
	Task   string `json:"task"`
	Name   string `json:"name"`
	Commit string `json:"commit"`
}
type fixVerdict struct {
	Verdict          string   `json:"verdict"`
	BlockingFindings int      `json:"blockingFindings"`
	FindingTitles    []string `json:"findingTitles"`
	Source           string   `json:"source"`
}
type fixGate struct {
	Commands []string `json:"commands"`
	Timeout  string   `json:"timeout"`
	Source   string   `json:"source"`
}
type fixLineageReport struct {
	RootRun        string `json:"rootRun"`
	RootReviewTask string `json:"rootReviewTask"`
	RoundsUsed     int    `json:"roundsUsed"`
	RoundLimit     int    `json:"roundLimit"`
	RoundsDeclared int    `json:"roundsDeclared"`
	FirstRound     int    `json:"firstRound"`
}
type campaignFixDocument struct {
	SchemaVersion int                `json:"schemaVersion"`
	RunID         string             `json:"runId"`
	Replay        bool               `json:"replay"`
	SourceRun     string             `json:"sourceRun"`
	ReviewTask    string             `json:"reviewTask"`
	Commit        fixReviewedCommit  `json:"reviewedCommit"`
	Verdict       fixVerdict         `json:"verdict"`
	Lineage       fixLineageReport   `json:"lineage"`
	Gate          fixGate            `json:"gate"`
	Submission    campaignSubmission `json:"submission"`
}
type resolvedCampaignFix struct {
	Source  campaign.FixSource
	Options campaign.FixOptions
	Commit  fixReviewedCommit
	Verdict fixVerdict
	Gate    fixGate
	Lineage fixLineageReport
}
type fixCommitCandidate struct {
	run    string
	task   backlogadmin.TaskDetail
	name   string
	detail backlogadmin.WorkflowDetail
}

func fixTask(d backlogadmin.WorkflowDetail, name string) (backlogadmin.TaskDetail, error) {
	for _, t := range d.Tasks {
		if t.Task.Name == name || t.Task.ID == name {
			return t, nil
		}
	}
	return backlogadmin.TaskDetail{}, fmt.Errorf("run %s has no task %q", d.Summary.Run.ID, name)
}
func fixSucceeded(t backlogadmin.TaskDetail) error {
	if t.Attempt == nil || t.Attempt.Progress != domain.ProgressSucceeded {
		return fmt.Errorf("task %s latest attempt must be terminal and succeeded", t.Task.Name)
	}
	return nil
}
func fixDeclaredCommit(t domain.Task, name string) bool {
	for _, o := range t.Outputs {
		if o.Name == name && o.Commit != nil {
			return true
		}
	}
	return false
}
func fixArtifact(t backlogadmin.TaskDetail, name string) (backlogadmin.ArtifactMetadata, error) {
	for _, a := range t.Artifacts {
		m := a.Metadata
		if m.Name == name && (m.AttemptID == "" || t.Attempt != nil && m.AttemptID == t.Attempt.ID) {
			return m, nil
		}
	}
	return backlogadmin.ArtifactMetadata{}, fmt.Errorf("task %s has no retained artifact %q for latest attempt", t.Task.Name, name)
}
func (c campaignFixCLI) readArtifact(ctx context.Context, m backlogadmin.ArtifactMetadata) ([]byte, error) {
	if c.open == nil {
		return nil, errors.New("coordinator artifact transport unavailable")
	}
	limit := c.limits.MaxBytes
	if limit <= 0 {
		return nil, errors.New("campaign limits unavailable")
	}
	if m.Size < 0 || m.Size > limit {
		return nil, fmt.Errorf("artifact %s exceeds campaign byte limit %d", m.Name, limit)
	}
	a, e := c.open(ctx, m.ID)
	if e != nil {
		return nil, e
	}
	if a.Content == nil {
		return nil, fmt.Errorf("artifact %s content unavailable", m.Name)
	}
	defer a.Content.Close()
	raw, e := io.ReadAll(io.LimitReader(a.Content, limit+1))
	if e != nil {
		return nil, e
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("artifact %s exceeds campaign byte limit %d", m.Name, limit)
	}
	return raw, nil
}
func (c campaignFixCLI) verdict(ctx context.Context, t backlogadmin.TaskDetail) (*domain.ReviewVerdict, string, error) {
	if e := fixSucceeded(t); e != nil {
		return nil, "", e
	}
	if t.Attempt.ReviewVerdict != nil {
		v := t.Attempt.ReviewVerdict
		if (v.Verdict != "accept" && v.Verdict != "changes-requested") || v.BlockingFindings < 0 {
			return nil, "", errors.New("invalid recorded review verdict")
		}
		return v, "recorded", nil
	}
	if t.Task.ReviewOutput == nil {
		declared := false
		for _, o := range t.Task.Outputs {
			if o.Name == "review.md" && o.Commit == nil {
				declared = true
			}
		}
		if declared {
			m, e := fixArtifact(t, "review.md")
			if e != nil {
				return nil, "", e
			}
			raw, e := c.readArtifact(ctx, m)
			if e != nil {
				return nil, "", e
			}
			line, _, _ := strings.Cut(string(raw), "\n")
			switch line {
			case "VERDICT: ACCEPT":
				return &domain.ReviewVerdict{Verdict: "accept"}, "review.md first line", nil
			case "VERDICT: CHANGES_REQUESTED":
				return &domain.ReviewVerdict{Verdict: "changes-requested"}, "review.md first line", nil
			default:
				return nil, "", errors.New("review.md first line must be exactly VERDICT: ACCEPT or VERDICT: CHANGES_REQUESTED")
			}
		}
	}
	return nil, "", errors.New("no recorded verdict: declare review_output, or the coordinator predates rc.116")
}
func fixRounds(used, limit int, v *domain.ReviewVerdict, root string) (int, error) {
	remaining := limit - used
	if remaining <= 0 {
		return 0, fmt.Errorf("review-round-limit-exhausted: lineage %s used %d of round_limit %d fix rounds; latest review requested changes (blocking %d): %s; the lead decides: a design pass, or campaign fix ... --round-limit N to allow more rounds", root, used, limit, v.BlockingFindings, strings.Join(v.FindingTitles, ", "))
	}
	if remaining > 2 {
		return 2, nil
	}
	return remaining, nil
}
func (c campaignFixCLI) resolve(ctx context.Context, a campaignFixArgs) (resolvedCampaignFix, error) {
	var out resolvedCampaignFix
	if c.detail == nil {
		return out, errors.New("coordinator detail transport unavailable")
	}
	cache := map[string]backlogadmin.WorkflowDetail{}
	get := func(run string) (backlogadmin.WorkflowDetail, error) {
		if d, ok := cache[run]; ok {
			return d, nil
		}
		d, e := c.detail(ctx, run)
		if e != nil {
			return d, e
		}
		if d.Summary.Run.ID != run {
			return d, fmt.Errorf("coordinator returned run %q for %q", d.Summary.Run.ID, run)
		}
		cache[run] = d
		return d, nil
	}
	d, e := get(a.node.RunID)
	if e != nil {
		return out, e
	}
	r, e := fixTask(d, a.node.TaskID)
	if e != nil {
		return out, e
	}
	v, source, e := c.verdict(ctx, r)
	if e != nil {
		return out, e
	}
	// Resolve only declared commits. Repeated references to the same declaration
	// collapse; output names alone never prove that a file is a commit.
	candidates := map[string]fixCommitCandidate{}
	add := func(run, task, name string) error {
		pd, e := get(run)
		if e != nil {
			return e
		}
		p, e := fixTask(pd, task)
		if e != nil {
			return e
		}
		if !fixDeclaredCommit(p.Task, name) {
			return nil
		}
		key := run + "/" + p.Task.Name + "/" + name
		candidates[key] = fixCommitCandidate{run: run, task: p, name: name, detail: pd}
		return nil
	}
	if a.commit != "" {
		run, task, name, e := parseFixCommitRef(a.commit)
		if e != nil {
			return out, e
		}
		if e = add(run, task, name); e != nil {
			return out, e
		}
		if len(candidates) == 0 {
			return out, fmt.Errorf("--commit %s is not a declared commit", a.commit)
		}
	} else {
		producers := make([]string, 0, len(r.Task.DependencyInputs))
		for p := range r.Task.DependencyInputs {
			producers = append(producers, p)
		}
		sort.Strings(producers)
		for _, p := range producers {
			for _, name := range r.Task.DependencyInputs[p] {
				if e = add(a.node.RunID, p, name); e != nil {
					return out, e
				}
			}
		}
		for _, carried := range r.Task.CarriedInputs {
			if carried.SourceRunID == "" {
				continue
			}
			if e = add(carried.SourceRunID, carried.ProducerTaskID, carried.Name); e != nil {
				return out, e
			}
		}
	}
	if len(candidates) == 0 {
		return out, fmt.Errorf("no reviewed commit is resolvable: review task %s consumes no declared commit; pass --commit RUN/TASK/NAME", r.Task.Name)
	}
	if len(candidates) > 1 {
		names := make([]string, 0, len(candidates))
		for n := range candidates {
			names = append(names, n)
		}
		sort.Strings(names)
		return out, fmt.Errorf("ambiguous reviewed commits: %s; pass --commit RUN/TASK/NAME", strings.Join(names, ", "))
	}
	var p fixCommitCandidate
	for _, candidate := range candidates {
		p = candidate
	}
	if e = fixSucceeded(p.task); e != nil {
		return out, e
	}
	if d.Summary.Workflow.Environment.Type == "fresh" || p.detail.Summary.Workflow.Environment.Type == "fresh" {
		return out, errors.New("campaign fix refuses a type: fresh environment")
	}
	m, e := fixArtifact(p.task, p.name)
	if e != nil {
		return out, e
	}
	raw, e := c.readArtifact(ctx, m)
	if e != nil {
		return out, e
	}
	provenance, e := backlog.ParseCommitProvenance(raw)
	if e != nil {
		return out, e
	}
	if provenance.WorkflowRunID != p.run || provenance.TaskID != p.task.Task.ID || provenance.Name != p.name {
		return out, errors.New("reviewed commit provenance does not match its declared producer")
	}
	out.Commit = fixReviewedCommit{Run: p.run, Task: p.task.Task.Name, Name: p.name, Commit: provenance.Commit}
	if v.Verdict == "accept" {
		return out, fmt.Errorf("review %s/%s accepted %s; nothing to fix", a.node.RunID, r.Task.Name, provenance.Commit)
	}
	out.Verdict = fixVerdict{Verdict: v.Verdict, BlockingFindings: v.BlockingFindings, FindingTitles: v.FindingTitles, Source: source}
	lineage := campaign.FixLineage{Schema: "steward.fix-lineage/v1", RootRun: a.node.RunID, RootProducingTask: p.task.Task.Name, RootReviewTask: r.Task.Name, RoundLimit: 4}
	// Workflow inputs identify lineage/brief, rather than task outputs or
	// similarly named dependency artifacts. Read each once, bounded by limits.
	inputIDs := map[string]bool{}
	for _, id := range d.Summary.Workflow.InputArtifactIDs {
		inputIDs[id] = true
	}
	for _, id := range d.Summary.Run.InputArtifactIDs {
		inputIDs[id] = true
	}
	hasLineage := false
	for _, artifact := range d.Artifacts {
		m := artifact.Metadata
		if !inputIDs[m.ID] || m.Name != "inputs/fix/lineage.json" {
			continue
		}
		if hasLineage {
			return out, errors.New("duplicate fix lineage artifacts")
		}
		raw, e := c.readArtifact(ctx, m)
		if e != nil {
			return out, e
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if e = dec.Decode(&lineage); e != nil {
			return out, fmt.Errorf("invalid fix lineage: %w", e)
		}
		var extra any
		if dec.Decode(&extra) != io.EOF {
			return out, errors.New("invalid fix lineage: trailing JSON")
		}
		if lineage.Schema != "steward.fix-lineage/v1" || !safeFixComponent(lineage.RootRun) || !safeFixComponent(lineage.RootProducingTask) || !safeFixComponent(lineage.RootReviewTask) || lineage.RoundLimit < 1 || lineage.RoundLimit > 8 || lineage.RoundsUsedBefore < 0 || lineage.RoundsUsedBefore > 8 || lineage.RoundsDeclared < 1 || lineage.RoundsDeclared > 2 || lineage.RoundsUsedBefore+lineage.RoundsDeclared > lineage.RoundLimit {
			return out, errors.New("invalid fix lineage counters or root")
		}
		hasLineage = true
	}
	used := 0
	if hasLineage {
		used = lineage.RoundsUsedBefore
		f, e := fixTask(d, "fix1")
		if e != nil {
			return out, errors.New("fix lineage has no fix1 task")
		}
		if e = fixSucceeded(f); e != nil {
			return out, e
		}
		used++
		if lineage.RoundsDeclared == 2 {
			f, e := fixTask(d, "fix2")
			if e != nil {
				return out, errors.New("fix lineage has no fix2 task")
			}
			if e = fixSucceeded(f); e != nil {
				return out, e
			}
			intermediate, e := fixTask(d, "review2")
			if e != nil {
				return out, e
			}
			if e = fixSucceeded(intermediate); e != nil {
				return out, e
			}
			if intermediate.Attempt.ReviewVerdict == nil {
				return out, errors.New("fix lineage review2 lacks a recorded verdict")
			}
			switch intermediate.Attempt.ReviewVerdict.Verdict {
			case "accept":
			case "changes-requested":
				used++
			default:
				return out, errors.New("invalid intermediate recorded verdict")
			}
		}
		lineage.History = append(lineage.History, campaign.FixLineageEntry{Run: a.node.RunID, ReviewTask: r.Task.Name, ReviewedCommit: provenance.Commit, Verdict: v.Verdict, BlockingFindings: v.BlockingFindings})
	}
	limit := lineage.RoundLimit
	if a.roundLimit != 0 {
		limit = a.roundLimit
	}
	declared, e := fixRounds(used, limit, v, lineage.RootRun+"/"+lineage.RootReviewTask)
	if e != nil {
		return out, e
	}
	lineage.RoundLimit = limit
	lineage.RoundsUsedBefore = used
	lineage.RoundsDeclared = declared
	var gate *domain.TaskGate
	gateSource := "producer"
	switch {
	case len(a.gate) > 0:
		timeout := a.gateTimeout
		if timeout == 0 {
			timeout = 30 * time.Minute
		}
		gate = &domain.TaskGate{Commands: append([]string(nil), a.gate...), Timeout: timeout}
		gateSource = "flag"
	case a.noGate:
		gateSource = "no-gate"
	case hasLineage:
		gate = lineage.Gate
		gateSource = "lineage"
	default:
		gate = p.task.Task.Gate
	}
	if gate == nil && gateSource != "no-gate" && !(hasLineage && lineage.Gate == nil) {
		return out, errors.New("no gate is resolvable: pass --gate CMD [--gate-timeout DUR] or --no-gate")
	}
	if gate != nil {
		copyGate := *gate
		copyGate.Commands = append([]string(nil), gate.Commands...)
		gate = &copyGate
		if len(gate.Commands) == 0 {
			return out, errors.New("resolved gate has no commands")
		}
		if gate.Timeout == 0 {
			gate.Timeout = 30 * time.Minute
		}
		if gate.Timeout < 0 {
			return out, errors.New("resolved gate timeout must be positive")
		}
		out.Gate = fixGate{Commands: gate.Commands, Timeout: gate.Timeout.String(), Source: gateSource}
	} else {
		out.Gate = fixGate{Commands: []string{}, Source: gateSource}
	}
	lineage.Gate = gate
	out.Lineage = fixLineageReport{RootRun: lineage.RootRun, RootReviewTask: lineage.RootReviewTask, RoundsUsed: used, RoundLimit: limit, RoundsDeclared: declared, FirstRound: used + 1}
	var brief []campaign.CompiledFile
	var briefBytes int64
	addBrief := func(name string, m backlogadmin.ArtifactMetadata) error {
		if path.IsAbs(name) || path.Clean(name) != name || strings.HasPrefix(name, "../") {
			return fmt.Errorf("unsafe original brief path %q", name)
		}
		raw, e := c.readArtifact(ctx, m)
		if e != nil {
			return e
		}
		briefBytes += int64(len(raw))
		if briefBytes > c.limits.MaxBytes || len(brief)+1 > c.limits.MaxFiles {
			return fmt.Errorf("original brief file %s exceeds campaign limits", name)
		}
		brief = append(brief, campaign.CompiledFile{Path: name, Content: raw})
		return nil
	}
	if hasLineage {
		for _, artifact := range d.Artifacts {
			m := artifact.Metadata
			if inputIDs[m.ID] && strings.HasPrefix(m.Name, "inputs/fix/brief/") {
				if e = addBrief(m.Name, m); e != nil {
					return out, e
				}
			}
		}
		if len(brief) == 0 {
			return out, errors.New("fix lineage has no retained original brief")
		}
	} else {
		prompt := backlogadmin.ArtifactMetadata{ID: p.task.Task.PromptArtifactID, Name: "prompt.md"}
		if prompt.ID == "" {
			return out, errors.New("producing task has no prompt artifact")
		}
		if e = addBrief("inputs/fix/brief/prompt.md", prompt); e != nil {
			return out, e
		}
		ids := map[string]bool{}
		for _, id := range p.task.Task.InputArtifactIDs {
			ids[id] = true
		}
		found := map[string]bool{}
		for _, artifact := range p.detail.Artifacts {
			m := artifact.Metadata
			if ids[m.ID] {
				if e = addBrief("inputs/fix/brief/inputs/"+m.Name, m); e != nil {
					return out, e
				}
				found[m.ID] = true
			}
		}
		for id := range ids {
			if !found[id] {
				return out, fmt.Errorf("producing task input artifact %s metadata unavailable", id)
			}
		}
	}
	out.Source = campaign.FixSource{RunID: a.node.RunID, ProducerRunID: p.run, Workflow: d.Summary.Workflow, Producer: p.task.Task, Review: r.Task, ReviewedCommit: provenance.Commit}
	out.Options = campaign.FixOptions{Name: "fix-" + r.Task.Name, Lineage: lineage, Gate: gate, Brief: brief}
	return out, nil
}
func (c campaignFixCLI) run(ctx context.Context, args []string) error {
	a, e := parseCampaignFixArgs(args)
	if e != nil {
		return e
	}
	// Read local context before any coordinator access. Reject symlinks and
	// nonregular files, then bound the aggregate while streaming every input.
	var contexts []campaign.CompiledFile
	var total int64
	for _, file := range a.contexts {
		info, e := os.Lstat(file)
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("--context %s must be a regular file", file)
		}
		if info.Size() > c.limits.MaxBytes {
			return fmt.Errorf("--context %s exceeds campaign byte limit", file)
		}
		f, e := os.Open(file)
		if e != nil {
			return e
		}
		raw, e := io.ReadAll(io.LimitReader(f, c.limits.MaxBytes+1))
		closeErr := f.Close()
		if e != nil {
			return e
		}
		if closeErr != nil {
			return closeErr
		}
		total += int64(len(raw))
		if total > c.limits.MaxBytes || len(contexts)+1 > c.limits.MaxFiles {
			return fmt.Errorf("--context %s exceeds campaign limits", file)
		}
		contexts = append(contexts, campaign.CompiledFile{Path: "inputs/fix/context/" + filepath.Base(file), Content: raw})
	}
	resolved, e := c.resolve(ctx, a)
	if e != nil {
		return e
	}
	resolved.Options.Context = contexts
	unit, e := campaign.GenerateFixChain(resolved.Source, resolved.Options)
	if e != nil {
		return e
	}
	var bytesTotal int64
	for i, file := range unit.Files {
		bytesTotal += int64(len(file.Content))
		if i+1 > c.limits.MaxFiles || bytesTotal > c.limits.MaxBytes {
			return fmt.Errorf("generated file %s exceeds campaign limits", file.Path)
		}
	}
	target := a.out
	if target == "" {
		parent, e := os.MkdirTemp("", "steward-fix-")
		if e != nil {
			return e
		}
		defer os.RemoveAll(parent)
		target = filepath.Join(parent, "campaign")
	}
	target, e = campaign.WriteFixChain(target, unit, c.limits)
	if e != nil {
		return e
	}
	bundle, e := campaign.Prepare(target, c.limits)
	if e != nil {
		return e
	}
	document := campaignFixDocument{SchemaVersion: 1, SourceRun: a.node.RunID, ReviewTask: resolved.Source.Review.Name, Commit: resolved.Commit, Verdict: resolved.Verdict, Lineage: resolved.Lineage, Gate: resolved.Gate}
	if a.dryRun {
		if a.asJSON {
			return encodeCampaignJSON(c.stdout, struct {
				campaignFixDocument
				DryRun bool   `json:"dryRun"`
				Digest string `json:"digest"`
			}{document, true, bundle.ContentDigest})
		}
		_, e = fmt.Fprintf(c.stdout, "dry-run: %s; rounds %d-%d of round_limit %d; digest=%s; no submission\n", target, document.Lineage.FirstRound, document.Lineage.RoundsUsed+document.Lineage.RoundsDeclared, document.Lineage.RoundLimit, bundle.ContentDigest)
		return e
	}
	var buffer bytes.Buffer
	submit := c.campaignCLI
	submit.stdout = &buffer
	var replayWake startedRunWake
	if c.describe != nil {
		submit.describe = func(ctx context.Context, run string) (backlogadmin.WorkflowSummary, error) {
			summary, err := c.describe(ctx, run)
			if err != nil {
				replayWake.ProgressUnavailable = err.Error()
			} else {
				replayWake.Progress = string(summary.Run.Progress)
			}
			return summary, err
		}
	} else {
		replayWake.ProgressUnavailable = "coordinator query transport is unavailable"
	}
	submitArgs := []string{target, "--idempotency-key", a.key, "--json"}
	if a.noNotify {
		submitArgs = append(submitArgs, "--no-notify")
	} else if a.notify != "" {
		submitArgs = append(submitArgs, "--notify-thread", a.notify)
	}
	if e = submit.runSubmit(ctx, submitArgs); e != nil {
		return e
	}
	if e = json.Unmarshal(buffer.Bytes(), &document.Submission); e != nil {
		return fmt.Errorf("decode campaign submit receipt: %w", e)
	}
	document.RunID = document.Submission.RunID
	document.Replay = document.Submission.Replay
	if a.asJSON {
		return encodeCampaignJSON(c.stdout, document)
	}
	if _, e = fmt.Fprintf(c.stdout, "run %s\nfix %s/%s: verdict %s (blocking %d, %s) on commit %s (%s/%s)\n  rounds %d-%d of round_limit %d (lineage root %s/%s); gate: %s, %s\n", document.RunID, a.node.RunID, document.ReviewTask, document.Verdict.Verdict, document.Verdict.BlockingFindings, document.Verdict.Source, document.Commit.Commit, document.Commit.Task, document.Commit.Name, document.Lineage.FirstRound, document.Lineage.RoundsUsed+document.Lineage.RoundsDeclared, document.Lineage.RoundLimit, document.Lineage.RootRun, document.Lineage.RootReviewTask, strings.Join(document.Gate.Commands, "; "), document.Gate.Timeout); e != nil {
		return e
	}
	tasks := "fix1 -> review2"
	if document.Lineage.RoundsDeclared == 2 {
		tasks += " -> fix2 (gate) -> review3"
	}
	if _, e = fmt.Fprintf(c.stdout, "  tasks: %s\n", tasks); e != nil {
		return e
	}
	if document.Submission.Matrix != nil {
		matrix := *document.Submission.Matrix
		if _, e = fmt.Fprintf(c.stdout, "accepted: the campaign was accepted and its run is queued (accepted_waiting).\nIt is waiting for %s, and starts on its own once that clears.\nReadiness at submission:\n", campaignWaitingFor(matrix)); e != nil {
			return e
		}
		if e = renderCampaignCheck(c.stdout, campaignCheck{SchemaVersion: campaignCheckSchemaVersion, Source: target, Name: resolved.Options.Name, Digest: bundle.ContentDigest, Matrix: matrix}); e != nil {
			return e
		}
	}
	if _, e = fmt.Fprintf(c.stdout, "submission %s: workflow=%s run=%s state=%s replay=%t digest=%s\n", document.Submission.Key, document.Submission.WorkflowID, document.RunID, document.Submission.State, document.Replay, document.Submission.Digest); e != nil {
		return e
	}
	wake := startedRunWake{Run: document.RunID, Notify: document.Submission.Notify}
	if document.Replay {
		wake.Progress = replayWake.Progress
		wake.ProgressUnavailable = replayWake.ProgressUnavailable
	}
	renderRunProgress(c.stdout, wake)
	// Submit records notification disposition in the receipt; rendering the
	// same helpers preserves its wake instructions without submitting twice.
	renderWake(c.stdout, wake)
	_, e = fmt.Fprintf(c.stdout, "next:\n  t3-steward campaign show %s\n  t3-steward task result %s\n", document.RunID, document.RunID)
	return e
}
