package wait

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A wake summary is the answer a woken lead would otherwise collect with
// follow-up reads: for a node wait, each task's state, review verdict, gate
// result and head; for a GitHub wait, the annotation counts and groups. It is
// built once, on the delivering host, before the wake's payload is frozen, and
// is rendered into the trailer (one headline), the prose (a table) and the
// versioned JSON document "wait summary --json" prints.
//
// Everything a summary carries that came from an agent or a remote is either
// matched by a fixed pattern (verdict tokens, gate lines, SHAs, enum states,
// names from the manifest) or quoted with annotationQuote; nothing else reaches
// the trailer.

// WakeSummarySchema versions the summary document.
const WakeSummarySchema = "t3-steward.wake-summary/v1"

// wakeGuidance is the one line every interactive wake ends with. The fuller
// "inspect before acting" guidance lives in the t3-wait skill.
const wakeGuidance = "A met wait is not success: inspect every wait outcome and the actual result or review verdict (exit 0 does not establish task success or review ACCEPT). Cancellation and pause instructions take precedence; see the t3-wait skill."

// nodeSummaryTimeout bounds one summary build, whatever the source does. It is
// a variable so a test can shorten it.
var nodeSummaryTimeout = 10 * time.Second

const (
	summaryOpensPerTask = 3
	summaryOpensPerWake = 60
	// review.md, verdict.json and campaign commit records: the first 4 KiB.
	summaryHeadBytes = 4 << 10
	// gate.log: the last 4 KiB, reached by reading at most the artifact's size.
	summaryTailBytes = 4 << 10
	// A gate.log larger than this is not read at all.
	summaryGateMaxBytes = 64 << 20
	// A bundle: the first 8 KiB, which holds its header.
	summaryBundleBytes = 8 << 10
	summaryTableRows   = 30
	summaryGroupRows   = 60
	summaryMaxTasks    = 200
	summaryHeadlineMax = 200
	summaryFailureClip = 160
)

// WakeSummary is one wake's summary, for both kinds that have one.
type WakeSummary struct {
	Schema   string `json:"schema"`
	Kind     string `json:"kind"`
	Outcome  string `json:"outcome,omitempty"`
	Headline string `json:"headline,omitempty"`
	// Verdict and Head are the trailer pairs of a node summary, when known.
	Verdict string `json:"verdict,omitempty"`
	Head    string `json:"head,omitempty"`

	// Node waits.
	Run      string            `json:"run,omitempty"`
	Workflow string            `json:"workflow,omitempty"`
	Progress string            `json:"progress,omitempty"`
	Tasks    []WakeSummaryTask `json:"tasks,omitempty"`
	// Truncated counts the tasks left out of Tasks.
	Truncated int `json:"truncated,omitempty"`
	// Unavailable is the fixed category word of a summary that could not be
	// built; the wake then carries only the base trailer pairs.
	Unavailable string `json:"unavailable,omitempty"`

	// GitHub waits.
	Target     string             `json:"target,omitempty"`
	Conclusion string             `json:"conclusion,omitempty"`
	State      string             `json:"state,omitempty"`
	Counts     *WakeSummaryCounts `json:"counts,omitempty"`
	Groups     []WakeSummaryGroup `json:"groups,omitempty"`
	Noise      []WakeSummaryNoise `json:"noise,omitempty"`

	// sink says a node summary is of a whole run rather than one task.
	sink bool
}

// WakeSummaryTask is one task row, with explicit statuses for its cells.
type WakeSummaryTask struct {
	Task          string `json:"task"`
	TaskID        string `json:"taskId,omitempty"`
	Progress      string `json:"progress"`
	Failure       string `json:"failure,omitempty"`
	Verdict       string `json:"verdict,omitempty"`
	VerdictStatus string `json:"verdictStatus"`
	HeadStatus    string `json:"headStatus"`
	GateStatus    string `json:"gateStatus"`
	VerdictSource string `json:"verdictSource,omitempty"`
	Gate          string `json:"gate,omitempty"`
	Head          string `json:"head,omitempty"`
	HeadSource    string `json:"headSource,omitempty"`
	Result        string `json:"result,omitempty"`
}

// WakeSummaryCounts are the annotation counts by level; null is unknown,
// which is never the same as zero.
type WakeSummaryCounts struct {
	Failure *int `json:"failure"`
	Warning *int `json:"warning"`
	Notice  *int `json:"notice"`
	Unknown *int `json:"unknown"`
}

// WakeSummaryGroup is one distinct annotation and how often it was seen.
type WakeSummaryGroup struct {
	Level   string   `json:"level"`
	Count   int      `json:"count"`
	Path    string   `json:"path"`
	Start   int      `json:"start"`
	End     int      `json:"end"`
	Title   string   `json:"title"`
	Message string   `json:"message"`
	Checks  []string `json:"checks"`
}

// WakeSummaryNoise is one known-noise class: its records are counted, not
// shown.
type WakeSummaryNoise struct {
	Class  string `json:"class"`
	Level  string `json:"level"`
	Count  int    `json:"count"`
	Checks int    `json:"checks"`
}

// noiseWarnings is how many warning-level records were known noise.
func (s WakeSummary) noiseWarnings() int {
	n := 0
	for _, noise := range s.Noise {
		if noise.Level == "warning" {
			n += noise.Count
		}
	}
	return n
}

// NodeSummarySource is the coordinator as the summary builder reads it: one
// query for the run, one open per artifact. The steward wires it to the same
// transport "task result" uses.
type NodeSummarySource interface {
	SummaryRun(ctx context.Context, runID string) (SummaryRun, error)
	OpenSummaryArtifact(ctx context.Context, artifactID string) (io.ReadCloser, error)
}

// SummaryRun is a run's tasks in manifest order.
type SummaryRun struct {
	ID       string
	Workflow string
	Progress domain.ProgressState
	Tasks    []SummaryTask
}

// SummaryTask is one task with its latest attempt and that attempt's retained
// outputs.
type SummaryTask struct {
	ID   string
	Name string
	Sink bool
	// Attempt is the latest attempt, empty when the task has none.
	Attempt  string
	Progress domain.ProgressState
	Failure  string
	// Outputs are the task's declared outputs, in manifest order.
	Outputs       []domain.ArtifactDeclaration
	Artifacts     []SummaryArtifact
	ReviewOutput  *domain.ReviewOutput
	ReviewVerdict *domain.ReviewVerdict
}

// SummaryArtifact is one retained artifact's metadata.
type SummaryArtifact struct {
	ID   string
	Name string
	Kind domain.ArtifactKind
	Size int64
}

// SummaryError carries the fixed category a source reports a failure as. The
// wake prints the category and never the error's text.
type SummaryError struct {
	Category string
}

func (e SummaryError) Error() string { return "wake summary unavailable: " + e.Category }

var summaryCategories = map[string]bool{
	"no-transport": true, "authentication": true, "unavailable": true, "timeout": true,
	"protocol": true, "query-refused": true, "query-failed": true, "no-workflow": true,
	"run-mismatch": true, "unsafe-identity": true,
}

// summaryCategory turns a failure into its category word.
func summaryCategory(ctx context.Context, err error) string {
	var coded SummaryError
	switch {
	case errors.As(err, &coded) && summaryCategories[coded.Category]:
		return coded.Category
	case ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "query-failed"
	}
}

var (
	summaryVerdictLine = regexp.MustCompile(`^VERDICT: ([A-Z_]+)$`)
	summaryGateLine    = regexp.MustCompile(`^RESULT EXIT (-?[0-9]{1,10})$`)
	summarySHA         = regexp.MustCompile(`^[0-9a-f]{40}$`)
	summaryName        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	summaryOutputName  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,63}$`)
	summaryToken       = regexp.MustCompile(`^[a-z][a-z-]{0,31}$`)
)

// summaryVerdicts is the allow-list of review verdict tokens.
var summaryVerdicts = map[string]bool{"ACCEPT": true, "CHANGES_REQUESTED": true, "REJECT": true}

const (
	verdictUnrecognized = "unrecognized"
	gateNoResult        = "gate.log: no RESULT line"
	summaryReadTimeout  = 3 * time.Second
)

// parseVerdictLine reads the verdict from the first line of a review.md.
func parseVerdictLine(data []byte) string {
	line, _, _ := bytes.Cut(data, []byte("\n"))
	line = bytes.TrimSuffix(line, []byte("\r"))
	match := summaryVerdictLine.FindSubmatch(line)
	if match == nil || !summaryVerdicts[string(match[1])] {
		return verdictUnrecognized
	}
	return string(match[1])
}

// parseVerdictJSON reads the verdict field of a verdict.json.
func parseVerdictJSON(data []byte) string {
	var document struct {
		Verdict string `json:"verdict"`
	}
	if json.Unmarshal(data, &document) != nil {
		return verdictUnrecognized
	}
	switch document.Verdict {
	case "accept":
		return "ACCEPT"
	case "changes-requested":
		return "CHANGES_REQUESTED"
	}
	return verdictUnrecognized
}

// parseGateTail reads the gate result from the tail of a gate.log: its last
// non-empty line, when that line is a RESULT line. truncated says the tail is
// not the whole file, so a tail without a line break holds no whole line.
func parseGateTail(tail []byte, truncated bool) string {
	lines := bytes.Split(tail, []byte("\n"))
	for i := len(lines) - 1; i >= 0; i-- {
		line := bytes.TrimSuffix(lines[i], []byte("\r"))
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if i == 0 && truncated {
			return gateNoResult
		}
		match := summaryGateLine.FindSubmatch(line)
		if match == nil {
			return gateNoResult
		}
		code, err := strconv.ParseInt(string(match[1]), 10, 32)
		if err != nil {
			return gateNoResult
		}
		return "RESULT EXIT " + strconv.FormatInt(code, 10)
	}
	return gateNoResult
}

// parseBundleHead reads the first refs/heads/ tip from a git bundle header:
// the signature line, optional @capability lines (v3), -<sha> prerequisites,
// then <sha> <refname> lines, ended by an empty line. Git is never run on it,
// and a header that is malformed anywhere yields no head.
func parseBundleHead(data []byte) string {
	lines := strings.Split(string(data), "\n")
	if len(lines) < 2 {
		return ""
	}
	v3 := lines[0] == "# v3 git bundle"
	if lines[0] != "# v2 git bundle" && !v3 {
		return ""
	}
	head := ""
	// The last element follows the last line break: it is either partial or
	// empty, and in neither case a header line.
	for _, line := range lines[1 : len(lines)-1] {
		switch {
		case line == "":
			return head
		case v3 && strings.HasPrefix(line, "@"):
			continue
		case strings.HasPrefix(line, "-"):
			sha, _, _ := strings.Cut(line[1:], " ")
			if !summarySHA.MatchString(sha) {
				return ""
			}
			continue
		}
		sha, ref, ok := strings.Cut(line, " ")
		if !ok || !summarySHA.MatchString(sha) || ref == "" {
			return ""
		}
		if head == "" && strings.HasPrefix(ref, "refs/heads/") {
			head = sha
		}
	}
	return ""
}

// parseCommitRecord reads the commit of a campaign-commit/v1 provenance
// record, which is the retained output of a declared commit.
func parseCommitRecord(data []byte) string {
	var record struct {
		Version string `json:"version"`
		Commit  string `json:"commit"`
	}
	if json.Unmarshal(data, &record) != nil || record.Version != "campaign-commit/v1" || !summarySHA.MatchString(record.Commit) {
		return ""
	}
	return record.Commit
}

// callWithin runs one source call and returns when it answers or the context
// ends, whichever is first, so a source that ignores its context cannot hold
// delivery past the bound. The abandoned call finishes on its own.
func callWithin[T any](ctx context.Context, call func() (T, error)) (T, error) {
	type answer struct {
		value T
		err   error
	}
	done := make(chan answer, 1)
	go func() {
		value, err := call()
		done <- answer{value, err}
	}()
	select {
	case a := <-done:
		return a.value, a.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// summaryBudget is the artifact-open allowance of one wake.
type summaryBudget struct {
	opens int
}

type summaryRead int

const (
	readHead summaryRead = iota
	readTail
	readBundle
)

type summaryBytes struct {
	data      []byte
	truncated bool
}

// read opens one artifact within the per-task and per-wake allowances and
// returns its bounded bytes, its explicit read status.
func (b *summaryBudget) read(ctx context.Context, source NodeSummarySource, opens *int, artifact SummaryArtifact, mode summaryRead) (summaryBytes, string) {
	if *opens >= summaryOpensPerTask || b.opens <= 0 || ctx.Err() != nil {
		return summaryBytes{}, "not-read"
	}
	if mode == readTail && (artifact.Size < 0 || artifact.Size > summaryGateMaxBytes) {
		return summaryBytes{}, "unreadable"
	}
	*opens++
	b.opens--
	readCtx, cancel := context.WithTimeout(ctx, summaryReadTimeout)
	defer cancel()
	result, err := callWithin(readCtx, func() (summaryBytes, error) {
		body, err := source.OpenSummaryArtifact(readCtx, artifact.ID)
		if err != nil {
			return summaryBytes{}, err
		}
		defer body.Close()
		switch mode {
		case readTail:
			return readSummaryTail(io.LimitReader(body, artifact.Size))
		default:
			limit := int64(summaryHeadBytes)
			extra := int64(1) // JSON records need a byte to detect truncation.
			if mode == readBundle {
				limit = summaryBundleBytes
				extra = 0 // A bundle header needs no truncation probe beyond its allowance.
			}
			data, err := io.ReadAll(io.LimitReader(body, limit+extra))
			if err != nil {
				return summaryBytes{}, err
			}
			if int64(len(data)) > limit {
				return summaryBytes{data: data[:limit], truncated: true}, nil
			}
			return summaryBytes{data: data}, nil
		}
	})
	if err != nil {
		return summaryBytes{}, "unreadable"
	}
	return result, "known"
}

// readSummaryTail keeps the last summaryTailBytes of a stream.
func readSummaryTail(r io.Reader) (summaryBytes, error) {
	var tail []byte
	chunk := make([]byte, 32<<10)
	total := 0
	for {
		n, err := r.Read(chunk)
		total += n
		tail = append(tail, chunk[:n]...)
		if len(tail) > summaryTailBytes {
			tail = append(tail[:0], tail[len(tail)-summaryTailBytes:]...)
		}
		if errors.Is(err, io.EOF) {
			return summaryBytes{data: tail, truncated: total > len(tail)}, nil
		}
		if err != nil {
			return summaryBytes{}, err
		}
	}
}

// artifact finds a retained output by its base name.
func (t SummaryTask) artifact(name string) (SummaryArtifact, bool) {
	for _, a := range t.Artifacts {
		if a.Kind == domain.ArtifactOutput && (a.Name == name || path.Base(a.Name) == name) {
			return a, true
		}
	}
	return SummaryArtifact{}, false
}

// declares reports whether the task declares an output of this base name.
func (t SummaryTask) declares(name string) bool {
	for _, output := range t.Outputs {
		if output.Name == name || path.Base(output.Name) == name {
			return true
		}
	}
	return false
}

// ran reports whether the task's latest attempt executed, so that a declared
// output that is missing is unreadable rather than never produced.
func (t SummaryTask) ran() bool {
	return t.Attempt != "" && t.Progress != domain.ProgressSkipped
}

// summaryTaskName is the task's manifest name when it is a safe one.
func summaryTaskName(t SummaryTask) string {
	switch {
	case summaryName.MatchString(t.Name):
		return t.Name
	case summaryName.MatchString(t.ID):
		return t.ID
	}
	return "unnamed task"
}

// summaryTaskState is the latest attempt's progress, skipped, or not started.
func summaryTaskState(t SummaryTask) string {
	if t.Attempt == "" && t.Progress != domain.ProgressSkipped {
		return "not started"
	}
	if summaryToken.MatchString(string(t.Progress)) {
		return string(t.Progress)
	}
	return "unknown state"
}

// summaryDisplayOutput is an output name safe to print as a head source.
func summaryDisplayOutput(name string) string {
	if summaryOutputName.MatchString(name) && !strings.Contains(name, "..") {
		return name
	}
	return "output"
}

// summaryText makes free text valid, bounded UTF-8 for the JSON document.
func summaryText(s string, n int) string {
	return strings.ToValidUTF8(annotationClip(s, n), "�")
}

// summarizeTask fills one row from the task's retained outputs.
func (b *summaryBudget) summarizeTask(ctx context.Context, source NodeSummarySource, run string, t SummaryTask) WakeSummaryTask {
	row := WakeSummaryTask{Task: summaryTaskName(t), Progress: summaryTaskState(t), Failure: summaryText(t.Failure, 1000)}
	if summaryName.MatchString(t.ID) {
		row.TaskID = t.ID
	}
	if row.Task != "unnamed task" {
		row.Result = "t3-steward task result " + run + "/" + row.Task
	}
	opens := 0
	cell := func(s summarySource, declared bool) summaryCell {
		artifact, retained := t.artifact(s.name)
		if !retained {
			if !declared && !t.declares(s.name) {
				return summaryCell{status: "none"}
			}
			if !t.ran() {
				return summaryCell{status: "not-run"}
			}
			return summaryCell{status: "missing"}
		}
		read, status := b.read(ctx, source, &opens, artifact, s.mode)
		if status != "known" {
			return summaryCell{status: status}
		}
		value := s.parse(read)
		if value == "" || value == verdictUnrecognized || value == gateNoResult {
			return summaryCell{value: value, status: "unrecognized"}
		}
		return summaryCell{value: value, status: "known"}
	}
	firstAnswer := func(sources []summarySource, declared, head bool) (summaryCell, string) {
		first := summaryCell{status: "none"}
		label := ""
		for _, s := range sources {
			found := cell(s, declared)
			if found.status == "none" {
				continue
			}
			if found.status == "known" || (found.status == "unrecognized" && !head) {
				return found, s.label
			}
			if label == "" {
				first, label = found, s.label
			}
		}
		return first, label
	}
	var verdict summaryCell
	if t.ReviewVerdict != nil {
		verdict = summaryCell{value: parseVerdictJSON(mustVerdictJSON(t.ReviewVerdict.Verdict)), status: "known"}
		if verdict.value == verdictUnrecognized {
			verdict.status = "unrecognized"
		}
		row.VerdictSource = "review_output"
	} else {
		sources := []summarySource{
			{name: "review.md", label: "review.md", mode: readHead, parse: func(r summaryBytes) string { return parseVerdictLine(r.data) }},
			{name: "verdict.json", label: "verdict.json", mode: readHead, parse: func(r summaryBytes) string {
				if r.truncated {
					return verdictUnrecognized
				}
				return parseVerdictJSON(r.data)
			}},
		}
		if t.ReviewOutput != nil {
			name := t.ReviewOutput.Path()
			parse := sources[0].parse
			if t.ReviewOutput.Verdict != "" {
				parse = sources[1].parse
			}
			sources = []summarySource{{name: name, label: summaryDisplayOutput(name), mode: readHead, parse: parse}}
		}
		verdict, row.VerdictSource = firstAnswer(sources, t.ReviewOutput != nil, false)
	}
	row.VerdictStatus = verdict.status
	row.Verdict = verdict.text("verdict", "")
	gate := cell(summarySource{name: "gate.log", mode: readTail, parse: func(r summaryBytes) string { return parseGateTail(r.data, r.truncated) }}, false)
	row.GateStatus, row.Gate = gate.status, gate.text("gate", "")
	var heads []summarySource
	for _, output := range t.Outputs {
		if output.Commit != nil {
			heads = append(heads, summarySource{name: output.Name, label: "commit " + summaryDisplayOutput(output.Name), mode: readHead, parse: func(r summaryBytes) string {
				if r.truncated {
					return ""
				}
				return parseCommitRecord(r.data)
			}})
			break
		}
	}
	for _, name := range summaryBundleNames(t) {
		heads = append(heads, summarySource{name: name, label: summaryDisplayOutput(path.Base(name)), mode: readBundle, parse: func(r summaryBytes) string { return parseBundleHead(r.data) }})
	}
	head, label := firstAnswer(heads, false, true)
	row.HeadSource, row.HeadStatus = label, head.status
	row.Head = head.text("head", label)
	return row
}

// summarySource is one entry of an ordered source list for a cell: the output
// it reads, the label shown as the cell's source, and how it is read and
// parsed. A head parse that yields an empty string is unrecognized.
type summarySource struct {
	name, label string
	mode        summaryRead
	parse       func(summaryBytes) string
}

// summaryBundleNames are the task's bundle outputs: declared ones in manifest
// order, then retained ones it did not declare.
func summaryBundleNames(t SummaryTask) []string {
	var names []string
	seen := map[string]bool{}
	add := func(name string) {
		base := path.Base(name)
		if strings.HasSuffix(base, ".bundle") && !seen[base] {
			seen[base] = true
			names = append(names, base)
		}
	}
	for _, output := range t.Outputs {
		if output.Commit == nil {
			add(output.Name)
		}
	}
	for _, a := range t.Artifacts {
		if a.Kind == domain.ArtifactOutput {
			add(a.Name)
		}
	}
	return names
}

// NodeWaitHasSummary reports whether a node wake gets a summary: a settled
// node (not quota) wait whose observation is terminal.
func NodeWaitHasSummary(w domain.NodeWait) bool {
	return w.Request.Quota == nil && w.Observation != nil && w.Observation.Progress.Terminal()
}

// BuildNodeSummary builds the summary of one node wake within the bounds.
// It never fails: a summary that cannot be built says why in Unavailable.
func BuildNodeSummary(ctx context.Context, source NodeSummarySource, w domain.NodeWait) WakeSummary {
	ctx, cancel := context.WithTimeout(ctx, nodeSummaryTimeout)
	defer cancel()
	return buildNodeSummary(ctx, source, w, &summaryBudget{opens: summaryOpensPerWake})
}

// buildNodeSummaries builds the summaries of one wake's members under one
// bound and one artifact allowance; a member that gets none is nil.
func buildNodeSummaries(ctx context.Context, source NodeSummarySource, members []domain.NodeWait) []*WakeSummary {
	out := make([]*WakeSummary, len(members))
	if source == nil {
		return out
	}
	ctx, cancel := context.WithTimeout(ctx, nodeSummaryTimeout)
	defer cancel()
	budget := &summaryBudget{opens: summaryOpensPerWake}
	for i, member := range members {
		if !NodeWaitHasSummary(member) {
			continue
		}
		summary := buildNodeSummary(ctx, source, member, budget)
		out[i] = &summary
	}
	return out
}

func buildNodeSummary(ctx context.Context, source NodeSummarySource, w domain.NodeWait, budget *summaryBudget) WakeSummary {
	s := WakeSummary{Schema: WakeSummarySchema, Kind: string(domain.WaitKindNode)}
	if w.Observation == nil {
		s.Unavailable = "no-workflow"
		return s
	}
	observation := *w.Observation
	s.Outcome = string(domain.NodeObservationOutcome(observation))
	if summaryToken.MatchString(string(observation.Progress)) {
		s.Progress = string(observation.Progress)
	}
	run := observation.Target.RunID
	if !commandSafeRunID(run) {
		s.Unavailable = "unsafe-identity"
		return s
	}
	s.Run = run
	if source == nil {
		s.Unavailable = "no-transport"
		return s
	}
	detail, err := callWithin(ctx, func() (SummaryRun, error) { return source.SummaryRun(ctx, run) })
	if err != nil {
		s.Unavailable = summaryCategory(ctx, err)
		return s
	}
	if detail.ID != run {
		s.Unavailable = "run-mismatch"
		return s
	}
	if summaryName.MatchString(detail.Workflow) {
		s.Workflow = detail.Workflow
	}
	sink := observation.Target.TaskID == domain.SinkTaskName || w.Request.Target.TaskID == domain.SinkTaskName
	for _, task := range detail.Tasks {
		if task.Sink && task.ID == observation.Target.TaskID {
			sink = true
		}
	}
	var tasks []SummaryTask
	for _, task := range detail.Tasks {
		if task.Sink || task.Name == domain.SinkTaskName {
			continue
		}
		if sink || task.ID == observation.Target.TaskID || task.Name == observation.Target.TaskID {
			tasks = append(tasks, task)
		}
	}
	if !sink && len(tasks) != 1 {
		s.Unavailable = "run-mismatch"
		return s
	}
	for i, task := range tasks {
		if i == summaryMaxTasks {
			s.Truncated = len(tasks) - summaryMaxTasks
			break
		}
		s.Tasks = append(s.Tasks, budget.summarizeTask(ctx, source, run, task))
	}
	s.sink = sink
	s.Headline, s.Verdict, s.Head = s.nodeHeadline()
	return s
}

// lastKnown finds the last task in manifest order with a value in the given
// cell. A task whose cell is unreadable, or tasks left out, make the answer
// unknown: an earlier value never stands in for a later one.
func (s WakeSummary) lastKnown(cell func(WakeSummaryTask) summaryCell) (WakeSummaryTask, bool) {
	if s.Truncated > 0 {
		return WakeSummaryTask{}, false
	}
	for i := len(s.Tasks) - 1; i >= 0; i-- {
		switch cell(s.Tasks[i]).status {
		case "none", "not-run":
			continue
		case "known", "unrecognized":
		default:
			return WakeSummaryTask{}, false
		}
		return s.Tasks[i], true
	}
	return WakeSummaryTask{}, false
}

// nodeHeadline is the trailer's summary value and the verdict and head pairs.
func (s WakeSummary) nodeHeadline() (string, string, string) {
	var segments []string
	verdict, head := "", ""
	// The segment each trailer pair is shown in, so a pair never outlives its
	// segment when the headline is shortened.
	verdictAt, headAt := -1, -1
	if s.sink {
		status := s.Progress
		switch domain.ProgressState(s.Progress) {
		case domain.ProgressSucceeded:
			status = "SUCCEEDED"
		case domain.ProgressCancelled:
			status = "CANCELLED"
		case domain.ProgressFailed:
			status = "FAILED"
			if failed, ok := s.firstFailed(); ok {
				status += " at " + failed.Task
				if strings.HasPrefix(failed.Gate, "RESULT EXIT ") {
					status += " (" + failed.Gate + ")"
				}
			}
		}
		if s.Workflow != "" {
			status = s.Workflow + ": " + status
		}
		segments = append(segments, status)
		if review, ok := s.lastKnown(func(t WakeSummaryTask) summaryCell { return summaryCell{value: t.Verdict, status: t.VerdictStatus} }); ok && review.Task != "unnamed task" {
			verdictAt = len(segments)
			segments = append(segments, review.Task+" "+review.Verdict)
			verdict = review.Verdict
		}
		if last, ok := s.lastKnown(func(t WakeSummaryTask) summaryCell { return summaryCell{value: t.Head, status: t.HeadStatus} }); ok {
			if summarySHA.MatchString(last.Head) {
				head = last.Head
			}
		}
	} else if len(s.Tasks) == 1 {
		task := s.Tasks[0]
		name := task.Task
		if s.Workflow != "" {
			name = s.Workflow + "/" + name
		}
		segments = append(segments, name+": "+task.Progress)
		if summaryCellAnswers(task.VerdictStatus) {
			verdictAt = len(segments)
			segments = append(segments, task.Verdict)
			verdict = task.Verdict
		}
		if summarySHA.MatchString(task.Head) {
			head = task.Head
		}
	}
	if summarySHA.MatchString(head) {
		headAt = len(segments)
		segments = append(segments, "head "+head[:7])
	}
	headline := strings.Join(segments, " | ")
	for len(headline) > summaryHeadlineMax && len(segments) > 1 {
		segments = segments[:len(segments)-1]
		headline = strings.Join(segments, " | ")
	}
	if verdictAt >= len(segments) {
		verdict = ""
	}
	if headAt >= len(segments) {
		head = ""
	}
	if verdict != "" {
		verdict = strings.ToLower(strings.ReplaceAll(verdict, "_", "-"))
	}
	return annotationClip(headline, summaryHeadlineMax), verdict, head
}

// firstFailed is the first failed task in manifest order.
func (s WakeSummary) firstFailed() (WakeSummaryTask, bool) {
	for _, task := range s.Tasks {
		if task.Progress == string(domain.ProgressFailed) && task.Task != "unnamed task" {
			return task, true
		}
	}
	return WakeSummaryTask{}, false
}

// summaryTrailerFields are the pairs a summary adds to a trailer.
func summaryTrailerFields(s *WakeSummary) []Field {
	if s == nil || s.Unavailable != "" || s.Headline == "" {
		return nil
	}
	return []Field{F("summary", s.Headline), F("verdict", s.Verdict), F("head", s.Head)}
}

// unavailableLine is the fixed line of a node wake whose summary could not be
// built.
func (s WakeSummary) unavailableLine() string {
	if commandSafeRunID(s.Run) {
		return "Summary unavailable (" + s.Unavailable + "); collect with t3-steward task result " + s.Run + "."
	}
	return "Summary unavailable (" + s.Unavailable + ")."
}

// nodeProse is the human part of a summarized node wake: one headline
// sentence, the task table, and where the outputs are. rows is the number of
// table rows this wake may still print, shared by the members of a group.
func (s WakeSummary) nodeProse(rows *int) string {
	sink := s.sink
	var b strings.Builder
	subject := "Run " + s.Run
	if s.Workflow != "" {
		subject = s.Workflow + " (" + s.Run + ")"
	}
	failure := ""
	if sink {
		b.WriteString(subject + " " + s.Progress)
		if failed, ok := s.firstFailed(); ok && s.Progress == string(domain.ProgressFailed) {
			b.WriteString(" at " + failed.Task)
			failure = failed.Failure
		}
	} else if len(s.Tasks) == 1 {
		task := s.Tasks[0]
		subject = task.Task + " (" + s.Run + ")"
		if s.Workflow != "" {
			subject = s.Workflow + "/" + subject
		}
		b.WriteString(subject + " " + task.Progress)
		failure = task.Failure
	}
	if failure != "" {
		b.WriteString(": " + annotationQuote(failure, summaryFailureClip))
	}
	b.WriteString(".\n")
	if *rows < 0 {
		*rows = 0
	}
	shown := len(s.Tasks)
	if shown > summaryTableRows {
		shown = summaryTableRows
	}
	if shown > *rows {
		shown = *rows
	}
	*rows -= shown
	if shown > 0 {
		b.WriteString(renderSummaryTable(s.Tasks[:shown]))
	}
	if more := len(s.Tasks) + s.Truncated - shown; more > 0 {
		fmt.Fprintf(&b, "and %d more tasks\n", more)
	}
	if sink {
		b.WriteString("Outputs: t3-steward task result " + s.Run + "[/<task>]\n")
	} else if len(s.Tasks) == 1 && s.Tasks[0].Result != "" {
		b.WriteString("Outputs: " + s.Tasks[0].Result + "\n")
	}
	return b.String()
}

// renderSummaryTable is the fixed-width task table. Cells carry only parsed
// values: states, verdict tokens, gate lines, short SHAs and output names.
func renderSummaryTable(tasks []WakeSummaryTask) string {
	rows := [][]string{{"task", "state", "verdict", "gate", "head"}}
	for _, task := range tasks {
		head := orDashCell(task.Head)
		if summarySHA.MatchString(task.Head) {
			head = task.Head[:7] + " (" + task.HeadSource + ")"
		}
		rows = append(rows, []string{task.Task, task.Progress, orDashCell(task.Verdict), orDashCell(task.Gate), head})
	}
	widths := make([]int, len(rows[0]))
	for _, row := range rows {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	var b strings.Builder
	for _, row := range rows {
		var line strings.Builder
		for i, cell := range row {
			line.WriteString(cell)
			if i < len(row)-1 {
				line.WriteString(strings.Repeat(" ", widths[i]-len(cell)+2))
			}
		}
		b.WriteString(strings.TrimRight(line.String(), " "))
		b.WriteString("\n")
	}
	return b.String()
}

func orDashCell(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

// Text renders a summary for "wait summary" without --json.
func (s WakeSummary) Text() string {
	if s.Unavailable != "" {
		return s.unavailableLine() + "\n"
	}
	var b strings.Builder
	b.WriteString(s.Headline + "\n")
	switch s.Kind {
	case string(domain.WaitKindNode):
		if len(s.Tasks) != 0 {
			b.WriteString(renderSummaryTable(s.Tasks))
		}
		if s.Truncated > 0 {
			fmt.Fprintf(&b, "and %d more tasks\n", s.Truncated)
		}
	case string(domain.WaitKindGitHub):
		for _, group := range s.Groups {
			fmt.Fprintf(&b, "%s x%d %s:%d-%d %s %s\n", annotationLevel(group.Level), group.Count, annotationQuote(group.Path, 120), group.Start, group.End,
				annotationQuote(group.Title, 80), annotationQuote(group.Message, 240))
		}
		for _, noise := range s.Noise {
			fmt.Fprintf(&b, "known noise (%s): %d x %s (%d checks)\n", noise.Level, noise.Count, noise.Class, noise.Checks)
		}
	}
	return b.String()
}
