package wait

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Bounds apply to transport as well as enumeration, including injected runners.
// Reserve the twentieth call for the final observation consistency check.
const (
	gitHubResponseBytes     = 256 << 10
	gitHubTotalBytes        = 2 << 20
	gitHubAnnotationCalls   = 20
	gitHubAnnotationPages   = 3
	gitHubAnnotationChecks  = 20
	gitHubAnnotationRecords = 150
	gitHubAnnotationOutput  = 4000
	// Distinct non-failure groups shown; every distinct failure is shown.
	gitHubAnnotationOthers = 8
	// Message clips: a failure is what the reader acts on, so it gets more.
	gitHubFailureMessage = 1000
	gitHubOtherMessage   = 240
)

var errGitHubResponseCap = errors.New("github response-cap")

// Private transport ceiling for the remaining enrichment byte budget.
type gitHubLimitKey struct{}

type gitHubBuffer struct {
	bytes.Buffer
	limit  int
	capped bool
}

func (b *gitHubBuffer) Write(p []byte) (int, error) {
	n := len(p)
	keep := b.limit - b.Len()
	if len(p) > keep {
		b.capped = true
		p = p[:keep]
	}
	_, _ = b.Buffer.Write(p)
	return n, nil
}

type annotationRollup struct {
	Type        string `json:"__typename"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	Conclusion  string `json:"conclusion"`
	State       string `json:"state"`
	DetailsURL  string `json:"detailsUrl"`
	StartedAt   string `json:"startedAt"`
	CompletedAt string `json:"completedAt"`
	Context     string `json:"context"`
	TargetURL   string `json:"targetUrl"`
}
type annotationSnapshot struct {
	ID         int64              `json:"databaseId"`
	Attempt    int                `json:"attempt"`
	Head       string             `json:"headSha"`
	PRHead     string             `json:"headRefOid"`
	URL        string             `json:"url"`
	Status     string             `json:"status"`
	Conclusion string             `json:"conclusion"`
	State      string             `json:"state"`
	Checks     []annotationRollup `json:"statusCheckRollup"`
}
type annotationCheck struct {
	ID          int64  `json:"id"`
	Head        string `json:"head_sha"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	Conclusion  string `json:"conclusion"`
	URL         string `json:"url"`
	DetailsURL  string `json:"details_url"`
	StartedAt   string `json:"started_at"`
	CompletedAt string `json:"completed_at"`
	Output      struct {
		Count *int `json:"annotations_count"`
	} `json:"output"`
}
type annotationRecord struct {
	Level   string `json:"annotation_level"`
	Path    string `json:"path"`
	Start   int    `json:"start_line"`
	End     int    `json:"end_line"`
	Title   string `json:"title"`
	Message string `json:"message"`
}
type annotationCollection struct {
	ctx                   context.Context
	run                   GitHubRunner
	dir, repo             string
	calls, bytes, records int
	problems              []string
	links                 []string
	// found are the well-formed records collected, in collection order, and
	// checkNames the bound checks they came from.
	found      []annotationFound
	checkNames []annotationCheckName
	counts     map[string]int
	checks     int
	invalid    bool
}

// annotationFound is one collected record and the check it was read from.
type annotationFound struct {
	check  int64
	name   string
	record annotationRecord
}

// annotationCheckName is one bound check.
type annotationCheckName struct {
	id   int64
	name string
}

func (c *annotationCollection) problem(s string) {
	for _, old := range c.problems {
		if old == s {
			return
		}
	}
	c.problems = append(c.problems, s)
}
func annotationError(ctx context.Context, err error) string {
	if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	if errors.Is(err, errGitHubResponseCap) {
		return "response-cap"
	}
	// Never expose stderr, which can contain credentials or hostile remote text.
	return "api-unavailable"
}
func (c *annotationCollection) fetch(args []string, final bool) (string, error) {
	limit := gitHubAnnotationCalls - 1
	if final {
		limit = gitHubAnnotationCalls
	}
	if c.calls >= limit {
		return "", errors.New("call-cap")
	}
	if c.ctx.Err() != nil {
		return "", errors.New("timeout")
	}
	remaining := gitHubTotalBytes - c.bytes
	if remaining <= 0 {
		return "", errors.New("byte-cap")
	}
	c.calls++
	s, err := c.run(context.WithValue(c.ctx, gitHubLimitKey{}, remaining), c.dir, args)
	if err != nil {
		charged := gitHubResponseBytes
		if charged > remaining {
			charged = remaining
		}
		c.bytes += charged
		if errors.Is(err, errGitHubResponseCap) && remaining < gitHubResponseBytes {
			c.bytes = gitHubTotalBytes
			return "", errors.New("byte-cap")
		}
		return "", errors.New(annotationError(c.ctx, err))
	}
	if len(s) > gitHubResponseBytes {
		// Injected runners may return more than the real transport retains.
		// Charge a ceiling, never the unbounded returned string length.
		c.bytes += min(remaining, gitHubResponseBytes)
		return "", errors.New("response-cap")
	}
	if len(s) > remaining {
		c.bytes = gitHubTotalBytes
		return "", errors.New("byte-cap")
	}
	c.bytes += len(s)
	return s, nil
}
func (c *annotationCollection) api(endpoint string, out any) error {
	s, err := c.fetch([]string{"api", "--method", "GET", "--hostname", "github.com", endpoint}, false)
	if err != nil {
		return err
	}
	if !utf8.ValidString(s) || len(strings.TrimSpace(s)) == 0 || strings.TrimSpace(s) == "null" {
		return errors.New("malformed")
	}
	if json.Unmarshal([]byte(s), out) != nil {
		return errors.New("malformed")
	}
	return nil
}

var annotationRepo = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
var annotationSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

func annotationURL(raw, host string) (*url.URL, bool) {
	u, err := url.Parse(raw)
	return u, err == nil && len(raw) <= 512 && u.Scheme == "https" && u.Host == host && u.User == nil && u.RawQuery == "" && u.Fragment == "" && u.RawPath == ""
}
func annotationRepository(t GitHubTarget, s annotationSnapshot) (string, error) {
	u, ok := annotationURL(s.URL, "github.com")
	if !ok {
		return "", errors.New("unsupported-host-or-url")
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	suffix := []string{"pull", t.ID}
	if t.Kind == "run" {
		suffix = []string{"actions", "runs", t.ID}
	}
	if len(parts) != 2+len(suffix) || !reflect.DeepEqual(parts[2:], suffix) {
		return "", errors.New("target-mismatch")
	}
	repo := parts[0] + "/" + parts[1]
	if parts[0] == "." || parts[0] == ".." || parts[1] == "." || parts[1] == ".." {
		return "", errors.New("repository-mismatch")
	}
	if !annotationRepo.MatchString(repo) || (t.Repo != "" && !strings.EqualFold(repo, t.Repo)) {
		return "", errors.New("repository-mismatch")
	}
	return repo, nil
}
func annotationCheckID(raw, repo string) (int64, bool) {
	u, ok := annotationURL(raw, "api.github.com")
	if !ok {
		return 0, false
	}
	prefix := "/repos/" + repo + "/check-runs/"
	if !strings.HasPrefix(u.Path, prefix) {
		return 0, false
	}
	id, err := strconv.ParseInt(strings.TrimPrefix(u.Path, prefix), 10, 64)
	return id, err == nil && id > 0 && u.Path == prefix+strconv.FormatInt(id, 10)
}
func (c *annotationCollection) checkValid(ch annotationCheck, head string) bool {
	if ch.ID <= 0 || ch.Head != head || ch.Output.Count == nil || *ch.Output.Count < 0 || *ch.Output.Count > 1000000 || ch.Name == "" {
		return false
	}
	if ch.URL != "" {
		id, ok := annotationCheckID(ch.URL, c.repo)
		if !ok || id != ch.ID {
			return false
		}
	}
	// Details links are printed only, never followed. Use the validated fixed
	// check URL when an application uses an external details URL.
	return true
}
func (c *annotationCollection) checkLink(ch annotationCheck) string {
	if u, ok := annotationURL(ch.DetailsURL, "github.com"); ok && strings.HasPrefix(u.Path, "/"+c.repo+"/") {
		return ch.DetailsURL
	}
	return "https://github.com/" + c.repo + "/runs/" + strconv.FormatInt(ch.ID, 10)
}
func (c *annotationCollection) runChecks(s annotationSnapshot) ([]annotationCheck, error) {
	var checks []annotationCheck
	seen := map[int64]bool{}
	total := -1
	observed := 0
	for page := 1; page <= gitHubAnnotationPages; page++ {
		var body struct {
			Total *int `json:"total_count"`
			Jobs  []struct {
				Run     int64  `json:"run_id"`
				Attempt int    `json:"run_attempt"`
				Head    string `json:"head_sha"`
				URL     string `json:"check_run_url"`
			} `json:"jobs"`
		}
		endpoint := fmt.Sprintf("repos/%s/actions/runs/%d/attempts/%d/jobs?per_page=50&page=%d", c.repo, s.ID, s.Attempt, page)
		if err := c.api(endpoint, &body); err != nil {
			return checks, err
		}
		if body.Total == nil || body.Jobs == nil || *body.Total < 0 || len(body.Jobs) > 50 || (total >= 0 && total != *body.Total) {
			return checks, errors.New("malformed")
		}
		total = *body.Total
		for _, job := range body.Jobs {
			observed++
			// The attempt is pinned by the endpoint; older GHES/API schemas omit run_attempt.
			if job.Run != s.ID || job.Head != s.Head || (job.Attempt != 0 && job.Attempt != s.Attempt) {
				return nil, errors.New("provenance-mismatch")
			}
			id, ok := annotationCheckID(job.URL, c.repo)
			if !ok || seen[id] {
				return nil, errors.New("check-binding-mismatch")
			}
			seen[id] = true
			if len(checks) >= gitHubAnnotationChecks {
				c.problem("check-cap")
				continue
			}
			var check annotationCheck
			if err := c.api(fmt.Sprintf("repos/%s/check-runs/%d", c.repo, id), &check); err != nil {
				c.problem(err.Error())
				continue
			}
			if check.ID != id || !c.checkValid(check, s.Head) {
				return nil, errors.New("check-binding-mismatch")
			}
			checks = append(checks, check)
			// Spend the bounded budget on evidence for each bound check before
			// enumerating more metadata, so large runs retain useful diagnostics.
			c.collect(check)
			if c.invalid {
				return nil, errors.New("check-binding-mismatch")
			}
		}
		if observed == total {
			return checks, nil
		}
		if observed > total || len(body.Jobs) < 50 {
			return checks, errors.New("malformed")
		}
	}
	return checks, errors.New("page-cap")
}
func annotationMatches(r annotationRollup, ch annotationCheck) bool {
	return r.Type == "CheckRun" && r.Name == ch.Name && r.DetailsURL == ch.DetailsURL &&
		strings.EqualFold(r.Status, ch.Status) && strings.EqualFold(r.Conclusion, ch.Conclusion) &&
		r.StartedAt != "" && r.CompletedAt != "" && r.StartedAt == ch.StartedAt && r.CompletedAt == ch.CompletedAt
}
func (c *annotationCollection) prChecks(s annotationSnapshot) ([]annotationCheck, error) {
	if len(s.Checks) > 100 {
		return nil, errors.New("check-cap")
	}
	if len(s.Checks) == 0 {
		return nil, errors.New("missing-evaluated-checks")
	}
	statusOnly := true
	for _, check := range s.Checks {
		if check.Type != "StatusContext" {
			statusOnly = false
		}
	}
	if statusOnly {
		c.problem("status-context-no-annotations")
		return nil, nil
	}
	var all []annotationCheck
	total := -1
	seen := map[int64]bool{}
	for page := 1; page <= gitHubAnnotationPages; page++ {
		var body struct {
			Total  *int              `json:"total_count"`
			Checks []annotationCheck `json:"check_runs"`
		}
		if err := c.api(fmt.Sprintf("repos/%s/commits/%s/check-runs?filter=all&per_page=50&page=%d", c.repo, s.PRHead, page), &body); err != nil {
			return nil, err
		}
		if body.Total == nil || body.Checks == nil || *body.Total < 0 || len(body.Checks) > 50 || (total >= 0 && total != *body.Total) {
			return nil, errors.New("malformed")
		}
		total = *body.Total
		for _, ch := range body.Checks {
			if seen[ch.ID] || !c.checkValid(ch, s.PRHead) {
				return nil, errors.New("check-binding-mismatch")
			}
			seen[ch.ID] = true
			all = append(all, ch)
		}
		if len(all) == total {
			break
		}
		if len(all) > total || len(body.Checks) < 50 {
			return nil, errors.New("malformed")
		}
		if page == gitHubAnnotationPages {
			return nil, errors.New("page-cap")
		}
	}
	var selected []annotationCheck
	used := map[int64]bool{}
	for _, r := range s.Checks {
		if r.Type == "StatusContext" {
			c.problem("status-context-no-annotations")
			continue
		}
		if r.Type != "CheckRun" {
			return nil, errors.New("unknown-check-kind")
		}
		if !strings.EqualFold(r.Status, "completed") {
			c.problem("nonterminal-check-no-annotations")
			continue
		}
		matches := []annotationCheck{}
		for _, ch := range all {
			if annotationMatches(r, ch) {
				matches = append(matches, ch)
			}
		}
		// Never guess between reruns or duplicate same-name application checks.
		if len(matches) != 1 || used[matches[0].ID] {
			return nil, errors.New("ambiguous-check-binding")
		}
		used[matches[0].ID] = true
		if len(selected) >= gitHubAnnotationChecks {
			c.problem("check-cap")
			continue
		}
		selected = append(selected, matches[0])
	}
	return selected, nil
}
func (c *annotationCollection) collect(ch annotationCheck) {
	defer func() {
		var final annotationCheck
		if err := c.api(fmt.Sprintf("repos/%s/check-runs/%d", c.repo, ch.ID), &final); err != nil {
			c.problem(err.Error())
			return
		}
		if !reflect.DeepEqual(ch, final) {
			c.invalid = true
		}
	}()
	c.checks++
	link := c.checkLink(ch)
	c.links = append(c.links, fmt.Sprintf("> check %d %s: %s", ch.ID, annotationQuote(ch.Name, 100), annotationQuote(link, 512)))
	c.checkNames = append(c.checkNames, annotationCheckName{id: ch.ID, name: ch.Name})
	expected := *ch.Output.Count
	observed := 0
	for page := 1; observed < expected && page <= gitHubAnnotationPages; page++ {
		if c.records >= gitHubAnnotationRecords {
			c.problem("record-cap")
			return
		}
		var records []annotationRecord
		endpoint := fmt.Sprintf("repos/%s/check-runs/%d/annotations?per_page=50&page=%d", c.repo, ch.ID, page)
		if err := c.api(endpoint, &records); err != nil {
			c.problem(err.Error())
			return
		}
		if records == nil || len(records) > 50 || observed+len(records) > expected {
			c.problem("annotation-count-mismatch")
			return
		}
		for _, a := range records {
			if c.records >= gitHubAnnotationRecords {
				c.problem("record-cap")
				return
			}
			c.records++
			observed++
			if a.Path == "" || a.Start <= 0 || a.End < a.Start || a.Message == "" {
				c.problem("malformed-annotation")
				continue
			}
			switch a.Level {
			case "warning", "failure", "notice":
				c.counts[a.Level]++
			default:
				c.counts["unknown"]++
				c.problem("unknown-severity")
			}
			c.found = append(c.found, annotationFound{check: ch.ID, name: ch.Name, record: a})
		}
		if observed < expected && len(records) < 50 {
			c.problem("annotation-count-mismatch")
			return
		}
	}
	if observed < expected {
		c.problem("page-cap")
	}
}
func annotationClip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
func annotationQuote(s string, n int) string {
	clipped := len(s) > n
	s = strings.ToValidUTF8(annotationClip(s, n), "�")
	if clipped {
		s += "…"
	}
	s = strconv.Quote(s)
	// A remote fence or trailer-looking token remains literal quoted data in
	// both the interactive code block and the task prompt.
	s = strings.NewReplacer("`", "\\u0060", "<", "\\u003c", ">", "\\u003e", "t3-steward-wait", "t3\\u002dsteward-wait").Replace(s)
	return s
}
func annotationOutput(s string) string {
	s = strings.ToValidUTF8(s, "�")
	if len(s) <= gitHubAnnotationOutput {
		return s
	}
	const caveat = "\nAdditional/capped display; see check/target links. Missing data is not zero warnings/errors."
	return annotationClip(s, gitHubAnnotationOutput-len(caveat)) + caveat
}

func (r *Runner) gitHubAnnotations(ctx context.Context, t GitHubTarget, dir string, reading GitHubReading) (output string, summary WakeSummary) {
	// All complete, partial and unavailable exits share the final ceiling.
	// Remote fields are quoted before this UTF8-safe display cap.
	defer func() { output = annotationOutput(output) }()
	provenance := ""
	unavailable := func(category string) (string, WakeSummary) {
		empty := annotationCollection{counts: map[string]int{}}
		return annotationQuote(reading.Reason, 400) + "\nAnnotations unavailable (" + annotationQuote(category, 240) + "); " + provenance + "; no clean result inferred.",
			empty.summary(t, reading.Fields, "unavailable")
	}
	var snapshot annotationSnapshot
	if len(reading.observation) > gitHubResponseBytes || json.Unmarshal(reading.observation, &snapshot) != nil {
		return unavailable("malformed")
	}
	repo, err := annotationRepository(t, snapshot)
	if err != nil {
		return unavailable(err.Error())
	}
	switch t.Kind {
	case "run":
		if snapshot.ID <= 0 || strconv.FormatInt(snapshot.ID, 10) != t.ID || snapshot.Attempt < 1 {
			return unavailable("missing-run-provenance")
		}
		if !annotationSHA.MatchString(snapshot.Head) {
			return unavailable("missing-head-provenance")
		}
		provenance = fmt.Sprintf("run=%d attempt=%d head=%s", snapshot.ID, snapshot.Attempt, snapshot.Head)
	case "pr":
		id, err := strconv.ParseInt(t.ID, 10, 64)
		if err != nil || id <= 0 || strconv.FormatInt(id, 10) != t.ID {
			return unavailable("missing-pr-provenance")
		}
		if !annotationSHA.MatchString(snapshot.PRHead) {
			return unavailable("missing-head-provenance")
		}
		provenance = fmt.Sprintf("PR %d head=%s", id, snapshot.PRHead)
	default:
		return unavailable("unsupported-target")
	}
	provenance += " in " + repo
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	run := r.GitHub
	if run == nil {
		run = ExecGitHub
	}
	c := annotationCollection{ctx: cctx, run: run, dir: dir, repo: repo, counts: map[string]int{}}
	var checks []annotationCheck
	if t.Kind == "run" {
		checks, err = c.runChecks(snapshot)
	} else {
		checks, err = c.prChecks(snapshot)
	}
	if err != nil {
		// Binding failures invalidate the entire collection; API/cap failures keep
		// only already bound checks and explicitly partial counts.
		if strings.Contains(err.Error(), "mismatch") || strings.Contains(err.Error(), "binding") {
			return unavailable(err.Error())
		}
		c.problem(err.Error())
	}
	if t.Kind == "pr" {
		for _, ch := range checks {
			c.collect(ch)
		}
	}
	if c.invalid {
		return unavailable("check-snapshot-changed")
	}
	// Detect head/rerun/check changes without changing the original verdict.
	// This shares the enrichment budget, and never runs on registration/replay.
	if err := c.consistent(t, snapshot); err != nil {
		return unavailable(err.Error())
	}
	state := "complete"
	if len(c.problems) > 0 {
		state = "partial"
		if c.checks == 0 {
			state = "unavailable"
		}
	}
	if state == "unavailable" {
		return unavailable(strings.Join(c.problems, ", "))
	}
	count := func(level string) string {
		if state != "complete" && c.counts[level] == 0 {
			return "unknown"
		}
		return strconv.Itoa(c.counts[level])
	}
	header := fmt.Sprintf("%s\nAnnotations %s; %s. Observed warning=%s failure=%s notice=%s unknown=%s; checks=%d records=%d.",
		annotationQuote(reading.Reason, 400), state, provenance, count("warning"), count("failure"), count("notice"), count("unknown"), c.checks, c.records)
	if state != "complete" {
		header += " Missing data is not zero warnings/errors: " + annotationQuote(strings.Join(c.problems, ", "), 240) + "."
	} else if c.records == 0 {
		header += " No annotations."
	}
	return c.detail(header, snapshot.URL), c.summary(t, reading.Fields, state)
}

// annotationNoise are the known-noise classes: warnings and notices every run
// of these workflows repeats and nobody acts on. A failure is never noise,
// whatever its message says. Suppression changes the display only; the
// counts and the completeness state are computed before it.
var annotationNoise = []struct {
	class string
	match func(lower string) bool
}{
	{"Node.js 20 deprecation", func(m string) bool { return strings.Contains(m, "node.js 20") && strings.Contains(m, "deprecat") }},
	{"cache restore", func(m string) bool {
		return strings.Contains(m, "failed to restore") || strings.Contains(m, "cache not found for input keys") || strings.Contains(m, "cache restore failed")
	}},
}

// annotationNoiseClass is the known-noise class of a record, or empty.
func annotationNoiseClass(r annotationRecord) string {
	if r.Level != "warning" && r.Level != "notice" {
		return ""
	}
	message := strings.ToLower(r.Message)
	for _, noise := range annotationNoise {
		if noise.match(message) {
			return noise.class
		}
	}
	return ""
}

// annotationGroup is every collected record with one (level, path, start,
// end, title, message), and the checks it came from.
type annotationGroup struct {
	record annotationRecord
	count  int
	checks []string
	seen   map[int64]bool
}

func (g *annotationGroup) add(f annotationFound) {
	g.count++
	if !g.seen[f.check] {
		g.seen[f.check] = true
		g.checks = append(g.checks, f.name)
	}
}

// line renders one group: level, count, the first three checks, location,
// title and message, every remote field quoted.
func (g *annotationGroup) line(messageClip int) string {
	level := annotationLevel(g.record.Level)
	names := make([]string, 0, 3)
	for i, name := range g.checks {
		if i == 3 {
			break
		}
		names = append(names, annotationQuote(name, 80))
	}
	checks := strings.Join(names, ", ")
	if more := len(g.checks) - len(names); more > 0 {
		checks += fmt.Sprintf(" +%d more", more)
	}
	return fmt.Sprintf("> %s x%d %s | %s:%d-%d | %s %s", level, g.count, checks,
		annotationQuote(g.record.Path, 120), g.record.Start, g.record.End, annotationQuote(g.record.Title, 80), annotationQuote(g.record.Message, messageClip))
}

// annotationLevel prints a level: a known one as is, anything else quoted.
func annotationLevel(level string) string {
	if level != "failure" && level != "warning" && level != "notice" {
		return annotationQuote(level, 30)
	}
	return level
}

// annotationNoiseTally is one known-noise class at one level.
type annotationNoiseTally struct {
	level, class string
	count        int
	checks       map[int64]bool
}

// grouped aggregates every collected record: distinct failures, other
// distinct groups, and known noise, each in first-seen order.
func (c *annotationCollection) grouped() (failures, others []*annotationGroup, noise []*annotationNoiseTally) {
	groups := map[annotationRecord]*annotationGroup{}
	tallies := map[[2]string]*annotationNoiseTally{}
	for _, f := range c.found {
		if class := annotationNoiseClass(f.record); class != "" {
			key := [2]string{f.record.Level, class}
			tally := tallies[key]
			if tally == nil {
				tally = &annotationNoiseTally{level: f.record.Level, class: class, checks: map[int64]bool{}}
				tallies[key] = tally
				noise = append(noise, tally)
			}
			tally.count++
			tally.checks[f.check] = true
			continue
		}
		group := groups[f.record]
		if group == nil {
			group = &annotationGroup{record: f.record, seen: map[int64]bool{}}
			groups[f.record] = group
			if f.record.Level == "failure" {
				failures = append(failures, group)
			} else {
				others = append(others, group)
			}
		}
		group.add(f)
	}
	return failures, others, noise
}

// noiseLines are one line per level, each naming every class at that level.
func noiseLines(noise []*annotationNoiseTally) []string {
	var lines []string
	for _, level := range []string{"warning", "notice"} {
		var parts []string
		for _, tally := range noise {
			if tally.level != level {
				continue
			}
			checks := fmt.Sprintf("%d checks", len(tally.checks))
			if len(tally.checks) == 1 {
				checks = "1 check"
			}
			parts = append(parts, fmt.Sprintf("%d x %s (%s)", tally.count, tally.class, checks))
		}
		if len(parts) != 0 {
			lines = append(lines, "Known noise ("+level+"): "+strings.Join(parts, ", ")+".")
		}
	}
	return lines
}

// checksLine is the per-check links collapsed into one line, keeping the run
// or pull request URL.
func (c *annotationCollection) checksLine(targetURL string) string {
	// Bounded by quoted length, which escaping can make several times the
	// name's, so the line always fits and always ends with the URL.
	const limit = 1200
	links := " (links: " + annotationQuote(targetURL, 512) + ")"
	line := "Checks: "
	shown := 0
	for _, check := range c.checkNames {
		part := fmt.Sprintf("%s %d", annotationQuote(check.name, 40), check.id)
		if shown > 0 {
			part = ", " + part
		}
		if len(line)+len(part) > limit {
			break
		}
		line += part
		shown++
	}
	if more := len(c.checkNames) - shown; more > 0 {
		line += fmt.Sprintf(" +%d more", more)
	}
	return line + links
}

// detail is the header followed by the links and the aggregated detail
// section. Links and the completeness statement precede details so output
// clipping never hides the access/cap caveat. Every distinct failure comes
// before any warning, so a warning never displaces one; known noise is one
// line; other groups are capped, with a count of the rest.
func (c *annotationCollection) detail(header, targetURL string) string {
	// The closing lines (the count of groups not shown and the count of lines
	// omitted) are written into the margin above the budget, so they always fit
	// under the output ceiling.
	budget := gitHubAnnotationOutput - 300
	// Room kept for the closing count of groups not shown.
	const reserve = 120
	failures, others, noise := c.grouped()
	var fixed []string
	for _, group := range failures {
		fixed = append(fixed, group.line(gitHubFailureMessage))
	}
	fixed = append(fixed, noiseLines(noise)...)
	var rest []string
	for i, group := range others {
		if i == gitHubAnnotationOthers {
			break
		}
		rest = append(rest, group.line(gitHubOtherMessage))
	}
	links := c.links
	total := len(header)
	for _, line := range append(append(append([]string(nil), links...), fixed...), rest...) {
		total += len(line) + 1
	}
	if total > budget-reserve && len(c.checkNames) != 0 {
		links = []string{c.checksLine(targetURL)}
	}
	var b strings.Builder
	b.WriteString(header)
	omitted := 0
	for _, line := range append(append([]string(nil), links...), fixed...) {
		if b.Len()+len(line)+1 > budget {
			omitted++
			continue
		}
		b.WriteByte('\n')
		b.WriteString(line)
	}
	shown := 0
	for _, line := range rest {
		if b.Len()+len(line)+1 > budget-reserve {
			break
		}
		b.WriteByte('\n')
		b.WriteString(line)
		shown++
	}
	if more := len(others) - shown; more > 0 {
		fmt.Fprintf(&b, "\nand %d more distinct warnings. Additional/capped display; see check/target links.", more)
	}
	if len(failures) == 0 && len(others) == 0 && len(noise) != 0 {
		fmt.Fprintf(&b, "\nNo other annotations among the %d collected records.", c.records)
	}
	if omitted > 0 {
		fmt.Fprintf(&b, "\nAdditional/capped display: %d summary lines omitted. See check/target links.", omitted)
	}
	return b.String()
}

var (
	summaryTargetID   = regexp.MustCompile(`^[0-9]{1,20}$`)
	summaryConclusion = regexp.MustCompile(`^[a-z_]{1,32}$`)
)

// summary is the collection as a wake summary: the counts exactly as the
// header states them, every non-noise group and the noise tallies.
func (c *annotationCollection) summary(t GitHubTarget, fields map[string]string, state string) WakeSummary {
	s := WakeSummary{Schema: WakeSummarySchema, Kind: string(domain.WaitKindGitHub), Target: summaryText(t.Ref(), 200), State: state, Counts: &WakeSummaryCounts{}}
	if summaryConclusion.MatchString(fields["conclusion"]) {
		s.Conclusion = fields["conclusion"]
	}
	count := func(level string) *int {
		if state == "unavailable" || state != "complete" && c.counts[level] == 0 {
			return nil
		}
		n := c.counts[level]
		return &n
	}
	s.Counts.Failure, s.Counts.Warning, s.Counts.Notice, s.Counts.Unknown = count("failure"), count("warning"), count("notice"), count("unknown")
	failures, others, noise := c.grouped()
	for _, group := range append(failures, others...) {
		checks := make([]string, 0, len(group.checks))
		for _, name := range group.checks {
			checks = append(checks, summaryText(name, 100))
		}
		level := group.record.Level
		if level != "failure" && level != "warning" && level != "notice" {
			level = "unknown"
		}
		s.Groups = append(s.Groups, WakeSummaryGroup{
			Level: level, Count: group.count, Path: summaryText(group.record.Path, 120),
			Start: group.record.Start, End: group.record.End, Title: summaryText(group.record.Title, 80),
			Message: summaryText(group.record.Message, gitHubFailureMessage), Checks: checks,
		})
	}
	for _, tally := range noise {
		s.Noise = append(s.Noise, WakeSummaryNoise{Class: tally.class, Level: tally.level, Count: tally.count, Checks: len(tally.checks)})
	}
	label := ""
	if summaryTargetID.MatchString(t.ID) {
		switch t.Kind {
		case "run":
			label = "run " + t.ID
		case "pr":
			label = "PR " + t.ID
		}
	}
	shown := func(n *int) string {
		if n == nil {
			return "unknown"
		}
		return strconv.Itoa(*n)
	}
	var segments []string
	if first := strings.TrimSpace(label + " " + s.Conclusion); first != "" {
		segments = append(segments, first)
	}
	segments = append(segments,
		"failures "+shown(s.Counts.Failure),
		fmt.Sprintf("warnings %s (%d known noise)", shown(s.Counts.Warning), s.noiseWarnings()),
		"annotations "+state)
	s.Headline = strings.Join(segments, " | ")
	return s
}
