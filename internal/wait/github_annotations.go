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
	gitHubAnnotationInline  = 10
	gitHubAnnotationOutput  = 4000
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
	lines                 []string
	links                 []string
	counts                map[string]int
	checks                int
	invalid               bool
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
			if len(c.lines) < gitHubAnnotationInline {
				c.lines = append(c.lines, fmt.Sprintf("> check %d %s | %s | %s:%d-%d | %s %s",
					ch.ID, annotationQuote(ch.Name, 80), annotationQuote(a.Level, 30), annotationQuote(a.Path, 120), a.Start, a.End, annotationQuote(a.Title, 80), annotationQuote(a.Message, 240)))
			}
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

func (r *Runner) gitHubAnnotations(ctx context.Context, t GitHubTarget, dir string, reading GitHubReading) (output string) {
	// All complete, partial and unavailable exits share the final ceiling.
	// Remote fields are quoted before this UTF8-safe display cap.
	defer func() { output = annotationOutput(output) }()
	provenance := ""
	unavailable := func(category string) string {
		return annotationQuote(reading.Reason, 400) + "\nAnnotations unavailable (" + annotationQuote(category, 240) + "); " + provenance + "; no clean result inferred."
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
	// Links and the completeness statement precede details so output clipping
	// never hides the access/cap caveat. Full URLs survive for the inline checks.
	var b strings.Builder
	b.WriteString(header)
	omitted := 0
	for _, line := range append(append([]string(nil), c.links...), c.lines...) {
		if b.Len()+len(line)+1 > gitHubAnnotationOutput-150 {
			omitted++
			continue
		}
		b.WriteByte('\n')
		b.WriteString(line)
	}
	additional := c.records - len(c.lines)
	if omitted > 0 || additional > 0 {
		fmt.Fprintf(&b, "\nAdditional/capped display: %d records beyond inline limit; %d summary lines omitted. See check/target links.", additional, omitted)
	}
	return b.String()
}
