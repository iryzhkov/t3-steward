package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/blockingwait"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// campaignResultSchema versions the document "campaign result --json" prints.
const campaignResultSchema = "t3-steward.campaign-result/v1"

// campaignResultDocument is the one-command answer to "was this run's unit
// accepted": the run's state, one row per task and the acceptance fact.
//
// Everything in it is read from what the coordinator recorded: the attempts'
// progress, failure and review verdict, the review gate, the declared commit
// records and the verification and gate reports. Nothing is inferred from an
// exit code or from a task having succeeded.
type campaignResultDocument struct {
	SchemaVersion string                   `json:"schemaVersion"`
	Run           string                   `json:"run"`
	State         string                   `json:"state"`
	Terminal      bool                     `json:"terminal"`
	Tasks         []campaignResultTask     `json:"tasks"`
	Acceptance    campaignResultAcceptance `json:"acceptance"`
}

// campaignResultTask is one task's line.
type campaignResultTask struct {
	Task      string `json:"task"`
	TaskID    string `json:"taskId"`
	AttemptID string `json:"attemptId,omitempty"`
	State     string `json:"state"`
	// FailureClass is derived from the recorded failure text by its stable
	// prefixes; see campaignFailureClass.
	FailureClass string                  `json:"failureClass,omitempty"`
	Failure      string                  `json:"failure,omitempty"`
	Verdict      *campaignResultVerdict  `json:"verdict,omitempty"`
	ReviewGate   *campaignResultGate     `json:"reviewGate,omitempty"`
	Commits      []campaignResultCommit  `json:"commits,omitempty"`
	Verification campaignResultVerifying `json:"verification"`
	// Reviewed are the declared commits of other tasks this task's verdict
	// is about: the commit outputs it consumed as dependency inputs.
	Reviewed []campaignResultCommit `json:"reviewed,omitempty"`
}

// campaignResultVerdict is the review verdict the coordinator recorded for
// the task's latest attempt.
type campaignResultVerdict struct {
	// Verdict is ACCEPT or CHANGES_REQUESTED.
	Verdict          string   `json:"verdict"`
	BlockingFindings int      `json:"blockingFindings"`
	FindingTitles    []string `json:"findingTitles,omitempty"`
}

// campaignResultGate is the review completion gate of a review-declared task.
type campaignResultGate struct {
	Passed       bool   `json:"passed"`
	Code         string `json:"code"`
	ReviewedHead string `json:"reviewedHead,omitempty"`
	RoundVerdict string `json:"roundVerdict,omitempty"`
}

// campaignResultCommit is one declared commit output and the commit its
// record names. Error says why the record could not be read.
type campaignResultCommit struct {
	Task   string `json:"task,omitempty"`
	Name   string `json:"name"`
	Commit string `json:"commit,omitempty"`
	Error  string `json:"error,omitempty"`
}

// Verification states of a task's latest attempt.
const (
	verificationNotDeclared = "not-declared"
	verificationPending     = "pending"
	verificationPassed      = "passed"
	verificationFailed      = "failed"
	verificationMissing     = "missing"
	verificationUnavailable = "unavailable"
)

// campaignResultVerifying is what the retained verification and gate reports
// of the latest attempt say.
type campaignResultVerifying struct {
	State    string `json:"state"`
	Declared int    `json:"declared"`
	Passed   int    `json:"passed"`
	// Gate is the worker-owned gate's report: passed, failed, missing or
	// unavailable, and empty when the task declares no gate.
	Gate   string `json:"gate,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// campaignResultAcceptance is the acceptance fact: a recorded review ACCEPT
// and recorded passing verification on one and the same commit.
type campaignResultAcceptance struct {
	Accepted bool   `json:"accepted"`
	Commit   string `json:"commit,omitempty"`
	// ReviewTask is the task whose recorded verdict accepted the commit, and
	// VerifiedTask the one whose verification passed on it. They are the same
	// task for a review-declared task, whose gate binds both to one head.
	ReviewTask   string `json:"reviewTask,omitempty"`
	VerifiedTask string `json:"verifiedTask,omitempty"`
	Reason       string `json:"reason"`
}

// campaignResultArtifactLimit bounds one commit record or report read.
const campaignResultArtifactLimit = 1 << 20

func (c campaignCLI) runResult(ctx context.Context, args []string) error {
	clean, opts, err := blockingwait.Parse(args)
	if err != nil {
		return err
	}
	runID, asJSON := "", false
	for _, arg := range clean {
		switch {
		case arg == "--json":
			if asJSON {
				return errors.New("duplicate --json")
			}
			asJSON = true
		case strings.HasPrefix(arg, "-"):
			return fmt.Errorf("unknown campaign result flag %q; it takes <run> [--json] [--wait [--timeout D]]", arg)
		case runID != "":
			return fmt.Errorf("campaign result takes one run, not %q and %q", runID, arg)
		case strings.Contains(arg, "/"):
			return fmt.Errorf("campaign result takes a run, not the task %q; \"t3-steward task result %s\" collects one task", arg, arg)
		default:
			runID = arg
		}
	}
	if runID == "" {
		return errors.New("campaign result needs a run: t3-steward campaign result <run> [--json] [--wait [--timeout D]]")
	}
	if c.detail == nil {
		return errors.New("coordinator workflow query transport is unavailable")
	}
	var detail *backlogadmin.WorkflowDetail
	probe := func(ctx context.Context) (bool, error) {
		latest, err := c.detail(ctx, runID)
		if err != nil {
			return false, err
		}
		detail = &latest
		return latest.Summary.Run.Progress.Terminal(), nil
	}
	reattach := "t3-steward campaign result " + runID + " --wait"
	if opts.Enabled {
		waitErr := blockingwait.Run(ctx, opts.Timeout, probe)
		// A timeout still prints the last state it saw, as task result does.
		if waitErr != nil && (!errors.Is(waitErr, context.DeadlineExceeded) || detail == nil) {
			return blockingWaitError(waitErr, reattach)
		}
	} else if _, err := probe(ctx); err != nil {
		return err
	}
	document := buildCampaignResult(ctx, *detail, c.openArtifact)
	if asJSON {
		if err := encodeCampaignJSON(c.stdout, document); err != nil {
			return err
		}
	} else {
		renderCampaignResult(c.stdout, document)
	}
	return afterDocument(campaignResultVerdictError(document, reattach))
}

// campaignResultVerdictError is the exit code, as task result's: 0 for a run
// that succeeded, 2 for one that failed or was cancelled, 1 for one that has
// not ended. Acceptance is reported in the document, not in the exit code.
func campaignResultVerdictError(document campaignResultDocument, reattach string) error {
	switch domain.ProgressState(document.State) {
	case domain.ProgressSucceeded, domain.ProgressSkipped:
		return nil
	case domain.ProgressFailed, domain.ProgressCancelled:
		return exitCodeError{code: 2, error: fmt.Errorf("run %s ended %s", document.Run, document.State)}
	default:
		return exitCodeError{code: 1, error: fmt.Errorf("run %s is %s and not terminal yet; reattach with %s", document.Run, document.State, reattach)}
	}
}

// buildCampaignResult reads the run's recorded facts into the document. open
// reads one artifact; a nil open leaves every commit and report unavailable,
// which can only make the acceptance fact no.
func buildCampaignResult(ctx context.Context, detail backlogadmin.WorkflowDetail,
	open func(context.Context, string) (backlogadmin.ArtifactContent, error)) campaignResultDocument {
	run := detail.Summary.Run
	document := campaignResultDocument{
		SchemaVersion: campaignResultSchema, Run: run.ID, State: string(run.Progress), Terminal: run.Progress.Terminal(),
		Tasks: []campaignResultTask{},
	}
	reader := campaignResultReader{ctx: ctx, open: open}
	byName := map[string]int{}
	var tasks []backlogadmin.TaskDetail
	for _, task := range manifestOrder(detail) {
		if task.Sink != nil || task.Task.Name == domain.SinkTaskName {
			continue
		}
		byName[task.Task.Name] = len(tasks)
		tasks = append(tasks, task)
		document.Tasks = append(document.Tasks, reader.task(task))
	}
	// A verdict is about the commits the reviewing task consumed: the commit
	// outputs of its producers, which the graph records as dependency inputs.
	for i, task := range tasks {
		if document.Tasks[i].Verdict == nil {
			continue
		}
		producers := make([]string, 0, len(task.Task.DependencyInputs))
		for producer := range task.Task.DependencyInputs {
			producers = append(producers, producer)
		}
		sort.Strings(producers)
		for _, producer := range producers {
			j, ok := byName[producer]
			if !ok {
				continue
			}
			for _, name := range task.Task.DependencyInputs[producer] {
				for _, commit := range document.Tasks[j].Commits {
					if commit.Name == path.Clean(name) {
						document.Tasks[i].Reviewed = append(document.Tasks[i].Reviewed, commit)
					}
				}
			}
		}
	}
	document.Acceptance = campaignAcceptance(document.Tasks, byName)
	return document
}

// campaignAcceptance decides the acceptance fact. A commit is accepted when a
// recorded ACCEPT names it and the recorded verification of the attempt that
// produced it passed, and no recorded verdict asks for changes on it:
//
//   - a review-declared task's gate passed as accepted-head binds its latest
//     accepted review round, its clean workspace and its declared commit to
//     one head, which its own verification ran on;
//   - a task whose latest attempt recorded an ACCEPT verdict is about the
//     commit outputs it consumed, each produced by a succeeded attempt whose
//     own verification is what is checked.
//
// When several commits qualify, the last in manifest order is reported.
func campaignAcceptance(tasks []campaignResultTask, byName map[string]int) campaignResultAcceptance {
	type candidate struct{ commit, review, verified string }
	var candidates []candidate
	var reasons []string
	reason := func(format string, args ...any) {
		line := fmt.Sprintf(format, args...)
		for _, seen := range reasons {
			if seen == line {
				return
			}
		}
		reasons = append(reasons, line)
	}
	rejected := map[string]string{}
	verdicts := 0
	for _, task := range tasks {
		if task.Verdict != nil {
			verdicts++
		}
		if task.Verdict != nil && task.Verdict.Verdict != "ACCEPT" {
			for _, commit := range task.Reviewed {
				if commit.Commit != "" {
					rejected[commit.Commit] = task.Task
				}
			}
			reason("%s recorded %s", task.Task, task.Verdict.Verdict)
		}
		if task.ReviewGate != nil {
			verdicts++
			if !task.ReviewGate.Passed || task.ReviewGate.Code != string(domain.ReviewGateAccepted) {
				reason("%s review gate %s", task.Task, task.ReviewGate.Code)
			}
		}
	}
	for _, task := range tasks {
		// A review-declared task binds verdict and verification itself.
		if gate := task.ReviewGate; gate != nil && gate.Passed && gate.Code == string(domain.ReviewGateAccepted) {
			bound := false
			for _, commit := range task.Commits {
				if commit.Commit == "" || commit.Commit != gate.ReviewedHead {
					continue
				}
				bound = true
				switch {
				case task.State != string(domain.ProgressSucceeded):
					reason("%s is %s", task.Task, task.State)
				case task.Verification.State != verificationPassed:
					reason("verification of %s is %s", task.Task, task.Verification.State)
				default:
					candidates = append(candidates, candidate{commit.Commit, task.Task, task.Task})
				}
			}
			if !bound {
				reason("%s reviewed head %s is not its recorded declared commit", task.Task, shortCommit(gate.ReviewedHead))
			}
		}
		if task.Verdict == nil || task.Verdict.Verdict != "ACCEPT" {
			continue
		}
		if task.State != string(domain.ProgressSucceeded) {
			reason("%s recorded ACCEPT but is %s", task.Task, task.State)
			continue
		}
		if len(task.Reviewed) == 0 {
			reason("%s recorded ACCEPT but consumed no recorded commit", task.Task)
		}
		for _, commit := range task.Reviewed {
			producer := tasks[byName[commit.Task]]
			switch {
			case commit.Commit == "":
				reason("the commit %s of %s %s accepted is unreadable: %s", commit.Name, commit.Task, task.Task, commit.Error)
			case producer.State != string(domain.ProgressSucceeded):
				reason("%s is %s", producer.Task, producer.State)
			case producer.Verification.State != verificationPassed:
				reason("verification of %s is %s", producer.Task, producer.Verification.State)
			default:
				candidates = append(candidates, candidate{commit.Commit, task.Task, producer.Task})
			}
		}
	}
	for i := len(candidates) - 1; i >= 0; i-- {
		c := candidates[i]
		if by, ok := rejected[c.commit]; ok {
			reason("%s also recorded changes requested on %s", by, shortCommit(c.commit))
			continue
		}
		detail := c.review + " accepted, " + c.verified + " verification passed"
		if c.review == c.verified {
			detail = c.review + " review gate accepted-head, verification passed"
		}
		return campaignResultAcceptance{Accepted: true, Commit: c.commit, ReviewTask: c.review, VerifiedTask: c.verified, Reason: detail}
	}
	if verdicts == 0 {
		return campaignResultAcceptance{Reason: "no review verdict is recorded"}
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "no recorded ACCEPT names a recorded commit")
	}
	return campaignResultAcceptance{Reason: strings.Join(reasons, "; ")}
}

// campaignResultReader reads one task's recorded facts.
type campaignResultReader struct {
	ctx  context.Context
	open func(context.Context, string) (backlogadmin.ArtifactContent, error)
}

func (r campaignResultReader) task(task backlogadmin.TaskDetail) campaignResultTask {
	row := campaignResultTask{Task: task.Task.Name, TaskID: task.Task.ID, State: "not started"}
	attempt := task.Attempt
	if attempt != nil {
		row.AttemptID = attempt.ID
		row.State = string(attempt.Progress)
		row.Failure = attempt.Failure
		row.FailureClass = campaignFailureClass(attempt.Progress, attempt.Failure)
		if verdict := attempt.ReviewVerdict; verdict != nil {
			row.Verdict = &campaignResultVerdict{
				Verdict:          strings.ToUpper(strings.ReplaceAll(verdict.Verdict, "-", "_")),
				BlockingFindings: verdict.BlockingFindings,
				FindingTitles:    verdict.FindingTitles,
			}
		}
		if gate := attempt.ReviewGate; gate != nil {
			row.ReviewGate = &campaignResultGate{Passed: gate.Passed, Code: string(gate.Code), ReviewedHead: gate.ReviewedHead, RoundVerdict: gate.RoundVerdict}
		}
	}
	for _, output := range task.Task.Outputs {
		if output.Commit == nil || attempt == nil {
			continue
		}
		name := path.Clean(output.Name)
		commit := campaignResultCommit{Task: task.Task.Name, Name: name}
		record := latestArtifact(task, domain.ArtifactOutput, name)
		switch {
		case record != nil:
			raw, err := r.read(record.Metadata.ID)
			if err == nil {
				var provenance backlog.CommitProvenance
				provenance, err = backlog.ParseCommitProvenance(raw)
				switch {
				case err == nil && provenance.FailedAttempt != nil:
					err = errors.New("the record is of a commit retained from a failed attempt")
				case err == nil:
					commit.Commit = provenance.Commit
				}
			}
			if err != nil {
				commit.Error = string(safeTerminalText([]byte(err.Error())))
			}
		case attempt.Progress == domain.ProgressSucceeded:
			commit.Error = "no commit record was retained"
		default:
			continue
		}
		row.Commits = append(row.Commits, commit)
	}
	row.Verification = r.verification(task)
	return row
}

// latestArtifact is the artifact of the kind and name the latest attempt
// retained, or nil.
func latestArtifact(task backlogadmin.TaskDetail, kind domain.ArtifactKind, name string) *backlogadmin.Artifact {
	var found *backlogadmin.Artifact
	for i := range task.Artifacts {
		artifact := task.Artifacts[i]
		if ofSelectedAttempt(task, artifact) && artifact.Metadata.Kind == kind && artifact.Metadata.Name == name {
			found = &task.Artifacts[i]
		}
	}
	return found
}

// verification reads the latest attempt's verification reports and gate
// report. Verification is passed only when every declared command has a
// report with exit 0 and a declared gate reported passed.
func (r campaignResultReader) verification(task backlogadmin.TaskDetail) campaignResultVerifying {
	v := campaignResultVerifying{Declared: len(task.Task.Verification)}
	if v.Declared == 0 && task.Task.Gate == nil {
		v.State = verificationNotDeclared
		return v
	}
	if task.Attempt == nil || !task.Attempt.Progress.Terminal() {
		v.State = verificationPending
		return v
	}
	reports, failed, unavailable := 0, "", ""
	for _, artifact := range task.Artifacts {
		if !ofSelectedAttempt(task, artifact) || artifact.Metadata.Kind != domain.ArtifactVerification {
			continue
		}
		reports++
		raw, err := r.read(artifact.Metadata.ID)
		var report backlog.VerificationReport
		if err == nil {
			report, err = parseVerificationReport(raw)
		}
		switch {
		case err != nil:
			if unavailable == "" {
				unavailable = artifact.Metadata.Name + ": " + err.Error()
			}
		case report.ExitCode != 0:
			if failed == "" {
				failed = fmt.Sprintf("%s (exit %d)", report.Command, report.ExitCode)
			}
		default:
			v.Passed++
		}
	}
	if task.Task.Gate != nil {
		v.Gate = verificationMissing
		if artifact := latestArtifact(task, domain.ArtifactGate, "gate"); artifact != nil {
			v.Gate = verificationUnavailable
			if raw, err := r.read(artifact.Metadata.ID); err == nil {
				var report backlog.GateReport
				if json.Unmarshal(raw, &report) == nil && report.Attempt == task.Attempt.ID {
					v.Gate = verificationFailed
					if report.Passed {
						v.Gate = verificationPassed
					}
				}
			}
		}
	}
	switch {
	case failed != "":
		v.State, v.Detail = verificationFailed, safeCampaignText(failed)
	case v.Gate == verificationFailed:
		v.State, v.Detail = verificationFailed, "gate failed"
	case unavailable != "":
		v.State, v.Detail = verificationUnavailable, safeCampaignText(unavailable)
	case v.Gate == verificationUnavailable:
		v.State, v.Detail = verificationUnavailable, "gate report unreadable"
	case reports < v.Declared:
		v.State, v.Detail = verificationMissing, fmt.Sprintf("%d of %d commands reported", reports, v.Declared)
	case v.Gate == verificationMissing:
		v.State, v.Detail = verificationMissing, "no gate report"
	default:
		v.State = verificationPassed
	}
	return v
}

// read returns one artifact's bytes, bounded.
func (r campaignResultReader) read(id string) ([]byte, error) {
	if r.open == nil {
		return nil, backlogadmin.ErrArtifactContentUnavailable
	}
	content, err := r.open(r.ctx, id)
	if err != nil {
		return nil, err
	}
	defer content.Content.Close()
	raw, err := io.ReadAll(io.LimitReader(content.Content, campaignResultArtifactLimit+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > campaignResultArtifactLimit {
		return nil, fmt.Errorf("artifact %s exceeds %d bytes", id, campaignResultArtifactLimit)
	}
	return raw, nil
}

// campaignFailureClass names the kind of a recorded failure from the stable
// prefixes the coordinator writes. It is a reading aid for the result line;
// nothing is decided from it.
func campaignFailureClass(progress domain.ProgressState, failure string) string {
	if progress == domain.ProgressCancelled {
		return "cancelled"
	}
	if progress != domain.ProgressFailed {
		return ""
	}
	for _, rule := range []struct{ prefix, class string }{
		{"verification command failed", "verification"},
		{"missing verification evidence", "verification"},
		{"gate command failed", "gate"},
		{"review gate ", "review-gate"},
		{"missing declared output", "missing-output"},
		{"declared commit", "declared-commit"},
		{"terminal dependency prevented execution", "dependency"},
		{"missing explicit success", "no-success"},
		{backlog.SessionNotReadyFailure, "infrastructure"},
		{backlog.TurnStartRefusedFailure, "infrastructure"},
		{"permanent collection", "infrastructure"},
	} {
		if strings.HasPrefix(failure, rule.prefix) {
			return rule.class
		}
	}
	return "other"
}

// renderCampaignResult prints the short block: the run, one line per task and
// the acceptance line.
func renderCampaignResult(out io.Writer, document campaignResultDocument) {
	counts := map[string]int{}
	var states []string
	for _, task := range document.Tasks {
		if counts[task.State] == 0 {
			states = append(states, task.State)
		}
		counts[task.State]++
	}
	sort.Strings(states)
	parts := make([]string, 0, len(states))
	for _, state := range states {
		parts = append(parts, fmt.Sprintf("%d %s", counts[state], state))
	}
	fmt.Fprintf(out, "run %s: %s (%d tasks: %s)\n", document.Run, document.State, len(document.Tasks), strings.Join(parts, ", "))
	width := 0
	for _, task := range document.Tasks {
		width = max(width, len(task.Task))
	}
	for _, task := range document.Tasks {
		fields := []string{fmt.Sprintf("%-*s  %s", width, task.Task, task.State)}
		if task.FailureClass != "" {
			failure := task.FailureClass
			if task.Failure != "" {
				failure += ": " + clipCampaignText(task.Failure, 120)
			}
			fields = append(fields, "failure "+failure)
		}
		if v := task.Verdict; v != nil {
			line := fmt.Sprintf("verdict %s blocking=%d", v.Verdict, v.BlockingFindings)
			if len(task.Reviewed) > 0 {
				var about []string
				for _, commit := range task.Reviewed {
					about = append(about, commit.Task+" "+shortCommit(commit.Commit))
				}
				line += " on " + strings.Join(about, ", ")
			}
			fields = append(fields, line)
		}
		if g := task.ReviewGate; g != nil {
			fields = append(fields, "review gate "+g.Code)
		}
		for _, commit := range task.Commits {
			if commit.Commit != "" {
				fields = append(fields, "commit "+shortCommit(commit.Commit))
			} else {
				fields = append(fields, "commit "+commit.Name+" unreadable: "+clipCampaignText(commit.Error, 80))
			}
		}
		if v := task.Verification; v.State != verificationNotDeclared {
			line := "verification " + v.State
			if v.Declared > 0 {
				line += fmt.Sprintf(" %d/%d", v.Passed, v.Declared)
			}
			if v.Gate != "" {
				line += " gate " + v.Gate
			}
			if v.Detail != "" && v.State != verificationPassed {
				line += " (" + clipCampaignText(v.Detail, 100) + ")"
			}
			fields = append(fields, line)
		}
		fmt.Fprintln(out, "  "+strings.Join(fields, "; "))
	}
	answer := "no"
	if document.Acceptance.Accepted {
		answer = "yes, " + shortCommit(document.Acceptance.Commit)
	}
	fmt.Fprintf(out, "review ACCEPT and verification passed on the same commit: %s (%s)\n", answer, clipCampaignText(document.Acceptance.Reason, 400))
}

// shortCommit is the first twelve digits of a commit id.
func shortCommit(commit string) string {
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}

// safeCampaignText keeps recorded text to printable characters.
func safeCampaignText(s string) string {
	return string(safeTerminalText([]byte(s)))
}

// clipCampaignText is recorded text on one line, clipped to n bytes.
func clipCampaignText(s string, n int) string {
	s = strings.Join(strings.Fields(safeCampaignText(s)), " ")
	if len(s) <= n {
		return s
	}
	for n > 0 && s[n]&0xC0 == 0x80 {
		n--
	}
	return s[:n] + "..."
}
