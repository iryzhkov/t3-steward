package backlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// PreflightRunner executes one bounded preflight command inside the attempt
// environment. It is deliberately the same shape as ProcessRunner, so a
// contained worker runs preflight exactly the way it runs verification, and any
// ProcessRunner satisfies it.
//
// It is a separate interface, and preflight has a separate entry point, because
// the two run at opposite ends of the attempt: verification asserts that the
// provider supervisor has stopped, while preflight runs before a provider
// session exists at all.
type PreflightRunner interface {
	Run(context.Context, ProcessRequest) (ProcessResult, error)
}

// PreflightOutcome says whether the caller may create a provider session. It is
// a typed decision so that no caller has to read a message to find out.
type PreflightOutcome string

const (
	// PreflightReady means every step produced preserved evidence and no
	// blocking policy fired. A failing check under the record policy is ready:
	// the attempt launches with the failing baseline visible.
	PreflightReady PreflightOutcome = "ready"
	// PreflightBlocked means no provider session may be created.
	PreflightBlocked PreflightOutcome = "blocked"
)

// PreflightBlockKind distinguishes a policy decision from an execution fault.
type PreflightBlockKind string

const (
	PreflightBlockNone PreflightBlockKind = ""
	// PreflightBlockRequirePass is a require-pass step that ran and failed.
	PreflightBlockRequirePass PreflightBlockKind = "require-pass"
	// PreflightBlockExecution is a step that could not run, or whose receipt
	// could not be preserved. It blocks under every failure policy.
	PreflightBlockExecution PreflightBlockKind = "execution"
)

// PreflightRequest is one attempt's preflight work. The digests and the
// revision are what bind the resulting evidence to these exact bytes.
type PreflightRequest struct {
	InputDigests      map[string]string
	Log               io.Writer
	TaskID            string
	AttemptID         string
	WorkspaceDir      string
	EnvironmentDigest string
	SourceRevision    string
	WorkerID          string
	Steps             []ManifestPreflightStep
	Tools             []string
	Cached            []domain.PreflightReceipt
	Freshness         time.Duration
}

// PreflightReport is the complete result of one preflight pass.
type PreflightReport struct {
	Bundle      domain.ContextBundle
	Outcome     PreflightOutcome
	Block       PreflightBlockKind
	BlockedStep string
	BlockReason string
	Receipts    []domain.PreflightReceipt
	Reused      []string
}

// MayLaunch reports whether a provider session may be created.
func (r PreflightReport) MayLaunch() bool { return r.Outcome == PreflightReady }

// Receipt returns one receipt by step ID.
func (r PreflightReport) Receipt(stepID string) (domain.PreflightReceipt, bool) {
	for _, receipt := range r.Receipts {
		if receipt.Identity.StepID == stepID {
			return receipt, true
		}
	}
	return domain.PreflightReceipt{}, false
}

// PreflightEngine executes declared preflight steps and collects their
// evidence. It performs no dispatch and creates no provider session.
type PreflightEngine struct {
	Runner   PreflightRunner
	Probes   map[string]Probe
	Now      func() time.Time
	Redactor Redactor
}

func (e PreflightEngine) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now().UTC()
}

func (e PreflightEngine) probes() map[string]Probe {
	if e.Probes != nil {
		return e.Probes
	}
	return BuiltinProbes()
}

func (e PreflightEngine) redactor() Redactor {
	if len(e.Redactor.Patterns) == 0 && len(e.Redactor.Literals) == 0 {
		return DefaultRedactor()
	}
	return e.Redactor
}

// Run executes the declared steps in order and stops at the first blocking
// step. A step that fails under a non-blocking policy is recorded and execution
// continues. Every executed step produces a receipt, and every context step
// produces a fact, including a failed optional one: a probe that vanished
// silently would be a bug, not a tidy prompt.
func (e PreflightEngine) Run(ctx context.Context, request PreflightRequest) (PreflightReport, error) {
	if err := validatePreflightRequest(request); err != nil {
		return PreflightReport{}, err
	}
	report := PreflightReport{
		Outcome: PreflightReady,
		Block:   PreflightBlockNone,
		Bundle: domain.ContextBundle{
			CreatedAt: e.now(),
			TaskID:    request.TaskID,
			AttemptID: request.AttemptID,
		},
	}
	for _, step := range request.Steps {
		identity := domain.PreflightIdentity{
			StepID:            step.ID,
			Command:           step.Command,
			Probe:             step.Probe,
			EnvironmentDigest: request.EnvironmentDigest,
			SourceRevision:    request.SourceRevision,
			InputDigests:      request.InputDigests,
		}
		receipt, reused := e.reusableReceipt(request, identity)
		if !reused {
			receipt = e.execute(ctx, request, step, identity)
		}
		if err := receipt.Validate(); err != nil {
			// An unpreservable receipt is not evidence, so it blocks under
			// every failure policy.
			receipt.Status = domain.PreflightErrored
			receipt.Error = err.Error()
			report.Receipts = append(report.Receipts, receipt)
			report.block(PreflightBlockExecution, step.ID, err.Error())
			return report, nil
		}
		if reused {
			report.Reused = append(report.Reused, step.ID)
		}
		report.Receipts = append(report.Receipts, receipt)
		if step.Kind == PreflightKindContext {
			report.Bundle.Facts = append(report.Bundle.Facts, factFor(step, receipt))
		}
		if kind, reason := stepBlock(step, receipt); kind != PreflightBlockNone {
			report.block(kind, step.ID, reason)
			return report, nil
		}
	}
	if err := report.Bundle.Validate(); err != nil {
		return PreflightReport{}, err
	}
	return report, nil
}

func (r *PreflightReport) block(kind PreflightBlockKind, stepID, reason string) {
	r.Outcome = PreflightBlocked
	r.Block = kind
	r.BlockedStep = stepID
	r.BlockReason = reason
}

// stepBlock applies the failure policy. An errored step always blocks. A failed
// check blocks only under require-pass. A failed context step blocks only when
// it was declared required and carries require-pass; an optional one records
// the failure as a fact and lets the attempt continue.
func stepBlock(step ManifestPreflightStep, receipt domain.PreflightReceipt) (PreflightBlockKind, string) {
	switch receipt.Status {
	case domain.PreflightErrored:
		return PreflightBlockExecution, fmt.Sprintf("step %s could not run: %s", step.ID, receipt.Error)
	case domain.PreflightFailed:
		if step.Kind == PreflightKindContext && !step.Required {
			return PreflightBlockNone, ""
		}
		if step.FailurePolicy == PreflightPolicyRequirePass {
			return PreflightBlockRequirePass, fmt.Sprintf("step %s failed with exit %d under require-pass", step.ID, receipt.ExitCode)
		}
		return PreflightBlockNone, ""
	default:
		return PreflightBlockNone, ""
	}
}

// reusableReceipt returns cached evidence for this exact identity, inside its
// freshness window. Any difference in command, probe, environment, source
// revision or input digests changes the identity digest and misses the cache,
// which is what keeps evidence bound to the bytes it came from.
func (e PreflightEngine) reusableReceipt(request PreflightRequest, identity domain.PreflightIdentity) (domain.PreflightReceipt, bool) {
	now := e.now()
	for _, receipt := range request.Cached {
		if receipt.Reusable(identity, now) {
			return receipt, true
		}
	}
	return domain.PreflightReceipt{}, false
}

func (e PreflightEngine) execute(ctx context.Context, request PreflightRequest, step ManifestPreflightStep, identity domain.PreflightIdentity) domain.PreflightReceipt {
	receipt := domain.PreflightReceipt{
		Identity:       identity,
		IdentityDigest: identity.Digest(),
		Kind:           step.Kind,
		StartedAt:      e.now(),
		FreshFor:       request.Freshness,
	}
	output, exitCode, err := e.invoke(ctx, request, step)
	receipt.CompletedAt = e.now()
	receipt.ExitCode = exitCode
	var exitError *ProcessExitError
	switch {
	case err == nil:
		receipt.Status = domain.PreflightPassed
	case errors.As(err, &exitError):
		receipt.Status = domain.PreflightFailed
		receipt.ExitCode = exitError.ExitCode
	default:
		receipt.Status = domain.PreflightErrored
		receipt.Error = err.Error()
	}
	// Redact before truncating. Truncating first could cut a secret in half and
	// leave a fragment that no pattern matches any more.
	value, redacted := e.redactor().Redact(output)
	value, truncated := boundOutput(value, step.MaxOutputBytes)
	receipt.Stdout = value
	receipt.Redacted = redacted
	receipt.Truncated = truncated
	return receipt
}

func (e PreflightEngine) invoke(ctx context.Context, request PreflightRequest, step ManifestPreflightStep) (string, int, error) {
	if step.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, step.Timeout)
		defer cancel()
	}
	if step.Probe != "" {
		probe, ok := e.probes()[step.Probe]
		if !ok {
			// The manifest schema accepts any well-formed probe identifier on
			// purpose, so an unknown name has to fail loudly here.
			return "", 0, fmt.Errorf("unknown preflight probe %q", step.Probe)
		}
		result, err := probe(ctx, ProbeRequest{
			WorkspaceDir:      request.WorkspaceDir,
			EnvironmentDigest: request.EnvironmentDigest,
			SourceRevision:    request.SourceRevision,
			WorkerID:          request.WorkerID,
			TaskID:            request.TaskID,
			AttemptID:         request.AttemptID,
			Tools:             request.Tools,
			StepID:            step.ID,
			Runner:            e.Runner,
			Log:               request.Log,
		})
		return result.Output, result.ExitCode, err
	}
	if e.Runner == nil {
		return "", 0, errors.New("preflight has no process runner")
	}
	result, err := e.Runner.Run(ctx, ProcessRequest{
		ID:      preflightProcessID(request, step),
		Dir:     request.WorkspaceDir,
		Program: step.Command[0],
		Args:    step.Command[1:],
		Log:     request.Log,
	})
	return result.Output, result.ExitCode, err
}

func preflightProcessID(request PreflightRequest, step ManifestPreflightStep) string {
	attempt := request.AttemptID
	if attempt == "" {
		attempt = request.TaskID
	}
	return fmt.Sprintf("preflight-%s-%s", attempt, step.ID)
}

// PreflightReference is the stable name of one step's full output. The bytes
// stay in a referenced artifact; the reference is what a prompt may carry.
func PreflightReference(stepID, identityDigest string) string {
	return fmt.Sprintf("preflight/%s@%s", stepID, identityDigest)
}

func factFor(step ManifestPreflightStep, receipt domain.PreflightReceipt) domain.ContextFact {
	fact := domain.ContextFact{
		ProducedAt:    receipt.CompletedAt,
		ID:            step.ID,
		Label:         step.Probe,
		Include:       step.Include,
		ReceiptDigest: receipt.IdentityDigest,
		Required:      step.Required,
		Failed:        receipt.Status != domain.PreflightPassed,
		Truncated:     receipt.Truncated,
		Redacted:      receipt.Redacted,
	}
	if fact.Label == "" {
		fact.Label = step.Kind
	}
	if step.Include == PreflightIncludeSummary {
		fact.Value = receipt.Stdout
		if fact.Value == "" && receipt.Error != "" {
			fact.Value = receipt.Error
		}
	}
	if step.Include != PreflightIncludeSummary {
		fact.Reference = PreflightReference(step.ID, receipt.IdentityDigest)
	}
	return fact
}

// boundOutput truncates to the declared byte limit without splitting a rune.
// Truncation is reported to the caller; output is never silently dropped.
func boundOutput(value string, limit int) (string, bool) {
	if limit <= 0 || len(value) <= limit {
		return value, false
	}
	cut := value[:limit]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut, true
}

func validatePreflightRequest(request PreflightRequest) error {
	if request.TaskID == "" {
		return errors.New("preflight requires a task id")
	}
	if request.EnvironmentDigest == "" {
		return errors.New("preflight requires an environment digest")
	}
	if request.WorkspaceDir == "" || !filepath.IsAbs(request.WorkspaceDir) {
		return errors.New("preflight requires an absolute workspace directory")
	}
	return validatePreflight("preflight", ManifestPreflight{Steps: request.Steps})
}

// ProbeRequest is the bounded context one built-in probe may observe. A probe
// receives no credentials and no network authority: it reads steward state or
// runs a read-only command in the attempt workspace through the same runner the
// commands use.
type ProbeRequest struct {
	Runner            PreflightRunner
	Log               io.Writer
	WorkspaceDir      string
	EnvironmentDigest string
	SourceRevision    string
	WorkerID          string
	TaskID            string
	AttemptID         string
	StepID            string
	Tools             []string
}

// ProbeResult is one probe's bounded output and exit status.
type ProbeResult struct {
	Output   string
	ExitCode int
}

// Probe is one named, built-in observation.
type Probe func(context.Context, ProbeRequest) (ProbeResult, error)

// Built-in probe names. A workflow names one of these instead of shelling out
// for a basic fact; anything else fails as an unknown probe.
const (
	ProbeGitHead        = "git_head"
	ProbeGitStatus      = "git_status"
	ProbeGitDiffSummary = "git_diff_summary"
	ProbeToolVersions   = "tool_versions"
	ProbeWorkerIdentity = "worker_identity"
)

// BuiltinProbes is the probe registry. It is an explicit map so that an unknown
// name is a loud lookup failure rather than a silently empty fact.
func BuiltinProbes() map[string]Probe {
	return map[string]Probe{
		ProbeGitHead:        gitProbe("rev-parse", "HEAD"),
		ProbeGitStatus:      gitProbe("status", "--porcelain=v1"),
		ProbeGitDiffSummary: gitProbe("diff", "--stat"),
		ProbeToolVersions:   toolVersionsProbe,
		ProbeWorkerIdentity: workerIdentityProbe,
	}
}

func gitProbe(arguments ...string) Probe {
	return func(ctx context.Context, request ProbeRequest) (ProbeResult, error) {
		return runProbeCommand(ctx, request, "git", arguments...)
	}
}

func toolVersionsProbe(ctx context.Context, request ProbeRequest) (ProbeResult, error) {
	if len(request.Tools) == 0 {
		return ProbeResult{}, errors.New("tool_versions probe needs declared tools")
	}
	var builder strings.Builder
	for _, tool := range request.Tools {
		result, err := runProbeCommand(ctx, request, tool, "--version")
		if err != nil {
			var exitError *ProcessExitError
			if !errors.As(err, &exitError) {
				return ProbeResult{Output: builder.String()}, err
			}
			fmt.Fprintf(&builder, "%s: exit %d\n", tool, exitError.ExitCode)
			continue
		}
		fmt.Fprintf(&builder, "%s: %s\n", tool, firstLine(result.Output))
	}
	return ProbeResult{Output: builder.String()}, nil
}

// workerIdentityProbe answers from steward state. It runs no process, so it
// works before any tool exists in the environment.
func workerIdentityProbe(_ context.Context, request ProbeRequest) (ProbeResult, error) {
	var builder strings.Builder
	fmt.Fprintf(&builder, "worker: %s\n", request.WorkerID)
	fmt.Fprintf(&builder, "environment: %s\n", request.EnvironmentDigest)
	fmt.Fprintf(&builder, "source: %s\n", request.SourceRevision)
	fmt.Fprintf(&builder, "workspace: %s\n", request.WorkspaceDir)
	return ProbeResult{Output: builder.String()}, nil
}

func runProbeCommand(ctx context.Context, request ProbeRequest, program string, arguments ...string) (ProbeResult, error) {
	if request.Runner == nil {
		return ProbeResult{}, fmt.Errorf("probe %s has no process runner", request.StepID)
	}
	result, err := request.Runner.Run(ctx, ProcessRequest{
		ID:      fmt.Sprintf("probe-%s-%s", request.StepID, program),
		Dir:     request.WorkspaceDir,
		Program: program,
		Args:    arguments,
		Log:     request.Log,
	})
	return ProbeResult{Output: result.Output, ExitCode: result.ExitCode}, err
}

func firstLine(value string) string {
	if index := strings.IndexByte(value, '\n'); index >= 0 {
		return value[:index]
	}
	return value
}

// RedactionPlaceholder replaces every secret-looking span.
const RedactionPlaceholder = "[redacted]"

// Redactor removes secret-looking spans from output before it can become a
// context fact, a receipt excerpt or prompt bytes. Literals are exact values
// the caller already knows are sensitive; patterns catch the common shapes.
type Redactor struct {
	Patterns []*regexp.Regexp
	Literals []string
}

var defaultRedactionPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(?:api[_-]?key|secret|token|password|passwd|passphrase|credential)s?\s*[:=]\s*\S+`),
	// An Authorization header is redacted to the end of its line: replacing
	// only the first token would leave the credential itself in place.
	regexp.MustCompile(`(?i)\bauthorization\s*:[^\n]*`),
	regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/-]{8,}={0,2}`),
	regexp.MustCompile(`(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----`),
	regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{16,}`),
	regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{20,}`),
}

// DefaultRedactor is the redaction applied when a caller declares none. It
// deliberately does not treat long hexadecimal runs as secrets, because every
// digest in this system is one.
func DefaultRedactor() Redactor {
	return Redactor{Patterns: defaultRedactionPatterns}
}

// Redact returns the cleaned text and whether anything was replaced.
func (r Redactor) Redact(text string) (string, bool) {
	cleaned := text
	for _, literal := range r.Literals {
		if literal == "" {
			continue
		}
		cleaned = strings.ReplaceAll(cleaned, literal, RedactionPlaceholder)
	}
	for _, pattern := range r.Patterns {
		if pattern == nil {
			continue
		}
		cleaned = pattern.ReplaceAllString(cleaned, RedactionPlaceholder)
	}
	return cleaned, cleaned != text
}
