package quotatelemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Check stages.
const (
	StageVerification = "verification"
	StageGate         = "gate"
)

// gateReportName is the gate's JSON report; the gate's log, "gate/log.txt",
// shares the artifact kind and is never read.
const gateReportName = "gate"

// collectChecks records one check event per worker-measured command of a
// finished attempt. Only the command, exit code and times are kept; the output
// and the gate log are never stored. A report that is missing, oversized or
// malformed is skipped and counted.
func (r *Recorder) collectChecks(ctx context.Context, work Work, finishedAt time.Time, result *tickResult) error {
	artifacts, err := r.source.ListCheckArtifacts(ctx, work.TaskID, work.AttemptID)
	if malformed(err) {
		result.skippedChecks++
		return nil
	}
	if err != nil {
		return err
	}
	checkWork := work
	checkWork.Outcome, checkWork.DurationMs, checkWork.DispatchToStartMs = "", nil, nil
	for _, artifact := range artifacts {
		var checks []Check
		switch {
		case artifact.Kind == domain.ArtifactVerification:
			index, ok := verificationIndex(artifact.Name)
			if !ok {
				result.skippedChecks++
				continue
			}
			raw, err := r.readReport(ctx, artifact)
			if err != nil {
				result.skippedChecks++
				continue
			}
			check, err := parseVerificationReport(raw)
			if err != nil {
				result.skippedChecks++
				continue
			}
			check.Index = index
			checks = append(checks, check)
		case artifact.Kind == domain.ArtifactGate && artifact.Name == gateReportName:
			raw, err := r.readReport(ctx, artifact)
			if err != nil {
				result.skippedChecks++
				continue
			}
			gate, skipped, err := parseGateReport(raw)
			if err != nil {
				result.skippedChecks++
				continue
			}
			result.skippedChecks += int64(skipped)
			checks = append(checks, gate...)
		default:
			continue
		}
		for _, check := range checks {
			at := finishedAt
			if check.CompletedAt != nil {
				at = *check.CompletedAt
			}
			eventWork := checkWork
			checkCopy := check
			result.events = append(result.events, Event{
				EventID: fmt.Sprintf("%s:%s:%s:%d", KindCheck, work.AttemptID, check.Stage, check.Index),
				Kind:    KindCheck, At: at, QuotaPoolID: work.Route.QuotaPoolID, Work: &eventWork, Check: &checkCopy,
			})
		}
	}
	return nil
}

// commandSecretPatterns are credential shapes cut out of a stored command
// line: provider and forge tokens, and the password of a URL. A command is
// declared configuration rather than output, but one can still carry a token
// inline, and the store must never hold one.
var commandSecretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?:gh[pousr]_[A-Za-z0-9]{20,255}|github_pat_[A-Za-z0-9_]{20,255})`),
	regexp.MustCompile(`sk-[A-Za-z0-9_-]{20,255}`),
	regexp.MustCompile(`AKIA[A-Z0-9]{16}`),
	regexp.MustCompile(`(?i)(?:bearer|token|basic)\s+[A-Za-z0-9._~+/=-]{16,}`),
	regexp.MustCompile(`(?i)((?:password|passwd|secret|token|api[_-]?key)[=:])[^\s'"&]+`),
	regexp.MustCompile(`(://[^/\s:@]+:)[^@\s/]+@`),
}

// redactCommand replaces credential-shaped parts of a command with
// [redacted].
func redactCommand(command string) string {
	for _, pattern := range commandSecretPatterns {
		command = pattern.ReplaceAllStringFunc(command, func(match string) string {
			if groups := pattern.FindStringSubmatch(match); len(groups) > 1 {
				suffix := ""
				if strings.HasSuffix(match, "@") {
					suffix = "@"
				}
				return groups[1] + "[redacted]" + suffix
			}
			return "[redacted]"
		})
	}
	return command
}

// verificationIndex reads NNN from "verification/NNN.json".
func verificationIndex(name string) (int, bool) {
	digits, ok := strings.CutPrefix(name, "verification/")
	if !ok {
		return 0, false
	}
	digits, ok = strings.CutSuffix(digits, ".json")
	if !ok {
		return 0, false
	}
	index, err := strconv.Atoi(digits)
	if err != nil || index < 1 {
		return 0, false
	}
	return index, true
}

// readReport reads one report through the artifact store, refusing anything
// over MaxReportBytes before and while reading it.
func (r *Recorder) readReport(ctx context.Context, artifact domain.Artifact) ([]byte, error) {
	if r.OpenArtifact == nil {
		return nil, errors.New("no artifact reader")
	}
	if artifact.Size > MaxReportBytes {
		return nil, fmt.Errorf("report %s is %d bytes, over %d", artifact.ID, artifact.Size, MaxReportBytes)
	}
	content, err := r.OpenArtifact(ctx, artifact.ID)
	if err != nil {
		return nil, err
	}
	defer content.Close()
	raw, err := io.ReadAll(io.LimitReader(content, MaxReportBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxReportBytes {
		return nil, fmt.Errorf("report %s is over %d bytes", artifact.ID, MaxReportBytes)
	}
	return raw, nil
}

// reportCommand is the subset of a verification report or gate command the
// recorder reads. Output, error text and log fields are not decoded.
type reportCommand struct {
	Command     *string    `json:"command"`
	ExitCode    *int       `json:"exitCode"`
	StartedAt   *time.Time `json:"startedAt"`
	CompletedAt *time.Time `json:"completedAt"`
	Duration    *int64     `json:"duration"`
}

func (c reportCommand) check(stage string) (Check, error) {
	if c.Command == nil || *c.Command == "" || c.ExitCode == nil {
		return Check{}, errors.New("report has no command or exit code")
	}
	check := Check{Stage: stage, ExitCode: *c.ExitCode, StartedAt: utcPointer(c.StartedAt), CompletedAt: utcPointer(c.CompletedAt)}
	command := redactCommand(*c.Command)
	check.Command = truncateUTF8(command, MaxCommandBytes)
	check.CommandTruncated = len(check.Command) < len(command)
	switch {
	case c.Duration != nil && *c.Duration > 0:
		duration := time.Duration(*c.Duration).Milliseconds()
		check.DurationMs = &duration
	case check.StartedAt != nil && check.CompletedAt != nil && !check.CompletedAt.Before(*check.StartedAt):
		duration := check.CompletedAt.Sub(*check.StartedAt).Milliseconds()
		check.DurationMs = &duration
	}
	return check, nil
}

func parseVerificationReport(raw []byte) (Check, error) {
	var report reportCommand
	if err := json.Unmarshal(raw, &report); err != nil {
		return Check{}, err
	}
	return report.check(StageVerification)
}

// parseGateReport returns one check per gate command, from 1, and how many
// commands it skipped as malformed.
func parseGateReport(raw []byte) ([]Check, int, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, 0, err
	}
	listed, found := fields["commands"]
	if !found {
		return nil, 0, errors.New("gate report has no commands")
	}
	// A gate that failed before running a command reports "commands": null,
	// which is a valid report with nothing to record.
	var commands []reportCommand
	if err := json.Unmarshal(listed, &commands); err != nil {
		return nil, 0, err
	}
	var checks []Check
	skipped := 0
	for index, command := range commands {
		check, err := command.check(StageGate)
		if err != nil {
			skipped++
			continue
		}
		check.Index = index + 1
		checks = append(checks, check)
	}
	return checks, skipped, nil
}
