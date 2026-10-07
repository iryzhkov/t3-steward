package quotatelemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	check.Command = truncateUTF8(*c.Command, MaxCommandBytes)
	check.CommandTruncated = len(check.Command) < len(*c.Command)
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
	var report struct {
		Commands *[]reportCommand `json:"commands"`
	}
	if err := json.Unmarshal(raw, &report); err != nil {
		return nil, 0, err
	}
	if report.Commands == nil {
		return nil, 0, errors.New("gate report has no commands")
	}
	var checks []Check
	skipped := 0
	for index, command := range *report.Commands {
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
