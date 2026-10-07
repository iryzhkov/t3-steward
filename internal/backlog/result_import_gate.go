package backlog

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"io"
	"strings"
)

// GateEvidenceMaxBytes bounds structured metadata independently of the full log.
const GateEvidenceMaxBytes = 256 << 10

// ErrInvalidGateEvidence identifies stable malformed worker evidence which must
// be settled instead of retried indefinitely.
var ErrInvalidGateEvidence = errors.New("invalid gate evidence")

func decodeGateEvidence(raw []byte) (GateReport, error) {
	var report GateReport
	if len(raw) > GateEvidenceMaxBytes {
		return report, errors.New("result import gate metadata exceeds limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		return report, fmt.Errorf("result import gate: %w", err)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return report, errors.New("result import gate has trailing content")
	}
	return report, nil
}

func evaluateGateEvidence(task domain.Task, artifacts []domain.Artifact, payloads [][]byte, verificationPassed bool) (failure string, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %w", ErrInvalidGateEvidence, err)
		}
	}()
	reportIndex, logIndex := -1, -1
	for index, artifact := range artifacts {
		if artifact.Kind != domain.ArtifactGate {
			continue
		}
		if task.Gate == nil {
			return "", errors.New("result import gate is undeclared")
		}
		switch artifact.Name {
		case "gate":
			if reportIndex != -1 || artifact.MediaType != "application/json" {
				return "", errors.New("result import gate report identity mismatch")
			}
			reportIndex = index
		case "gate/log.txt":
			if logIndex != -1 || artifact.MediaType != "text/plain" {
				return "", errors.New("result import gate log identity mismatch")
			}
			logIndex = index
		default:
			return "", errors.New("result import gate artifact identity mismatch")
		}
	}
	if reportIndex < 0 && logIndex < 0 {
		if task.Gate != nil && verificationPassed {
			return "missing gate evidence", nil
		}
		return "", nil
	}
	if !verificationPassed {
		return "", errors.New("result import contains gate evidence after failed verification")
	}
	if reportIndex < 0 || logIndex < 0 {
		return "", errors.New("result import gate requires report and retained log")
	}
	report, err := decodeGateEvidence(payloads[reportIndex])
	if err != nil {
		return "", err
	}
	artifact := artifacts[reportIndex]
	if artifact.Producer != "" && artifact.Producer != "worker:"+report.Worker {
		return "", errors.New("result import gate worker identity mismatch")
	}
	if artifact.AttemptID != "" && !report.Cached && report.OriginalAttempt != artifact.AttemptID {
		return "", errors.New("result import gate attempt provenance mismatch")
	}
	if err := validateGateReport(task, report); err != nil {
		return "", err
	}
	if !report.Passed {
		return fmt.Sprintf("gate command failed (%d): %s: %s", report.Failure.ExitCode, report.Failure.Command, report.Failure.Reason), nil
	}
	return "", nil
}

func gateHex(value string, sizes ...int) bool {
	for _, size := range sizes {
		if len(value) == size {
			_, err := hex.DecodeString(value)
			return err == nil
		}
	}
	return false
}

func validateGateReport(task domain.Task, report GateReport) error {
	invalid := func(detail string) error { return fmt.Errorf("result import gate %s", detail) }
	if report.StartedAt.IsZero() || report.CompletedAt.Before(report.StartedAt) || report.Worker == "" || len(report.Worker) > 256 ||
		report.LogArtifact != "gate/log.txt" || len(report.OutputLimitation) > 4096 || len(report.OriginalAttempt) > 256 {
		return invalid("identity or timing mismatch")
	}
	// A postcondition failure follows commands that all passed: the gate
	// changed the tree (git status) or moved a declared revision (git
	// rev-parse), or a declared output changed after it (gateCaptureCommand).
	// Only the last can follow a cached report, since it is checked at capture.
	postcondition := report.Failure != nil && report.Failure.ExitCode == 1 && strings.TrimSpace(report.Failure.Reason) != "" &&
		len(report.Failure.Reason) <= gateFailureReasonMax &&
		(report.Failure.Command == gateCaptureCommand || (!report.Cached && (report.Failure.Command == "git status" || report.Failure.Command == "git rev-parse")))
	if report.Cached && (report.OriginalAttempt == "" || (!report.Passed && !postcondition)) {
		return invalid("cache provenance mismatch")
	}
	// Preparation failures still carry a report and a log. They cannot have a
	// cache identity because discovering that identity is what failed.
	if len(report.Commands) == 0 {
		if report.Passed || report.Cached || report.Failure == nil || report.Failure.Command == "" || report.Failure.ExitCode == 0 || report.Failure.Reason == "" ||
			len(report.Failure.Command) > 4096 || len(report.Failure.Reason) > 16384 {
			return invalid("preparation failure mismatch")
		}
		return nil
	}
	if !gateHex(report.TreeHash, 40, 64) || !gateHex(report.CacheKey, 64) || len(report.ToolVersions) == 0 || len(report.ToolVersions) > 64 {
		return invalid("tree or toolchain identity mismatch")
	}
	for key, value := range report.ToolVersions {
		if key == "" || value == "" || len(key) > 128 || len(value) > 4096 {
			return invalid("toolchain metadata exceeds limit")
		}
	}
	if len(report.Commands) > len(task.Gate.Commands) {
		return invalid("contains undeclared commands")
	}
	failed := -1
	for index, command := range report.Commands {
		if command.Command != task.Gate.Commands[index] || command.StartedAt.IsZero() || command.CompletedAt.Before(command.StartedAt) ||
			command.StartedAt.Before(report.StartedAt) || command.CompletedAt.After(report.CompletedAt) || command.Duration < 0 ||
			command.Duration != command.CompletedAt.Sub(command.StartedAt) || len(command.Error) > 16384 {
			return invalid("command identity or timing mismatch")
		}
		if index > 0 && command.StartedAt.Before(report.Commands[index-1].CompletedAt) {
			return invalid("commands overlap")
		}
		if command.ExitCode != 0 || command.Error != "" {
			if index != len(report.Commands)-1 {
				return invalid("contains commands after failure")
			}
			failed = index
		}
	}
	if report.Passed {
		if failed >= 0 || report.Failure != nil || len(report.Commands) != len(task.Gate.Commands) {
			return invalid("success verdict mismatch")
		}
	} else {
		if failed < 0 && postcondition && len(report.Commands) == len(task.Gate.Commands) {
			return nil
		}
		if failed < 0 || report.Failure == nil || report.Failure.Command != report.Commands[failed].Command ||
			report.Failure.ExitCode != report.Commands[failed].ExitCode || strings.TrimSpace(report.Failure.Reason) == "" || len(report.Failure.Reason) > 16384 {
			return invalid("failure verdict mismatch")
		}
	}
	return nil
}
