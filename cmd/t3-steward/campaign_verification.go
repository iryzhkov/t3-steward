package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Verification is read from the retained reports, never inferred from task progress.
func (c backlogAdminCLI) renderVerification(ctx context.Context, detail *backlogadmin.WorkflowDetail) {
	fmt.Fprintln(c.stdout, "verification:")
	for _, task := range detail.Tasks {
		if task.Sink != nil {
			continue
		}
		found := false
		for _, artifact := range task.Artifacts {
			if task.Attempt == nil || artifact.Metadata.AttemptID != task.Attempt.ID || artifact.Metadata.Kind != domain.ArtifactVerification {
				continue
			}
			found = true
			report, err := c.verificationReport(ctx, artifact.Metadata.ID)
			if err != nil {
				fmt.Fprintf(c.stdout, "  %s: unavailable (%v); next: t3-steward backlog artifact get %s\n", task.Task.Name, err, artifact.Metadata.ID)
				continue
			}
			verdict := "passed"
			if report.ExitCode != 0 {
				verdict = "failed"
			}
			fmt.Fprintf(c.stdout, "  %s: %s: %s (exit %d)\n", task.Task.Name, report.Command, verdict, report.ExitCode)
		}
		if !found {
			fmt.Fprintf(c.stdout, "  %s: not reported\n", task.Task.Name)
		}
	}
}

func (c backlogAdminCLI) verificationReport(ctx context.Context, id string) (backlog.VerificationReport, error) {
	var report backlog.VerificationReport
	if c.artifacts == nil {
		return report, backlogadmin.ErrArtifactContentUnavailable
	}
	content, err := c.artifacts.OpenArtifact(ctx, c.principal, id)
	if err != nil {
		return report, err
	}
	defer content.Content.Close()
	raw, err := io.ReadAll(io.LimitReader(content.Content, maxInlineArtifactBytes+1))
	if err != nil {
		return report, err
	}
	if len(raw) > maxInlineArtifactBytes {
		return report, fmt.Errorf("report exceeds %d bytes", maxInlineArtifactBytes)
	}
	// Require both fields: decoding {} as a passed report would invent evidence.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return report, err
	}
	if len(fields["command"]) == 0 || len(fields["exitCode"]) == 0 || string(fields["command"]) == "null" || string(fields["exitCode"]) == "null" {
		return report, fmt.Errorf("report has no command or exitCode")
	}
	if err := json.Unmarshal(raw, &report); err != nil {
		return report, err
	}
	if report.Command == "" {
		return report, fmt.Errorf("report has an empty command")
	}
	return report, nil
}
