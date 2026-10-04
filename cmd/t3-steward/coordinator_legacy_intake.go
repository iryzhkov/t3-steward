package main

import (
	"fmt"
	"os"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
)

// coordinatorLegacyIntake retains the phase-1 rollback adapter. Disabled means
// no directory resolution, alias validation, file reads or quarantine writes.
func coordinatorLegacyIntake(cfg config.Config, submitter backlog.SingleTaskSubmitter, quarantine backlog.LegacySubmissionQuarantine) (coordinatorLegacyTicker, error) {
	if !cfg.BacklogV2.Coordinator.LegacyFileIntakeEnabled {
		return nil, nil
	}
	targets := make(map[string]string, len(cfg.BacklogV2.Projects))
	for name, project := range cfg.BacklogV2.Projects {
		targets[name] = project.T3Project
	}
	aliases, err := backlog.LegacyProjectAliases(targets)
	if err != nil {
		return nil, fmt.Errorf("backlog_v2.coordinator.legacy_file_intake_enabled: %w", err)
	}
	dir, err := cfg.ResolveBacklogDir()
	if err != nil {
		return nil, fmt.Errorf("backlog_v2.coordinator.legacy_file_intake_enabled: %w", err)
	}
	return backlog.LegacySubmissionSource{
		Dir: dir, Submitter: submitter, ProjectAliases: aliases,
		MaxBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
		MaxFiles: cfg.BacklogV2.MessageLimits.MaxFiles, AllowedUID: uint32(os.Getuid()),
		Quarantine: quarantine,
	}, nil
}
