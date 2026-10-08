package backlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func isFailedCommitArtifactOf(task domain.Task, name string) bool {
	return slices.ContainsFunc(task.Outputs, func(o domain.ArtifactDeclaration) bool {
		return o.Commit != nil && FailedCommitArtifactName(o.Name) == name
	})
}

// failedCommitResultRecords binds quarantine metadata to the actual attempt and
// verification report before a missing ordinary commit output can be waived.
func failedCommitResultRecords(task domain.Task, attempt domain.Attempt, artifacts []domain.Artifact, payloads [][]byte, missing []string) ([]CommitProvenance, []string, error) {
	var records []CommitProvenance
	seen := map[string]bool{}
	for i, a := range artifacts {
		if a.Kind != domain.ArtifactGitState || !isFailedCommitArtifactOf(task, a.Name) {
			continue
		}
		p, err := ParseCommitProvenance(payloads[i])
		if err != nil {
			return nil, nil, err
		}
		if p.FailedAttempt == nil || p.FailedAttempt.ID != attempt.ID || p.WorkflowRunID != attempt.WorkflowRunID || p.TaskID != task.ID || FailedCommitArtifactName(p.Name) != a.Name || seen[p.Name] {
			return nil, nil, errors.New("failed commit result identity mismatch")
		}
		seen[p.Name] = true
		if !slices.Contains(missing, p.Name) {
			return nil, nil, fmt.Errorf("failed commit %q was also published as an ordinary output", p.Name)
		}
		var failures []string
		for j, command := range task.Verification {
			name := fmt.Sprintf("verification/%03d.json", j+1)
			for k, evidence := range artifacts {
				if evidence.Kind != domain.ArtifactVerification || evidence.Name != name {
					continue
				}
				var report VerificationReport
				if err := json.Unmarshal(payloads[k], &report); err != nil {
					return nil, nil, err
				}
				if report.Command != command {
					return nil, nil, errors.New("failed commit verification identity mismatch")
				}
				if report.ExitCode != 0 {
					failures = append(failures, fmt.Sprintf("verification command failed (%d): %s", report.ExitCode, command))
				}
			}
		}
		if !slices.Equal(failures, p.FailedAttempt.VerificationFailures) || len(failures) == 0 {
			return nil, nil, errors.New("failed commit verification failures do not match result evidence")
		}
		records = append(records, p)
		missing = slices.DeleteFunc(slices.Clone(missing), func(name string) bool { return name == p.Name })
	}
	return records, missing, nil
}

func validateFailedCommitResultOutcome(records []CommitProvenance, passed bool, failure string) error {
	for _, p := range records {
		if passed || failure != strings.Join(p.FailedAttempt.VerificationFailures, "; ") {
			return errors.New("failed commit result must fail only its recorded verification")
		}
	}
	return nil
}
