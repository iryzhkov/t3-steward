package backlog

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"slices"
)

// admitFailedCommitRecords treats quarantine records as optional evidence.
// A bad record cannot discard the summary, archive, or ordinary outputs.
// The strict validator still binds every retained record to this attempt.
func admitFailedCommitRecords(task domain.Task, attempt domain.Attempt, artifacts []domain.Artifact, payloads [][]byte, missing []string) ([]CommitProvenance, []domain.Artifact, [][]byte, []string) {
	baseArtifacts := make([]domain.Artifact, 0, len(artifacts))
	basePayloads := make([][]byte, 0, len(payloads))
	for i, a := range artifacts {
		if a.Kind == domain.ArtifactGitState && isFailedCommitArtifactOf(task, a.Name) {
			continue
		}
		baseArtifacts = append(baseArtifacts, a)
		basePayloads = append(basePayloads, payloads[i])
	}
	var records []CommitProvenance
	accepted := map[int]bool{}
	for i, a := range artifacts {
		if a.Kind != domain.ArtifactGitState || !isFailedCommitArtifactOf(task, a.Name) {
			continue
		}
		// Duplicate records for a declaration are ambiguous, even if one is valid.
		count := 0
		for _, other := range artifacts {
			if other.Kind == a.Kind && other.Name == a.Name {
				count++
			}
		}
		if count != 1 {
			continue
		}
		candidateArtifacts := append(slices.Clone(baseArtifacts), a)
		candidatePayloads := append(slices.Clone(basePayloads), payloads[i])
		valid, _, err := failedCommitResultRecords(task, attempt, candidateArtifacts, candidatePayloads, missing)
		if err != nil {
			continue
		}
		records = append(records, valid...)
		accepted[i] = true
	}
	keptArtifacts := make([]domain.Artifact, 0, len(artifacts))
	keptPayloads := make([][]byte, 0, len(payloads))
	for i, a := range artifacts {
		if a.Kind == domain.ArtifactGitState && isFailedCommitArtifactOf(task, a.Name) && !accepted[i] {
			continue
		}
		keptArtifacts = append(keptArtifacts, a)
		keptPayloads = append(keptPayloads, payloads[i])
	}
	missing = slices.DeleteFunc(slices.Clone(missing), func(name string) bool {
		return slices.ContainsFunc(records, func(p CommitProvenance) bool { return p.Name == name })
	})
	return records, keptArtifacts, keptPayloads, missing
}

func dropFailedCommitRecords(task domain.Task, artifacts []domain.Artifact, payloads [][]byte) ([]domain.Artifact, [][]byte) {
	keptArtifacts := make([]domain.Artifact, 0, len(artifacts))
	keptPayloads := make([][]byte, 0, len(payloads))
	for i, a := range artifacts {
		if a.Kind == domain.ArtifactGitState && isFailedCommitArtifactOf(task, a.Name) {
			continue
		}
		keptArtifacts = append(keptArtifacts, a)
		keptPayloads = append(keptPayloads, payloads[i])
	}
	return keptArtifacts, keptPayloads
}
