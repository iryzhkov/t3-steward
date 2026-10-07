package workerruntime

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// publishCollectedResult preserves ordinary failed evidence when secret scanning
// refuses an optional quarantined candidate. The second publication scans every
// remaining object normally; a secret in an ordinary output still refuses it.
func (d *LocalDriver) publishCollectedResult(ctx context.Context, pkg workerproto.ExecutionPackage, result PublishedResult) error {
	err := d.Publisher.PublishResult(ctx, pkg, result)
	var secret *SecretScanError
	if !errors.As(err, &secret) || secret.retryable() || result.Finalized.Completion.VerificationPassed {
		return err
	}
	withheld := map[string]bool{}
	for _, output := range pkg.Outputs {
		if output.Commit == nil {
			continue
		}
		for _, a := range result.Finalized.Artifacts {
			if a.Kind == domain.ArtifactGitState && a.Name == backlog.FailedCommitArtifactName(output.Name) {
				withheld[a.Name] = true
				withheld[backlog.CommitBundleArtifactName(output.Name)] = true
			}
		}
	}
	if len(withheld) == 0 {
		return err
	}
	kept := make([]domain.Artifact, 0, len(result.Finalized.Artifacts))
	for _, a := range result.Finalized.Artifacts {
		if a.Kind == domain.ArtifactGitState && withheld[a.Name] {
			continue
		}
		kept = append(kept, a)
	}
	result.Finalized.Artifacts = kept
	return d.Publisher.PublishResult(ctx, pkg, result)
}
