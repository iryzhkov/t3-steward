package workerruntime

import (
	"context"
	"io"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// commitBundleDeliveries hands preparation the commit bundles a package
// delivered, keyed by the campaign ref of the run and task that produced each
// commit. For a commit carried from another run that is the source run's ref,
// which is the ref its provenance record names; keying it by the consuming run
// would leave it where no record ever looks. A bundle the package named but
// could not carry is passed on with the coordinator's reason and nothing to
// open.
func commitBundleDeliveries(pkg workerproto.ExecutionPackage, open func(workerproto.ArtifactObject) (io.ReadCloser, error)) map[string]backlog.CommitBundleDelivery {
	deliveries := make(map[string]backlog.CommitBundleDelivery, len(pkg.CommitBundles))
	for _, input := range pkg.CommitBundles {
		ref := backlog.CampaignRef(input.WorkflowRunID, input.TaskID, input.Name)
		if input.Bundle == nil {
			deliveries[ref] = backlog.CommitBundleDelivery{Omitted: input.Omitted}
			continue
		}
		object := *input.Bundle
		deliveries[ref] = backlog.CommitBundleDelivery{
			SHA256: object.SHA256, Size: object.Size,
			Open: func(context.Context) (io.ReadCloser, error) { return open(object) },
		}
	}
	return deliveries
}

// dependencySources binds each dependency carried from another run to the run
// and task the package says produced it, keyed by the directory preparation
// materializes it in: the recorded artifact namespace, or the task ID for a
// dependency with no materialized artifacts.
func dependencySources(pkg workerproto.ExecutionPackage) map[string]backlog.DependencySource {
	var sources map[string]backlog.DependencySource
	for _, dependency := range pkg.Dependencies {
		if dependency.Provenance == nil {
			continue
		}
		if sources == nil {
			sources = make(map[string]backlog.DependencySource)
		}
		if len(dependency.Artifacts) == 0 {
			sources[dependency.TaskID] = backlog.DependencySource{
				WorkflowRunID: dependency.Provenance.RunID, TaskID: dependency.Provenance.TaskID,
			}
		}
		for _, object := range dependency.Artifacts {
			parts := strings.Split(object.Path, "/")
			if len(parts) >= 3 && parts[0] == "dependencies" {
				sources[parts[1]] = backlog.DependencySource{
					WorkflowRunID: dependency.Provenance.RunID, TaskID: dependency.Provenance.TaskID,
				}
			}
		}
	}
	return sources
}

// resultAdmitter is a publisher that can say, before anything is stored,
// whether it would accept a result's upload.
type resultAdmitter interface {
	AdmitResult(workerproto.ExecutionPackage, PublishedResult) error
}

// resultAdmission is the finalizer's check of a candidate result: the upload
// it makes together with the final message and the thread archive must fit
// the package's per-artifact and total limits, which the coordinator imports
// it under, and must be accepted by the publisher's own validation, which is
// what PublishResult applies. Only the publisher knows its limits, so a
// publisher that cannot answer is held to the package's alone.
func (d *LocalDriver) resultAdmission(pkg workerproto.ExecutionPackage, message string, archive []byte, continuation *ContinuationSnapshot) func([]domain.Artifact) error {
	return func(artifacts []domain.Artifact) error {
		result := PublishedResult{
			Finalized: backlog.FinalizedAttempt{Artifacts: artifacts}, FinalMessage: message, ThreadArchive: archive,
			Continuation: continuation,
		}
		// The archive is published compacted where it has no room, so it is
		// checked as it would be published. The admitter bounds its own.
		bounded := boundThreadArchive(pkg, result, uploadLimits{object: pkg.Limits.MaxArtifactBytes, total: pkg.Limits.MaxTotalBytes})
		planned, err := resultObjects(pkg, bounded)
		if err != nil {
			return err
		}
		objects := make([]workerproto.ArtifactObject, 0, len(planned))
		for _, entry := range planned {
			objects = append(objects, entry.object)
		}
		if _, err := workerproto.ValidateUploadObjects(objects, pkg.Limits.MaxArtifactBytes, pkg.Limits.MaxTotalBytes); err != nil {
			return err
		}
		if admitter, ok := d.Publisher.(resultAdmitter); ok {
			return admitter.AdmitResult(pkg, result)
		}
		return nil
	}
}
