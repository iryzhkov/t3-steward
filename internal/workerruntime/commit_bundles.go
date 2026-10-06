package workerruntime

import (
	"context"
	"io"

	"github.com/iryzhkov/t3-steward/internal/backlog"
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
// materializes it in, which is the dependency's task ID.
func dependencySources(pkg workerproto.ExecutionPackage) map[string]backlog.DependencySource {
	var sources map[string]backlog.DependencySource
	for _, dependency := range pkg.Dependencies {
		if dependency.Provenance == nil {
			continue
		}
		if sources == nil {
			sources = make(map[string]backlog.DependencySource)
		}
		sources[dependency.TaskID] = backlog.DependencySource{
			WorkflowRunID: dependency.Provenance.RunID, TaskID: dependency.Provenance.TaskID,
		}
	}
	return sources
}

// resultByteLimit is the total limit of the upload that carries an attempt's
// result: the package's, or the publisher's own when that is lower, since the
// publisher is what refuses an upload over it.
func (d *LocalDriver) resultByteLimit(pkg workerproto.ExecutionPackage) int64 {
	limit := pkg.Limits.MaxTotalBytes
	if publisher, ok := d.Publisher.(interface{ MaxResultBytes() int64 }); ok {
		if own := publisher.MaxResultBytes(); own > 0 && (limit <= 0 || own < limit) {
			limit = own
		}
	}
	return limit
}
