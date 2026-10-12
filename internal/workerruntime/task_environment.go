package workerruntime

import (
	"context"
	"slices"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// taskEnvironment is the single build-cache injection point for dispatch,
// including recovery dispatches that reuse a prepared workspace.
func (d *LocalDriver) taskEnvironment(ctx context.Context, pkg workerproto.ExecutionPackage) map[string]string {
	env := pkg.Identity.TaskEnvironment()
	if len(d.Config.Caches) == 0 {
		return env
	}
	if d.scoped {
		d.logger().Info("build caches skipped", "reason", "contained runtime does not mount worker build-cache root", "attempt", pkg.Identity.AttemptID, "thread", pkg.Identity.ThreadID)
		return env
	}
	resolved, skipped := ResolveBuildCaches(ctx, d.Config.BuildCacheRoot, d.Config.Caches, d.Config.CacheVersionRunner)
	names := make([]string, 0, len(skipped))
	for name := range skipped {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		d.logger().Warn("build cache skipped", "cache", name, "reason", skipped[name], "attempt", pkg.Identity.AttemptID, "thread", pkg.Identity.ThreadID)
	}
	for variable, path := range resolved {
		if _, identity := env[variable]; !identity {
			env[variable] = path
		}
	}
	return env
}
