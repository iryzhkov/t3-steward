package backlog

import (
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ManifestRetry declares the automatic retry budget for infrastructure
// failures, at workflow level as the default of every task and at task level
// as that task's own budget. An absent block means the coordinator defaults.
//
//	retry:
//	  infrastructure: 2   # automatic retries of infrastructure failures, 0..5
//	  backoff: 2m         # delay before the first one, doubled for each later one
//
// Code, protocol, policy, cancelled and unknown failures are never retried,
// whatever the budget says, and the coordinator's own ceiling caps it.
type ManifestRetry struct {
	Infrastructure *int           `yaml:"infrastructure,omitempty"`
	Backoff        *time.Duration `yaml:"backoff,omitempty"`
}

// effectiveManifestRetry merges a task's retry block over the workflow's.
// It returns nil when neither declares one, so the task keeps the defaults
// and its definition is byte-identical to one written before retries existed.
func effectiveManifestRetry(workflow, task *ManifestRetry) *ManifestRetry {
	if workflow == nil && task == nil {
		return nil
	}
	merged := ManifestRetry{}
	for _, layer := range []*ManifestRetry{workflow, task} {
		if layer == nil {
			continue
		}
		if layer.Infrastructure != nil {
			value := *layer.Infrastructure
			merged.Infrastructure = &value
		}
		if layer.Backoff != nil {
			value := *layer.Backoff
			merged.Backoff = &value
		}
	}
	return &merged
}

// taskRetryPolicy converts a merged retry block into the task's durable
// policy, filling what the block leaves out from the defaults.
func taskRetryPolicy(retry *ManifestRetry) *domain.TaskRetryPolicy {
	if retry == nil {
		return nil
	}
	policy := domain.TaskRetryPolicy{Infrastructure: domain.DefaultInfrastructureRetries, Backoff: domain.DefaultRetryBackoff}
	if retry.Infrastructure != nil {
		policy.Infrastructure = *retry.Infrastructure
	}
	if retry.Backoff != nil {
		policy.Backoff = *retry.Backoff
	}
	return &policy
}

// validateManifestRetry refuses a retry block outside the bounds
// domain.TaskRetryPolicy allows. A zero backoff is refused rather than read as
// the default: an author who writes it means "retry at once", which a
// failing worker would turn into a burst.
func validateManifestRetry(prefix string, retry *ManifestRetry) error {
	if retry == nil {
		return nil
	}
	if retry.Infrastructure == nil && retry.Backoff == nil {
		return fmt.Errorf("%s retry declares neither infrastructure nor backoff", prefix)
	}
	if retry.Backoff != nil && *retry.Backoff <= 0 {
		return fmt.Errorf("%s retry.backoff must be positive, got %s", prefix, *retry.Backoff)
	}
	if err := taskRetryPolicy(retry).Validate(); err != nil {
		return fmt.Errorf("%s %w", prefix, err)
	}
	return nil
}
