package workerruntime

import "github.com/iryzhkov/t3-steward/internal/config"

// WithCollectorRecovery updates authenticated declared coverage while preserving
// this host's bucket store, threads and unrelated local policy.
func (g HostQuotaGuard) WithCollectorRecovery(settings config.BacklogV2) QuotaGuard {
	g.Config.BacklogV2 = settings
	return g
}
