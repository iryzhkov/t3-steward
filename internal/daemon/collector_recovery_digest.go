package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// CollectorRecoveryDigest binds declared coverage and the loaded quota/recovery
// rules. It hashes only configuration policy, never credentials or resume prompts.
// Override order remains significant because the first matching rule wins.
func CollectorRecoveryDigest(cfg config.Config, pool domain.QuotaPool, route domain.ProviderRoute) string {
	cfg = cfg.CollectorRecoveryConfig()
	policy := cfg.Policy
	policy.IgnoreWindows = append([]string(nil), policy.IgnoreWindows...)
	sort.Strings(policy.IgnoreWindows)
	context := struct {
		Coverage                  string
		Policy                    config.Policy
		Overrides                 []config.Override
		BelowPercent              float64
		SettleDelay               config.Duration
		ResetConfirmationRequired bool
	}{domain.QuotaRecoveryCoverageDigest(pool, route), policy, cfg.Overrides, cfg.Resume.BelowPercent, cfg.Resume.ResetSettleDelay, cfg.Resume.ResetConfirmationRequired}
	encoded, err := json.Marshal(context)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
