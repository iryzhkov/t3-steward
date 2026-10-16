package config

import (
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"slices"
	"strings"
)

// CollectorRecoveryMetadata is declared policy, never execution authorization.
// It is retained only through authenticated catalog publication, not YAML input.
type CollectorRecoveryMetadata struct {
	Pools                     map[string]domain.QuotaPool `json:"pools"`
	Policy                    Policy                      `json:"policy"`
	Overrides                 []Override                  `json:"overrides"`
	BelowPercent              float64                     `json:"belowPercent"`
	ResetSettleDelay          Duration                    `json:"resetSettleDelay"`
	ResetConfirmationRequired bool                        `json:"resetConfirmationRequired"`
}

func (c Config) CollectorRecoveryMetadataForWorker(workerID string) *CollectorRecoveryMetadata {
	if c.BacklogV2.CollectorRecovery == nil && c.Policy.WarnPercent == 0 {
		return nil
	}
	worker, ok := c.BacklogV2.Workers[workerID]
	if workerID == "" {
		worker.Providers = map[string]V2Provider{}
		for id, binding := range c.BacklogV2.QuotaPools {
			if binding.Collector != nil {
				worker.Providers[id] = V2Provider{QuotaPool: id}
			}
		}
		ok = true
	}
	if !ok {
		return nil
	}
	m := &CollectorRecoveryMetadata{Pools: map[string]domain.QuotaPool{}, Policy: c.Policy, Overrides: c.Overrides,
		BelowPercent: c.Resume.BelowPercent, ResetSettleDelay: c.Resume.ResetSettleDelay, ResetConfirmationRequired: c.Resume.ResetConfirmationRequired}
	if existing := c.BacklogV2.CollectorRecovery; existing != nil {
		m.Policy = existing.Policy
		m.Overrides = existing.Overrides
		m.BelowPercent = existing.BelowPercent
		m.ResetSettleDelay = existing.ResetSettleDelay
		m.ResetConfirmationRequired = existing.ResetConfirmationRequired
	}
	for _, provider := range worker.Providers {
		if pool, covered := c.collectorPool(provider.QuotaPool); covered {
			m.Pools[pool.ID] = pool
		}
	}
	if len(m.Pools) == 0 {
		return nil
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	var out CollectorRecoveryMetadata
	if json.Unmarshal(raw, &out) != nil {
		return nil
	}
	return &out
}
func (m *CollectorRecoveryMetadata) Validate(worker V2Worker) error {
	if m == nil {
		return nil
	}
	if len(m.Pools) == 0 || len(m.Pools) > 256 {
		return errors.New("collector recovery metadata has invalid pool count")
	}
	sources := map[string]bool{}
	for id, pool := range m.Pools {
		c := pool.Collector
		parsed, err := uuid.Parse(pool.AccountID)
		if id == "" || id != pool.ID || strings.TrimSpace(id) != id || err != nil || parsed.String() != pool.AccountID || c == nil || c.AccountID != pool.AccountID ||
			c.SourceID == "" || strings.TrimSpace(c.SourceID) != c.SourceID || len(c.SourceID) > 256 || sources[c.SourceID] || c.MaxObservationAge <= 0 ||
			len(c.Origins) == 0 || len(c.Origins) > 256 || len(c.RequiredWindows) < 2 || len(c.RequiredWindows) > 256 || len(pool.Models) == 0 || len(pool.Models) > 4096 ||
			!strings.Contains(strings.ToLower(pool.Provider), "claude") {
			return errors.New("collector recovery metadata has invalid source/account/coverage")
		}
		sources[c.SourceID] = true
		bound := false
		for instance, provider := range worker.Providers {
			if provider.QuotaPool == id {
				bound = true
				if !slices.Contains(pool.ProviderInstanceIDs, instance) {
					return errors.New("collector metadata omits execution instance")
				}
				for _, model := range provider.Models {
					if !slices.Contains(pool.Models, model) {
						return errors.New("collector metadata omits execution model")
					}
				}
			}
		}
		if !bound {
			return errors.New("collector metadata names a pool outside this worker's execution catalog")
		}
		origins := map[string]bool{}
		for _, origin := range c.Origins {
			key := origin.WorkerID + "\x00" + origin.ProviderInstanceID
			if origin.WorkerID == "" || strings.TrimSpace(origin.WorkerID) != origin.WorkerID || origin.ProviderInstanceID == "" || strings.TrimSpace(origin.ProviderInstanceID) != origin.ProviderInstanceID || origins[key] || !slices.Contains(pool.ProviderInstanceIDs, origin.ProviderInstanceID) {
				return errors.New("collector metadata has invalid origin")
			}
			origins[key] = true
		}
		windows := map[string]bool{}
		for _, w := range c.RequiredWindows {
			if w.Window == "" || strings.TrimSpace(w.Window) != w.Window || windows[w.Window] {
				return errors.New("collector metadata has invalid required windows")
			}
			windows[w.Window] = true
			base := w.Window == "five_hour" || w.Window == "seven_day"
			if base != (len(w.Models) == 0) {
				return errors.New("collector metadata has invalid model window")
			}
			models := map[string]bool{}
			for _, model := range w.Models {
				if model == "" || models[model] || !slices.Contains(pool.Models, model) {
					return errors.New("collector metadata has unbound model window")
				}
				models[model] = true
			}
		}
		if !windows["five_hour"] || !windows["seven_day"] {
			return errors.New("collector metadata requires both base windows")
		}
	}
	cfg := Default()
	cfg.Policy = m.Policy
	cfg.Overrides = m.Overrides
	cfg.Resume.BelowPercent = m.BelowPercent
	cfg.Resume.ResetSettleDelay = m.ResetSettleDelay
	cfg.Resume.ResetConfirmationRequired = m.ResetConfirmationRequired
	return cfg.Validate()
}

// CollectorRecoveryConfig selects shared collector rules without changing legacy
// host rules. The source and execution bindings remain in the caller's config.
func (c Config) CollectorRecoveryConfig() Config {
	if m := c.BacklogV2.CollectorRecovery; m != nil {
		c.Policy = m.Policy
		c.Overrides = m.Overrides
		c.Resume.BelowPercent = m.BelowPercent
		c.Resume.ResetSettleDelay = m.ResetSettleDelay
		c.Resume.ResetConfirmationRequired = m.ResetConfirmationRequired
	}
	return c
}
