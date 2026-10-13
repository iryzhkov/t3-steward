package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/source/claudequota"
)

// ClaudeQuotaCollector uses only an existing local subscription login.
// Disabled configuration never reads credentials or contacts the provider.
type ClaudeQuotaCollector struct {
	Enabled            bool                      `yaml:"enabled"`
	ProviderInstanceID string                    `yaml:"provider_instance_id"`
	AccountID          string                    `yaml:"account_id"`
	QuotaPool          string                    `yaml:"quota_pool"`
	CredentialDir      string                    `yaml:"credential_dir"`
	StatusPath         string                    `yaml:"status_path"`
	Interval           Duration                  `yaml:"interval"`
	Jitter             Duration                  `yaml:"jitter"`
	Timeout            Duration                  `yaml:"timeout"`
	PipelineBudget     Duration                  `yaml:"pipeline_budget"`
	ModelBindings      []ClaudeQuotaModelBinding `yaml:"model_bindings"`
}

type ClaudeQuotaModelBinding struct {
	DisplayName string   `yaml:"display_name"`
	SourceKey   string   `yaml:"source_key"`
	Window      string   `yaml:"window"`
	Label       string   `yaml:"label"`
	Selector    string   `yaml:"selector"`
	Models      []string `yaml:"models"`
	Optional    bool     `yaml:"optional"`
}

// ClaudeQuotaOptions projects validated configuration without reading auth.
// Runtime supplies the default status path next to its own state database.
func (c Config) ClaudeQuotaOptions() claudequota.Options {
	s := c.ClaudeQuotaCollector
	o := claudequota.Options{ProviderInstanceID: s.ProviderInstanceID, AccountID: s.AccountID,
		CredentialDir: s.CredentialDir, StatusPath: s.StatusPath, Interval: s.Interval.D(), Jitter: s.Jitter.D(),
		Timeout: s.Timeout.D(), PipelineBudget: s.PipelineBudget.D(),
		MaxObservationAge: c.BacklogV2.QuotaPools[s.QuotaPool].ObservationMaxAge.D()}
	for _, b := range s.ModelBindings {
		o.ModelBindings = append(o.ModelBindings, claudequota.ModelBinding{DisplayName: b.DisplayName,
			SourceKey: b.SourceKey, Window: b.Window, Label: b.Label, Selector: b.Selector,
			Models: append([]string(nil), b.Models...), Optional: b.Optional})
	}
	return o
}

func (c Config) validateClaudeQuotaCollector() error {
	// Validate new pool fields even when this host's backlog mode is disabled.
	for id, pool := range c.BacklogV2.QuotaPools {
		if strings.TrimSpace(pool.AccountID) != pool.AccountID || pool.ObservationMaxAge < 0 {
			return fmt.Errorf("backlog_v2: quota pool %q requires a trimmed account_id and nonnegative observation_max_age", id)
		}
	}
	s := c.ClaudeQuotaCollector
	if !s.Enabled {
		return nil
	}
	pool, ok := c.BacklogV2.QuotaPools[s.QuotaPool]
	if !ok || strings.TrimSpace(s.QuotaPool) != s.QuotaPool || pool.Provider != "claude" || pool.AccountID == "" || pool.AccountID != s.AccountID {
		return errors.New("claude_quota_collector: an explicit matching Claude quota pool account_id is required")
	}
	if c.BacklogV2.Scheduling.Interval.D() <= 0 || c.BacklogV2.Transport.RequestTimeout.D() <= 0 {
		return errors.New("claude_quota_collector: positive scheduling interval and transport request timeout are required")
	}
	// An observation may await an exchange pass, its request deadline, then
	// the coordinator's next scheduling pass. Runtime processing must fit
	// inside the declared budget too; activation qualification proves that.
	scheduler, request := c.BacklogV2.Scheduling.Interval.D(), c.BacklogV2.Transport.RequestTimeout.D()
	// Each addend must fit before summation, including hostile duration values.
	if scheduler > s.PipelineBudget.D()/2 || request > s.PipelineBudget.D()-2*scheduler {
		return errors.New("claude_quota_collector: pipeline_budget does not cover exchange, transport and scheduler bounds")
	}
	if pool.ObservationMaxAge.D() < time.Second {
		return errors.New("claude_quota_collector: pool observation_max_age must be explicit")
	}
	if err := claudequota.ValidateOptions(c.ClaudeQuotaOptions()); err != nil {
		return fmt.Errorf("claude_quota_collector: %w", err)
	}
	catalog := map[string]bool{}
	instanceBound := false
	for _, worker := range c.BacklogV2.Workers {
		for instance, provider := range worker.Providers {
			if provider.QuotaPool != s.QuotaPool {
				continue
			}
			instanceBound = instanceBound || instance == s.ProviderInstanceID
			for _, model := range provider.Models {
				catalog[model] = true
			}
		}
	}
	if !instanceBound {
		return errors.New("claude_quota_collector: provider_instance_id must be explicitly bound to the quota pool")
	}
	for _, binding := range s.ModelBindings {
		for _, model := range binding.Models {
			if !catalog[model] {
				return errors.New("claude_quota_collector: model binding must name exact models already present in the pool catalog")
			}
		}
	}
	return nil
}
