package main

import (
	"path/filepath"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/daemon"
	"github.com/iryzhkov/t3-steward/internal/source"
	"github.com/iryzhkov/t3-steward/internal/source/claudequota"
)

// quotaSources leaves provider-log collection in place for every provider.
// Construction is inert; only the watchdog's Run starts an enabled collector.
func quotaSources(cfg config.Config, logs daemon.Source) (daemon.Source, error) {
	if !cfg.ClaudeQuotaCollector.Enabled {
		return logs, nil
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	options := cfg.ClaudeQuotaOptions()
	if options.StatusPath == "" {
		path, err := cfg.ResolveStatePath()
		if err != nil {
			return nil, err
		}
		options.StatusPath = filepath.Join(filepath.Dir(path), "claude-native-quota-status.json")
	}
	collector, err := claudequota.New(options)
	if err != nil {
		return nil, err
	}
	return source.Compose(logs, collector), nil
}
