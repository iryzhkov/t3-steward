package main

import (
	"os"

	"github.com/iryzhkov/t3-steward/internal/config"
)

// localHostName identifies this machine in retained archive records.
func localHostName(cfg config.Config) string {
	if cfg.Backlog.HostName != "" {
		return cfg.Backlog.HostName
	}
	h, _ := os.Hostname()
	return h
}
