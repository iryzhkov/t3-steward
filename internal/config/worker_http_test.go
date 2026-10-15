package config

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestWorkerHTTPConfiguration(t *testing.T) {
	cfg := validBacklogV2Config(t)
	if err := cfg.Validate(); err != nil || cfg.BacklogV2.Coordinator.WorkerHTTPEnabled || cfg.BacklogV2.Workers["normandy"].ExchangeTransport != "" {
		t.Fatalf("SSH defaults: %v, %+v", err, cfg.BacklogV2)
	}
	for _, selector := range []string{"", "ssh"} {
		worker := cfg.BacklogV2.Workers["normandy"]
		worker.ExchangeTransport = selector
		cfg.BacklogV2.Workers["normandy"] = worker
		if err := cfg.Validate(); err != nil {
			t.Fatalf("selector %q: %v", selector, err)
		}
	}
	if err := yaml.Unmarshal([]byte("backlog_v2:\n  coordinator:\n    worker_http_enabled: true\n    alerts:\n      bind: 127.0.0.1:9080\n  workers:\n    normandy:\n      exchange_transport: http\n"), &cfg); err != nil {
		t.Fatal(err)
	}
	worker := validBacklogV2Config(t).BacklogV2.Workers["normandy"]
	if worker.Connection != "" {
		t.Fatal("HTTP bound cases require absent Connection")
	}
	oldCatalog, _ := json.Marshal(worker)
	worker.ExchangeTransport = cfg.BacklogV2.Workers["normandy"].ExchangeTransport
	newCatalog, _ := json.Marshal(worker)
	if !bytes.Equal(oldCatalog, newCatalog) {
		t.Fatal("carrier choice changed signed catalog bytes")
	}
	cfg.BacklogV2.Workers["normandy"] = worker
	cfg.BacklogV2.MessageLimits.MaxBytes = 8 << 20
	cfg.BacklogV2.MessageLimits.MaxArtifactBytes = 16 << 20
	cfg.BacklogV2.Transport.RequestTimeout = Duration(2 * time.Minute)
	cfg.BacklogV2.Freshness.WorkerMaxAge = Duration(5 * time.Minute)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	setWorker := func(c *Config, change func(*V2Worker)) {
		w := worker
		change(&w)
		c.BacklogV2.Workers = map[string]V2Worker{"normandy": w}
	}
	for name, mutate := range map[string]func(*Config){
		"missing address":    func(c *Config) { setWorker(c, func(w *V2Worker) { w.Address = "" }) },
		"missing credential": func(c *Config) { setWorker(c, func(w *V2Worker) { w.Credential = "" }) },
		"missing epoch":      func(c *Config) { setWorker(c, func(w *V2Worker) { w.Epoch = "" }) },
		"invalid connection": func(c *Config) { setWorker(c, func(w *V2Worker) { w.Connection = "http" }) },
		"transport kind":     func(c *Config) { c.BacklogV2.Transport.Kind = "http" },
		"disabled gate":      func(c *Config) { c.BacklogV2.Coordinator.WorkerHTTPEnabled = false },
		"missing bind":       func(c *Config) { c.BacklogV2.Coordinator.Alerts.Bind = "" },
		"frame":              func(c *Config) { c.BacklogV2.MessageLimits.MaxBytes = 8<<20 + 1 },
		"artifact":           func(c *Config) { c.BacklogV2.MessageLimits.MaxArtifactBytes++ },
		"timeout":            func(c *Config) { c.BacklogV2.Transport.RequestTimeout = Duration(2*time.Minute + 1) },
		"freshness":          func(c *Config) { c.BacklogV2.Freshness.WorkerMaxAge = Duration(5*time.Minute + 1) },
		"unknown transport": func(c *Config) {
			w := worker
			w.ExchangeTransport = "https"
			c.BacklogV2.Workers = map[string]V2Worker{"normandy": w}
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := cfg
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("invalid HTTP configuration accepted")
			}
		})
	}
}
