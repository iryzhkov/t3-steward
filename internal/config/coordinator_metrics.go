package config

import "github.com/iryzhkov/t3-steward/internal/coordinatormetrics"

// V2CoordinatorMetrics is disabled unless a literal loopback bind is supplied.
type V2CoordinatorMetrics struct {
	Bind string `yaml:"bind"`
}

func (m V2CoordinatorMetrics) validate() error { return coordinatormetrics.ValidateBind(m.Bind) }
