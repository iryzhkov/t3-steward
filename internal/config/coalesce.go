package config

import "time"

// CoalesceConfig is a host opt-in; a nil window differs from explicit zero.
type CoalesceConfig struct {
	Enabled bool      `yaml:"enabled"`
	Window  *Duration `yaml:"window"`
}

func (c CoalesceConfig) EffectiveWindow() time.Duration {
	if !c.Enabled {
		return 0
	}
	if c.Window == nil {
		return 2 * time.Minute
	}
	return c.Window.D()
}
