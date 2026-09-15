package config

import "errors"

// ValidateWorkerCatalog validates a worker configuration reconstructed from an
// authenticated catalog projection. Explicit empty authorization means revoked,
// so it is allowed here without weakening LoadFile's local configuration checks.
// The caller must check projection identity, bootstrap authority, and digest.
func (c *Config) ValidateWorkerCatalog() error {
	if c.BacklogV2.Mode != "worker" {
		return errors.New("worker catalog validation requires worker mode")
	}
	candidate := *c
	candidate.coordinatorFleetApplied = true
	return candidate.Validate()
}
