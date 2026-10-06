package config

import "testing"

func TestResultSecretScanConfig(t *testing.T) {
	for _, policy := range []string{"", "default", "block", "warn", "invalid"} {
		c := Default()
		c.BacklogV2.ResultSecretScan.PatternPolicy = policy
		err := c.validateBacklogV2()
		if (err != nil) != (policy == "invalid") {
			t.Fatalf("policy %q: %v", policy, err)
		}
	}
	c := Default()
	c.BacklogV2.ResultSecretScan.MaxObjectBytes = -1
	if c.validateBacklogV2() == nil {
		t.Fatal("negative scan cap accepted")
	}
}
