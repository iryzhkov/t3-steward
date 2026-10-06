package config

import (
	"testing"
	"time"
)

func TestGateCacheAgeDefault(t *testing.T) {
	c := Default()
	if c.BacklogV2.Verification.GateCacheAge.D() != 24*time.Hour {
		t.Fatal("gate cache default")
	}
	c = validBacklogV2Config(t)
	c.BacklogV2.Verification.GateCacheAge = -1
	if c.Validate() == nil {
		t.Fatal("negative gate cache age accepted")
	}
}
