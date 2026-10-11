package config

import (
	"strings"
	"testing"
)

func TestBacklogV2TotalArtifactBound(t *testing.T) {
	if got := Default().BacklogV2.MessageLimits.MaxTotalArtifactBytes; got != 1<<30 {
		t.Fatalf("default total artifact bound = %d", got)
	}
	for _, total := range []int64{-1, (1 << 30) + 1, (16 << 20) - 1} {
		cfg := validBacklogV2Config(t)
		cfg.BacklogV2.MessageLimits.MaxArtifactBytes = 16 << 20
		cfg.BacklogV2.MessageLimits.MaxTotalArtifactBytes = total
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "max_total_artifact_bytes") {
			t.Fatalf("total %d accepted or unclear error: %v", total, err)
		}
	}
	for _, total := range []int64{0, 16 << 20, 1 << 30} {
		cfg := validBacklogV2Config(t)
		cfg.BacklogV2.MessageLimits.MaxArtifactBytes = 16 << 20
		cfg.BacklogV2.MessageLimits.MaxTotalArtifactBytes = total
		if err := cfg.Validate(); err != nil {
			t.Fatalf("bounded total %d: %v", total, err)
		}
	}
}

func TestLegacyTotalArtifactBound(t *testing.T) {
	limits := V2MessageLimits{MaxArtifactBytes: 16 << 20}
	if limits.TotalArtifactBytes() != 16<<20 {
		t.Fatal("legacy aggregate changed")
	}
	limits.MaxTotalArtifactBytes = 64 << 20
	if limits.TotalArtifactBytes() != 64<<20 {
		t.Fatal("explicit aggregate ignored")
	}
}

func TestCoordinatorClientTotalArtifactBound(t *testing.T) {
	cfg := Default()
	cfg.BacklogV2.CoordinatorClient = V2CoordinatorClient{
		CoordinatorID: "coordinator", Address: "host", Credential: "secretref:f03-admin/client",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.BacklogV2.CoordinatorClient.MessageLimits.MaxTotalArtifactBytes != 1<<30 {
		t.Fatal("client did not inherit bounded aggregate default")
	}
	cfg.BacklogV2.CoordinatorClient.MessageLimits.MaxTotalArtifactBytes = (1 << 30) + 1
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "max_total_artifact_bytes") {
		t.Fatalf("unbounded client aggregate accepted: %v", err)
	}
}
