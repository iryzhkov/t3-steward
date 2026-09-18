package main

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// --quota <pool> with exactly one of --below, --phase normal and --reset.
func TestCoordinatorWaitSpecParsesQuota(t *testing.T) {
	spec, err := parseCoordinatorWaitSpec([]string{"--quota", "claude", "--below", "40"})
	if err != nil {
		t.Fatal(err)
	}
	if spec.Kind != domain.WaitKindQuota || spec.Quota == nil || spec.Quota.Pool != "claude" || spec.Quota.Below == nil || *spec.Quota.Below != 40 || spec.Condition != "quota claude below 40" {
		t.Fatalf("spec = %+v quota=%+v", spec, spec.Quota)
	}
	if spec, err := parseCoordinatorWaitSpec([]string{"--quota", "claude", "--phase", "normal"}); err != nil || spec.Quota.Phase != domain.PhaseNormal {
		t.Fatalf("phase spec = %+v err=%v", spec, err)
	}
	if spec, err := parseCoordinatorWaitSpec([]string{"--quota", "claude", "--reset", "--task", "current"}); err != nil || !spec.Quota.Reset {
		t.Fatalf("reset spec = %+v err=%v", spec, err)
	}
	for _, args := range [][]string{
		{"--quota", "claude"},
		{"--quota", "claude", "--below", "40", "--reset"},
		{"--quota", "claude", "--phase", "stopped"},
		{"--quota", "claude", "--below", "140"},
		{"--quota", "claude", "--below", "40", "--node", "run-1"},
		{"--quota", "claude", "--below", "40", "--", "true"},
	} {
		if _, err := parseCoordinatorWaitSpec(args); err == nil {
			t.Fatalf("%v was accepted", args)
		}
	}
	if !coordinatorWaitArgs([]string{"--task", "current", "--quota", "claude", "--reset"}) {
		t.Fatal("--quota is not routed as a coordinator kind")
	}
}

// --task current --quota parks the attempt on a coordinator record; an
// unknown pool is refused with the configured pools named.
func TestTaskBoundQuotaWaitRegistersAndRefusesUnknownPools(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{
		{ID: "claude", Provider: "claudeAgent", ProviderInstanceIDs: []string{"claudeAgent"}, Admission: domain.AdmissionOpen, MaxConcurrent: 2},
	}}); err != nil {
		t.Fatal(err)
	}
	err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "unknown", "--quota", "nope", "--phase", "normal"})
	if err == nil || !strings.Contains(err.Error(), "claude") {
		t.Fatalf("an unknown pool was accepted or the pools were not listed: %v", err)
	}
	if err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "quota", "--quota", "claude", "--phase", "normal"}); err != nil {
		t.Fatal(err)
	}
	waits, err := store.ListTaskWaits(ctx)
	if err != nil || len(waits) != 1 || waits[0].Kind != domain.WaitKindQuota || waits[0].Quota == nil || waits[0].Quota.Phase != domain.PhaseNormal {
		t.Fatalf("waits=%+v err=%v", waits, err)
	}
	if rows, err := store.ListWaits(ctx, ""); err != nil || len(rows) != 0 {
		t.Fatalf("a coordinator kind left a local row: %v %v", rows, err)
	}
}
