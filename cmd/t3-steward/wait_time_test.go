package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// --at and --for describe a time wait: no command, an instant in the future,
// and a deadline that is extended to cover the instant when none was given.
func TestTimeWaitRegistrationParsesAtAndFor(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	spec, err := parseLocalWaitSpec([]string{"--for", "2h"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Kind != domain.WaitKindTime || spec.At == nil || !spec.At.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("--for 2h parsed as %+v", spec)
	}
	if spec.Condition != "time at 2030-01-01T14:00:00Z" {
		t.Fatalf("condition text = %q", spec.Condition)
	}
	if spec.Timeout <= 2*time.Hour {
		t.Fatalf("the default deadline %s does not cover the instant", spec.Timeout)
	}
	spec, err = parseLocalWaitSpec([]string{"--at", "2030-01-02T00:00:00Z", "--timeout", "1h"}, now)
	if err == nil {
		t.Fatalf("an explicit --timeout before the instant was accepted: %+v", spec)
	}
	if !strings.Contains(err.Error(), "--timeout") {
		t.Fatalf("the refusal does not name --timeout: %v", err)
	}
	for _, args := range [][]string{
		{"--at", "2029-12-31T00:00:00Z"},
		{"--for", "-5m"},
		{"--for", "1h", "--", "true"},
		{"--at", "2030-01-02T00:00:00Z", "--for", "1h"},
		{"--at", "tomorrow"},
	} {
		if _, err := parseLocalWaitSpec(args, now); err == nil {
			t.Fatalf("%v was accepted", args)
		}
	}
	// A plain command is still a shell wait.
	spec, err = parseLocalWaitSpec([]string{"--", "true"}, now)
	if err != nil || spec.Kind != domain.WaitKindShell || len(spec.Command) != 1 {
		t.Fatalf("shell spec = %+v err=%v", spec, err)
	}
}

// --task current --for parks the attempt on a coordinator record whose
// condition text is the instant, and leaves a local row of kind time that runs
// no command.
func TestTaskBoundTimeWaitRegistersTheInstant(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	if err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "lunch", "--for", "90m"}); err != nil {
		t.Fatal(err)
	}
	waits, err := store.ListTaskWaits(ctx)
	if err != nil || len(waits) != 1 {
		t.Fatalf("waits=%v err=%v", waits, err)
	}
	if waits[0].Kind != domain.WaitKindTime || !strings.HasPrefix(waits[0].Condition, "time at ") {
		t.Fatalf("coordinator record = %+v", waits[0])
	}
	local, err := store.ListWaits(ctx, "")
	if err != nil || len(local) != 1 {
		t.Fatalf("local=%v err=%v", local, err)
	}
	if local[0].Kind != domain.WaitKindTime || local[0].At == nil || len(local[0].Command) != 0 || local[0].TaskWaitID != waits[0].ID {
		t.Fatalf("local row = %+v", local[0])
	}
	if !local[0].At.After(time.Now().Add(80 * time.Minute)) {
		t.Fatalf("the instant %s is not 90 minutes out", local[0].At)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil || records.Attempts[0].Progress != domain.ProgressWaitingExternal {
		t.Fatalf("the attempt is not parked: %+v err=%v", records.Attempts, err)
	}
}
