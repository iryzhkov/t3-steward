package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// The quarantine view is the only place an operator can see intake the
// coordinator refused: the audit event has no workflow run, so "backlog events"
// cannot reach it. It has to name the key, the digest, the time and the reason,
// and say that changed content is tried again.
func TestBacklogQuarantineRendersTheReasonAndTheRetryRule(t *testing.T) {
	at := time.Date(2026, 9, 14, 8, 30, 0, 0, time.UTC)
	fake := &fakeAdminQueryService{response: backlogadmin.Response{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryQuarantine,
		Quarantine: []backlogadmin.QuarantinedIntake{{
			Key: "legacy-abc", RecordKey: "quarantine:legacy-abc", Digest: "digest",
			QuarantinedAt: at, Reason: "references unmapped project",
			Retry: backlogadmin.QuarantineRetryAdvice,
		}},
	}}
	var out bytes.Buffer
	cli := backlogAdminCLI{
		service:   fake,
		principal: backlogadmin.Principal{ID: "operator", Roles: []string{"remote-admin"}},
		stdout:    &out,
	}
	if err := cli.runBacklog(context.Background(), []string{"quarantine"}); err != nil {
		t.Fatal(err)
	}
	if len(fake.queries) != 1 || fake.queries[0].Kind != backlogadmin.QueryQuarantine ||
		fake.queries[0].Version != backlogadmin.Version {
		t.Fatalf("queries = %+v", fake.queries)
	}
	for _, want := range []string{
		"legacy-abc", "quarantine:legacy-abc", "digest",
		"2026-09-14T08:30:00Z", "references unmapped project",
		backlogadmin.QuarantineRetryAdvice,
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output %q does not report %q", out.String(), want)
		}
	}
}

// An empty answer is a real answer, and it has to look like one.
func TestBacklogQuarantineSaysWhenNothingIsRefused(t *testing.T) {
	fake := &fakeAdminQueryService{response: backlogadmin.Response{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryQuarantine,
	}}
	var out bytes.Buffer
	cli := backlogAdminCLI{service: fake, principal: backlogadmin.Principal{ID: "operator"}, stdout: &out}
	if err := cli.runBacklog(context.Background(), []string{"quarantine"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "no quarantined intake") {
		t.Fatalf("output = %q", out.String())
	}
}

// It is a read, so it reaches the coordinator over the admin transport from a
// non-coordinator host, it takes no arguments, and the usage documents it.
func TestBacklogQuarantineIsARemoteReadableRead(t *testing.T) {
	if !isCoordinatorAdmin([]string{"quarantine"}) {
		t.Fatal("quarantine does not reach the coordinator transport")
	}
	if !backlogadmin.IsQueryKind(backlogadmin.QueryQuarantine) {
		t.Fatal("the remote-admin role cannot read the quarantine view")
	}
	if err := authorizeRemoteAdmin(backlogadmin.Action{Kind: backlogadmin.QueryQuarantine}); err != nil {
		t.Fatalf("remote-admin refused a read: %v", err)
	}
	if _, _, err := parseBacklogAdminQuery([]string{"quarantine", "legacy-abc"}); err == nil {
		t.Fatal("quarantine accepted an argument it cannot filter by")
	}
	if !strings.Contains(backlogUsage, "quarantine [--json]") {
		t.Fatal("backlog usage does not document the quarantine view")
	}
}
