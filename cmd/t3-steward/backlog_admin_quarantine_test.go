package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
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

type fakeQuarantineService struct {
	principal backlogadmin.Principal
	request   backlogadmin.QuarantineReleaseRequest
	release   domain.QuarantineRelease
	calls     int
}

func (f *fakeQuarantineService) ReleaseQuarantine(
	_ context.Context,
	principal backlogadmin.Principal,
	request backlogadmin.QuarantineReleaseRequest,
) (domain.QuarantineRelease, error) {
	f.calls++
	f.principal, f.request = principal, request
	return f.release, nil
}

// Editing the file is what clears a quarantine the file caused. A quarantine
// the configuration caused needs this instead, because fixing the configuration
// changes no byte of the file and therefore no digest.
func TestBacklogQuarantineReleaseSendsTheKeyAndTheReason(t *testing.T) {
	fake := &fakeQuarantineService{release: domain.QuarantineRelease{
		Key: "legacy-abc", Released: true, Digest: "digest",
		Reason: "references unmapped project",
	}}
	var out bytes.Buffer
	cli := backlogAdminCLI{
		quarantine: fake,
		principal:  backlogadmin.Principal{ID: "operator", Roles: []string{"remote-admin"}},
		stdout:     &out,
	}
	if err := cli.runBacklog(context.Background(), []string{
		"quarantine", "release", "legacy-abc", "--reason", "mapped the project",
	}); err != nil {
		t.Fatal(err)
	}
	if fake.calls != 1 || fake.request.Key != "legacy-abc" || fake.request.Reason != "mapped the project" ||
		fake.principal.ID != "operator" {
		t.Fatalf("calls %d, request %+v, principal %+v", fake.calls, fake.request, fake.principal)
	}
	for _, want := range []string{"released legacy-abc", "references unmapped project", "reads the file again"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output %q does not report %q", out.String(), want)
		}
	}

	// Repeating it is safe and says so.
	out.Reset()
	fake.release = domain.QuarantineRelease{Key: "legacy-abc"}
	if err := cli.runBacklog(context.Background(), []string{
		"quarantine", "release", "legacy-abc", "--reason", "mapped the project",
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "nothing to release") {
		t.Fatalf("output = %q", out.String())
	}
}

// A release is a mutation, so it asks for the reason every mutation asks for,
// and it is authorized under its own operation rather than as a read.
func TestBacklogQuarantineReleaseNeedsAReason(t *testing.T) {
	fake := &fakeQuarantineService{}
	cli := backlogAdminCLI{quarantine: fake, principal: backlogadmin.Principal{ID: "operator"}, stdout: io.Discard}
	for name, args := range map[string][]string{
		"no reason":     {"quarantine", "release", "legacy-abc"},
		"no key":        {"quarantine", "release"},
		"unknown flag":  {"quarantine", "release", "legacy-abc", "--force", "yes"},
		"dangling flag": {"quarantine", "release", "legacy-abc", "--reason"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := cli.runBacklog(context.Background(), args); err == nil {
				t.Fatal("an incomplete release was sent")
			}
		})
	}
	if fake.calls != 0 {
		t.Fatalf("an incomplete release reached the coordinator %d time(s)", fake.calls)
	}
	if isReadQueryKind(backlogadmin.QuarantineReleaseKind) {
		t.Fatal("a release is authorized as a read")
	}
	if err := authorizeRemoteAdmin(backlogadmin.Action{
		Kind: backlogadmin.QuarantineReleaseKind,
	}); err != nil {
		t.Fatalf("remote-admin refused a quarantine release: %v", err)
	}
}

// It is a read, so it reaches the coordinator over the admin transport from a
// non-coordinator host, it takes no arguments, and the usage documents it.
func TestBacklogQuarantineIsARemoteReadableRead(t *testing.T) {
	if !isCoordinatorAdmin([]string{"quarantine"}) {
		t.Fatal("quarantine does not reach the coordinator transport")
	}
	if !isReadQueryKind(backlogadmin.QueryQuarantine) {
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
