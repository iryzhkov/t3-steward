package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

type quarantineReadAuthorizer struct{}

func (quarantineReadAuthorizer) Authorize(_ context.Context, _ backlogadmin.Principal, action backlogadmin.Action) error {
	return authorizeRemoteAdmin(action)
}

// Check meaning at the consumer boundary, independently of the shared constant.
func assertHistoricalQuarantineAdvice(t *testing.T, advice string) {
	t.Helper()
	for _, want := range []string{
		"no longer scanned or retried",
		"authenticated t3-steward backlog quarantine release <key> --reason TEXT",
		"deliberately clears the marker",
		"never retries a file or reenables intake",
		"new work with t3-steward task run or campaign submit",
	} {
		if !strings.Contains(advice, want) {
			t.Errorf("advice %q lacks %q", advice, want)
		}
	}
	for _, obsolete := range []string{
		"change the file", "edit the file", "different content digest",
		"explicitly enabled", "intake defaults off", "next cycle", "reads the file again",
	} {
		if strings.Contains(advice, obsolete) {
			t.Errorf("advice %q recommends obsolete recovery %q", advice, obsolete)
		}
	}
}

func TestQuarantineRetirementAdviceThroughConsumers(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	at := time.Date(2026, 10, 4, 8, 30, 0, 0, time.UTC)
	const digest = "7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"
	if _, _, err := store.QuarantineSubmission(ctx, "legacy-retained", digest, "unmapped project", at); err != nil {
		t.Fatal(err)
	}
	service, err := backlogadmin.New(store, quarantineReadAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	for _, asJSON := range []bool{false, true} {
		name := "text"
		if asJSON {
			name = "json"
		}
		t.Run("quarantine/"+name, func(t *testing.T) {
			var out bytes.Buffer
			cli := backlogAdminCLI{service: service, principal: backlogadmin.Principal{ID: "operator", Roles: []string{"remote-admin"}}, stdout: &out}
			args := []string{"quarantine"}
			if asJSON {
				args = append(args, "--json")
			}
			if err := cli.runBacklog(ctx, args); err != nil {
				t.Fatal(err)
			}
			advice := out.String()
			if asJSON {
				var response backlogadmin.Response
				if err := json.Unmarshal(out.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if len(response.Quarantine) != 1 || response.Quarantine[0].Key != "legacy-retained" || response.Quarantine[0].Digest != digest {
					t.Fatalf("quarantine = %+v", response.Quarantine)
				}
				advice = response.Quarantine[0].Retry
			}
			assertHistoricalQuarantineAdvice(t, advice)
		})
		t.Run("triage/"+name, func(t *testing.T) {
			fixture := &triageFixture{now: at}
			output, err := runTriageFixture(t, fixture, asJSON)
			if err != nil {
				t.Fatal(err)
			}
			advice := output
			if asJSON {
				var report triageReport
				if err := json.Unmarshal([]byte(output), &report); err != nil {
					t.Fatal(err)
				}
				advice = ""
				for _, item := range report.Items {
					if item.Kind == "intake-quarantined" {
						advice = item.Summary
					}
				}
			}
			assertHistoricalQuarantineAdvice(t, advice)
		})
	}
	// Diagnostic reads preserve the exact retained marker.
	response, err := service.Query(ctx, backlogadmin.Query{Version: backlogadmin.Version, Kind: backlogadmin.QueryQuarantine, Principal: backlogadmin.Principal{ID: "operator"}})
	if err != nil || len(response.Quarantine) != 1 || response.Quarantine[0].Digest != digest || !response.Quarantine[0].QuarantinedAt.Equal(at) {
		t.Fatalf("marker after diagnostic reads = %+v, %v", response.Quarantine, err)
	}
}

func TestQuarantineRetirementHelpAndEmptyView(t *testing.T) {
	for _, want := range []string{"Authenticated, reason-bound", "never retries a historical file or reenables intake"} {
		if !strings.Contains(backlogUsage, want) {
			t.Errorf("quarantine help lacks %q", want)
		}
	}
	var out bytes.Buffer
	renderQuarantine(&out, nil)
	if !strings.Contains(out.String(), "historical Markdown files are not scanned or retried") || strings.Contains(out.String(), "every submission source is being read") {
		t.Fatalf("empty quarantine diagnostic = %q", out.String())
	}
}
