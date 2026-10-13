package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/diagnostics"
)

func TestCampaignAuthoringJSONDiagnostics(t *testing.T) {
	for _, verb := range []string{"validate", "compile"} {
		t.Run(verb, func(t *testing.T) {
			var out bytes.Buffer
			cli := campaignTestCLI(t, &out)
			var args []string
			var original error
			if verb == "validate" {
				root := campaignFixture(t)
				broken := strings.Replace(campaignFixtureManifest, "prompt_file:", "bogus: 1\n    prompt_file:", 1)
				if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), []byte(broken), 0600); err != nil {
					t.Fatal(err)
				}
				_, _, original = cli.prepare(root)
				args = []string{verb, root, "--json"}
			} else {
				path := filepath.Join(t.TempDir(), "plan.md")
				raw := []byte("# Missing header\n")
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
				_, original = campaign.ParseCompilePlan(path, raw)
				args = []string{verb, path, "--out", filepath.Join(t.TempDir(), "out"), "--json"}
			}
			var runErr error
			globalOutput := captureStdout(t, func() {
				runErr = reportJSONError(append([]string{"campaign"}, args...), cli.run(context.Background(), args))
			})
			if runErr == nil || original == nil || exitCodeFor(runErr) != 1 {
				t.Fatalf("refusal = %v, original = %v", runErr, original)
			}
			if globalOutput != "" {
				t.Fatalf("second envelope: %s", globalOutput)
			}
			var document struct {
				backlogadmin.TransportErrorEnvelope
				Diagnostics diagnostics.Document `json:"diagnostics"`
			}
			if err := json.Unmarshal(out.Bytes(), &document); err != nil {
				t.Fatalf("not one document: %v\n%s", err, &out)
			}
			if document.Kind != "error" || document.Class != unclassifiedErrorClass || document.Operation != "campaign" || document.Message != original.Error() {
				t.Fatalf("changed original envelope: %+v", document)
			}
			wantVersion := 0
			if verb == "validate" {
				wantVersion = campaignValidationSchemaVersion
			}
			if document.SchemaVersion != wantVersion || document.Diagnostics.SchemaVersion != "diagnostics/v1" || document.Diagnostics.OK || len(document.Diagnostics.Entries) == 0 {
				t.Fatalf("diagnostics envelope = %+v", document)
			}
			if verb == "validate" {
				var owner *backlog.UnknownManifestFieldError
				if !errors.As(runErr, &owner) {
					t.Fatalf("lost manifest owner error: %v", runErr)
				}
			} else {
				var owner *campaign.CompilePlanError
				if !errors.As(runErr, &owner) {
					t.Fatalf("lost compile owner error: %v", runErr)
				}
			}
		})
	}
	t.Run("printed invalid unit report", func(t *testing.T) {
		cause := errors.New("unit refused")
		doc := diagnostics.FromError(cause, "out/unit/workflow.yaml")
		report := campaignCompileReport{SchemaVersion: campaignCompileSchemaVersion, Out: "out", Units: []campaignCompileUnit{
			{ID: "unit", Directory: "out/unit", Errors: []string{cause.Error()}, Diagnostics: &doc},
		}}
		var out bytes.Buffer
		if err := encodeCampaignJSON(&out, report); err != nil {
			t.Fatal(err)
		}
		output := captureStdout(t, func() {
			err := reportJSONError([]string{"campaign", "compile", "--json"}, afterDocument(campaignCompileVerdict(report)))
			if err == nil || exitCodeFor(err) != 1 {
				t.Fatalf("report verdict = %v", err)
			}
		})
		if output != "" {
			t.Fatalf("second report document: %s", output)
		}
		var decoded campaignCompileReport
		if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Units[0].Valid || decoded.Units[0].Diagnostics == nil || decoded.Units[0].Diagnostics.Entries[0].Code != "legacy" {
			t.Fatalf("unit report = %+v", decoded)
		}
		out.Reset()
		if err := renderCampaignCompile(&out, report); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "unit refused") || !strings.Contains(out.String(), "out/unit/workflow.yaml") || !strings.Contains(out.String(), "legacy") {
			t.Fatalf("text = %s", &out)
		}
	})
}

func TestMalformedCompileDiagnosticsHaveNoEffects(t *testing.T) {
	for _, field := range []string{"after", "group", "command"} {
		t.Run(field, func(t *testing.T) {
			path, raw := compileFixturePlan(t)
			broken := strings.Replace(string(raw), "    title: Campaign", "    "+field+": unsupported\n    title: Campaign", 1)
			if err := os.WriteFile(path, []byte(broken), 0600); err != nil {
				t.Fatal(err)
			}
			outDir := filepath.Join(t.TempDir(), "out")
			var out bytes.Buffer
			err := campaignTestCLI(t, &out).run(context.Background(), []string{"compile", path, "--out", outDir, "--check", "--json"})
			if err == nil || exitCodeFor(err) != 1 {
				t.Fatalf("refusal = %v", err)
			}
			if _, err := os.Stat(outDir); !os.IsNotExist(err) {
				t.Fatalf("created output: %v", err)
			}
			var document struct {
				Diagnostics diagnostics.Document `json:"diagnostics"`
			}
			if err := json.Unmarshal(out.Bytes(), &document); err != nil {
				t.Fatal(err)
			}
			if len(document.Diagnostics.Entries) != 1 || document.Diagnostics.Entries[0].Code != "yaml-unknown-field" || document.Diagnostics.Entries[0].Path != "units[0]."+field {
				t.Fatalf("unsupported field diagnostics = %+v", document)
			}
		})
	}
}

func TestAuthoringDiagnosticsPreserveTextAndSuccess(t *testing.T) {
	cause := &campaign.CompilePlanError{Source: "plan.md", Line: 4, Reason: "bad ref"}
	err := campaignCLI{}.authoringFailure(cause, "plan.md", false)
	var owner *campaign.CompilePlanError
	if !errors.As(err, &owner) || owner != cause || !strings.HasPrefix(err.Error(), cause.Error()+"\n") {
		t.Fatalf("text/chain = %v", err)
	}
	for _, verb := range []string{"validate", "compile"} {
		var out bytes.Buffer
		args := []string{verb, campaignFixture(t), "--json"}
		if verb == "compile" {
			path, _ := compileFixturePlan(t)
			args = []string{verb, path, "--out", filepath.Join(t.TempDir(), "out"), "--json"}
		}
		if err := campaignTestCLI(t, &out).run(context.Background(), args); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), `"diagnostics"`) {
			t.Fatalf("changed successful %s shape: %s", verb, &out)
		}
	}
}
