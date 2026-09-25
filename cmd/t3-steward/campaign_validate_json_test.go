package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// An invalid campaign under --json prints the error envelope with the
// schemaVersion the validation document carries, so a reader that reads
// schemaVersion first finds it on either outcome, and the message names the
// manifest object rather than the Go type it was decoded into.
func TestCampaignValidateJSONErrorCarriesTheSchemaVersion(t *testing.T) {
	root := campaignFixture(t)
	manifestPath := filepath.Join(root, "workflow.yaml")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	broken := strings.Replace(string(raw), "prompt_file:", "bogus: 1\n    prompt_file:", 1)
	if broken == string(raw) {
		t.Fatal("the fixture has no prompt_file to put an unknown field beside")
	}
	if err := os.WriteFile(manifestPath, []byte(broken), 0o600); err != nil {
		t.Fatal(err)
	}
	cli := campaignCLI{limits: campaignTestLimits}
	args := []string{"campaign", "validate", root, "--json"}
	var runErr error
	output := captureStdout(t, func() {
		runErr = reportJSONError(args, cli.runValidate(args[2:]))
	})
	if runErr == nil {
		t.Fatal("an invalid campaign validated")
	}
	var envelope struct {
		Kind          string `json:"kind"`
		SchemaVersion int    `json:"schemaVersion"`
		Message       string `json:"message"`
	}
	if err := json.Unmarshal([]byte(output), &envelope); err != nil {
		t.Fatalf("stdout is not one JSON document: %v\n%s", err, output)
	}
	if envelope.Kind != "error" || envelope.SchemaVersion != campaignValidationSchemaVersion {
		t.Fatalf("envelope = %+v", envelope)
	}
	if strings.Contains(envelope.Message, "backlog.Manifest") || !strings.Contains(envelope.Message, "not found in a task") {
		t.Fatalf("message = %q", envelope.Message)
	}
}
