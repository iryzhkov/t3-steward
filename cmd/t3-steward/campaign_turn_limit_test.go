package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/compat"
)

// Each authoring entry point refuses a task whose first turn T3 would refuse
// for its length, before anything reaches the coordinator: validate, check
// and submit (task run has its own test).
func TestCampaignEntryPointsRefuseAFirstTurnOverT3sInputLimit(t *testing.T) {
	for _, command := range [][]string{
		{"validate"},
		{"validate", "--json"},
		{"check"},
		{"submit", "--idempotency-key", "campaign-1"},
	} {
		t.Run(strings.Join(command, " "), func(t *testing.T) {
			root := campaignFixture(t)
			if err := os.WriteFile(filepath.Join(root, "prompts", "review.md"), []byte(strings.Repeat("x", compat.MaxTurnInputLength)), 0o644); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			cli, requests := campaignCheckCLI(t, &out, campaignReadyMatrix)
			args := append([]string{command[0], root}, command[1:]...)
			var err error
			captured := captureStdout(t, func() { err = cli.run(context.Background(), args) })
			if err == nil || !strings.Contains(err.Error(), "task review (prompts/review.md)") ||
				!strings.Contains(err.Error(), "turn input limit of 120000 characters") || !strings.Contains(err.Error(), "inputs:") {
				t.Fatalf("error = %v\n%s%s", err, out.String(), captured)
			}
			if len(*requests) != 0 {
				t.Fatalf("the coordinator was asked about a campaign that cannot run: %d viability queries", len(*requests))
			}
		})
	}
}
