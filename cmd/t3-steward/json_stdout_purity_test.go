package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// assertExactlyOneJSONDocument fails unless the buffer holds one JSON document
// and nothing else: no human line before it, no trailer after it. A consumer
// piping --json into jq gets exactly this or gets a parse error.
func assertExactlyOneJSONDocument(t *testing.T, label string, out []byte) {
	t.Helper()
	if len(out) == 0 {
		t.Fatalf("%s: no output at all", label)
	}
	if out[0] != '{' {
		t.Fatalf("%s: stdout does not start with a JSON object: %q", label, out)
	}
	decoder := json.NewDecoder(bytes.NewReader(out))
	var first map[string]any
	if err := decoder.Decode(&first); err != nil {
		t.Fatalf("%s: stdout is not a JSON document: %v\n%s", label, err, out)
	}
	var second any
	if err := decoder.Decode(&second); !errors.Is(err, io.EOF) {
		t.Fatalf("%s: stdout carries more than one document (second decode: %v)\n%s", label, err, out)
	}
}

// Every --json rendering of the campaign authoring verbs writes the document
// alone to stdout, and anything advisory goes to stderr.
func TestCampaignJSONCommandsWriteOnlyTheDocumentToStdout(t *testing.T) {
	root := campaignFixture(t)
	for _, verb := range []string{"validate", "plan"} {
		t.Run(verb, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			cli := campaignTestCLI(t, &stdout)
			cli.stderr = &stderr
			if err := cli.run(context.Background(), []string{verb, root, "--json"}); err != nil {
				t.Fatal(err)
			}
			assertExactlyOneJSONDocument(t, verb, stdout.Bytes())
			if stderr.Len() != 0 {
				t.Fatalf("%s wrote to stderr without a warning to give: %q", verb, stderr.String())
			}
		})
	}
	t.Run("check", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		cli, _ := campaignCheckCLI(t, &stdout, campaignWaitingOnQuota)
		cli.stderr = &stderr
		if err := cli.run(context.Background(), []string{"check", root, "--json"}); err != nil {
			t.Fatal(err)
		}
		assertExactlyOneJSONDocument(t, "check", stdout.Bytes())
		if stderr.Len() != 0 {
			t.Fatalf("check wrote to stderr without a warning to give: %q", stderr.String())
		}
	})
	t.Run("check impossible", func(t *testing.T) {
		// The refusal is the exit status and the error, never a line on stdout
		// ahead of the document.
		var stdout, stderr bytes.Buffer
		cli, _ := campaignCheckCLI(t, &stdout, campaignImpossibleMatrix)
		cli.stderr = &stderr
		if err := cli.run(context.Background(), []string{"check", root, "--json"}); err == nil {
			t.Fatal("an impossible campaign passed check")
		}
		assertExactlyOneJSONDocument(t, "check impossible", stdout.Bytes())
	})
}

// The campaign CLI's advisory channel is stderr whenever one is wired, which
// is what runCampaign does; stdout is the fallback only for a CLI built with no
// stderr at all.
func TestCampaignWarningsGoToStderrWhenWired(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := campaignCLI{stdout: &stdout, stderr: &stderr}
	if cli.warnings() != &stderr {
		t.Fatal("warnings are not routed to stderr")
	}
}

// diagnose --json, reached both as "t3-steward diagnose <run> --json" and as
// "backlog diagnose <run> --json", writes the response document alone. Without
// --json it prints a text summary (F-18), covered by TestBacklogDiagnoseTextSummary.
func TestBacklogDiagnoseJSONWritesOnlyTheDocumentToStdout(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	fake := &fakeAdminQueryService{response: backlogadmin.Response{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryDiagnose, GeneratedAt: now,
		Diagnosis: &backlogadmin.Diagnosis{
			GraphRevision: 3, GeneratedAt: now,
			Workflow: backlogadmin.WorkflowDetail{Summary: backlogadmin.WorkflowSummary{
				Run: domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressActive},
			}},
		},
	}}
	for _, args := range [][]string{
		{"diagnose", "run-1", "--json"},
	} {
		var stdout bytes.Buffer
		cli := backlogAdminCLI{service: fake, principal: backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}}, stdout: &stdout}
		if err := cli.runBacklog(context.Background(), args); err != nil {
			t.Fatal(err)
		}
		assertExactlyOneJSONDocument(t, "backlog "+args[0], stdout.Bytes())
	}
}
