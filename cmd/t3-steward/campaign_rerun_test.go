package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCampaignRerunRequiresARunATaskAndAKey(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"run-1"},
		{"run-1", "--from", "implement"},
		{"--from", "implement", "--idempotency-key", "k"},
		{"run-1", "run-2", "--from", "implement", "--idempotency-key", "k"},
		{"run-1", "--from", "implement", "--idempotency-key", "k", "--task", "x"},
		{"run-1", "--from", "a", "--from", "b", "--idempotency-key", "k"},
	} {
		if _, err := parseCampaignRerunArgs(args); err == nil {
			t.Fatalf("campaign rerun accepted %v", args)
		}
	}
	parsed, err := parseCampaignRerunArgs([]string{
		"run-1", "--from", "implement", "--idempotency-key", "k", "--reason", "why", "--json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.run != "run-1" || parsed.from != "implement" || parsed.key != "k" ||
		parsed.reason != "why" || !parsed.asJSON {
		t.Fatalf("parsed = %+v", parsed)
	}
}

// campaignRerunCLI is a rerun CLI whose coordinator seams are recorded rather
// than reached.
func campaignRerunCLI(out io.Writer, sent *domain.GraphAmendment, result domain.GraphAmendmentResult, failure error) campaignCLI {
	return campaignCLI{
		limits: campaignTestLimits,
		stdout: out,
		describe: func(_ context.Context, runID string) (backlogadmin.WorkflowSummary, error) {
			return backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{
				ID: runID, WorkflowID: "workflow-1", GraphRevision: 4,
				Progress: domain.ProgressFailed,
			}}, nil
		},
		amend: func(_ context.Context, request domain.GraphAmendment) (domain.GraphAmendmentResult, error) {
			*sent = request
			return result, failure
		},
	}
}

func campaignRerunResult() domain.GraphAmendmentResult {
	provenance := domain.RerunProvenance{
		SourceRunID: "run-1", SourceTaskID: "implement", SourceAttemptID: "attempt-3",
		IdempotencyKey: "rerun-1", Reason: "the clone failed",
	}
	return domain.GraphAmendmentResult{
		Run: domain.WorkflowRun{ID: "run:rerun:rerun-1", WorkflowID: "workflow-1", GraphRevision: 1},
		Graph: domain.GraphDefinition{
			RunID: "run:rerun:rerun-1", Revision: 1, RerunOf: &provenance,
			Tasks: []domain.Task{{
				ID: "task:rerun:rerun-1:0", Name: "implement",
				CarriedInputs: []domain.CarriedInput{{
					Producer: "review", ProducerTaskID: "task-review",
					Name: "review.md", ArtifactID: "input:rerun:rerun-1:1",
				}},
			}},
		},
	}
}

// The amendment must be fenced on the graph revision the source run had when
// its scope was read, so a run that changed under the command is refused rather
// than reran from a scope nobody looked at.
func TestCampaignRerunFencesOnTheSourceRunAndReportsBothRuns(t *testing.T) {
	var out bytes.Buffer
	var sent domain.GraphAmendment
	cli := campaignRerunCLI(&out, &sent, campaignRerunResult(), nil)
	err := cli.run(context.Background(), []string{
		"rerun", "run-1", "--from", "implement",
		"--idempotency-key", "rerun-1", "--reason", "the clone failed",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := domain.GraphAmendment{
		ID: "rerun-1", RunID: "run-1", ExpectedRevision: 4,
		Operation: "rerun", TaskID: "implement", Reason: "the clone failed",
	}
	if sent != want {
		t.Fatalf("amendment = %+v, want %+v", sent, want)
	}
	for _, fragment := range []string{
		"run=run:rerun:rerun-1",
		"source  run-1 from task implement",
		"carried implement <- review/review.md by reference",
		"the source run is unchanged and still readable",
		"t3-steward campaign show run-1",
	} {
		if !strings.Contains(out.String(), fragment) {
			t.Fatalf("rerun output does not say %q:\n%s", fragment, out.String())
		}
	}
}

func TestCampaignRerunJSONIsVersionedAndCarriesProvenance(t *testing.T) {
	var out bytes.Buffer
	var sent domain.GraphAmendment
	cli := campaignRerunCLI(&out, &sent, campaignRerunResult(), nil)
	if err := cli.run(context.Background(), []string{
		"rerun", "run-1", "--from", "implement", "--idempotency-key", "rerun-1", "--json",
	}); err != nil {
		t.Fatal(err)
	}
	var document campaignRerun
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if document.SchemaVersion != campaignRerunSchemaVersion {
		t.Fatalf("schemaVersion = %d", document.SchemaVersion)
	}
	if document.SourceRunID != "run-1" || document.RunID != "run:rerun:rerun-1" {
		t.Fatalf("document names = %+v", document)
	}
	if document.Provenance.SourceAttemptID != "attempt-3" || document.Provenance.IdempotencyKey != "rerun-1" {
		t.Fatalf("provenance = %+v", document.Provenance)
	}
	if len(document.Carried) != 1 || document.Carried[0].Producer != "review" {
		t.Fatalf("carried = %+v", document.Carried)
	}
}

// A refusal from the coordinator is returned as it was written. The command
// must not soften it into a success with a warning.
func TestCampaignRerunReturnsTheCoordinatorRefusal(t *testing.T) {
	var sent domain.GraphAmendment
	refusal := errors.New("artifact output-inspect is no longer retrievable")
	cli := campaignRerunCLI(io.Discard, &sent, domain.GraphAmendmentResult{}, refusal)
	err := cli.run(context.Background(), []string{
		"rerun", "run-1", "--from", "implement", "--idempotency-key", "rerun-1",
	})
	if !errors.Is(err, refusal) {
		t.Fatalf("err = %v", err)
	}
}

// rerun is documented in a help topic because the usage block is capped; the
// topic has to exist and has to answer the contract questions.
func TestCampaignRerunAndNotifyHelpTopicsAnswerTheContract(t *testing.T) {
	topics := map[string]string{}
	for _, topic := range campaign.HelpTopics() {
		topics[topic.Name] = topic.Body
	}
	for name, fragments := range map[string][]string{
		"rerun": {
			"rerun is mutating and live",
			"never changes the source run",
			"reference: the new run points at the same stored content",
			"are not carried over",
			"no longer retrievable",
			"Idempotency",
			"A complete example",
			"Required configuration",
			"Exit codes",
			"Safe recovery",
		},
		"notify": {
			"--notify-thread",
			"node wait",
			"resolved, not",
			"creates and alters no workflow state",
			"t3-steward wait add --run",
		},
	} {
		body, ok := topics[name]
		if !ok {
			t.Fatalf("campaign help has no %q topic", name)
		}
		for _, fragment := range fragments {
			if !strings.Contains(body, fragment) {
				t.Fatalf("the %q topic does not cover %q", name, fragment)
			}
		}
		for index, line := range strings.Split(strings.TrimSuffix(body, "\n"), "\n") {
			if len(line) > 88 {
				t.Fatalf("%s help line %d is %d columns wide: %q", name, index+1, len(line), line)
			}
		}
	}
}
