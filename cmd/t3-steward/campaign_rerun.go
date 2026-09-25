package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// campaignRerunSchemaVersion versions the rerun document.
const campaignRerunSchemaVersion = 1

// campaignRerun is what rerun reports. It names both runs, because the whole
// value of the operation is that there are two of them and the first one is
// still readable.
type campaignRerun struct {
	SchemaVersion int                    `json:"schemaVersion"`
	SourceRunID   string                 `json:"sourceRunId"`
	SourceTaskID  string                 `json:"sourceTaskId"`
	RunID         string                 `json:"runId"`
	WorkflowID    string                 `json:"workflowId"`
	Provenance    domain.RerunProvenance `json:"provenance"`
	Rerun         []string               `json:"rerun"`
	Carried       []campaignCarried      `json:"carried,omitempty"`
	Replay        bool                   `json:"replay"`
}

// campaignCarried is one ancestor output the new run took by reference.
type campaignCarried struct {
	Task       string `json:"task"`
	Producer   string `json:"producer"`
	Name       string `json:"name"`
	ArtifactID string `json:"artifactId"`
}

// campaignRerunArgs is one parsed rerun command line. It has its own parser
// because rerun names a run rather than a directory, and reusing the authoring
// parser would mean teaching it that its one positional is sometimes not a
// path at all.
type campaignRerunArgs struct {
	run    string
	from   string
	key    string
	reason string
	prompt string
	asJSON bool
}

func parseCampaignRerunArgs(args []string) (campaignRerunArgs, error) {
	var parsed campaignRerunArgs
	for index := 0; index < len(args); index++ {
		switch argument := args[index]; argument {
		case "--json":
			if parsed.asJSON {
				return campaignRerunArgs{}, errors.New("--json may be supplied only once")
			}
			parsed.asJSON = true
		case "--from", "--idempotency-key", "--reason", "--prompt":
			if index+1 >= len(args) || args[index+1] == "" {
				return campaignRerunArgs{}, fmt.Errorf("%s needs one nonempty value", argument)
			}
			target := &parsed.from
			switch argument {
			case "--idempotency-key":
				target = &parsed.key
			case "--reason":
				target = &parsed.reason
			case "--prompt":
				target = &parsed.prompt
			}
			if *target != "" {
				return campaignRerunArgs{}, fmt.Errorf("%s may be supplied only once", argument)
			}
			index++
			*target = args[index]
		default:
			if strings.HasPrefix(argument, "-") {
				return campaignRerunArgs{}, fmt.Errorf("unknown campaign rerun option %q", argument)
			}
			if parsed.run != "" {
				return campaignRerunArgs{}, errors.New("campaign rerun accepts exactly one run")
			}
			parsed.run = argument
		}
	}
	if parsed.run == "" {
		return campaignRerunArgs{}, errors.New("campaign rerun needs the run to rerun from")
	}
	if parsed.from == "" {
		return campaignRerunArgs{}, errors.New("campaign rerun requires --from TASK, the task to start again")
	}
	if parsed.key == "" {
		// The key is required rather than generated, for the reason submit
		// requires one: a generated key turns a retry into a second run.
		return campaignRerunArgs{}, errors.New("campaign rerun requires --idempotency-key KEY")
	}
	return parsed, nil
}

func (c campaignCLI) runRerun(ctx context.Context, args []string) error {
	parsed, err := parseCampaignRerunArgs(args)
	if err != nil {
		return err
	}
	if c.amend == nil || c.describe == nil {
		return errors.New("coordinator admin transport is unavailable")
	}
	// The source run is read before the amendment so the request can name the
	// graph revision it was computed against. A run that changed in between is
	// refused rather than reran from a scope nobody looked at.
	summary, err := c.describe(ctx, parsed.run)
	if err != nil {
		return err
	}
	reason := parsed.reason
	if reason == "" {
		reason = fmt.Sprintf("rerun of %s from %s", parsed.run, parsed.from)
	}
	result, err := c.amend(ctx, domain.GraphAmendment{
		ID:               parsed.key,
		RunID:            summary.Run.ID,
		ExpectedRevision: summary.Run.GraphRevision,
		Operation:        "rerun",
		TaskID:           parsed.from,
		Reason:           reason,
		Prompt:           parsed.prompt,
	})
	if err != nil {
		return err
	}
	document := campaignRerunDocument(parsed, summary.Run.ID, result)
	if parsed.asJSON {
		return encodeCampaignJSON(c.stdout, document)
	}
	return renderCampaignRerun(c.stdout, document)
}

func campaignRerunDocument(parsed campaignRerunArgs, sourceRunID string, result domain.GraphAmendmentResult) campaignRerun {
	document := campaignRerun{
		SchemaVersion: campaignRerunSchemaVersion,
		SourceRunID:   sourceRunID,
		SourceTaskID:  parsed.from,
		RunID:         result.Run.ID,
		WorkflowID:    result.Run.WorkflowID,
		Rerun:         []string{},
		Replay:        result.Replay,
	}
	if result.Graph.RerunOf != nil {
		document.Provenance = *result.Graph.RerunOf
		document.SourceTaskID = result.Graph.RerunOf.SourceTaskID
	}
	for _, task := range result.Graph.Tasks {
		document.Rerun = append(document.Rerun, task.Name)
		for _, carried := range task.CarriedInputs {
			document.Carried = append(document.Carried, campaignCarried{
				Task: task.Name, Producer: carried.Producer,
				Name: carried.Name, ArtifactID: carried.ArtifactID,
			})
		}
	}
	return document
}

func renderCampaignRerun(out interface{ Write([]byte) (int, error) }, document campaignRerun) error {
	// The first line is the run, as on "task run" and "campaign submit".
	if _, err := fmt.Fprintf(out,
		"run %s\nrerun %s: run=%s workflow=%s replay=%t\n  source  %s from task %s\n  reruns  %s\n",
		document.RunID, document.Provenance.IdempotencyKey, document.RunID, document.WorkflowID, document.Replay,
		document.SourceRunID, document.SourceTaskID, campaignList(document.Rerun)); err != nil {
		return err
	}
	for _, carried := range document.Carried {
		if _, err := fmt.Fprintf(out, "  carried %s <- %s/%s by reference\n",
			carried.Task, carried.Producer, carried.Name); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(out,
		"the source run is unchanged and still readable:\n  t3-steward campaign show %s\nnext:\n  t3-steward campaign show %s\n",
		document.SourceRunID, document.RunID)
	return err
}

// describeCampaignRun reads one run over the same admin transport every other
// live campaign verb uses.
func describeCampaignRun(ctx context.Context, cfg config.Config, runID string) (backlogadmin.WorkflowSummary, error) {
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return backlogadmin.WorkflowSummary{}, err
	}
	response, err := transport.client.Query(ctx, backlogadmin.Query{
		Version:       backlogadmin.Version,
		Kind:          backlogadmin.QueryWorkflow,
		Principal:     transport.principal,
		WorkflowRunID: runID,
	})
	if err != nil {
		return backlogadmin.WorkflowSummary{}, err
	}
	if response.Workflow == nil {
		return backlogadmin.WorkflowSummary{}, &backlogadmin.TransportError{
			Class:     backlogadmin.ClassRejected,
			Operation: "workflow",
			Err:       fmt.Errorf("the coordinator knows no run %q", runID),
		}
	}
	return response.Workflow.Summary, nil
}
