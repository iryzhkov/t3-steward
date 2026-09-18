package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// campaignCancelRunDocument is what the run form of cancel prints. It names
// the tasks the one command covers, because the command's own target is a
// single anchor attempt and an operator reading only that would not know what
// else it took with it.
type campaignCancelRunDocument struct {
	Run     string               `json:"run"`
	Tasks   []string             `json:"tasks"`
	Anchor  string               `json:"anchorAttempt"`
	Command backlogadmin.Command `json:"command"`
	Event   backlogadmin.Event   `json:"event,omitzero"`
}

// describeCampaignRunDetail reads one run with its tasks and attempts over the
// same admin transport every other live campaign verb uses.
func describeCampaignRunDetail(ctx context.Context, cfg config.Config, runID string) (backlogadmin.WorkflowDetail, error) {
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return backlogadmin.WorkflowDetail{}, err
	}
	response, err := transport.client.Query(ctx, backlogadmin.Query{
		Version:       backlogadmin.Version,
		Kind:          backlogadmin.QueryWorkflow,
		Principal:     transport.principal,
		WorkflowRunID: runID,
	})
	if err != nil {
		return backlogadmin.WorkflowDetail{}, err
	}
	if response.Workflow == nil {
		return backlogadmin.WorkflowDetail{}, &backlogadmin.TransportError{
			Class:     backlogadmin.ClassRejected,
			Operation: "workflow",
			Err:       fmt.Errorf("the coordinator knows no run %q", runID),
		}
	}
	return *response.Workflow, nil
}

// isCampaignRunCancel reports the run form: "cancel <run>" with no task.
func isCampaignRunCancel(args []string) bool {
	return len(args) >= 2 && args[0] == "cancel" &&
		strings.TrimSpace(args[1]) != "" && !strings.Contains(args[1], "/") &&
		!strings.HasPrefix(args[1], "-")
}

// runCampaignCancelRun cancels every non-terminal task of one run with one
// command. The task form, "cancel <run>/<task>", is unchanged and still
// forwards to the backlog path.
func (c campaignCLI) runCampaignCancelRun(ctx context.Context, args []string) error {
	runID := args[1]
	reason, commandID, asJSON, err := parseCampaignCancelRunArgs(args[2:])
	if err != nil {
		return err
	}
	if c.detail == nil || c.mutate == nil {
		return errors.New("coordinator admin transport is unavailable")
	}
	detail, err := c.detail(ctx, runID)
	if err != nil {
		return err
	}
	attempts := make([]domain.Attempt, 0, len(detail.Tasks))
	names := map[string]string{}
	for _, task := range detail.Tasks {
		if task.Sink != nil || task.Attempt == nil {
			continue
		}
		attempt := *task.Attempt
		attempt.WorkflowRunID = runID
		attempts = append(attempts, attempt)
		names[attempt.ID] = task.Task.Name
	}
	anchor, ok := backlogadmin.RunCancelAnchorID(attempts, runID)
	if !ok {
		return fmt.Errorf("run %s has no task to cancel; every task of it is already terminal", runID)
	}
	var cancelled []string
	var expectedRevision int64
	for _, attempt := range attempts {
		if attempt.Progress.Terminal() {
			continue
		}
		cancelled = append(cancelled, names[attempt.ID])
		if attempt.ID == anchor {
			expectedRevision = attempt.Revision
		}
	}
	if commandID == "" {
		commandID, err = newAdminCommandID()
		if err != nil {
			return fmt.Errorf("create command id: %w", err)
		}
	}
	response, err := c.mutate(ctx, backlogadmin.Mutation{
		Version: backlogadmin.Version, Principal: backlogadmin.Principal{ID: c.submissionPrincipal()},
		ID: commandID, Kind: domain.AdminCommandCancel, WorkflowRunID: runID,
		ExpectedRevision: expectedRevision, Reason: reason,
		Payload: json.RawMessage(`{"scope":"` + backlogadmin.MutationScopeRun + `"}`),
	})
	if err != nil {
		return err
	}
	document := campaignCancelRunDocument{
		Run: runID, Tasks: cancelled, Anchor: anchor,
		Command: response.Command, Event: response.Event,
	}
	if asJSON {
		return encodeCampaignJSON(c.stdout, document)
	}
	fmt.Fprintf(c.stdout, "command %s cancels %d task(s) of run %s: %s\n",
		response.Command.ID, len(cancelled), runID, campaignList(cancelled))
	fmt.Fprintf(c.stdout, "state %s, fenced on attempt %s at revision %d\n",
		response.Command.State, anchor, expectedRevision)
	if response.Command.Failure != "" {
		fmt.Fprintf(c.stdout, "failure: %s\n", response.Command.Failure)
	}
	_, err = fmt.Fprintf(c.stdout, "next:\n  t3-steward campaign show %s\n", runID)
	return err
}

// parseCampaignCancelRunArgs takes the flags of the run form. --reason is
// required, as it is for every mutating verb: a cancellation nobody can
// explain later is what the reason exists to prevent.
func parseCampaignCancelRunArgs(args []string) (string, string, bool, error) {
	clean, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return "", "", false, err
	}
	reason, commandID := "", ""
	for i := 0; i < len(clean); i++ {
		switch clean[i] {
		case "--reason", "--command-id":
			if i+1 >= len(clean) || strings.TrimSpace(clean[i+1]) == "" {
				return "", "", false, fmt.Errorf("%s needs a value", clean[i])
			}
			if clean[i] == "--reason" {
				reason = clean[i+1]
			} else {
				commandID = clean[i+1]
			}
			i++
		default:
			return "", "", false, fmt.Errorf("campaign cancel usage: cancel <run>[/<task>] --reason TEXT [--command-id ID] [--json] (got %q)", clean[i])
		}
	}
	if reason == "" {
		return "", "", false, errors.New("campaign cancel needs --reason TEXT")
	}
	return reason, commandID, asJSON, nil
}
