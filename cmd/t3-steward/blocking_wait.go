package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/blockingwait"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// All blocking verbs use these local-wait exit codes. Transport failures keep
// their existing codes. An interrupted wait has no effect on coordinator work.
func blockingWaitError(err error, reattach string) error {
	if err == nil {
		return nil
	}
	var transport *backlogadmin.TransportError
	if errors.As(err, &transport) {
		return fmt.Errorf("%w; work continues; reattach with %s", err, reattach)
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return exitCodeError{code: 1, error: fmt.Errorf("wait timeout; work continues; reattach with %s", reattach)}
	case errors.Is(err, context.Canceled):
		return exitCodeError{code: 130, error: fmt.Errorf("wait interrupted; work continues; reattach with %s", reattach)}
	default:
		return fmt.Errorf("%w; work continues; reattach with %s", err, reattach)
	}
}

// waitForCommand only queries the immutable command ID; no mutation is replayed.
func waitForCommand(ctx context.Context, opts blockingwait.Options, response backlogadmin.MutationResponse,
	query func(context.Context, backlogadmin.Query) (backlogadmin.Response, error)) (backlogadmin.MutationResponse, error) {
	if !opts.Enabled || response.Command.State != domain.AdminCommandPending {
		return response, nil
	}
	id := response.Command.ID
	if query == nil {
		return response, errors.New("coordinator command query transport is unavailable")
	}
	err := blockingwait.Run(ctx, opts.Timeout, func(ctx context.Context) (bool, error) {
		result, err := query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryCommands, CommandID: id})
		if err != nil {
			return false, err
		}
		for _, command := range result.Commands {
			if command.ID == id {
				response.Command = command
				// The submission event and target snapshot describe intake, not application.
				response.Event = backlogadmin.Event{}
				response.CurrentTarget = nil
				return command.State != domain.AdminCommandPending, nil
			}
		}
		return false, fmt.Errorf("coordinator returned no command %q", id)
	})
	return response, blockingWaitError(err, "t3-steward backlog command show "+id+" --wait")
}

func (c backlogAdminCLI) finishMutation(ctx context.Context, invocation adminMutationInvocation, response backlogadmin.MutationResponse, note string) error {
	response, waitErr := waitForCommand(ctx, invocation.wait, response, c.ask)
	if err := c.renderMutation(invocation, response, note); err != nil {
		return err
	}
	return afterDocument(waitErr)
}

func (c backlogAdminCLI) runCommandWait(ctx context.Context, args []string, opts blockingwait.Options) error {
	query, display, err := parseBacklogAdminQuery(args)
	if err != nil {
		return err
	}
	var response backlogadmin.Response
	result, waitErr := waitForCommand(ctx, opts, backlogadmin.MutationResponse{Command: backlogadmin.Command{ID: query.CommandID, State: domain.AdminCommandPending}},
		func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
			latest, err := c.ask(ctx, q)
			if err == nil {
				response = latest
			}
			return latest, err
		})
	if len(response.Commands) == 0 {
		return waitErr
	}
	// Preserve the existing command-show response shape and rendering.
	response.Commands = []backlogadmin.Command{result.Command}
	if display.JSON {
		if err := encodeCampaignJSON(c.stdout, response); err != nil {
			return err
		}
	} else {
		if err := renderAdminResponse(c.stdout, response, "", display); err != nil {
			return err
		}
	}
	return afterDocument(waitErr)
}

func (c campaignCLI) waitForShow(ctx context.Context, args []string) ([]string, error) {
	clean, opts, err := blockingwait.Parse(args)
	if err != nil || !opts.Enabled {
		return clean, err
	}
	// Validate all flags before polling or printing.
	query, _, err := parseBacklogAdminQuery(clean)
	if err != nil {
		return nil, err
	}
	if c.detail == nil {
		return nil, errors.New("coordinator workflow query transport is unavailable")
	}
	err = blockingwait.Run(ctx, opts.Timeout, func(ctx context.Context) (bool, error) {
		detail, err := c.detail(ctx, query.WorkflowRunID)
		return detail.Summary.Run.Progress.Terminal(), err
	})
	return clean, blockingWaitError(err, "t3-steward campaign show "+query.WorkflowRunID+" --wait")
}
