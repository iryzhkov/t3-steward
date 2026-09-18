package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type adminMutationService interface {
	Mutate(context.Context, backlogadmin.Mutation) (backlogadmin.MutationResponse, error)
}

type adminMutationInvocation struct {
	kind          domain.AdminCommandKind
	workflowRunID string
	taskID        string
	scheduleID    string
	reason        string
	commandID     string
	payload       json.RawMessage
	asJSON        bool
	// expectedRevision is the operator's explicit fence; nil means the CLI
	// reads the current revision itself and may re-read it once after a
	// stale-revision rejection.
	expectedRevision *int64
}

func isBacklogMutation(command string) bool {
	switch command {
	case "start", "delay", "pause", "resume", "cancel", "retry", "skip", "rewake":
		return true
	default:
		return false
	}
}

func isScheduleMutation(command string) bool {
	switch command {
	case "run", "delay-next", "enable", "disable":
		return true
	default:
		return false
	}
}

func parseBacklogMutation(args []string) (adminMutationInvocation, error) {
	if len(args) < 2 || !isBacklogMutation(args[0]) {
		return adminMutationInvocation{}, errors.New("backlog control usage: <start|delay|pause|resume|cancel|retry|skip|rewake> <workflow-run>/<task> --reason <reason> [--expected-revision N] [--command-id ID] [--json]")
	}
	runID, taskID, err := splitTaskTarget(args[1])
	if err != nil {
		return adminMutationInvocation{}, err
	}
	invocation := adminMutationInvocation{
		kind: domain.AdminCommandKind(args[0]), workflowRunID: runID, taskID: taskID,
	}
	if err := parseMutationOptions(args[2:], &invocation); err != nil {
		return adminMutationInvocation{}, err
	}
	if err := validateMutationOptions(invocation); err != nil {
		return adminMutationInvocation{}, err
	}
	return invocation, nil
}

func parseScheduleMutation(args []string) (adminMutationInvocation, error) {
	if len(args) < 2 || !isScheduleMutation(args[0]) {
		return adminMutationInvocation{}, errors.New("schedule control usage: <run|delay-next|enable|disable> <schedule> --reason <reason>")
	}
	if strings.TrimSpace(args[1]) != args[1] || args[1] == "" || strings.Contains(args[1], "/") {
		return adminMutationInvocation{}, fmt.Errorf("invalid schedule id %q", args[1])
	}
	kind := domain.AdminCommandKind(args[0])
	if args[0] == "run" {
		kind = domain.AdminCommandScheduleRun
	}
	invocation := adminMutationInvocation{kind: kind, scheduleID: args[1]}
	if err := parseMutationOptions(args[2:], &invocation); err != nil {
		return adminMutationInvocation{}, err
	}
	if err := validateMutationOptions(invocation); err != nil {
		return adminMutationInvocation{}, err
	}
	return invocation, nil
}

func parseMutationOptions(args []string, invocation *adminMutationInvocation) error {
	var until string
	now := false
	for len(args) > 0 {
		switch args[0] {
		case "--json":
			if invocation.asJSON {
				return errors.New("--json may only be specified once")
			}
			invocation.asJSON = true
			args = args[1:]
		case "--now":
			if now {
				return errors.New("--now may only be specified once")
			}
			now = true
			args = args[1:]
		case "--reason", "--command-id", "--until", "--expected-revision":
			if len(args) < 2 || args[1] == "" {
				return fmt.Errorf("%s needs a value", args[0])
			}
			switch args[0] {
			case "--expected-revision":
				if invocation.expectedRevision != nil {
					return errors.New("--expected-revision may only be specified once")
				}
				value, err := strconv.ParseInt(args[1], 10, 64)
				if err != nil || value < 0 {
					return fmt.Errorf("--expected-revision must be a non-negative integer, got %q", args[1])
				}
				invocation.expectedRevision = &value
			case "--reason":
				if invocation.reason != "" {
					return errors.New("--reason may only be specified once")
				}
				invocation.reason = args[1]
			case "--command-id":
				if invocation.commandID != "" {
					return errors.New("--command-id may only be specified once")
				}
				invocation.commandID = args[1]
			case "--until":
				if until != "" {
					return errors.New("--until may only be specified once")
				}
				until = args[1]
			}
			args = args[2:]
		default:
			return fmt.Errorf("unknown control flag %q", args[0])
		}
	}
	if strings.TrimSpace(invocation.reason) != invocation.reason || invocation.reason == "" {
		return errors.New("--reason is required and cannot have surrounding whitespace")
	}
	if strings.TrimSpace(invocation.commandID) != invocation.commandID {
		return errors.New("--command-id cannot have surrounding whitespace")
	}
	switch {
	case invocation.kind == domain.AdminCommandPause:
		invocation.payload = mustJSON(map[string]bool{"now": now})
	case now:
		return errors.New("--now is only valid with pause")
	case invocation.kind == domain.AdminCommandDelay || invocation.kind == domain.AdminCommandDelayNext:
		if until == "" {
			return errors.New("--until is required for delay and delay-next")
		}
		value, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return fmt.Errorf("parse --until: %w", err)
		}
		invocation.payload = mustJSON(map[string]string{"until": value.UTC().Format(time.RFC3339)})
	case until != "":
		return errors.New("--until is only valid with delay or delay-next")
	}
	return nil
}

func validateMutationOptions(invocation adminMutationInvocation) error {
	if invocation.commandID != "" && strings.TrimSpace(invocation.commandID) != invocation.commandID {
		return errors.New("invalid command id")
	}
	return nil
}

func mustJSON(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}

func (c backlogAdminCLI) runBacklogMutation(ctx context.Context, args []string) error {
	invocation, err := parseBacklogMutation(args)
	if err != nil {
		return err
	}
	if invocation.expectedRevision != nil {
		return c.submitMutation(ctx, invocation, *invocation.expectedRevision)
	}
	revision, err := c.currentAttemptRevision(ctx, invocation)
	if err != nil {
		return err
	}
	// The revision is read and then submitted; the coordinator's own tick
	// can move it in between (a cancel it has just applied, a worker
	// observation). Without an explicit --expected-revision that race is
	// not the operator's to resolve: the target is re-read once and the
	// command resubmitted under a new id, and the response says so (S-4).
	// An explicit --command-id is a replay handle and is never rewritten.
	response, err := c.mutate(ctx, invocation, revision)
	if err != nil {
		return err
	}
	var note string
	if invocation.commandID == "" && staleRevisionRejection(response) {
		current, err := c.currentAttemptRevision(ctx, invocation)
		if err != nil {
			return err
		}
		if current != revision {
			note = fmt.Sprintf("revision %d was stale (%s); re-read the target and resubmitted at revision %d as command %%s", revision, response.Command.Failure, current)
			if response, err = c.mutate(ctx, invocation, current); err != nil {
				return err
			}
			note = fmt.Sprintf(note, response.Command.ID)
		}
	}
	return c.renderMutation(invocation, response, note)
}

// staleRevisionRejection reports a submission the coordinator refused because
// the expected revision was behind the target's current one.
func staleRevisionRejection(response backlogadmin.MutationResponse) bool {
	return response.Command.State == domain.AdminCommandRejected &&
		strings.HasPrefix(response.Command.Failure, "stale revision")
}

func (c backlogAdminCLI) currentAttemptRevision(ctx context.Context, invocation adminMutationInvocation) (int64, error) {
	response, err := c.service.Query(ctx, backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryTask, Principal: c.principal,
		WorkflowRunID: invocation.workflowRunID, TaskID: invocation.taskID,
	})
	if err != nil {
		return 0, err
	}
	if response.Task == nil || response.Task.Attempt == nil {
		return 0, fmt.Errorf("task %s/%s has no attempt to control", invocation.workflowRunID, invocation.taskID)
	}
	return response.Task.Attempt.Revision, nil
}

func (c backlogAdminCLI) runScheduleMutation(ctx context.Context, args []string) error {
	invocation, err := parseScheduleMutation(args)
	if err != nil {
		return err
	}
	response, err := c.service.Query(ctx, backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QuerySchedules, Principal: c.principal,
	})
	if err != nil {
		return err
	}
	for _, item := range response.Schedules {
		if item.Schedule.ID == invocation.scheduleID {
			return c.submitMutation(ctx, invocation, item.Schedule.Revision)
		}
	}
	return fmt.Errorf("%w: schedule %q", backlogadmin.ErrNotFound, invocation.scheduleID)
}

func (c backlogAdminCLI) submitMutation(ctx context.Context, invocation adminMutationInvocation, expectedRevision int64) error {
	response, err := c.mutate(ctx, invocation, expectedRevision)
	if err != nil {
		return err
	}
	return c.renderMutation(invocation, response, "")
}

// mutate submits one command under the given revision, with the operator's
// command id or a fresh one.
func (c backlogAdminCLI) mutate(ctx context.Context, invocation adminMutationInvocation, expectedRevision int64) (backlogadmin.MutationResponse, error) {
	if c.mutator == nil {
		return backlogadmin.MutationResponse{}, backlogadmin.ErrReadOnly
	}
	commandID := invocation.commandID
	if commandID == "" {
		generate := c.newCommandID
		if generate == nil {
			generate = newAdminCommandID
		}
		var err error
		commandID, err = generate()
		if err != nil {
			return backlogadmin.MutationResponse{}, fmt.Errorf("create command id: %w", err)
		}
	}
	return c.mutator.Mutate(ctx, backlogadmin.Mutation{
		Version: backlogadmin.Version, Principal: c.principal, ID: commandID,
		Kind: invocation.kind, WorkflowRunID: invocation.workflowRunID, TaskID: invocation.taskID,
		ScheduleID: invocation.scheduleID, ExpectedRevision: expectedRevision,
		Reason: invocation.reason, Payload: invocation.payload,
	})
}

// mutationDocument is the JSON form of a mutation result; Resubmitted is set
// when a stale-revision rejection was retried once with a re-read revision.
type mutationDocument struct {
	backlogadmin.MutationResponse
	Resubmitted string `json:"resubmitted,omitempty"`
}

func (c backlogAdminCLI) renderMutation(invocation adminMutationInvocation, response backlogadmin.MutationResponse, note string) error {
	if invocation.asJSON {
		encoder := json.NewEncoder(c.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(mutationDocument{MutationResponse: response, Resubmitted: note})
	}
	renderMutationResponse(c.stdout, response)
	if note != "" {
		fmt.Fprintf(c.stdout, "note: %s\n", note)
	}
	return nil
}

func renderMutationResponse(out io.Writer, response backlogadmin.MutationResponse) {
	command := response.Command
	fmt.Fprintf(out, "command: %s\nkind: %s\ntarget: %s/%s\nstate: %s\nexpected revision: %d\n",
		command.ID, command.Kind, command.TargetType, command.TargetID, command.State, command.ExpectedRevision)
	if command.Failure != "" {
		fmt.Fprintf(out, "failure: %s\n", command.Failure)
	}
	if response.CurrentTarget != nil {
		fmt.Fprintf(out, "current revision: %d\n", response.CurrentTarget.Revision)
	}
	fmt.Fprintf(out, "event: %s\n", response.Event.ID)
}

func newAdminCommandID() (string, error) {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "admin-" + hex.EncodeToString(value[:]), nil
}
