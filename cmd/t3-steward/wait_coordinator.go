package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// coordinatorWaitSpec is a parsed `wait add` for a coordinator kind: node or
// quota. The coordinator settles these from its own records; the worker
// registers no local check.
type coordinatorWaitSpec struct {
	Kind  domain.WaitKind
	Node  *domain.NodeWaitCondition
	Quota *domain.QuotaWaitCondition

	Name, Condition string
	Timeout         time.Duration
	OrTimeout       bool
	WakeMode        string
	Thread, Group   string
	RequestID       string
	JSON            bool
	// Task is the --task value: current for a task-bound wait, empty for an
	// interactive one.
	Task string
}

// coordinatorWaitArgs reports whether the arguments name a coordinator kind.
func coordinatorWaitArgs(args []string) bool {
	for _, arg := range args {
		if arg == "--" {
			return false
		}
		for _, name := range []string{"--node", "-node", "--quota", "-quota"} {
			if arg == name || strings.HasPrefix(arg, name+"=") {
				return true
			}
		}
	}
	return false
}

// parseCoordinatorWaitSpec parses the flags of `wait add` for the coordinator
// kinds. Exactly one of --node and --quota names the kind; a command, --at,
// --for or --github alongside is refused, because one wait has one kind.
func parseCoordinatorWaitSpec(args []string) (coordinatorWaitSpec, error) {
	var spec coordinatorWaitSpec
	fs := flag.NewFlagSet("wait add", flag.ContinueOnError)
	fs.SetOutput(discardWriter{})
	fs.StringVar(&spec.Task, "task", "", "current")
	fs.StringVar(&spec.Name, "name", "", "what is being waited for")
	fs.DurationVar(&spec.Timeout, "timeout", 24*time.Hour, "deadline")
	fs.BoolVar(&spec.OrTimeout, "or-timeout", false, "treat the deadline as a normal outcome")
	fs.StringVar(&spec.WakeMode, "wake", "each", "each or all")
	fs.StringVar(&spec.Thread, "thread", "", "thread id")
	fs.StringVar(&spec.Group, "group", "", "group name")
	fs.StringVar(&spec.RequestID, "request-id", "", "stable registration ID")
	fs.BoolVar(&spec.JSON, "json", false, "print the registered wait as JSON")
	node := fs.String("node", "", "<run>[/<task>]")
	state := fs.String("state", "", "node state")
	quota := fs.String("quota", "", "quota pool")
	below := fs.String("below", "", "usage percent the pool must be below")
	phase := fs.String("phase", "", "phase the pool must be in (normal)")
	reset := fs.Bool("reset", false, "wait for the pool's window to reset")
	// The local-kind flags are declared so they are refused by name rather
	// than reported as unknown.
	at := fs.String("at", "", "")
	after := fs.String("for", "", "")
	github := fs.String("github", "", "")
	if err := fs.Parse(normalizeKindArgs(args)); err != nil {
		return spec, err
	}
	if *at != "" || *after != "" || *github != "" || fs.NArg() != 0 {
		return spec, errors.New("one wait has one kind; --node and --quota take no command, --at, --for or --github")
	}
	if spec.WakeMode != "each" && spec.WakeMode != "all" {
		return spec, errors.New("--wake must be each or all")
	}
	if spec.Timeout <= 0 || spec.Timeout > domain.MaxTaskWaitDuration {
		return spec, fmt.Errorf("--timeout must be above zero and at most %s", domain.MaxTaskWaitDuration)
	}
	if *node != "" && *quota != "" {
		return spec, errors.New("one wait has one kind; give --node or --quota, not both")
	}
	if *quota == "" && (*below != "" || *phase != "" || *reset) {
		return spec, errors.New("--below, --phase and --reset belong to --quota")
	}
	if *node == "" && *state != "" {
		return spec, errors.New("--state belongs to --node (and to --github)")
	}
	switch {
	case *quota != "":
		condition := domain.QuotaWaitCondition{Pool: strings.TrimSpace(*quota), Phase: domain.Phase(*phase), Reset: *reset}
		if *below != "" {
			value, err := strconv.ParseFloat(strings.TrimSuffix(*below, "%"), 64)
			if err != nil {
				return spec, fmt.Errorf("--below %q is not a percent: %w", *below, err)
			}
			condition.Below = &value
		}
		if err := condition.Validate(); err != nil {
			return spec, err
		}
		spec.Kind = domain.WaitKindQuota
		spec.Quota = &condition
		spec.Condition = condition.String()
	case *node != "":
		target, err := parseNodeTarget(*node)
		if err != nil {
			return spec, err
		}
		parsedState, err := domain.ParseNodeWaitState(*state)
		if err != nil {
			return spec, err
		}
		spec.Kind = domain.WaitKindNode
		spec.Node = &domain.NodeWaitCondition{Target: target, State: parsedState}
		spec.Condition = spec.Node.String()
	default:
		return spec, errors.New("--node <run>[/<task>] or --quota <pool> names the coordinator-settled wait")
	}
	if spec.Name == "" {
		spec.Name = spec.Condition
	}
	return spec, nil
}

// parseNodeTarget reads <run> (the run's sink) or <run>/<task>.
func parseNodeTarget(value string) (domain.NodeRef, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return domain.NodeRef{}, errors.New("--node needs <run> or <run>/<task>")
	}
	if !strings.Contains(value, "/") {
		return domain.NodeRef{RunID: value, TaskID: domain.SinkTaskName}, nil
	}
	return domain.ParseNodeRef(value)
}

// cmdCoordinatorWaitAdd registers an interactive wait of a coordinator kind
// (--node or --quota) as a native wait: the coordinator holds and settles
// it, and this host's runner delivers the wake.
func cmdCoordinatorWaitAdd(ctx context.Context, cfg config.Config, client coordinatorNodeWaitClient, args []string) error {
	spec, err := parseCoordinatorWaitSpec(args)
	if err != nil {
		return err
	}
	if spec.Task != "" {
		return errors.New("--task current is a task-bound wait and is routed before this point")
	}
	if spec.WakeMode == "all" && spec.Group == "" {
		return errors.New("--wake all needs --group")
	}
	threadID, err := resolveThread(cfg, spec.Thread)
	if err != nil {
		return err
	}
	logger := newLogger("error")
	t3, _, err := connectForCallerThread(cfg, logger)
	if err != nil {
		return err
	}
	cctx, cancel := context.WithTimeout(ctx, cfg.T3.RequestTimeout.D())
	defer cancel()
	target, err := t3control.New(t3, logger, true).GetThread(cctx, threadID)
	if err != nil {
		return err
	}
	if target == nil || target.ArchivedAt != nil {
		return errors.New("wait thread is unavailable on this host")
	}
	if spec.RequestID == "" {
		spec.RequestID = "nw-" + strings.TrimPrefix(newWaitID(), "w-")
	}
	if spec.Group != "" {
		// The local checks of this thread are the other side of a mixed group.
		statePath, err := cfg.ResolveStatePath()
		if err != nil {
			return err
		}
		store, err := sqlite.Open(statePath)
		if err != nil {
			return err
		}
		local, err := store.ListWaits(ctx, threadID)
		store.Close()
		if err != nil {
			return err
		}
		if err := refuseMixedGroup(threadID, spec.Group, spec.Kind, local, nil); err != nil {
			return err
		}
	}
	result, err := client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: "register", Request: coordinatorWaitRequest(spec, threadID)})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}
	if len(result.Waits) == 1 && result.Waits[0].Delivery != "delivered" && result.Waits[0].Delivery != "cancelled" {
		fmt.Fprintln(os.Stderr, "End this turn now; the coordinator has registered the wait.")
	}
	return nil
}

// coordinatorWaitRequest is the native wait registration of an interactive
// coordinator-kind spec. Every flag the spec parsed that the coordinator
// settles on travels here; the default node state is left empty so the
// registration is unchanged for a coordinator that predates states.
func coordinatorWaitRequest(spec coordinatorWaitSpec, threadID string) domain.NodeWaitRequest {
	request := domain.NodeWaitRequest{
		ID: spec.RequestID, ThreadID: threadID, Name: spec.Name, Timeout: spec.Timeout, OrTimeout: spec.OrTimeout,
		Quota: spec.Quota, Group: spec.Group, Wake: domain.WakeMode(spec.WakeMode),
	}
	if spec.Node != nil {
		request.Target = spec.Node.Target
		if spec.Node.State != domain.NodeStateTerminal {
			request.State = spec.Node.State
		}
	}
	return request
}

// refuseMixedGroup refuses a registration into an interactive --group that
// already holds a live member from the other side: a local kind into a group
// with a coordinator wait, or a coordinator kind into a group with a local
// check. The two sides settle on different hosts and the contract does not
// guarantee a mixed group. The refusal names both members.
func refuseMixedGroup(threadID, group string, kind domain.WaitKind, local []wait.Wait, native []domain.NodeWait) error {
	if group == "" {
		return nil
	}
	coordinator := kind.OrShell().Coordinator()
	for _, w := range local {
		if w.ThreadID != threadID || w.Group != group || w.Status != wait.StatusWaiting || !coordinator {
			continue
		}
		return fmt.Errorf("group %q on thread %s already holds local wait %s (%s); a %s wait is a coordinator kind and cannot join it: use another --group, or wait for %s to settle",
			group, threadID, w.ID, w.Kind.OrShell(), kind, w.ID)
	}
	for _, w := range native {
		if w.Request.ThreadID != threadID || w.Request.Group != group || w.Delivery == "delivered" || w.Delivery == "cancelled" || coordinator {
			continue
		}
		return fmt.Errorf("group %q on thread %s already holds coordinator wait %s (%s); a %s wait is a local kind and cannot join it: use another --group, or wait for %s to settle",
			group, threadID, w.Request.ID, w.Request.Kind(), kind.OrShell(), w.Request.ID)
	}
	return nil
}

// coordinatorNodeWaitClient is the transport method the coordinator kinds use.
type coordinatorNodeWaitClient interface {
	NodeWait(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error)
}

// cmdTaskCoordinatorWaitAdd registers a task-bound wait of a coordinator
// kind: the coordinator parks the attempt and settles the condition from its
// own records; nothing is registered on this host.
func cmdTaskCoordinatorWaitAdd(ctx context.Context, cfg config.Config, args []string) error {
	spec, err := parseCoordinatorWaitSpec(args)
	if err != nil {
		return err
	}
	if spec.Task != "current" {
		return errors.New("cmdTaskCoordinatorWaitAdd requires --task current")
	}
	if spec.Thread != "" || spec.Group != "" {
		return errors.New("--thread and --group do not apply to --task current: the wait is bound to this task's own thread, and --wake all is scoped to the attempt")
	}
	identity, err := resolveTaskIdentity(os.Getenv)
	if err != nil {
		return err
	}
	spec.RequestID = taskWaitRequestID(spec.RequestID, identity, os.Stderr)
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	response, err := transport.client.NodeWait(ctx, backlogadmin.NodeWaitOperation{
		Action: "register-task",
		Task: &domain.TaskWaitRegistration{
			RequestID: spec.RequestID, WorkflowRunID: identity.WorkflowRunID, TaskID: identity.TaskID,
			AttemptID: identity.AttemptID, IssuedRevision: identity.AttemptRevision,
			ThreadID: identity.ThreadID, Wake: domain.WakeMode(spec.WakeMode), MaxDuration: spec.Timeout,
			Name: spec.Name, Condition: spec.Condition, Kind: spec.Kind, OrTimeout: spec.OrTimeout,
			Node: spec.Node, Quota: spec.Quota,
		},
	})
	if err != nil {
		return err
	}
	if len(response.TaskWaits) != 1 {
		return errors.New("the coordinator did not return exactly one task-bound wait")
	}
	registered := response.TaskWaits[0]
	if !registered.Live() {
		return fmt.Errorf("task-bound wait %s is already settled, so this task is not parked; register a new wait with a different --request-id", registered.ID)
	}
	if spec.JSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(registered); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "This task is now parked. End this turn now: nothing is collected and nothing is verified")
		fmt.Fprintln(os.Stderr, "until the steward resumes this same thread with the outcome.")
		return nil
	}
	fmt.Printf("task-bound wait %s (%s) registered for attempt %s on thread %s: %s; settled by the coordinator from its own records, giving up after %s.\n",
		registered.ID, spec.Kind, registered.AttemptID, registered.ThreadID, spec.Condition, spec.Timeout)
	fmt.Println("This task is now parked. End this turn now: nothing is collected and nothing is verified")
	fmt.Println("until the steward resumes this same thread with the outcome.")
	return nil
}
