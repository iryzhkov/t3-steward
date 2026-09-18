package domain

import (
	"fmt"
	"strings"
)

// WaitKind names how a wait is settled and what its wake carries.
//
// The local kinds are settled on the registering host by its wait runner, with
// a local check row and no coordinator involvement beyond the task-wait
// registration that parks an attempt. The coordinator kinds are settled by the
// coordinator from its own records, with no local check at all.
type WaitKind string

const (
	// WaitKindShell polls a command: exit 0 is met, exit 2 gives up, anything
	// else is not yet.
	WaitKindShell WaitKind = "shell"
	// WaitKindTime is met when a wall-clock instant passes.
	WaitKindTime WaitKind = "time"
	// WaitKindGitHub observes a GitHub workflow run or pull request through gh.
	WaitKindGitHub WaitKind = "github"
	// WaitKindNode observes a workflow run or task in the coordinator's records.
	WaitKindNode WaitKind = "node"
	// WaitKindQuota observes a quota pool's merged bucket observations.
	WaitKindQuota WaitKind = "quota"
)

// Local reports whether the registering host's wait runner settles this kind.
func (k WaitKind) Local() bool {
	switch k {
	case WaitKindShell, WaitKindTime, WaitKindGitHub, "":
		return true
	}
	return false
}

// Coordinator reports whether the coordinator settles this kind from its own
// records.
func (k WaitKind) Coordinator() bool {
	return k == WaitKindNode || k == WaitKindQuota
}

// OrShell is the kind with the historical default applied: a record written
// before kinds existed is a shell wait.
func (k WaitKind) OrShell() WaitKind {
	if k == "" {
		return WaitKindShell
	}
	return k
}

// WaitKinds lists every kind, local first, in the order help describes them.
func WaitKinds() []WaitKind {
	return []WaitKind{WaitKindShell, WaitKindTime, WaitKindGitHub, WaitKindNode, WaitKindQuota}
}

// NodeWaitState is the state a node wait waits for.
type NodeWaitState string

const (
	// NodeStateTerminal is met when the node reaches any terminal progress
	// other than cancelled, which is the cancelled outcome. It is the default,
	// and what a campaign notification waits for; the wake carries progress=
	// and, for a failed run, failed=.
	NodeStateTerminal NodeWaitState = "terminal"
	// NodeStateSucceeded is met on success and failed on any other terminal
	// progress except cancelled.
	NodeStateSucceeded NodeWaitState = "succeeded"
	// NodeStatePaused is met when the latest attempt is paused, by its
	// coordinator control or by a quota pause the worker reports.
	NodeStatePaused NodeWaitState = "paused"
	// NodeStateWaitingExternal is met when the latest attempt is parked on a
	// task-bound wait.
	NodeStateWaitingExternal NodeWaitState = "waiting-external"
	// NodeStateActive is met when the latest attempt is running a turn.
	NodeStateActive NodeWaitState = "active"
)

// NodeWaitStates lists the states in the order help describes them.
func NodeWaitStates() []NodeWaitState {
	return []NodeWaitState{NodeStateTerminal, NodeStateSucceeded, NodeStatePaused, NodeStateWaitingExternal, NodeStateActive}
}

// ParseNodeWaitState reads a --state value; empty is terminal.
func ParseNodeWaitState(value string) (NodeWaitState, error) {
	if value == "" {
		return NodeStateTerminal, nil
	}
	for _, state := range NodeWaitStates() {
		if NodeWaitState(value) == state {
			return state, nil
		}
	}
	return "", fmt.Errorf("--state %q is not a node state; the states are %s", value, joinNodeWaitStates())
}

func joinNodeWaitStates() string {
	names := make([]string, 0, 5)
	for _, state := range NodeWaitStates() {
		names = append(names, string(state))
	}
	return strings.Join(names, ", ")
}

// Terminal reports whether the state is one of the run-level terminal states
// a sink can reach; the others need an attempt.
func (s NodeWaitState) Terminal() bool {
	return s == "" || s == NodeStateTerminal || s == NodeStateSucceeded
}

// NodeWaitCondition is the structured condition of a node wait: a target and
// the state waited for. It is what a task-bound node wait carries instead of
// a shell command, and what the coordinator's settlement pass evaluates.
type NodeWaitCondition struct {
	Target NodeRef       `json:"target"`
	State  NodeWaitState `json:"state"`
}

// String is the condition text: "node <run>/<task> <state>".
func (c NodeWaitCondition) String() string {
	state := c.State
	if state == "" {
		state = NodeStateTerminal
	}
	return "node " + c.Target.String() + " " + string(state)
}
