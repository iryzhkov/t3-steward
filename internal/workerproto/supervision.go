package workerproto

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// MaxActivationPromptBytes bounds the activation snapshot carried inside an
// execution package.
//
// The snapshot travels in the package rather than as an artifact because it is
// built for exactly one activation at exactly one epoch and is never read
// again: making it a retained artifact would mint one immutable object per
// wake-up for no reader. It is bounded for the same reason the prompt envelope
// it is rendered from is bounded, and the bound is checked here so an
// oversized snapshot is refused before it reaches a worker.
const MaxActivationPromptBytes = 64 << 10

// SupervisionAction is one action an activation is scoped to perform, and the
// exact command line that performs it.
//
// The command is carried verbatim because the CLI on the worker host is the
// overseer's only authority: it holds no other way to reach the coordinator,
// and a command it had to compose from prose is a command it can compose
// wrongly. The coordinator still authorizes every invocation on its own side;
// this is instruction, never permission.
type SupervisionAction struct {
	Name string `json:"name"`
	// Command is the exact argument vector, beginning with the CLI name.
	Command []string `json:"command"`
	// Constraints are the limits on this action, rendered for the overseer.
	Constraints []string `json:"constraints,omitempty"`
}

// SupervisionActivation is everything a worker needs to run one overseer
// activation, and nothing that would let it decide anything.
//
// A package carrying it is an activation package: it publishes no outputs,
// satisfies no verification, releases no dependents and is never collected as
// a task result. A worker that does not advertise CapabilityCampaignSupervision
// must never be sent one, which is why the capability is a worker inventory
// capability rather than a package capability: only the worker can say which
// build is running on its host.
type SupervisionActivation struct {
	ActivationID string `json:"activationId"`
	RunID        string `json:"runId"`
	Epoch        int64  `json:"epoch"`
	// RecordRevision is the supervision record revision every decision must
	// name as its expected revision.
	RecordRevision int64 `json:"recordRevision"`
	GraphRevision  int64 `json:"graphRevision"`
	// Principal is the supervisor identity the CLI authenticates as. It is
	// bound to this run and this epoch by the coordinator's authorizer; the
	// worker cannot widen it by sending a different one.
	Principal string `json:"principal"`
	// CredentialReference names the admin credential the CLI resolves on this
	// host, in the existing admin credential convention. It is a name, never a
	// secret.
	//
	// Co-tenancy limitation, stated rather than papered over: a full-access
	// campaign task running on the same host can read the same credential
	// store. Scope is enforced server-side by the coordinator's supervisor
	// authorizer, which is what actually bounds this capability to one run and
	// one epoch. This field does not isolate the credential and must not be
	// described as if it did.
	CredentialReference string `json:"credentialReference,omitempty"`
	// LeaseToken and LeaseExpiresAt are the coordinator-issued lease. Expiry
	// revokes decision authority immediately rather than after a grace period.
	LeaseToken     string    `json:"leaseToken"`
	LeaseExpiresAt time.Time `json:"leaseExpiresAt"`
	// Deadline is the activation's maximum elapsed time, which a lease renewal
	// extends never.
	Deadline time.Time `json:"deadline,omitempty"`
	// MaxTurns is max_turns_per_activation for this run.
	MaxTurns int `json:"maxTurns"`
	// Prompt is the rendered bounded activation snapshot.
	Prompt string `json:"prompt"`
	// Actions is the exact scoped action set. An action absent from it is not
	// available, whatever the prompt says.
	Actions []SupervisionAction `json:"actions"`
	// SubagentsDisabled reports that the worker was able to start this thread
	// with native subagent spawning off. It is evidence of what happened, not a
	// request: a false value means the harness offered no such switch, and no
	// caller may read it as a guarantee that delegation was prevented.
	SubagentsDisabled bool `json:"subagentsDisabled,omitempty"`
}

// The two environment variables an overseer thread is started with, naming the
// admin client its CLI must present to the coordinator.
//
// They exist because the prompt was not enough. An activation told the overseer
// "admin credential reference: X" in prose, and the CLI had no way to act on
// it: on a worker host the CLI authenticates with the credential named in
// backlog_v2.coordinator_client, which the coordinator knows as that host's
// ordinary remote-admin client. Since exactly one supervisor client exists
// fleet-wide, no configuration of two worker hosts could make an overseer on
// either of them authenticate as the supervisor, so every decision it tried was
// refused for unauthorized scope.
//
// They are a selector, never a secret. The value is a credential reference the
// CLI resolves through the same admin credential store it already uses, and a
// host without that secret fails to resolve it rather than gaining authority.
const (
	// SupervisorCredentialEnvironment names the admin credential reference the
	// CLI resolves for supervision commands, in place of this host's own
	// coordinator client credential.
	SupervisorCredentialEnvironment = "T3_STEWARD_SUPERVISOR_CREDENTIAL"
	// SupervisorClientEnvironment names the admin client the resolved credential
	// must belong to. The CLI refuses a credential that resolves to a different
	// principal, so a stale or mismatched reference is a refusal on this host
	// rather than an unauthorized-scope refusal one round trip later.
	SupervisorClientEnvironment = "T3_STEWARD_SUPERVISOR_CLIENT"
)

// ActivationEnvironment is the supervisor identity as the environment an
// overseer thread is started with. It is empty when the activation names no
// credential, which is the deployment that configured none.
func (a SupervisionActivation) ActivationEnvironment() map[string]string {
	if a.CredentialReference == "" {
		return nil
	}
	return map[string]string{
		SupervisorCredentialEnvironment: a.CredentialReference,
		SupervisorClientEnvironment:     a.Principal,
	}
}

// SupervisorIdentityDir and SupervisorIdentityFile name the worker-written
// record of the same two selectors, inside the activation workspace.
//
// It is the primary mechanism, and the thread environment stays as an
// additional channel. Passing an environment through the provider makes the
// overseer's identity depend on a field of the T3 create command that no tested
// release verifies, and which is therefore off by default and off on the fleet;
// an overseer started that way fell back to its host's own coordinator client
// and decided gates with full operator authority. A file the worker writes into
// a workspace it already owns depends on nothing outside this repository, and
// the CLI discovers it the way it already discovers a task's identity record.
//
// The directory is the one a task identity record already uses, so a workspace
// holds one private steward directory rather than two.
const (
	SupervisorIdentityDir  = ".t3-steward"
	SupervisorIdentityFile = SupervisorIdentityDir + "/supervisor.env"
)

// SupervisorIdentityNames lists the recorded selectors in a stable order.
func SupervisorIdentityNames() []string {
	return []string{SupervisorCredentialEnvironment, SupervisorClientEnvironment}
}

// RenderSupervisorIdentityFile writes the two selectors as KEY=value lines.
//
// It carries selectors and nothing else. No secret, no lease and no dispatch
// token: an overseer reading it learns which admin client to resolve, never how
// to claim an authority it was not given, and a host without that credential
// fails to resolve it rather than gaining one.
func RenderSupervisorIdentityFile(values map[string]string) (string, error) {
	var builder strings.Builder
	builder.WriteString("# Written by t3-steward. Selectors only: this file grants nothing.\n")
	for _, name := range SupervisorIdentityNames() {
		value, ok := values[name]
		if !ok || strings.TrimSpace(value) == "" {
			return "", fmt.Errorf("supervision identity record is missing %s", name)
		}
		if strings.ContainsAny(value, "\n\r\x00") {
			return "", fmt.Errorf("supervision identity value for %s contains a line break", name)
		}
		fmt.Fprintf(&builder, "%s=%s\n", name, value)
	}
	return builder.String(), nil
}

// ParseSupervisorIdentityFile reads the rendered form back. Unknown keys are
// refused rather than ignored: this file decides which client a command speaks
// for, and a reader that tolerates extra keys is a reader that can be fed
// something else.
func ParseSupervisorIdentityFile(content string) (map[string]string, error) {
	allowed := make(map[string]bool, len(SupervisorIdentityNames()))
	for _, name := range SupervisorIdentityNames() {
		allowed[name] = true
	}
	values := make(map[string]string, len(allowed))
	for number, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, found := strings.Cut(line, "=")
		if !found || !allowed[name] {
			return nil, fmt.Errorf("supervision identity record line %d is not a known selector", number+1)
		}
		if _, duplicate := values[name]; duplicate {
			return nil, fmt.Errorf("supervision identity record repeats %s", name)
		}
		values[name] = value
	}
	for name := range allowed {
		if strings.TrimSpace(values[name]) == "" {
			return nil, fmt.Errorf("supervision identity record is missing %s", name)
		}
	}
	return values, nil
}

// ActivationIdentityRecord renders this activation's supervision identity
// record, or reports that the activation names no credential and therefore has
// no record to write.
func (a SupervisionActivation) ActivationIdentityRecord() (string, bool, error) {
	environment := a.ActivationEnvironment()
	if len(environment) == 0 {
		return "", false, nil
	}
	content, err := RenderSupervisorIdentityFile(environment)
	if err != nil {
		return "", false, err
	}
	return content, true, nil
}

// IsActivation reports whether this package carries an overseer activation
// rather than a declared task.
func (p ExecutionPackage) IsActivation() bool {
	return p.Supervision != nil
}

// validateSupervisionActivation rejects an activation package that a worker
// could not execute faithfully, or that carries a task's shape.
func validateSupervisionActivation(pkg ExecutionPackage) error {
	activation := pkg.Supervision
	if activation == nil {
		return nil
	}
	required := map[string]string{
		"activationId": activation.ActivationID,
		"runId":        activation.RunID,
		"principal":    activation.Principal,
		"leaseToken":   activation.LeaseToken,
	}
	for name, value := range required {
		if !identityPattern.MatchString(value) {
			return fmt.Errorf("execution package supervision: invalid %s", name)
		}
	}
	if activation.CredentialReference != "" && !identityPattern.MatchString(activation.CredentialReference) {
		return errors.New("execution package supervision: invalid credentialReference")
	}
	if activation.RunID != pkg.Identity.WorkflowRunID {
		return errors.New("execution package supervision: activation belongs to another run")
	}
	if activation.Epoch < 1 || activation.RecordRevision < 0 || activation.MaxTurns < 1 {
		return errors.New("execution package supervision: epoch, revision and turn budget must be positive")
	}
	if activation.LeaseExpiresAt.IsZero() {
		return errors.New("execution package supervision: the lease must expire")
	}
	if !activation.Deadline.IsZero() && activation.Deadline.Before(activation.LeaseExpiresAt) {
		return errors.New("execution package supervision: the lease outlives the activation deadline")
	}
	if strings.TrimSpace(activation.Prompt) == "" {
		return errors.New("execution package supervision: the activation snapshot is empty")
	}
	if len(activation.Prompt) > MaxActivationPromptBytes {
		return fmt.Errorf("execution package supervision: activation snapshot is %d bytes over its %d byte cap",
			len(activation.Prompt)-MaxActivationPromptBytes, MaxActivationPromptBytes)
	}
	if len(activation.Actions) == 0 {
		return errors.New("execution package supervision: an activation with no scoped action can do nothing")
	}
	names := make(map[string]struct{}, len(activation.Actions))
	for _, action := range activation.Actions {
		if strings.TrimSpace(action.Name) == "" || len(action.Command) == 0 {
			return errors.New("execution package supervision: a scoped action requires a name and a command")
		}
		if _, duplicate := names[action.Name]; duplicate {
			return fmt.Errorf("execution package supervision: duplicate scoped action %q", action.Name)
		}
		names[action.Name] = struct{}{}
		for _, argument := range action.Command {
			if strings.ContainsRune(argument, 0) {
				return errors.New("execution package supervision: scoped action command is malformed")
			}
		}
	}
	// An activation is not a task, so it must not arrive carrying a task's
	// obligations. Each of these would be silently unmet: nothing collects an
	// activation's outputs, nothing runs its verification, and it has no
	// producers to take dependencies from.
	if len(pkg.Outputs) != 0 || len(pkg.Verification) != 0 ||
		len(pkg.Dependencies) != 0 || len(pkg.Preflight) != 0 {
		return errors.New("execution package supervision: an activation declares no outputs, verification, dependencies or preflight")
	}
	// A fresh task-scoped workspace is what keeps an activation off every
	// workspace lock the run's own tasks need. An overseer waiting on a lock
	// held by the task it is reviewing is a deadlock with extra steps.
	if pkg.Environment.Type != "fresh" || pkg.Environment.Scope != "task" ||
		len(pkg.Environment.ResourceLocks) != 0 {
		return errors.New("execution package supervision: an activation runs in a fresh task-scoped workspace and holds no resource lock")
	}
	if !slices.Contains(pkg.RequiredCapabilities, CapabilityCampaignSupervision) {
		return fmt.Errorf("execution package supervision: an activation must require %q", CapabilityCampaignSupervision)
	}
	return nil
}
