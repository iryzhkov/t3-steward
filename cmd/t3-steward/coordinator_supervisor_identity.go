package main

// The admin client identity an overseer talks to the coordinator as.
//
// An overseer runs on a worker host, and that host already has a coordinator
// client: the ordinary remote-admin client named in
// backlog_v2.coordinator_client or in the UpKeeper-owned
// coordinator-client.json. It is the wrong identity for a review. The
// coordinator authorizes supervision by principal, against the activation it
// recorded, and it records exactly one supervisor principal fleet-wide, so a
// request signed as that host's own admin client is refused every supervision
// operation on every worker host. No configuration of two worker hosts could
// fix that, because there is only one supervisor principal to go round.
//
// The activation therefore carries the credential reference the CLI must
// present, and the worker puts it in the overseer thread's environment. This
// file reads it back and applies it to the remote carrier for supervision
// commands and for nothing else.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// supervisorIdentity is the client identity override in force for one command.
// Its zero value is no override, which is what every command other than
// supervision uses.
type supervisorIdentity struct {
	// CredentialReference is the admin credential reference to present instead
	// of this host's own coordinator client credential.
	CredentialReference string
	// ExpectedPrincipal is the admin client the reference must resolve to, when
	// the activation named one. It turns a stale or mistyped reference into a
	// refusal here rather than an unauthorized-scope refusal a round trip later,
	// which is the same refusal a genuinely expired activation produces and
	// would be indistinguishable from it.
	ExpectedPrincipal string
}

func (s supervisorIdentity) declared() bool {
	return strings.TrimSpace(s.CredentialReference) != ""
}

// supervisorIdentityFromEnvironment reads the override the worker set on an
// overseer thread. A thread that is not an overseer has neither variable and
// gets the zero value.
func supervisorIdentityFromEnvironment(lookup func(string) (string, bool)) supervisorIdentity {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	var identity supervisorIdentity
	if reference, ok := lookup(workerproto.SupervisorCredentialEnvironment); ok {
		identity.CredentialReference = strings.TrimSpace(reference)
	}
	if principal, ok := lookup(workerproto.SupervisorClientEnvironment); ok {
		identity.ExpectedPrincipal = strings.TrimSpace(principal)
	}
	return identity
}

// validate refuses a reference outside the admin credential namespace before it
// is resolved, so a worker protocol reference or an arbitrary environment value
// cannot be turned into an admin credential lookup.
func (s supervisorIdentity) validate() error {
	if !s.declared() {
		return nil
	}
	if err := backlogadmin.ValidateAdminCredentialReference(s.CredentialReference); err != nil {
		return &backlogadmin.TransportError{
			Class: backlogadmin.ClassClientConfiguration,
			Err: fmt.Errorf("the supervisor credential in %s is not usable: %w",
				workerproto.SupervisorCredentialEnvironment, err),
		}
	}
	return nil
}

// checkResolved refuses a credential that belongs to a different admin client
// than the activation named.
func (s supervisorIdentity) checkResolved(credentials backlogadmin.AdminCredentials) error {
	if s.ExpectedPrincipal == "" || credentials.ClientPrincipal == s.ExpectedPrincipal {
		return nil
	}
	return &backlogadmin.TransportError{
		Class: backlogadmin.ClassClientConfiguration,
		Err: fmt.Errorf(
			"the supervisor credential %q resolves to admin client %q, but this activation authenticates as %q",
			s.CredentialReference, credentials.ClientPrincipal, s.ExpectedPrincipal),
	}
}

// supervisorIdentityFromWorkspace reads the record the worker wrote into the
// activation workspace, from the current directory or an ancestor, the way a
// tool finds the repository it is inside.
//
// This is the channel that does not depend on the provider. The environment
// above reaches the CLI only when t3.send_thread_environment is on, which it is
// not by default and was not on the fleet, and an overseer with neither channel
// silently signs its decisions as the host's own coordinator client.
//
// The record is accepted only as a private regular file owned by this user. It
// decides which admin client a command presents, so a copy anyone could have
// written, or a symlink pointing somewhere else, is refused rather than read.
func supervisorIdentityFromWorkspace() (supervisorIdentity, error) {
	directory, err := os.Getwd()
	if err != nil {
		return supervisorIdentity{}, nil
	}
	for {
		path := filepath.Join(directory, filepath.FromSlash(workerproto.SupervisorIdentityFile))
		info, statErr := os.Lstat(path)
		switch {
		case statErr != nil:
		case !info.Mode().IsRegular():
			return supervisorIdentity{}, fmt.Errorf(
				"%s is not a regular file; refusing to read a supervisor identity from it", path)
		case info.Mode().Perm() != 0o600:
			return supervisorIdentity{}, fmt.Errorf(
				"%s has mode %04o, want 0600; refusing to read a supervisor identity that is not private",
				path, info.Mode().Perm())
		default:
			if err := taskIdentityFileIsOwned(info); err != nil {
				return supervisorIdentity{}, fmt.Errorf("%s: %w", path, err)
			}
			content, readErr := os.ReadFile(path)
			if readErr != nil {
				return supervisorIdentity{}, readErr
			}
			values, parseErr := workerproto.ParseSupervisorIdentityFile(string(content))
			if parseErr != nil {
				return supervisorIdentity{}, fmt.Errorf("%s: %w", path, parseErr)
			}
			return supervisorIdentity{
				CredentialReference: strings.TrimSpace(values[workerproto.SupervisorCredentialEnvironment]),
				ExpectedPrincipal:   strings.TrimSpace(values[workerproto.SupervisorClientEnvironment]),
			}, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			// No record anywhere above this directory. That is the ordinary case
			// for a task workspace and for an interactive session, and it leaves
			// the command as this host's own admin client.
			return supervisorIdentity{}, nil
		}
		directory = parent
	}
}

// resolveSupervisorIdentity decides the client identity one supervision command
// presents, from the three channels in the order of how specific they are.
//
// An explicit flag wins, so an operator can decide a gate by hand from a host
// whose overseer thread has an identity of its own. The thread environment
// comes next, because a sandbox sets it for exactly one execution. The
// workspace record is last and is the one that always exists inside an
// activation, because a workspace can in principle be reached from a shell that
// belongs to another execution.
//
// It is called from the supervision verbs and from nowhere else, which is what
// keeps the strongest credential on a worker host out of every command that
// starts or amends work. A malformed or unsafe workspace record is a refusal
// here rather than a silent fall back to the host's admin client: falling back
// is exactly the failure this whole path exists to end.
func resolveSupervisorIdentity(flagCredential string) (supervisorIdentity, error) {
	if reference := strings.TrimSpace(flagCredential); reference != "" {
		return supervisorIdentity{CredentialReference: reference}, nil
	}
	if identity := supervisorIdentityFromEnvironment(nil); identity.declared() {
		return identity, nil
	}
	return supervisorIdentityFromWorkspace()
}

// supervisorCredentialFlag is the operator's spelling of the same override, for
// deciding a gate by hand from a host that is not the coordinator.
const supervisorCredentialFlag = "--supervisor-credential"
