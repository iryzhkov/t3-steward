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

// supervisorCredentialFlag is the operator's spelling of the same override, for
// deciding a gate by hand from a host that is not the coordinator.
const supervisorCredentialFlag = "--supervisor-credential"
