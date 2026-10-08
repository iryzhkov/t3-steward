package workerproto

import "strings"

// Host capabilities describe what the host a worker runs on can do for a task,
// as opposed to what the worker's build understands. A build capability is
// constant for a release; a host capability is observed on every snapshot and
// can appear or disappear without a release, when an operator installs a
// coordinator client, a push credential or a Huyang trust root.
//
// A task asks for one with placement.requires, exactly like any other
// capability. Because a missing host capability can be supplied later,
// readiness reports it as a temporary capability-missing finding: the run is
// accepted and waits, typed, for a capable worker rather than being refused.
const (
	// CapabilityCoordinatorClient says a task on this host can reach the
	// coordinator's administrator API: either a coordinator client is
	// configured and its credential resolves on this host, or this host is the
	// coordinator. Commands such as steward-fleet-configuration need it.
	CapabilityCoordinatorClient = "coordinator-client-v1"
	// CapabilityAskRelay says "t3-steward ask" from a task on this host parks
	// the task and has its answer delivered: the host reaches the coordinator
	// and its steward is the worker the task runs on, so the wake and the relay
	// thread are delivered here.
	CapabilityAskRelay = "ask-relay-v1"
	// CapabilityHuyangTrusted says the Huyang trust roots on this host cover
	// the worker's task workspace root, so Huyang runs a task's verification
	// commands instead of refusing an untrusted workspace.
	CapabilityHuyangTrusted = "huyang-trusted-v1"
	// CapabilityGitPushPrefix prefixes the per-project push capability; see
	// GitPushCapability.
	CapabilityGitPushPrefix = "git-push-"
)

// GitPushCapability names the capability a worker advertises when push
// credentials for the project's repository are present on its host. It is
// empty for a project name that would not form a valid capability name.
func GitPushCapability(project string) string {
	if !hostCapabilityNamePart(project) {
		return ""
	}
	return CapabilityGitPushPrefix + project
}

// HostObservedCapability reports whether a capability is one a host observes
// for itself rather than one a build supplies or an operator declares.
func HostObservedCapability(name string) bool {
	switch name {
	case CapabilityCoordinatorClient, CapabilityAskRelay, CapabilityHuyangTrusted:
		return true
	}
	project, ok := strings.CutPrefix(name, CapabilityGitPushPrefix)
	return ok && hostCapabilityNamePart(project)
}

// hostCapabilityNamePart accepts the manifest identifier form, lower-case
// words joined by single hyphens or underscores, so that every capability
// formed here is also one a manifest can require.
func hostCapabilityNamePart(value string) bool {
	if value == "" || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	previous := byte(0)
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-' || c == '_':
			if previous == '-' || previous == '_' || i == len(value)-1 {
				return false
			}
		default:
			return false
		}
		previous = c
	}
	return true
}
