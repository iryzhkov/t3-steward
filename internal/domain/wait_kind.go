package domain

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
