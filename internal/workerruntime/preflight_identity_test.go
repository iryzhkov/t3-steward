package workerruntime

import "testing"

// Preflight evidence was identified by a prefix of its own content hash. Two
// tasks running the same probe against the same repository produce byte
// identical output, so they claimed one identity, and the coordinator refused
// the second publication as conflicting with immutable metadata belonging to a
// different task. A live batch hit this on its first parallel wave.
func TestPreflightArtifactIDSeparatesAttemptsAndSteps(t *testing.T) {
	first := preflightArtifactID("attempt-1", "repository_state")
	second := preflightArtifactID("attempt-2", "repository_state")
	if first == second {
		t.Fatal("two attempts running the same step share one artifact identity")
	}

	other := preflightArtifactID("attempt-1", "toolchain")
	if first == other {
		t.Fatal("two steps of one attempt share one artifact identity")
	}

	// Stability matters as much as uniqueness: republishing the same attempt's
	// evidence must reuse its identity rather than create a second record.
	if first != preflightArtifactID("attempt-1", "repository_state") {
		t.Fatal("the identity of one attempt's step is not stable")
	}

	if len(first) == 0 || first[:len("preflight-")] != "preflight-" {
		t.Fatalf("identity %q must carry the preflight prefix the import contract requires", first)
	}
}
