package campaign

import (
	"strings"
	"testing"
)

// The repository-free path was documented nowhere an agent reads: no help
// topic said fresh, the readiness code list lacked workspace-type-mismatch,
// and nothing said how a fresh project comes to exist.
func TestFreshHelpIsDiscoverable(t *testing.T) {
	var fresh string
	for _, topic := range HelpTopics() {
		if topic.Name == "fresh" {
			fresh = topic.Body
		}
	}
	for _, want := range []string{"type: fresh", "t3-steward backlog projects", "upkeeper project add scratch --type fresh",
		"task run --fresh", ".t3/dependencies/<task>/", "workspace-type-mismatch"} {
		if !strings.Contains(fresh, want) {
			t.Fatalf("fresh help topic does not say %q", want)
		}
	}
	for _, want := range []string{"workspace-type-mismatch", "supervisor-client-missing", "campaign help fresh"} {
		if !strings.Contains(ReadinessHelp, want) {
			t.Fatalf("readiness help does not name %q", want)
		}
	}
}

// F-14: the readiness help printed an enrollment form (--catalog-revision)
// that the enroll verb does not accept; the operator form pins the current
// catalog and records a reason, on the coordinator host.
func TestReadinessHelpNamesTheEnrollmentForm(t *testing.T) {
	want := "t3-steward worker enroll <worker> --current-catalog --reason TEXT   (on the coordinator host)"
	if !strings.Contains(ReadinessHelp, want) {
		t.Fatalf("readiness help does not print %q", want)
	}
	if strings.Contains(ReadinessHelp, "--catalog-revision") {
		t.Fatal("readiness help still prints the --catalog-revision form")
	}
}
