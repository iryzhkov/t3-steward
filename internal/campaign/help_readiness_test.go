package campaign

import (
	"strings"
	"testing"
)

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
