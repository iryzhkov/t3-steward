package backlog

import (
	"strings"
	"testing"
)

// A misspelt field used to be refused with the advice that a newer release may
// be required, which sent authors looking for an upgrade instead of the typo.
// A field within a typo of a known field of the same object is named instead,
// and a field that is not close to anything keeps the newer-release advice.
func TestUnknownManifestFieldSuggestsTheNearbyField(t *testing.T) {
	t.Cleanup(func() { SetReleaseVersion("dev") })
	SetReleaseVersion("0.11.0-rc.99-test")
	raw := strings.Join([]string{
		"version: 2",
		"name: typo",
		"environment: {project: scratch, type: fresh}",
		"tasks:",
		"  only:",
		"    prompt_file: prompts/only.md",
		"    ouputs: [review.md]",
		"    supervision_from_a_later_release: true",
		"",
	}, "\n")
	_, err := ParseManifest([]byte(raw))
	if err == nil {
		t.Fatal("unknown fields were accepted")
	}
	for _, fragment := range []string{
		"field ouputs (line 7) is not a field of this object in release 0.11.0-rc.99-test; did you mean outputs? If not, a newer t3-steward release may be required",
		"field supervision_from_a_later_release (line 8) is not supported by this release 0.11.0-rc.99-test; a newer t3-steward release may be required",
	} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("refusal %q is missing %q", err, fragment)
		}
	}
}

func TestSuggestManifestFieldUsesTheReportedObject(t *testing.T) {
	for _, tc := range []struct{ typeName, field, want string }{
		{"backlog.ManifestTask", "prompt_fiel", "prompt_file"},
		{"backlog.ManifestTask", "Verify", "verify"},
		{"backlog.Manifest", "taks", "tasks"},
		{"backlog.Manifest", "escalation_policy", ""},
		{"backlog.ManifestTask", "name", ""},
	} {
		if got := suggestManifestField(&Manifest{}, tc.typeName, tc.field); got != tc.want {
			t.Errorf("suggest(%s, %s) = %q, want %q", tc.typeName, tc.field, got, tc.want)
		}
	}
}
