package backlog

import (
	"strings"
	"testing"
)

func TestM8InlinePromptRefusal(t *testing.T) {
	_, err := ParseManifest([]byte("version: 2\nname: inline\nenvironment:\n  project: steward\ntasks:\n  review:\n    prompt: review it\n    outputs: [review.md]\n"))
	if err == nil {
		t.Fatal("inline prompt accepted")
	}
	for _, want := range []string{"prompt_file", "t3-steward campaign help authoring"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%v lacks %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "newer t3-steward release") {
		t.Fatalf("misleading upgrade advice: %v", err)
	}
}
