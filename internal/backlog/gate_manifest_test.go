package backlog

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestWorkerOwnedGateManifestEvidenceBounds(t *testing.T) {
	text := "version: 2\nname: gate-test\nenvironment:\n  project: steward\ntasks:\n  build:\n    prompt_file: build.md\n    gate:\n      commands:\n"
	for i := 0; i < 400; i++ {
		text += fmt.Sprintf("        - %q\n", fmt.Sprintf("printf %d #%s", i, strings.Repeat("x", 1000)))
	}
	if _, err := ParseManifest([]byte(text)); err == nil {
		t.Fatal("accepted gate whose evidence exceeds coordinator bound")
	}
}

func TestWorkerOwnedGateManifest(t *testing.T) {
	base := "version: 2\nname: gate-test\nenvironment:\n  project: steward\ntasks:\n  build:\n    prompt_file: build.md\n    gate:\n      commands: [make check-review]\n"
	m := mustParseManifest(t, base)
	if m.Tasks["build"].Gate == nil || m.Tasks["build"].Gate.Timeout != 45*time.Minute {
		t.Fatalf("gate defaults = %+v", m.Tasks["build"].Gate)
	}
	for _, suffix := range []string{"      timeout: 7h\n", "      timeout: -1s\n"} {
		if _, err := ParseManifest([]byte(base + suffix)); err == nil {
			t.Fatalf("accepted %s", suffix)
		}
	}
	m = mustParseManifest(t, base+"  review:\n    prompt_file: review.md\n    needs: [build]\n    inputs_from:\n      build: [gate]\n")
	if len(m.Tasks["review"].InputsFrom["build"]) != 1 {
		t.Fatal("missing gate input")
	}
	_, err := ParseManifest([]byte(strings.Replace(base, "[make check-review]", "[]", 1)))
	if err == nil {
		t.Fatal("empty gate accepted")
	}
}
