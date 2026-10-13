package diagnostics

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestDiagnosticsPreserveWrappedJoinedErrors(t *testing.T) {
	first := Attach(errors.New("first"), Refusal("yaml-type", "one.yaml", 3, "first", "hint"))
	second := Attach(errors.New("second"), Refusal("yaml-unknown-field", "two.yaml", 2, "second", "hint"))
	original := fmt.Errorf("outer: %w", errors.Join(first, fmt.Errorf("inner: %w", second), errors.New("legacy refusal")))
	document := FromError(original, "fallback.yaml")
	if document.OK || len(document.Entries) != 3 {
		t.Fatalf("%#v", document)
	}
	if document.Entries[0].Code != "legacy" || document.Entries[0].Line != 0 || document.Entries[0].Column != 0 || document.Entries[0].Path != "" {
		t.Fatalf("invented legacy location: %#v", document)
	}
	if document.Entries[1].File != "two.yaml" || document.Entries[2].File != "one.yaml" {
		t.Fatalf("lost entries/order: %#v", document)
	}
	if !errors.Is(original, first) || first.Error() != "first" {
		t.Fatal("carrier changed error chain/message")
	}
	if text := Text(errors.New("refusal"), "file.yaml"); !strings.Contains(text, "file.yaml:?:?: legacy: refusal") {
		t.Fatalf("unknown coordinates: %s", text)
	}
}

func TestAmbiguousYAMLDiagnosticsDoNotInventAPath(t *testing.T) {
	// Both scalars on the same line have the failed value's tag.
	raw := []byte("values: [wrong, also-wrong]\n")
	err := errors.New("yaml: line 1: could not find expected ':'")
	document := YAML(err, raw, "workflow.yaml", nil, nil)
	entry := document.Entries[0]
	if entry.Path != "" || entry.Column != 0 || !strings.Contains(entry.Hint, "block-style") {
		t.Fatalf("ambiguous evidence: %#v", entry)
	}
}
