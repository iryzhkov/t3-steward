package main

import (
	"strings"
	"testing"
)

func TestTriageHelpHasNoLiteralNewlines(t *testing.T) {
	if strings.Contains(triageUsage, "\\n") {
		t.Fatal("triage help contains literal backslash-n instead of line breaks")
	}
}
