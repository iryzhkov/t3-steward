package domain

import (
	"strings"
	"testing"
	"time"
)

func TestTaskGateEvidenceBounds(t *testing.T) {
	for _, commands := range [][]string{make([]string, 65), {strings.Repeat("x", 4097)}, {strings.Repeat("a", 4096), strings.Repeat("b", 4096), strings.Repeat("c", 4096), strings.Repeat("d", 4096), "e"}, {string([]byte{255})}} {
		if err := (TaskGate{Commands: commands, Timeout: time.Second}).Validate(); err == nil {
			t.Fatal("oversized/malformed gate accepted")
		}
	}
	if err := (TaskGate{Commands: []string{strings.Repeat("x", 4096)}, Timeout: time.Nanosecond}).Validate(); err != nil {
		t.Fatal(err)
	}
}
