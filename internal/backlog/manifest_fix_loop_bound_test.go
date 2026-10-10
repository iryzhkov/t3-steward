package backlog

import (
	"strings"
	"testing"
)

func TestManifestFixLoopRequiresIntegerBound(t *testing.T) {
	for _, value := range []string{"4.5", "4.0", "4e0", "null", "true", "\"4\""} {
		t.Run(value, func(t *testing.T) {
			raw := strings.Replace(fixLoopManifest, "max_rounds: 4", "max_rounds: "+value, 1)
			if _, err := ParseManifest([]byte(raw)); err == nil {
				t.Fatal("non-integer bound accepted")
			}
		})
	}
}
