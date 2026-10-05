package backlog

import (
	"strings"
	"testing"
)

func TestIndependentProfileLegacyMergedNullRefused(t *testing.T) {
	for _, memberFields := range []string{
		"execution: null",
		"<<: {execution: null}",
		"<<: [{execution: null}]",
	} {
		t.Run(memberFields, func(t *testing.T) {
			raw := strings.Replace(declaredManifestYAML(), "required: true}", "required: true, "+memberFields+"}", 1)
			if raw == declaredManifestYAML() {
				t.Fatal("fixture unchanged")
			}
			if _, err := ParseManifest([]byte(raw)); err == nil {
				t.Fatal("version 1 accepted explicitly declared execution via " + memberFields)
			} else {
				t.Log(err)
			}
		})
	}
}
