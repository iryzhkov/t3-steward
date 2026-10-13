package t3api

import (
	"encoding/json"
	"maps"
	"reflect"
	"testing"
)

func TestBuildModelSelection(t *testing.T) {
	tests := []struct {
		name, instance string
		options        map[string]string
		want           string
		wantErr        bool
	}{
		{"absent", "codex", nil, "", false},
		{"empty", "codex", map[string]string{}, "", false},
		{"low", "codex", map[string]string{"effort": "low"}, `[{"id":"reasoningEffort","value":"low"}]`, false},
		{"medium", "codex", map[string]string{"effort": "medium"}, `[{"id":"reasoningEffort","value":"medium"}]`, false},
		{"high", "codex", map[string]string{"effort": "high"}, `[{"id":"reasoningEffort","value":"high"}]`, false},
		{"canonical", "codex", map[string]string{"reasoningEffort": "low"}, `[{"id":"reasoningEffort","value":"low"}]`, false},
		{"equal", "codex", map[string]string{"effort": "high", "reasoningEffort": "high"}, `[{"id":"reasoningEffort","value":"high"}]`, false},
		{"conflict", "codex", map[string]string{"effort": "high", "reasoningEffort": "medium"}, "", true},
		{"blank generic", "codex", map[string]string{"effort": ""}, "", true},
		{"blank canonical", "codex", map[string]string{"reasoningEffort": " \t"}, "", true},
		{"blank dual", "codex", map[string]string{"effort": " ", "reasoningEffort": " "}, "", true},
		{"unrelated", "codex", map[string]string{"z": "", "effort": "high", "a": " untouched "}, `[{"id":"a","value":" untouched "},{"id":"reasoningEffort","value":"high"},{"id":"z","value":""}]`, false},
		{"exact value", "codex", map[string]string{"effort": " high "}, `[{"id":"reasoningEffort","value":" high "}]`, false},
		{"claude", "claude", map[string]string{"effort": "high", "reasoningEffort": "low"}, `[{"id":"effort","value":"high"},{"id":"reasoningEffort","value":"low"}]`, false},
		{"custom", "codex-custom", map[string]string{"effort": "high", "unknown": ""}, `[{"id":"effort","value":"high"},{"id":"unknown","value":""}]`, false},
		{"custom canonical", "custom", map[string]string{"reasoningEffort": "high"}, `[{"id":"reasoningEffort","value":"high"}]`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := maps.Clone(tt.options)
			for repeat := 0; repeat < 10; repeat++ {
				got, err := BuildModelSelection(tt.instance, "caller-model", tt.options)
				if (err != nil) != tt.wantErr {
					t.Fatalf("selection=%v error=%v", got, err)
				}
				if !reflect.DeepEqual(tt.options, before) {
					t.Fatal("caller options mutated")
				}
				if tt.wantErr {
					if got != nil {
						t.Fatal("invalid selection returned")
					}
					continue
				}
				if got["instanceId"] != tt.instance || got["model"] != "caller-model" {
					t.Fatalf("caller identity changed: %v", got)
				}
				options, present := got["options"]
				if tt.want == "" {
					if present {
						t.Fatal("absent options gained a default")
					}
				} else if encoded, err := json.Marshal(options); err != nil || string(encoded) != tt.want {
					t.Fatalf("options=%s error=%v want=%s", encoded, err, tt.want)
				}
			}
		})
	}
}
