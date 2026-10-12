package domain

import (
	"fmt"
	"testing"
)

func TestValidateGraphTasksGateOutputs(t *testing.T) {
	for _, tc := range []struct {
		name        string
		gate        bool
		declared    bool
		missingEdge bool
		wantError   string
	}{
		{name: "gate", gate: true},
		{name: "gate/log.txt", gate: true},
		{name: "gate", wantError: "required dependency output is undeclared"},
		{name: "gate/log.txt", wantError: "required dependency output is undeclared"},
		{name: "gate/other.txt", gate: true, wantError: "required dependency output is undeclared"},
		{name: "gate/log.txt", declared: true},
		{name: "gate/log.txt", gate: true, missingEdge: true, wantError: "artifact input requires dependency edge"},
	} {
		t.Run(fmt.Sprintf("%s/gate=%t/declared=%t/missingEdge=%t", tc.name, tc.gate, tc.declared, tc.missingEdge), func(t *testing.T) {
			t.Parallel()
			producer := Task{ID: "producer", WorkflowID: "workflow", Name: "producer", Class: TaskClassSurplus,
				MaxTurns: 1, PromptArtifactID: "prompt", Routes: []ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt"}}}
			if tc.gate {
				producer.Gate = &TaskGate{Commands: []string{"true"}}
			}
			if tc.declared {
				producer.Outputs = []ArtifactDeclaration{{Name: tc.name}}
			}
			consumer := producer
			consumer.ID, consumer.Name, consumer.Gate, consumer.Outputs = "consumer", "consumer", nil, nil
			consumer.Needs = []string{"producer"}
			if tc.missingEdge {
				consumer.Needs = nil
			}
			consumer.DependencyInputs = map[string][]string{"producer": {tc.name}}
			err := ValidateGraphTasks(WorkflowRun{WorkflowID: "workflow"}, []Task{producer, consumer})
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || err.Error() != tc.wantError {
				t.Fatalf("error = %v, want %q", err, tc.wantError)
			}
		})
	}
}
