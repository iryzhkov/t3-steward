package backlogadmin

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestM8ProjectRefusalListsTypes(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		settings := viabilityCatalog(t)
		task := viabilityTaskRequest()
		if mismatch {
			task.Type = "fresh"
			task.Ref = ""
		} else {
			task.Project = "missing"
		}
		matrix := viabilityView(t, nil).viability(context.Background(), settings, ViabilityRequest{Tasks: []ViabilityTask{task}})
		if matrix.Outcome != ViabilityImpossible {
			t.Fatalf("matrix = %+v", matrix)
		}
		detail := matrix.Tasks[0].Reasons[0].Detail
		for _, project := range settings.Projects {
			kind := project.Type
			if kind == "" {
				kind = "git"
			}
			want := fmt.Sprintf("%q (%s)", project.Name, kind)
			if !strings.Contains(detail, want) {
				t.Errorf("%q lacks %q", detail, want)
			}
		}
		for _, want := range []string{"t3-steward campaign help fresh", "t3-steward backlog projects"} {
			if !strings.Contains(detail, want) {
				t.Errorf("%q lacks %q", detail, want)
			}
		}
	}
}
