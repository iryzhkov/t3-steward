package campaign

import (
	"strings"
	"testing"
)

func TestProjectRolesAndOrigin(t *testing.T) {
	plan := project(t, headerYAML+`role: execute
options: {effort: medium}
tasks:
  inherited: {prompt_file: inherited.md}
  own: {prompt_file: own.md, role: read, options: {effort: low}}
  same: {prompt_file: same.md, role: execute}
`)
	for _, tc := range []struct {
		name, role, effort string
		origin             Origin
	}{{"inherited", "execute", "medium", OriginInherited}, {"own", "read", "low", OriginTask}, {"same", "execute", "", OriginTask}} {
		task := taskByName(t, plan, tc.name)
		if task.Role != tc.role || task.RoleEffort != tc.effort || task.RoleFrom != tc.origin {
			t.Fatalf("%s = %#v", tc.name, task)
		}
	}
	if plan.Role != "execute" || plan.RoleEffort != "medium" {
		t.Fatalf("workflow role = %#v", plan)
	}
	text := RenderText(plan)
	for _, want := range []string{"execute (inherited)", "read (task)", "effort override: low"} {
		if !strings.Contains(text, want) {
			t.Fatalf("text missing %q:\n%s", want, text)
		}
	}
}
func TestProjectReviewType(t *testing.T) {
	plan := project(t, headerYAML+"tasks:\n  work: {prompt_file: work.md, role: execute, outputs: [review.md], review_output: {verdict_line: review.md}}\n")
	if !taskByName(t, plan, "work").ReviewType {
		t.Fatal("review_output not projected")
	}
}
func TestRoleHelpContracts(t *testing.T) {
	for _, help := range []string{AuthoringHelp, RoutesHelp} {
		for _, want := range []string{"role:", "options: {effort: low}", "mutually exclusive", "inputs_from", "review_output:", "schedule"} {
			if !strings.Contains(help, want) {
				t.Errorf("help missing %q", want)
			}
		}
	}
	if strings.Contains(RoutesHelp, "coordinator never invents a route") {
		t.Fatal("obsolete routes claim")
	}
}
