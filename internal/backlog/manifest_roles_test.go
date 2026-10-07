package backlog

import (
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/review"
)

const roleHeader = "version: 2\nname: role-example\nenvironment: {project: example}\n"

func TestManifestRoleInheritance(t *testing.T) {
	m, err := ParseManifest([]byte(roleHeader + `role: execute
options: {effort: medium}
tasks:
  inherited: {prompt_file: inherited.md}
  explicit: {prompt_file: explicit.md, routes: [{instance: codex, model: exact}]}
  own: {prompt_file: own.md, role: read, options: {effort: low}}
  same: {prompt_file: same.md, role: execute}
`))
	if err != nil {
		t.Fatal(err)
	}
	if m.Tasks["inherited"].Role != "execute" || m.Tasks["inherited"].Options["effort"] != "medium" || !m.Tasks["inherited"].RoleInherited {
		t.Fatalf("inherited = %#v", m.Tasks["inherited"])
	}
	if m.Tasks["explicit"].Role != "" || len(m.Tasks["explicit"].Options) != 0 || len(m.Tasks["explicit"].Routes) != 1 {
		t.Fatalf("explicit = %#v", m.Tasks["explicit"])
	}
	if m.Tasks["own"].Role != "read" || m.Tasks["own"].Options["effort"] != "low" || m.Tasks["own"].RoleInherited {
		t.Fatalf("own = %#v", m.Tasks["own"])
	}
	if m.Tasks["same"].RoleInherited || len(m.Tasks["same"].Options) != 0 {
		t.Fatalf("same = %#v", m.Tasks["same"])
	}
	task := m.Tasks["inherited"]
	task.Options["effort"] = "low"
	if m.Options["effort"] != "medium" {
		t.Fatal("inherited options alias workflow")
	}
	m, err = ParseManifest([]byte(roleHeader + `routes: [{instance: codex, model: exact}]
tasks:
  own: {prompt_file: own.md, role: read}
  inherited: {prompt_file: inherited.md}
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Tasks["own"].Routes) != 0 || m.Tasks["own"].Role != "read" || len(m.Tasks["inherited"].Routes) != 1 {
		t.Fatalf("route inheritance: %#v", m.Tasks)
	}
}

func TestManifestRoleRefusals(t *testing.T) {
	for _, tc := range []struct{ name, yaml, want string }{
		{"workflow mixed", "role: execute\nroutes: [{instance: codex, model: exact}]\ntasks: {}\n", "role and routes are mutually exclusive on workflow; use one"},
		{"task mixed", "tasks:\n  work: {prompt_file: work.md, role: read, routes: [{instance: codex, model: exact}]}\n", "role and routes are mutually exclusive on task work; use one"},
		{"workflow options", "options: {effort: low}\ntasks: {}\n", "options without role"},
		{"inherited options", "role: execute\ntasks:\n  work: {prompt_file: work.md, options: {effort: low}}\n", "options without role"},
		{"unknown option", "role: read\noptions: {temperature: low}\ntasks: {}\n", "unknown role option"},
		{"max", "role: read\noptions: {effort: max}\ntasks: {}\n", "low, medium, or high"},
		{"empty effort", "role: read\noptions: {effort: ''}\ntasks: {}\n", "low, medium, or high"},
		{"invalid role", "role: 'bad/name'\ntasks: {}\n", "invalid role"},
		{"unknown field", "role: read\nunknown: true\ntasks: {}\n", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseManifest([]byte(roleHeader + tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v; want %q", err, tc.want)
			}
		})
	}
}

func TestManifestRoleReviewRoundRefused(t *testing.T) {
	m := Manifest{Review: &review.Round{}, Tasks: map[string]ManifestTask{"work": {Role: "review"}}}
	if err := validateManifest(m); err == nil || !strings.Contains(err.Error(), "role on task work is not supported in a manifest review: round") {
		t.Fatalf("error = %v", err)
	}
}
