package backlog

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// "task run" refuses a bad --output before it contacts the coordinator, and
// the coordinator refuses the same manifest later. Both have to be one rule,
// or a name the CLI accepts is refused after submission, or the other way
// round, so each input here is put through both and the answers compared.
func TestValidateOutputPathsMatchesTheManifestRule(t *testing.T) {
	for _, tc := range []struct {
		name    string
		paths   []string
		refused bool
	}{
		{"none", nil, false},
		{"two plain names", []string{"findings.md", "notes.md"}, false},
		{"nested", []string{"reports/a.md"}, false},
		{"a comma is a character", []string{"a,b.md"}, false},
		{"dot segment inside", []string{"reports/./a.md"}, false},
		{"repeated", []string{"a.md", "a.md"}, true},
		{"parent", []string{"../x.md"}, true},
		{"parent only", []string{".."}, true},
		{"current directory", []string{"."}, true},
		{"absolute", []string{"/abs"}, true},
		{"glob star", []string{"r*.md"}, true},
		{"glob class", []string{"r[ab].md"}, true},
		{"glob question", []string{"r?.md"}, true},
		{"empty", []string{""}, true},
		{"NUL", []string{"a\x00b"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			direct := ValidateOutputPaths("task task outputs", tc.paths)
			manifestErr := parseManifestWithOutputs(t, tc.paths)
			if (direct != nil) != tc.refused {
				t.Fatalf("ValidateOutputPaths(%q) = %v, want refused=%t", tc.paths, direct, tc.refused)
			}
			if (manifestErr != nil) != tc.refused {
				t.Fatalf("manifest validation of outputs %q = %v, want refused=%t", tc.paths, manifestErr, tc.refused)
			}
			if direct != nil && !strings.Contains(manifestErr.Error(), direct.Error()) {
				t.Fatalf("the two refusals differ:\n  direct:   %v\n  manifest: %v", direct, manifestErr)
			}
		})
	}
}

// The label is the caller's, so "task run" can name its own flag.
func TestValidateOutputPathsUsesTheCallersLabel(t *testing.T) {
	err := ValidateOutputPaths("--output", []string{"a.md", "a.md"})
	if err == nil || err.Error() != `--output path "a.md" is duplicated` {
		t.Fatalf("error = %v", err)
	}
	err = ValidateOutputPaths("--output", []string{"../x.md"})
	if err == nil || err.Error() != `--output path "../x.md": path escapes the workflow bundle` {
		t.Fatalf("error = %v", err)
	}
}

// parseManifestWithOutputs validates a one-task manifest declaring outputs.
func parseManifestWithOutputs(t *testing.T, outputs []string) error {
	t.Helper()
	declared, err := yaml.Marshal(outputs)
	if err != nil {
		t.Fatal(err)
	}
	list := strings.TrimSpace(string(declared))
	if len(outputs) == 0 {
		list = "[]"
	}
	raw := "version: 2\nname: outputs\nenvironment:\n  project: scratch\n  type: fresh\n" +
		"tasks:\n  task:\n    prompt_file: prompts/task.md\n    outputs:\n" + indent(list, "      ") + "\n"
	_, err = ParseManifest([]byte(raw))
	return err
}

func indent(text, prefix string) string {
	lines := strings.Split(text, "\n")
	for i := range lines {
		lines[i] = prefix + lines[i]
	}
	return strings.Join(lines, "\n")
}
