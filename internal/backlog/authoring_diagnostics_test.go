package backlog

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/diagnostics"
	"gopkg.in/yaml.v3"
)

func TestAuthoringDiagnosticsRetainAllYAMLTypeErrors(t *testing.T) {
	raw := "version: wrong\nname: example\nenvironment:\n  project: project\n  unrecognized: true\ntasks:\n  inspect:\n    prompt_flie: inspect.md\n    max_turns: wrong\n"
	_, err := ParseManifest([]byte(raw))
	doc := diagnostics.FromError(err, "workflow.yaml")
	if err == nil || doc.OK || doc.SchemaVersion != "diagnostics/v1" || len(doc.Entries) != 4 {
		t.Fatalf("refusal %v, diagnostics %#v", err, doc)
	}
	want := []struct {
		code, path string
		line       int
	}{
		{"yaml-type", "version", 1},
		{"yaml-unknown-field", "environment.unrecognized", 5},
		{"yaml-unknown-field", "tasks.inspect.prompt_flie", 8},
		{"yaml-type", "tasks.inspect.max_turns", 9},
	}
	for i, entry := range doc.Entries {
		if entry.Code != want[i].code || entry.Path != want[i].path || entry.Line != want[i].line || entry.File != "workflow.yaml" || entry.Severity != "error" || entry.Column <= 0 {
			t.Errorf("entry %d = %#v, want %#v", i, entry, want[i])
		}
	}
	if !strings.Contains(doc.Entries[2].Hint, "prompt_file") {
		t.Errorf("unknown-field hint lost owner suggestion: %q", doc.Entries[2].Hint)
	}
	var owner *UnknownManifestFieldError
	var decoder *yaml.TypeError
	wrapped := fmt.Errorf("load: %w", errors.Join(err, errors.New("another refusal")))
	if !errors.As(wrapped, &owner) || !errors.As(wrapped, &decoder) || len(owner.Fields) != 2 || len(decoder.Errors) != 4 {
		t.Fatalf("owner/decoder chain lost: %v", wrapped)
	}
}

func TestAuthoringYAMLHints(t *testing.T) {
	for _, tc := range []struct{ name, raw, code, hint string }{
		{"duplicates", "version: 2\nversion: 2\nname: first\nname: second\n", "yaml-duplicate-key", "unique"},
		{"tabs", "version: 2\ntasks:\n\tinspect: {}\n", "yaml-syntax", "tab"},
		{"colon", "version: 2\nname: title: subtitle\n", "yaml-syntax", "quot"},
		{"documents", "version: 2\n---\nversion: 2\n", "yaml-multiple-documents", "single"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseManifest([]byte(tc.raw))
			doc := diagnostics.FromError(err, "workflow.yaml")
			if err == nil || doc.OK || len(doc.Entries) == 0 {
				t.Fatalf("expected diagnostic refusal: %v %#v", err, doc)
			}
			for _, entry := range doc.Entries {
				if entry.Code != tc.code || entry.Line <= 0 || entry.Column < 0 || !strings.Contains(strings.ToLower(entry.Hint), tc.hint) {
					t.Errorf("diagnostic = %#v", entry)
				}
			}
			if tc.name == "duplicates" {
				if len(doc.Entries) != 2 || doc.Entries[0].Line != 2 || doc.Entries[0].Path != "version" || doc.Entries[1].Line != 4 || doc.Entries[1].Path != "name" {
					t.Fatalf("both duplicate keys and source locations must survive: %#v", doc)
				}
			}
		})
	}
}

func TestHintsDoNotInterpretLiteralContent(t *testing.T) {
	for _, command := range []string{
		`"echo title: detail, next"`,
		"|\n        echo title: detail, next\n        \techo tab\n        ```yaml\n        unknown: data\n        ```",
	} {
		raw := "version: 2\nname: literals\nenvironment: {project: project}\ntasks:\n  inspect:\n    prompt_file: inspect.md\n    verify:\n      - " + command + "\n"
		_, err := ParseManifest([]byte(raw))
		if err != nil {
			t.Fatalf("literal command refused: %v", err)
		}
		if doc := diagnostics.FromError(err, "workflow.yaml"); !doc.OK || len(doc.Entries) != 0 {
			t.Fatalf("literal diagnostics: %#v", doc)
		}
	}
	if _, err := ParseManifest([]byte(fixLoopManifest)); err != nil {
		t.Fatalf("valid fix-loop compatibility: %v", err)
	}
}

func TestDiagnosticsPreserveErrorChainsAndLegacyRefusals(t *testing.T) {
	for _, suffix := range []string{
		"    prompt_file: ../outside.md\n",
		"    prompt_file: inspect.md\n    needs: [inspect]\n",
		"    prompt_file: inspect.md\n    needs_verdict: {inspect: accept}\n",
		"    prompt_file: inspect.md\n    needs_verdict: {inspect: changes-requested}\n",
	} {
		_, err := ParseManifest([]byte("version: 2\nname: example\nenvironment: {project: project}\ntasks:\n  inspect:\n" + suffix))
		doc := diagnostics.FromError(err, "workflow.yaml")
		if err == nil || doc.OK || len(doc.Entries) != 1 {
			t.Fatalf("semantic refusal: %v %#v", err, doc)
		}
		entry := doc.Entries[0]
		if entry.Code != "legacy" || entry.Message != err.Error() || entry.Line != 0 || entry.Column != 0 || entry.Path != "" {
			t.Fatalf("invented legacy evidence: %#v", entry)
		}
	}
	_, err := LoadManifest(t.TempDir())
	doc := diagnostics.FromError(err, "workflow.yaml")
	if err == nil || doc.OK || len(doc.Entries) != 1 || doc.Entries[0].Code != "legacy" || doc.Entries[0].Line != 0 || doc.Entries[0].Path != "" {
		t.Fatalf("IO refusal: %v %#v", err, doc)
	}
}
