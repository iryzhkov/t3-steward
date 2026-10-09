package backlog

import (
	"strings"
	"testing"
)

// identifierManifest is a one-task manifest whose task fields are replaced by
// body, indented as task fields.
func identifierManifest(body string) []byte {
	return []byte(`
version: 2
name: identifiers
environment:
  project: steward
  type: git
tasks:
  implement:
    prompt_file: prompts/implement.md
` + body)
}

func TestManifestRefusesUnsafeOutputPaths(t *testing.T) {
	long := strings.Repeat("a", 256)
	for name, test := range map[string]struct {
		output string
		want   string
	}{
		"newline":          {`"report\nfake.md"`, `"report\nfake.md": path contains a control character`},
		"carriage return":  {`"report\r.md"`, `"report\r.md": path contains a control character`},
		"escape":           {`"report\e[2J.md"`, `"report\x1b[2J.md": path contains a control character`},
		"tab":              {`"report\t.md"`, `"report\t.md": path contains a control character`},
		"delete":           {`"report\x7f.md"`, `"report\x7f.md": path contains a control character`},
		"long component":   {"docs/" + long + ".md", "path component is 259 bytes, longer than 255"},
		"5000-byte name":   {strings.Repeat("b", 5000), "path component is 5000 bytes, longer than 255"},
		"long final input": {long, "path component is 256 bytes, longer than 255"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseManifest(identifierManifest("    outputs: [" + test.output + "]\n"))
			if err == nil || !strings.Contains(err.Error(), "task implement outputs path ") || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("parse = %v, want a refusal naming the field and containing %q", err, test.want)
			}
		})
	}
}

func TestManifestAcceptsLongestOutputComponent(t *testing.T) {
	output := "docs/" + strings.Repeat("a", 252) + ".md"
	if _, err := ParseManifest(identifierManifest("    outputs: [" + output + ", handoff.md, \"notes with space.md\"]\n")); err != nil {
		t.Fatalf("a 255-byte component was refused: %v", err)
	}
}

func TestValidateOutputPathsRefusesControlCharacters(t *testing.T) {
	err := ValidateOutputPaths("--output", []string{"ok.md", "bad\x00\x01.md"})
	if err == nil || !strings.Contains(err.Error(), `--output path "bad\x00\x01.md": path contains a control character`) {
		t.Fatalf("err = %v", err)
	}
	err = ValidateOutputPaths("--output", []string{strings.Repeat("c", 300)})
	if err == nil || !strings.Contains(err.Error(), "path component is 300 bytes, longer than 255") {
		t.Fatalf("err = %v", err)
	}
}

func TestManifestRefusesUnsafeInputsFromArtifacts(t *testing.T) {
	_, err := ParseManifest([]byte(`
version: 2
name: identifiers
environment:
  project: steward
  type: git
tasks:
  implement:
    prompt_file: prompts/implement.md
    needs: [run-0123/probe]
    inputs_from:
      run-0123/probe: ["out\x1b.md"]
`))
	if err == nil || !strings.Contains(err.Error(), `artifact "out\x1b.md": path contains a control character`) {
		t.Fatalf("parse = %v", err)
	}
}

func TestManifestCommitNamesMustBePathSafeIDs(t *testing.T) {
	for name, commit := range map[string]string{
		"5000 characters": strings.Repeat("x", 5000),
		"129 characters":  strings.Repeat("x", 129),
		"leading dot":     ".hidden",
		"leading dash":    "-x",
		"space":           `"two words"`,
		"control":         `"impl\x01"`,
		"colon":           "impl:one",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseManifest(identifierManifest("    commits:\n      - name: " + commit + "\n"))
			if err == nil || !strings.Contains(err.Error(), "task implement commit name ") || !strings.Contains(err.Error(), "must be one safe path component") {
				t.Fatalf("parse = %v", err)
			}
			if strings.ContainsAny(err.Error(), "\x01") {
				t.Fatalf("refusal printed the value unescaped: %q", err.Error())
			}
		})
	}
	for _, commit := range []string{"implementation", "fix-1", "v1.2_final", strings.Repeat("x", 128)} {
		if _, err := ParseManifest(identifierManifest("    commits:\n      - name: " + commit + "\n")); err != nil {
			t.Fatalf("commit name %q refused: %v", commit, err)
		}
	}
}

func TestManifestRefusesUnsafeCrossRunNeeds(t *testing.T) {
	for name, need := range map[string]string{
		"parent directory":  "../probe",
		"current directory": "./probe",
		"option-like run":   "-x/probe",
		"control in run":    `"run\x01/probe"`,
		"newline in run":    `"run\n/probe"`,
		"control in task":   `"run-1/pro\x7fbe"`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseManifest(identifierManifest("    needs: [" + need + "]\n"))
			if err == nil || !strings.Contains(err.Error(), "task implement needs: node ") {
				t.Fatalf("parse = %v, want the node reference refused", err)
			}
		})
	}
	for _, need := range []string{"run-0123abcd/probe", "run:legacy:7/probe", "run-1/sink"} {
		if _, err := ParseManifest(identifierManifest("    needs: [\"" + need + "\"]\n")); err != nil {
			t.Fatalf("cross-run need %q refused: %v", need, err)
		}
	}
}
