package campaign

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRepinCompileUnit(t *testing.T) {
	old, target := "d2e827681336e76413fdab34392f029a3e131cf4", strings.Repeat("a", 40)
	options := CompileOptions{ReadInput: func(string) ([]byte, error) { return []byte(old), nil }}
	for _, quote := range []string{"", "'", "\""} {
		for _, eol := range []string{"\n", "\r\n"} {
			raw := bytes.ReplaceAll(readCompileFixture(t), []byte("ref: "+old), []byte("ref: "+quote+old+quote+" # keep"))
			raw = bytes.Replace(raw, []byte("class: required"), []byte("inputs: [nested/extra.md]\nclass: required"), 1)
			raw = append(raw, []byte("\n"+old+"\n")...)
			raw = bytes.ReplaceAll(raw, []byte("\n"), []byte(eol))
			before, err := ParseCompilePlanWith("plan.md", raw, options)
			repinMust(t, err)
			result, err := RepinCompileUnit(raw, "c1", target, options)
			repinMust(t, err)
			if result.OldRef != old || result.NewRef != target || !bytes.Equal(result.Plan, bytes.ReplaceAll(raw, []byte(old), []byte(target))) {
				t.Fatal("plan bytes or refs changed incorrectly")
			}
			for _, file := range before.Units[0].Files {
				want := strings.ReplaceAll(string(file.Content), old, target)
				if file.Path == "inputs/nested/extra.md" {
					want = old
				}
				if compiledFile(t, result.Unit, file.Path) != want {
					t.Fatalf("changed intent/bytes at %s", file.Path)
				}
			}
			same, err := RepinCompileUnit(result.Plan, "c1", target, options)
			if err != nil || compiledFile(t, same.Unit, ManifestFileName) != compiledFile(t, result.Unit, ManifestFileName) {
				t.Fatalf("same pin: %v", err)
			}
			parent := t.TempDir()
			original, err := WriteCompiledUnit(parent, before.Units[0], WriteOptions{})
			repinMust(t, err)
			if _, err = WriteCompiledUnit(t.TempDir(), result.Unit, WriteOptions{}); err != nil {
				t.Fatal(err)
			}
			_, err = WriteCompiledUnit(parent, result.Unit, WriteOptions{Force: true, WriteFile: func(string, []byte) error { return errors.New("injected") }})
			if err == nil {
				t.Fatal("write failure accepted")
			}
			got, err := os.ReadFile(filepath.Join(original, "inputs/plan.md"))
			entries, _ := os.ReadDir(parent)
			if err != nil || !bytes.Equal(got, raw) || len(entries) != 1 {
				t.Fatal("original lost or leftovers")
			}
		}
	}
	for _, syntax := range []string{"&pin " + old, "*pin", "\"\\x64" + old[1:] + "\"", "{value: " + old + "}"} {
		raw := bytes.Replace(readCompileFixture(t), []byte(old), []byte(syntax), 1)
		if _, err := RepinCompileUnit(raw, "c1", old, options); err == nil {
			t.Fatalf("accepted unsupported same-pin %s", syntax)
		}
	}
	numeric := bytes.ReplaceAll(readCompileFixture(t), []byte(old), []byte(strings.Repeat("1", 40)))
	_, err := RepinCompileUnit(numeric, "c1", target, options)
	repinMust(t, err)
	for _, target := range []string{"main", "abc", strings.Repeat("g", 40)} {
		if _, err := RepinCompileUnit(readCompileFixture(t), "c1", target, options); err == nil {
			t.Fatal("accepted invalid SHA")
		}
	}
}

func repinMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
