package campaign

import (
	"bytes"
	"strings"
	"testing"
)

func TestRepinIndentedDirectRef(t *testing.T) {
	old, target := "d2e827681336e76413fdab34392f029a3e131cf4", strings.Repeat("a", 40)
	for _, quote := range []string{"", "'", "\""} {
		for _, eol := range []string{"\n", "\r\n"} {
			t.Run(quote+eol, func(t *testing.T) {
				raw := bytes.Replace(readCompileFixture(t), []byte("ref: "+old), []byte("ref: "+quote+old+quote+" # keep"), 1)
				parts := bytes.SplitN(raw, []byte("---\n"), 3)
				front := bytes.ReplaceAll(parts[1], []byte("\n"), []byte("\n  "))
				front = bytes.TrimSuffix(front, []byte("  "))
				raw = bytes.Join([][]byte{nil, append([]byte("  "), front...), parts[2]}, []byte("---\n"))
				raw = bytes.ReplaceAll(raw, []byte("\n"), []byte(eol))
				_, err := ParseCompilePlan("plan.md", raw)
				repinMust(t, err)
				for _, to := range []string{target, old} {
					result, err := RepinCompileUnit(raw, "c1", to, CompileOptions{})
					repinMust(t, err)
					if !bytes.Equal(result.Plan, bytes.ReplaceAll(raw, []byte(old), []byte(to))) {
						t.Fatal("indented plan bytes changed incorrectly")
					}
				}
			})
		}
	}
}
