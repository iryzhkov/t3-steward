package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompileAcceptanceTestsMultiUnitNoWrites(t *testing.T) {
	for _, firstValid := range []bool{true, false} {
		for _, existing := range []bool{true, false} {
			planPath, raw := compileFixturePlan(t)
			text := strings.Replace(string(raw), "compile: v1", "compile: v1\nacceptance_tests: required", 1)
			if firstValid {
				text = strings.Replace(text, "Turn a plan into campaign directories.", "Acceptance tests:\n- `boundary`: refuses (edge)", 1)
			}
			if e := os.WriteFile(planPath, []byte(text), 0600); e != nil {
				t.Fatal(e)
			}
			out := filepath.Join(t.TempDir(), "out")
			sentinel := filepath.Join(out, "sentinel")
			if existing {
				if e := os.Mkdir(out, 0700); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(sentinel, []byte("unchanged"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			var stdout bytes.Buffer
			e := campaignTestCLI(t, &stdout).run(context.Background(), []string{"compile", planPath, "--out", out})
			line := func(id string) string {
				prefix := text[:strings.Index(text, "  - id: "+id)]
				return "plan.md:" + fmt.Sprint(strings.Count(prefix, "\n")+1) + ":"
			}
			if e == nil || !strings.Contains(e.Error(), `unit "b1"`) || !strings.Contains(e.Error(), line("b1")) {
				t.Fatalf("error=%v", e)
			}
			want := 1
			if !firstValid {
				want = 2
				if !strings.Contains(e.Error(), `unit "c1"`) || !strings.Contains(e.Error(), line("c1")) {
					t.Fatal(e)
				}
			}
			var textError authoringTextError
			if !errors.As(e, &textError) {
				t.Fatalf("missing diagnostic text wrapper: %v", e)
			}
			if strings.Count(textError.Unwrap().Error(), "unit \"") != want {
				t.Fatal(e)
			}
			if existing {
				assertDirectoryEntries(t, out, "sentinel")
				got, e := os.ReadFile(sentinel)
				if e != nil || string(got) != "unchanged" {
					t.Fatalf("sentinel=%q error=%v", got, e)
				}
			} else if _, e := os.Stat(out); !os.IsNotExist(e) {
				t.Fatalf("output created: %v", e)
			}
		}
	}
}
