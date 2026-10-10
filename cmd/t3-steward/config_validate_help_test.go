//go:build linux

package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigValidateStandaloneHelpAndFileData(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "t3-steward")
	if out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	root, _, raw := configValidationFixture(t)
	for _, e := range configValidationEnvironment(root) {
		key, value, _ := strings.Cut(e, "=")
		t.Setenv(key, value)
	}
	t.Chdir(root)
	callBinary := func(args []string) (int, string, string) {
		cmd := exec.Command(binary, args...)
		cmd.Env = configValidationEnvironment(root)
		var out, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &stderr
		if err := cmd.Run(); err != nil {
			if exit, ok := err.(*exec.ExitError); ok {
				return exit.ExitCode(), out.String(), stderr.String()
			}
			t.Fatal(err)
		}
		return 0, out.String(), stderr.String()
	}
	for _, call := range []func([]string) (int, string, string){
		func(args []string) (int, string, string) { return captureSpoolStd(t, args) }, callBinary,
	} {
		for _, help := range []string{"help", "-h", "--help"} {
			for _, args := range [][]string{
				{"config", help}, {"config", help, "full"},
				{"config", help, "validate"}, {"config", help, "full", "validate"},
				{"config", "validate", help}, {"config", "validate", help, "full"},
			} {
				code, out, stderr := call(args)
				if code != 0 || stderr != "" || !strings.Contains(out, "t3-steward config validate --file PATH --json") {
					t.Fatalf("standalone help %v: %d %q %q", args, code, out, stderr)
				}
				if args[len(args)-1] == "full" && args[1] == "validate" && !strings.Contains(strings.Join(strings.Fields(out), " "), "Coordinator Discord configurations refuse") {
					t.Fatal("full help omitted credential-dependent restriction")
				}
			}
			// Even help-looking file values must be parsed and hashed as data.
			path := filepath.Join(root, help)
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			code, out, stderr := call([]string{"config", "validate", "--file", help, "--json"})
			assertConfigValidationResult(t, code, out, stderr, raw)
		}
	}
}
