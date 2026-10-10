//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
	"gopkg.in/yaml.v3"
)

func configValidationFixture(t *testing.T) (string, string, []byte) {
	t.Helper()
	root := t.TempDir()
	for _, dir := range []string{"home", "state", "feedback", "tools"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config.Default()
	cfg.StatePath = filepath.Join(root, "state", "PRODUCTION.db")
	cfg.T3.URL = "http://127.0.0.1:1"
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.BacklogV2.Workers = map[string]config.V2Worker{
		"normandy": {Address: "normandy", Epoch: "worker-1", AcceptBacklog: true, Credential: "ssh:normandy", Providers: map[string]config.V2Provider{"codex": {Models: []string{"gpt"}, QuotaPool: "codex-main"}}},
	}
	cfg.BacklogV2.Projects = map[string]config.V2Project{
		"steward": {Repository: "git@example/steward", DefaultRef: "main", T3Project: "development", Workers: []string{"normandy"}},
	}
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{"codex-main": {Provider: "codex", MaxConcurrent: 2}}
	cfg.BacklogV2.Storage = config.V2Storage{Bundles: filepath.Join(root, "bundles"), Artifacts: filepath.Join(root, "artifacts"), Workspaces: filepath.Join(root, "workspaces")}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("invalid coordinator fixture: %v", err)
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "SECRET-staged.yaml")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	// Unsafe input controls exercise the same production invoke/binary matrix.
	if err := os.WriteFile(path+".unsafe", raw, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path+".unsafe", 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, path+".symlink"); err != nil {
		t.Fatal(err)
	}
	// These are intentionally unusable runtime inputs. A production DB open
	// must fail; external transport/service/provider execution writes a marker.
	if err := os.WriteFile(cfg.StatePath, []byte("NOT A DATABASE; NEVER OPEN"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tool := range []string{"ssh", "systemctl", "t3", "curl", "claude", "codex"} {
		script := fmt.Sprintf("#!/bin/sh\nprintf invoked > '%s'\nexit 99\n", filepath.Join(root, "FORBIDDEN-"+tool))
		if err := os.WriteFile(filepath.Join(root, "tools", tool), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	return root, path, raw
}

func configValidationEnvironment(root string) []string {
	var env []string
	for _, e := range os.Environ() {
		key, _, _ := strings.Cut(e, "=")
		if key == "HOME" || key == "PATH" || strings.HasPrefix(key, "XDG_") || strings.HasPrefix(key, "T3_STEWARD_") || strings.HasPrefix(key, "TOOLFEEDBACK_") {
			continue
		}
		env = append(env, e)
	}
	return append(env,
		"HOME="+filepath.Join(root, "home"), "XDG_CONFIG_HOME="+filepath.Join(root, "home", ".config"),
		"XDG_STATE_HOME="+filepath.Join(root, "state"), "TOOLFEEDBACK_DIR="+filepath.Join(root, "feedback"),
		"T3_STEWARD_FRICTION=1", "T3_STEWARD_DRY_RUN=SECRET-invalid", "T3_STEWARD_BACKLOG_V2_MODE=SECRET-invalid",
		"T3_STEWARD_T3_URL=SECRET-invalid", "T3_STEWARD_COORDINATOR_URL=SECRET-invalid",
		"PATH="+filepath.Join(root, "tools"))
}

func assertConfigValidationResult(t *testing.T, code int, out, stderr string, raw []byte) {
	t.Helper()
	sum := sha256.Sum256(raw)
	want := map[string]any{"schema_version": float64(1), "kind": "config-validation", "valid": true, "file_sha256": hex.EncodeToString(sum[:])}
	var got map[string]any
	d := json.NewDecoder(strings.NewReader(out))
	if code != 0 || stderr != "" || len(out) > 256 || d.Decode(&got) != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("validation code=%d out=%q stderr=%q", code, out, stderr)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		t.Fatalf("extra output %v", err)
	}
}

func assertConfigValidationRefusal(t *testing.T, code int, out, stderr string) {
	t.Helper()
	if code != 1 || stderr != "error: configuration validation refused\n" || len(out) > 512 ||
		strings.Contains(out, "SECRET") || strings.Contains(out, "valid") && strings.Contains(out, "\"valid\"") ||
		strings.Contains(out, "file_sha256") {
		t.Fatalf("refusal code=%d out=%q stderr=%q", code, out, stderr)
	}
	if out != "" {
		var got map[string]any
		if err := json.Unmarshal([]byte(out), &got); err != nil || got["kind"] != "error" || got["message"] != "configuration validation refused" {
			t.Fatalf("unsanitized error %q", out)
		}
	}
}

func configValidationBadArgs(path string) [][]string {
	bad := [][]string{
		{"config", "validate", "--json"},
		{"config", "validate", "--file", "help", "--json"},
		{"config", "validate", "--file", "", "--json"},
		{"config", "validate", "--file", path},
		{"config", "validate", "--file", path + ".missing", "--json"},
		{"config", "validate", "--file", path + ".unsafe", "--json"},
		{"config", "validate", "--file", path + ".symlink", "--json"},
		{"config", "validate", "--file", path, "--json", "--SECRET=VALUE"},
		{"config", "validate", "--file", path, "--json", "SECRET-operand"},
		{"config", "validate", "--file", path, "--json", "--config", "SECRET"},
		{"config", "validate", "--file", path, "--json=false"},
		{"config", "validate", "--file", path, "--json", "--json"},
		{"config", "validate", "--file", path, "--file", path, "--json"},
		{"config", "validate", "--file", path, "--json", "--dry-run"},
	}
	for _, help := range []string{"help", "-h", "--help"} {
		bad = append(bad,
			[]string{"config", "validate", help, "--file", path, "--json"},
			[]string{"config", "validate", help, "--file", path + ".missing", "--json", "--SECRET"},
			[]string{"config", "validate", help, "--file", path + ".unsafe", "--json"},
			[]string{"config", "validate", "--file", path + ".symlink", help, "--json"},
			[]string{"config", help, "full", "validate", "--file", path, "--json"},
			[]string{"config", "validate", "--file", path, help, "--json"},
			[]string{"config", "validate", "--file", path, "--json", help},
			[]string{"config", help, "validate", "--file", path, "--json"},
			[]string{"config", "validate", help, "--json"},
			[]string{"config", "validate", help, "SECRET-operand"},
		)
	}
	return bad
}

func TestConfigValidateInvokePureAndRedacted(t *testing.T) {
	root, path, raw := configValidationFixture(t)
	for _, e := range configValidationEnvironment(root) {
		key, value, _ := strings.Cut(e, "=")
		t.Setenv(key, value)
	}
	before := guidanceTree(t, root)
	code, out, stderr := captureSpoolStd(t, []string{"config", "validate", "--file", path, "--json"})
	assertConfigValidationResult(t, code, out, stderr, raw)
	for _, args := range configValidationBadArgs(path) {
		code, out, stderr = captureSpoolStd(t, args)
		assertConfigValidationRefusal(t, code, out, stderr)
	}
	if !reflect.DeepEqual(before, guidanceTree(t, root)) {
		t.Fatal("validator invocation wrote a runtime marker or spool")
	}
	for _, bad := range []string{"SECRET_PARSE: [SECRET_VALUE", "log_level: SECRET_VALUE\n", "backlog_v2:\n  coordinator_client:\n    credential: SECRET_NAMESPACE\n"} {
		if err := os.WriteFile(path, []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		before = guidanceTree(t, root)
		code, out, stderr = captureSpoolStd(t, []string{"config", "validate", "--file", path, "--json"})
		assertConfigValidationRefusal(t, code, out, stderr)
		if !reflect.DeepEqual(before, guidanceTree(t, root)) {
			t.Fatal("refusal wrote runtime marker or spool")
		}
	}
	// A nearby ordinary command still records instrumentation.
	code, _, _ = captureSpoolStd(t, []string{"version"})
	if code != 0 || len(spoolLines(t, filepath.Join(root, "feedback"))) == 0 {
		t.Fatal("ordinary instrumentation disabled")
	}
}

func configValidationProjectionRefusal(t *testing.T, root, path string, raw []byte, call func([]string) (int, string, string)) {
	t.Helper()
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	projection := filepath.Join(root, "home", config.CoordinatorFleetPath)
	if err := os.MkdirAll(filepath.Dir(projection), 0700); err != nil {
		t.Fatal(err)
	}
	doc := []byte(`{"kind":"steward-coordinator-catalog-input","schema_version":1,"coordinator_id":"SECRET-other","workers":{},"projects":{}}`)
	if err := os.WriteFile(projection, doc, 0600); err != nil {
		t.Fatal(err)
	}
	before := guidanceTree(t, root)
	code, out, stderr := call([]string{"config", "validate", "--file", path, "--json"})
	assertConfigValidationRefusal(t, code, out, stderr)
	if !reflect.DeepEqual(before, guidanceTree(t, root)) {
		t.Fatal("projection refusal wrote runtime markers")
	}
}

func TestConfigValidateActualBinaryNoEffects(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "t3-steward")
	build := exec.Command("go", "build", "-o", binary, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %v %s", err, out)
	}
	root, path, raw := configValidationFixture(t)
	call := func(args []string) (int, string, string) {
		ctx, cancel := context.WithTimeout(context.Background(), testtiming.Bound(10*time.Second))
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = configValidationEnvironment(root)
		cmd.Dir = root
		var out, stderr bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err := cmd.Run()
		if ctx.Err() != nil {
			t.Fatal("validator reached a hanging runtime path")
		}
		code := 0
		if err != nil {
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatal(err)
			}
			code = exit.ExitCode()
		}
		return code, out.String(), stderr.String()
	}
	before := guidanceTree(t, root)
	code, out, stderr := call([]string{"config", "validate", "--file", path, "--json"})
	assertConfigValidationResult(t, code, out, stderr, raw)
	for _, args := range configValidationBadArgs(path) {
		code, out, stderr = call(args)
		assertConfigValidationRefusal(t, code, out, stderr)
	}
	if !reflect.DeepEqual(before, guidanceTree(t, root)) {
		t.Fatal("built validator wrote runtime marker or spool")
	}
	if err := os.WriteFile(path, []byte("SECRET_UNKNOWN: SECRET_VALUE\n"), 0600); err != nil {
		t.Fatal(err)
	}
	before = guidanceTree(t, root)
	code, out, stderr = call([]string{"config", "validate", "--file", path, "--json"})
	assertConfigValidationRefusal(t, code, out, stderr)
	if !reflect.DeepEqual(before, guidanceTree(t, root)) {
		t.Fatal("built refusal wrote runtime marker or spool")
	}
	configValidationProjectionRefusal(t, root, path, raw, call)
	code, _, _ = call([]string{"version"})
	if code != 0 || len(spoolLines(t, filepath.Join(root, "feedback"))) == 0 {
		t.Fatal("built ordinary instrumentation disabled")
	}
}
