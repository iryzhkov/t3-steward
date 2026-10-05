package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/campaign"
)

func TestAgentGuidanceProjections(t *testing.T) {
	page := helpPages[""]
	fragment := guidanceText(guidanceFor(page.RuleIDs))
	if len(fragment) > 2500 || len(strings.Fields(fragment)) > 400 {
		t.Fatalf("fragment exceeds bound: %d bytes, %d words", len(fragment), len(strings.Fields(fragment)))
	}
	t.Logf("resident: %d UTF8 bytes, %d words; pages: %d; rules: %d", len(fragment), len(strings.Fields(fragment)), len(helpPages), len(agentGuidanceRules))
	var out bytes.Buffer
	if err := renderHelpExport(&out, page, "json"); err != nil {
		t.Fatal(err)
	}
	var doc guidanceDocument
	dec := json.NewDecoder(bytes.NewReader(out.Bytes()))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		t.Fatal(err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("trailing document: %v", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(out.Bytes(), &root); err != nil {
		t.Fatal(err)
	}
	keys := []string{}
	for key := range root {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	want := []string{"authority", "pages", "resident_fragment", "rules", "schema", "schema_version", "selected_path", "tool_id"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("closed keys: %v", keys)
	}
	if doc.Schema != "steward-help-guidance/v1" || doc.SchemaVersion != 1 || doc.ToolID != "t3-steward" || doc.Authority != "descriptive-only" || doc.SelectedPath != "" {
		t.Fatal("identity mismatch")
	}
	if doc.ResidentFragment != fragment || len(doc.Pages) != len(helpPages) {
		t.Fatal("missing registry projection")
	}
	for i, p := range doc.Pages {
		if p.Path != helpPagePaths()[i] {
			t.Fatal("noncanonical page order")
		}
		source := helpPages[p.Path]
		if p.ReferenceHelp != source.renderReference() || p.FullHelp != source.render() {
			t.Fatalf("lost full reference: %s", p.Path)
		}
		if source.Body != "" && (p.ReferenceHelp != source.Body || len(p.Flags) != 0 || len(p.Exits) != 0) {
			t.Fatal("invented body metadata")
		}
		for _, r := range doc.Rules {
			if (p.Path == "" && !strings.Contains(p.FullHelp, r.Text)) || !strings.Contains(fragment, r.Text) {
				t.Fatalf("rule parity: %s", r.ID)
			}
		}
	}
	var again bytes.Buffer
	if err := renderHelpExport(&again, page, "json"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), again.Bytes()) {
		t.Fatal("nondeterministic bytes")
	}
	// A private fixture rule edit must affect every projection immediately.
	old := agentGuidanceRules
	agentGuidanceRules = append([]agentGuidanceRule(nil), old...)
	defer func() { agentGuidanceRules = old }()
	agentGuidanceRules[0].Text = "private fixture changed owned rule"
	var changed bytes.Buffer
	if err := renderHelpExport(&changed, page, "json"); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{changed.String(), page.render(), guidanceText(guidanceFor(page.RuleIDs))} {
		if !strings.Contains(s, agentGuidanceRules[0].Text) {
			t.Fatal("rule edit missing from projection")
		}
	}
}

func TestAgentGuidanceScopedAdmission(t *testing.T) {
	for _, path := range helpPagePaths() {
		for _, format := range []string{"json", "agent-md"} {
			var out bytes.Buffer
			args := append(strings.Fields(path), "--help", format)
			ok, err := admitHelp(&out, nil, args)
			if !ok || err != nil {
				t.Fatalf("%v: %v", args, err)
			}
			if format == "json" {
				var doc guidanceDocument
				if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
					t.Fatal(err)
				}
				if doc.SelectedPath != path || len(doc.Pages) == 0 {
					t.Fatal("wrong scope")
				}
				for _, p := range doc.Pages {
					if path != "" && p.Path != path && !strings.HasPrefix(p.Path, path+" ") {
						t.Fatalf("scope leaked %s into %s", p.Path, path)
					}
				}
			}
		}
	}
	for _, topic := range campaign.HelpTopics() {
		var legacy bytes.Buffer
		handled, err := admitCampaignHelp(&legacy, []string{"help", topic.Name})
		if !handled || err != nil || legacy.String() != topic.Body {
			t.Fatalf("legacy topic %s changed: %v", topic.Name, err)
		}
		var export bytes.Buffer
		handled, err = admitCampaignHelp(&export, []string{"help", topic.Name, "json"})
		if !handled || err == nil || export.Len() != 0 {
			t.Fatalf("essay mislabeled as export: %s", topic.Name)
		}
	}
	var unknown bytes.Buffer
	handled, err := admitCampaignHelp(&unknown, []string{"help", "bogus"})
	if !handled || err == nil || !strings.Contains(err.Error(), "unknown campaign help topic") {
		t.Fatal("legacy unknown-topic diagnostic changed")
	}
}

type guidanceFailWriter struct{}

func (guidanceFailWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func TestAgentGuidanceStreamFailure(t *testing.T) {
	for _, format := range []string{"json", "agent-md"} {
		handled, err := admitHelp(guidanceFailWriter{}, nil, []string{"--help", format})
		if !handled || !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("%s: %v", format, err)
		}
	}
}

func TestAgentGuidanceExportClassification(t *testing.T) {
	cases := []struct {
		args   []string
		export bool
	}{
		{[]string{"--help", "json"}, true},
		{[]string{"help", "task", "run", "agent-md"}, true},
		{[]string{"campaign", "--config", "bad", "--help", "json"}, true},
		{[]string{"task", "run", "--help", "json", "full"}, true},
		{[]string{"campaign", "help", "plan", "json"}, true},
		{[]string{"--help", "bogusformat"}, true},
		{[]string{"--help"}, false},
		{[]string{"--help", "full"}, false},
		{[]string{"campaign", "help", "bogus"}, false},
		{[]string{"task", "run", "json"}, false},
		{[]string{"task", "run", "agent-md"}, false},
		{[]string{"task", "run", "--", "--help", "json"}, false},
		{[]string{"wait", "add", "--", "echo", "--help", "agent-md"}, false},
		{[]string{"--", "--help", "json"}, false},
	}
	for _, c := range cases {
		if got := isHelpExportInvocation(c.args); got != c.export {
			t.Fatalf("%v: %v", c.args, got)
		}
	}
}

func guidanceTree(t *testing.T, root string) map[string]string {
	t.Helper()
	tree := map[string]string{}
	err := filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		body := ""
		if !e.IsDir() {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			body = string(b)
		}
		tree[rel] = info.Mode().String() + ":" + body
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

func TestAgentGuidanceActualBinary(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "t3-steward")
	build := exec.Command("go", "build", "-o", binary, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	fixture := t.TempDir()
	for _, dir := range []string{"home", "config", "state", "feedback", "work"} {
		if err := os.Mkdir(filepath.Join(fixture, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	badConfig := filepath.Join(fixture, "config", "broken.yaml")
	if err := os.WriteFile(badConfig, []byte("not: [valid yaml"), 0600); err != nil {
		t.Fatal(err)
	}
	env := []string{}
	for _, e := range os.Environ() {
		key, _, _ := strings.Cut(e, "=")
		if key == "HOME" || strings.HasPrefix(key, "XDG_") || strings.HasPrefix(key, "T3_STEWARD_") || strings.HasPrefix(key, "TOOLFEEDBACK_") {
			continue
		}
		env = append(env, e)
	}
	env = append(env, "HOME="+filepath.Join(fixture, "home"), "XDG_CONFIG_HOME="+filepath.Join(fixture, "config"), "XDG_STATE_HOME="+filepath.Join(fixture, "state"), "TOOLFEEDBACK_DIR="+filepath.Join(fixture, "feedback"), "T3_STEWARD_FRICTION=1", "T3_STEWARD_DRY_RUN=invalid", "T3_STEWARD_COORDINATOR_URL=http://127.0.0.1:1")
	call := func(args []string) ([]byte, []byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Dir = filepath.Join(fixture, "work")
		cmd.Env = env
		var out, errOut bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &errOut
		err := cmd.Run()
		if ctx.Err() != nil {
			t.Fatalf("binary hung: %v", args)
		}
		return out.Bytes(), errOut.Bytes(), err
	}
	initial := guidanceTree(t, fixture)
	positives := [][]string{{"--help", "json"}, {"-h", "agent-md"}, {"help", "json"}, {"help", "task", "run", "json"}, {"campaign", "--help", "json"}, {"campaign", "--help", "agent-md"}, {"campaign", "submit", "--config", badConfig, "--help", "json"}, {"campaign", "help", "json", "submit"}, {"task", "--help", "json"}, {"task", "run", "--config", badConfig, "--help", "agent-md"}, {"task", "run", "--help", "json"}, {"--help", "json", "--config", badConfig}}
	for _, args := range positives {
		out, stderr, err := call(args)
		if err != nil || len(stderr) != 0 || len(out) == 0 {
			t.Fatalf("%v: %v stdout=%s stderr=%s", args, err, out, stderr)
		}
		if strings.Contains(strings.Join(args, " "), "json") {
			var doc guidanceDocument
			if err := json.Unmarshal(out, &doc); err != nil {
				t.Fatalf("%v: %v", args, err)
			}
		}
		if !reflect.DeepEqual(initial, guidanceTree(t, fixture)) {
			t.Fatalf("export mutated fixture: %v", args)
		}
	}
	negatives := [][]string{{"--help", "bogusformat"}, {"--help", "json", "json"}, {"--help", "json", "agent-md"}, {"--help", "full", "json"}, {"task", "run", "--help", "json", "full"}, {"--help", "json", "missing"}, {"task", "bogus", "--help", "json"}, {"campaign", "--help", "json", "json"}, {"campaign", "help", "readiness", "json"}, {"campaign", "help", "plan", "json"}, {"campaign", "help", "bogus", "json"}, {"campaign", "submit", "--help", "json", "bogus"}, {"--help", "json", "agent-md", "--json"}}
	for _, args := range negatives {
		out, stderr, err := call(args)
		if err == nil || len(out) != 0 || len(stderr) == 0 {
			t.Fatalf("refusal %v: %v stdout=%s stderr=%s", args, err, out, stderr)
		}
		if !reflect.DeepEqual(initial, guidanceTree(t, fixture)) {
			t.Fatalf("refusal mutated fixture: %v", args)
		}
	}
	// Positive instrumentation controls use actual binary invocations. They
	// cannot contact a service: help or unknown root command is the only route.
	controls := [][]string{{"--help"}, {"--help", "full"}, {"campaign", "help", "plan"}, {"campaign", "help", "bogus"}, {"--", "--help", "json"}, {"missing", "json"}, {"missing", "agent-md"}, {"missing", "--", "--help", "json"}}
	for _, args := range controls {
		before := guidanceTree(t, fixture)
		_, _, _ = call(args)
		if reflect.DeepEqual(before, guidanceTree(t, fixture)) {
			t.Fatalf("ordinary invocation lost instrumentation: %v", args)
		}
	}
	// Closed stdout is bounded for both new formats; SIGPIPE or an ordinary
	// write error is acceptable, success after a closed stream is not.
	for _, format := range []string{"json", "agent-md"} {
		rd, wr, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		rd.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, binary, "--help", format)
		cmd.Env = env
		cmd.Dir = filepath.Join(fixture, "work")
		cmd.Stdout = wr
		before := guidanceTree(t, fixture)
		err = cmd.Run()
		wr.Close()
		cancel()
		if err == nil {
			t.Fatal("closed pipe succeeded")
		}
		if !reflect.DeepEqual(before, guidanceTree(t, fixture)) {
			t.Fatal("broken-pipe export spooled")
		}
	}
	t.Logf("actual binary: %d positive exports, %d negative exports, %d instrumentation controls, 2 closed pipes", len(positives), len(negatives), len(controls))
}
