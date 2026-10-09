package backlog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/testutil"
)

// symlinkedTemp makes a real directory and a symbolic link to it, as on a
// host whose temporary directory was moved to another disk.
func symlinkedTemp(t *testing.T) (link, real string) {
	t.Helper()
	root := testutil.RealTempDir(t)
	real = filepath.Join(root, "steward-tmp")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	link = filepath.Join(root, "cache-tmp")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	return link, real
}

func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func TestResolveTempDirectoriesFollowsLinksAndCreatesMissingDirectories(t *testing.T) {
	link, real := symlinkedTemp(t)
	goTemp := filepath.Join(link, "go", "missing")
	got, err := ResolveTempDirectories(lookupFrom(map[string]string{"TMPDIR": link, "GOTMPDIR": goTemp}))
	if err != nil {
		t.Fatal(err)
	}
	want := []TempDirectory{
		{Variable: "TMPDIR", Configured: link, Resolved: real},
		{Variable: "GOTMPDIR", Configured: goTemp, Resolved: filepath.Join(real, "go", "missing")},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ResolveTempDirectories = %+v, want %+v", got, want)
	}
	if info, err := os.Stat(filepath.Join(real, "go", "missing")); err != nil || !info.IsDir() {
		t.Fatalf("GOTMPDIR was not created: %v", err)
	}
	if environment := tempEnvironment(got); !slices.Equal(environment, []string{"TMPDIR=" + real, "GOTMPDIR=" + filepath.Join(real, "go", "missing")}) {
		t.Fatalf("environment = %q", environment)
	}
}

func TestResolveTempDirectoriesDefaultsTMPDIRAndLeavesGOTMPDIRUnset(t *testing.T) {
	got, err := ResolveTempDirectories(lookupFrom(nil))
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks("/tmp")
	if err != nil {
		t.Fatal(err)
	}
	if want := []TempDirectory{{Variable: "TMPDIR", Resolved: resolved}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ResolveTempDirectories = %+v, want %+v", got, want)
	}
}

func TestTempDirectoryWarningsNameTheLinkAndItsTarget(t *testing.T) {
	link, real := symlinkedTemp(t)
	below := filepath.Join(link, "go")
	got := TempDirectoryWarnings(lookupFrom(map[string]string{"TMPDIR": link, "GOTMPDIR": below}))
	want := []string{
		fmt.Sprintf("TMPDIR %s is a symlink to %s", link, real),
		fmt.Sprintf("GOTMPDIR %s: %s is a symlink to %s", below, link, real),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("warnings = %q, want %q", got, want)
	}
	if got := TempDirectoryWarnings(lookupFrom(map[string]string{"TMPDIR": real})); len(got) != 0 {
		t.Fatalf("warnings for a real TMPDIR = %q", got)
	}
}

// Verification and gate commands get the resolved temporary directories in
// their environment, and both logs name them. The persisted reports must also
// decode against the frozen rc119 field sets, including nested gate results.
func TestVerificationAndGateRunWithResolvedTempDirectories(t *testing.T) {
	dir := h2GateRepository(t)
	storage := t.TempDir()
	link, real := symlinkedTemp(t)
	t.Setenv("TMPDIR", link)
	goTemp := filepath.Join(link, "go")
	t.Setenv("GOTMPDIR", goTemp)
	runner := &directRunner{}
	result, err := (AttemptFinalizer{StorageRoot: storage, Processes: runner}).Finalize(context.Background(), h2GateRequest(dir, "temp"))
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	wantDirectories := []TempDirectory{{Variable: "TMPDIR", Configured: link, Resolved: real}, {Variable: "GOTMPDIR", Configured: goTemp, Resolved: filepath.Join(real, "go")}}
	var commands int
	for _, call := range runner.calls {
		if call.Program != "/bin/sh" {
			continue
		}
		commands++
		if !slices.Equal(call.Environment, []string{"TMPDIR=" + real, "GOTMPDIR=" + filepath.Join(real, "go")}) {
			t.Fatalf("command %q environment = %q, want resolved TMPDIR", call.Args, call.Environment)
		}
	}
	if commands != 2 {
		t.Fatalf("ran %d verify and gate commands, want 2: %+v", commands, runner.calls)
	}
	gate := h2ReadGate(t, storage, result)
	if !gate.Passed {
		t.Fatalf("gate report = %+v", gate)
	}
	verification := findArtifact(t, result.Artifacts, domain.ArtifactVerification)
	var report VerificationReport
	if err := json.Unmarshal(readStoredArtifact(t, storage, verification), &report); err != nil {
		t.Fatal(err)
	}
	wantLog := tempDirectoryLog(wantDirectories)
	if !strings.HasPrefix(report.Output, wantLog) || !strings.HasSuffix(report.Output, "verified") {
		t.Fatalf("verification output = %q, want directory evidence and command output", report.Output)
	}
	var gateJSON, gateLog []byte
	for _, artifact := range result.Artifacts {
		if artifact.Name == "gate" {
			gateJSON = readStoredArtifact(t, storage, artifact)
		}
		if artifact.Name == "gate/log.txt" {
			gateLog = readStoredArtifact(t, storage, artifact)
		}
	}
	if !bytes.HasPrefix(gateLog, []byte(wantLog)) || !bytes.Contains(gateLog, []byte("gated")) {
		t.Fatalf("gate log = %q, want directory evidence and command output", gateLog)
	}
	decodeRC119Report(t, readStoredArtifact(t, storage, verification), &rc119VerificationReport{})
	decodeRC119Report(t, gateJSON, &rc119GateReport{})
	// Exercise all optional gate fields, not just the passing production path.
	gate.Failure = &GateFailure{Command: "false", ExitCode: 1, Reason: "failed"}
	gate.OutputLimitation = "contained"
	gate.Commands[0].Error = "failed"
	raw, err := json.Marshal(gate)
	if err != nil {
		t.Fatal(err)
	}
	decodeRC119Report(t, raw, &rc119GateReport{})
}

func TestSystemdScopeRunnerSetsRequestEnvironment(t *testing.T) {
	root := t.TempDir()
	argsPath := filepath.Join(root, "run.args")
	systemdRun := writeExecutable(t, root, "systemd-run", fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$@\" > %q\n", argsPath))
	systemctl := writeExecutable(t, root, "systemctl", "#!/bin/sh\nexit 99\n")
	runner := SystemdScopeRunner{SystemdRunBinary: systemdRun, SystemctlBinary: systemctl}
	request := ProcessRequest{ID: "verify-1", Dir: root, Program: "/bin/sh", Args: []string{"-c", "true"}, Environment: []string{"TMPDIR=/srv/real-tmp"}}
	if _, err := runner.Run(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	args := strings.Split(readAbsoluteTestFile(t, argsPath), "\n")
	setenv := slices.Index(args, "--setenv=TMPDIR=/srv/real-tmp")
	if setenv < 0 || setenv > slices.Index(args, "--") {
		t.Fatalf("systemd-run arguments do not set TMPDIR before the command: %q", args)
	}
	for _, bad := range []string{"TMPDIR", "=value", "TMPDIR=a\nb"} {
		request.Environment = []string{bad}
		if _, err := runner.Run(context.Background(), request); err == nil || !strings.Contains(err.Error(), "NAME=value") {
			t.Fatalf("environment entry %q error = %v", bad, err)
		}
	}
}

// These field sets are copied from 9f653a84. Do not derive them from current
// report types: new fields must fail compatibility checks until negotiated.
type rc119VerificationReport struct {
	Command     string    `json:"command"`
	ExitCode    int       `json:"exitCode"`
	Output      string    `json:"output"`
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`
}
type rc119GateReport struct {
	Commands         []rc119GateCommandResult `json:"commands"`
	TreeHash         string                   `json:"treeHash"`
	Worker           string                   `json:"worker"`
	ToolVersions     map[string]string        `json:"toolVersions"`
	StartedAt        time.Time                `json:"startedAt"`
	CompletedAt      time.Time                `json:"completedAt"`
	Attempt          string                   `json:"attempt"`
	Passed           bool                     `json:"passed"`
	Failure          *rc119GateFailure        `json:"failure,omitempty"`
	LogTruncated     bool                     `json:"logTruncated"`
	LogArtifact      string                   `json:"logArtifact"`
	OutputLimitation string                   `json:"outputLimitation,omitempty"`
}
type rc119GateCommandResult struct {
	Command     string        `json:"command"`
	ExitCode    int           `json:"exitCode"`
	StartedAt   time.Time     `json:"startedAt"`
	CompletedAt time.Time     `json:"completedAt"`
	Duration    time.Duration `json:"duration"`
	Error       string        `json:"error,omitempty"`
}
type rc119GateFailure struct {
	Command  string `json:"command"`
	ExitCode int    `json:"exitCode"`
	Reason   string `json:"reason"`
}

func decodeRC119Report(t *testing.T, raw []byte, target any) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("rc119 strict decoder rejected persisted report: %v\n%s", err, raw)
	}
}
