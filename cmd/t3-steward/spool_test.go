package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// spoolSecret is the argument value every leak test drives through the CLI. It
// is shaped like a credential on purpose: if any part of the spool ever copies
// an argument value instead of a name, this string is what shows up in the
// record, and the record is shipped to another host.
const spoolSecret = "sk-live-0xDEADBEEFCAFE-SECRET"

// useSpool points the spool at a scratch directory and turns it on, and
// answers with that directory. Every test in this file calls it: a test must
// never append to the operator's real spool under ~/.local/share/toolfeedback.
func useSpool(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("TOOLFEEDBACK_DIR", dir)
	t.Setenv("T3_STEWARD_FRICTION_DIR", "")
	t.Setenv("T3_STEWARD_FRICTION", "1")
	return dir
}

// spoolLines reads every record the spool holds, in file order.
func spoolLines(t *testing.T, dir string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, path := range spoolPaths(t, dir) {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, line := range strings.Split(strings.TrimRight(string(content), "\n"), "\n") {
			if line == "" {
				continue
			}
			var record map[string]any
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				t.Fatalf("record in %s is not JSON: %v (%q)", path, err, line)
			}
			records = append(records, record)
		}
	}
	return records
}

// spoolPaths lists the spool files this tool wrote under dir.
func spoolPaths(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*", "*.jsonl"))
	if err != nil {
		t.Fatalf("glob the spool: %v", err)
	}
	return matches
}

// spoolBytes is everything the spool holds, for the assertion that a value
// appears nowhere in it at all - not in a field this test knows to look at,
// and not in one added later either.
func spoolBytes(t *testing.T, dir string) string {
	t.Helper()
	var all strings.Builder
	for _, path := range spoolPaths(t, dir) {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		all.Write(content)
	}
	return all.String()
}

// captureSpoolStd runs one command line through the same entry point main uses
// and answers with its exit code and both output streams, so that a test can
// assert the instrumentation changed neither.
func captureSpoolStd(t *testing.T, args []string) (int, string, string) {
	t.Helper()
	stdout, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatalf("create stdout capture: %v", err)
	}
	defer stdout.Close()
	stderr, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatalf("create stderr capture: %v", err)
	}
	defer stderr.Close()

	real := os.Stdout
	os.Stdout = stdout
	code := invoke(args, stderr)
	os.Stdout = real

	return code, readCapture(t, stdout), readCapture(t, stderr)
}

func readCapture(t *testing.T, file *os.File) string {
	t.Helper()
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("rewind capture: %v", err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("read capture: %v", err)
	}
	return string(content)
}

func recordString(t *testing.T, record map[string]any, key string) string {
	t.Helper()
	value, present := record[key]
	if !present {
		t.Fatalf("record has no %q: %v", key, record)
	}
	text, ok := value.(string)
	if !ok {
		t.Fatalf("record %q is %T, want a string", key, value)
	}
	return text
}

// A successful invocation writes exactly one record, and its tool is the full
// verb path rather than the binary name.
func TestSpoolRecordsOneSuccessWithTheFullVerbPath(t *testing.T) {
	dir := useSpool(t)

	code, stdout, stderr := captureSpoolStd(t, []string{"wait", "add", "--help"})
	if code != 0 {
		t.Fatalf("exit code %d, want 0 (stderr %q)", code, stderr)
	}
	if stdout == "" {
		t.Fatal("the help page printed nothing")
	}

	records := spoolLines(t, dir)
	if len(records) != 1 {
		t.Fatalf("%d records, want exactly 1: %v", len(records), records)
	}
	record := records[0]
	if got := recordString(t, record, "tool"); got != "t3-steward wait add" {
		t.Errorf("tool is %q, want the full verb path %q", got, "t3-steward wait add")
	}
	if got := recordString(t, record, "outcome"); got != spoolOK {
		t.Errorf("outcome is %q, want %q", got, spoolOK)
	}
	if ok, _ := record["ok"].(bool); !ok {
		t.Errorf("ok is %v, want true", record["ok"])
	}
	if exit, _ := record["exit"].(float64); exit != 0 {
		t.Errorf("exit is %v, want 0", record["exit"])
	}
	for _, key := range []string{"ts", "host", "session", "proc", "seq", "tool", "outcome", "ok", "exit", "ms"} {
		if _, present := record[key]; !present {
			t.Errorf("record has no %q: %v", key, record)
		}
	}
}

// A refusal writes one record whose outcome is the refusal class, and the exit
// code the caller sees is unchanged: it is exactly what the uninstrumented
// dispatch returns for the same command line.
func TestSpoolRecordsARefusalWithoutChangingTheExitCode(t *testing.T) {
	dir := useSpool(t)

	args := []string{"backlog", "shwo", "--help"}
	code, _, stderr := captureSpoolStd(t, args)
	if code != 1 {
		t.Fatalf("exit code %d, want 1", code)
	}
	if !strings.Contains(stderr, "unknown backlog command") {
		t.Fatalf("stderr %q does not carry the refusal", stderr)
	}
	// dispatch is the path main took before the spool existed; it prints
	// nothing for this refusal and writes no record, so the comparison costs
	// the test neither output nor a second line.
	if want := exitCodeFor(dispatch(args)); want != code {
		t.Fatalf("invoke returned %d, the uninstrumented path returns %d", code, want)
	}
	records := spoolLines(t, dir)
	if len(records) != 1 {
		t.Fatalf("%d records, want exactly 1: %v", len(records), records)
	}
	record := records[0]
	if got := recordString(t, record, "outcome"); got != spoolUsage {
		t.Errorf("outcome is %q, want %q: a verb that does not exist is a refusal the caller can fix", got, spoolUsage)
	}
	if got := recordString(t, record, "err"); got != "unknown-verb" {
		t.Errorf("err is %q, want %q", got, "unknown-verb")
	}
	if ok, _ := record["ok"].(bool); ok {
		t.Error("ok is true for a refusal")
	}
	// The unknown word itself is not a documented verb, so the path stops at
	// the family. The word is never echoed.
	if got := recordString(t, record, "tool"); got != "t3-steward backlog" {
		t.Errorf("tool is %q, want %q", got, "t3-steward backlog")
	}
	if strings.Contains(spoolBytes(t, dir), "shwo") {
		t.Error("the spool echoed the word the caller typed where a verb should stand")
	}
}

// The test that matters most. A command line whose flag value, positional
// argument and trailing command all carry a recognisable secret produces a
// record in which the secret appears nowhere at all. A spool that leaks
// argument values is worse than no spool, because these records are shipped.
func TestSpoolNeverRecordsAnArgumentValue(t *testing.T) {
	for _, testCase := range []struct {
		name string
		args []string
		tool string
		keys []string
	}{
		{
			name: "flag value, positional and trailing command",
			args: []string{
				"replay", "--speed", "1", "--notify-thread", spoolSecret,
				"/tmp/" + spoolSecret + "/events.log",
				"--", "./deploy.sh", "--token", spoolSecret,
			},
			tool: "t3-steward replay",
			// --notify-thread is a flag this program declares, on another
			// verb, so it is recorded by name: "an option that exists, passed
			// where it is not accepted" is exactly the cluster this spool has
			// to be able to show. --token stands after the bare "--", so it
			// belongs to the caller's own command and is not scanned at all.
			keys: []string{"speed", "notify-thread"},
		},
		{
			name: "secret in the first positional, where a verb could stand",
			args: []string{"replay", "/tmp/" + spoolSecret + "/events.log"},
			tool: "t3-steward replay",
			keys: nil,
		},
		{
			name: "secret joined to the flag with an equals sign",
			args: []string{"replay", "--speed=" + spoolSecret},
			tool: "t3-steward replay",
			keys: []string{"speed"},
		},
		{
			name: "secret standing where a top-level command should",
			args: []string{spoolSecret, "--json"},
			tool: "t3-steward",
			keys: []string{"json"},
		},
		{
			name: "secret standing where a flag name should",
			args: []string{"replay", "--" + spoolSecret},
			tool: "t3-steward replay",
			// Shaped exactly like a flag, and not one: the allowlist is what
			// keeps it out, not the shape.
			keys: []string{spoolUndeclaredFlag},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dir := useSpool(t)
			captureSpoolStd(t, testCase.args)

			content := spoolBytes(t, dir)
			if content == "" {
				t.Fatal("no record was written")
			}
			if strings.Contains(content, spoolSecret) {
				t.Fatalf("the secret reached the spool: %s", content)
			}
			// The same assertion for the pieces a partial copy would leave
			// behind: a path fragment, or the value with its prefix stripped.
			for _, fragment := range []string{"DEADBEEF", "sk-live", "/tmp/", "deploy.sh"} {
				if strings.Contains(content, fragment) {
					t.Fatalf("the spool carries %q from the command line: %s", fragment, content)
				}
			}

			records := spoolLines(t, dir)
			if len(records) != 1 {
				t.Fatalf("%d records, want exactly 1", len(records))
			}
			if got := recordString(t, records[0], "tool"); got != testCase.tool {
				t.Errorf("tool is %q, want %q", got, testCase.tool)
			}
			var keys []string
			// args is omitted entirely when a command line carried no flag
			// key worth recording, which is what the reader expects.
			if recorded, present := records[0]["args"].([]any); present {
				for _, value := range recorded {
					keys = append(keys, value.(string))
				}
			}
			if strings.Join(keys, ",") != strings.Join(testCase.keys, ",") {
				t.Errorf("args are %v, want %v", keys, testCase.keys)
			}
		})
	}
}

// A flag this program declares is recorded by name; anything else that stood
// where a flag should is recorded as the fact that it was there.
func TestSpoolRecordsDeclaredFlagNamesOnly(t *testing.T) {
	declared := spoolDeclaredFlags()
	// A sample of options from both kinds of page: structured flags, and the
	// references a Body page carries in full.
	for _, name := range []string{"json", "config", "dry-run", "speed", "model", "reason", "task", "timeout"} {
		if _, present := declared[name]; !present {
			t.Errorf("--%s is documented and not in the allowlist, so it would record as %q", name, spoolUndeclaredFlag)
		}
	}
	for _, name := range []string{"totally-not-a-flag", spoolSecret, ""} {
		if _, present := declared[name]; present {
			t.Errorf("--%s is not a flag this program declares and is in the allowlist", name)
		}
	}

	keys := spoolArgKeys([]string{
		"task", "run", "--model", spoolSecret, "--" + spoolSecret,
		"--totally-not-a-flag", spoolSecret, "--json",
		"--", "./deploy.sh", "--token", spoolSecret,
	})
	// One "?" for both undeclared words: the record says a flag this program
	// does not have was passed, once, and never which one.
	if want := []string{"model", spoolUndeclaredFlag, "json"}; strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Errorf("args are %v, want %v", keys, want)
	}
}

// The allowlist is this program's options, in both directions.
//
// The claim the spool rests on is that a flag key in a record is a name this
// program declares. The first direction is the safety half and it held before
// this test existed: nothing a caller supplies can enter the set, because the
// set is built from compiled-in text. The second direction is the fidelity
// half, and it did not hold: the set was read out of the help prose with a
// pattern that also matched the tail of every hyphenated word, so it admitted
// "for", "now", "user" and "zero", and a caller passing "--now" had it
// recorded as though this program had such an option.
//
// The reference set is the one the help contract already derives from the
// source: the options each verb's own parser accepts, read out of the parser
// sites the pages name. That is the definition of "an option of this program"
// this repository already fails the build over, so the spool uses it rather
// than inventing a second one.
func TestSpoolAllowlistIsExactlyTheProgramsFlags(t *testing.T) {
	files := packageSource(t)
	accepted := map[string]bool{}
	for _, path := range helpPagePaths() {
		page, found := helpPageFor(path)
		if !found {
			t.Fatalf("the registry lists %q and has no page for it", path)
		}
		sites := page.Parsers
		if family, _, isFamilyVerb := strings.Cut(page.Path, " "); isFamilyVerb {
			sites = append(append([]parserSite{}, sites...), familyDispatchSite(family))
		}
		for _, site := range sites {
			for flag := range declaredFlags(t, files, site) {
				accepted[strings.TrimLeft(flag, "-")] = true
			}
		}
		// A documented flag counts as accepted: the help contract already
		// fails the build when a page names an option no parser of that verb
		// takes, so the two sets are the same set by the time this runs.
		for _, flag := range page.renderedFlags() {
			accepted[strings.TrimLeft(flag.Name, "-")] = true
		}
		for _, flag := range page.Undocumented {
			accepted[strings.TrimLeft(flag, "-")] = true
		}
	}
	// One option is spelled at its parser through a compiled-in constant
	// rather than as a literal, so the scan above cannot see it. Naming the
	// constant rather than the word is what keeps this line honest: deleting
	// the option fails the build here.
	accepted[strings.TrimLeft(supervisorCredentialFlag, "-")] = true
	if len(accepted) < 40 {
		t.Fatalf("the parser scan found only %d options, so this test proves nothing", len(accepted))
	}

	declared := spoolDeclaredFlags()
	global := map[string]bool{}
	for _, name := range spoolGlobalFlags {
		global[name] = true
	}

	var missing, extra []string
	for name := range accepted {
		if _, present := declared[name]; !present {
			missing = append(missing, name)
		}
	}
	for name := range declared {
		if !accepted[name] && !global[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) != 0 {
		t.Errorf("a parser of this program accepts %v and the spool would record each as %q", missing, spoolUndeclaredFlag)
	}
	if len(extra) != 0 {
		t.Errorf("the spool would record %v as options of this program, and no parser accepts them", extra)
	}
}

// The English the prose scan used to admit is recorded as the placeholder,
// which is what tells a reader that a caller passed an option this program
// does not have.
//
// Each word below is the tail of a hyphenated word in this program's help -
// "end-user", "non-zero", "read-only", "t3-steward", "pre-flight" - or, in
// the case of "jq", another program's option inside a worked example. None of
// them is an option of this program. The review that found this
// also listed "for", "now", "days", "every", "until", "outcome", "peak" and
// "options" as English admitted by the same defect; they are not, and are
// deliberately absent from this list, because every one of them is a real
// option of this program: "wait add --for", "backlog pause --now", "report
// --days", "wait add --every", "backlog delay --until", "backlog admin
// close-assignment --outcome", "report --peak" and "backlog amend --options".
func TestSpoolDoesNotAdmitTheProsesEnglish(t *testing.T) {
	for _, word := range []string{
		"user", "zero", "only", "steward", "flight", "jq",
	} {
		if _, present := spoolDeclaredFlags()[word]; present {
			t.Errorf("%q is a word from the help prose, not an option, and the allowlist admits it", word)
		}
		if key := spoolFlagKey(word); key != spoolUndeclaredFlag {
			t.Errorf("--%s records as %q, want %q", word, key, spoolUndeclaredFlag)
		}
	}
}

// A spool that cannot be written changes neither the exit code nor a byte of
// either output stream. This is the one place in this program where silence is
// the correct behaviour.
func TestUnwritableSpoolChangesNothingTheCallerSees(t *testing.T) {
	args := []string{"backlog", "shwo", "--help"}

	writable := useSpool(t)
	wantCode, wantStdout, wantStderr := captureSpoolStd(t, args)
	if len(spoolPaths(t, writable)) != 1 {
		t.Fatalf("the writable run wrote %d spool files, want 1", len(spoolPaths(t, writable)))
	}

	// A regular file where the spool's parent directory must be. MkdirAll
	// fails with ENOTDIR on it for every user, including root, so this test
	// does not quietly pass by running as somebody who can write anywhere.
	blocked := t.TempDir()
	obstruction := filepath.Join(blocked, "not-a-directory")
	if err := os.WriteFile(obstruction, []byte(""), 0o600); err != nil {
		t.Fatalf("write the obstruction: %v", err)
	}
	t.Setenv("TOOLFEEDBACK_DIR", filepath.Join(obstruction, "spool"))
	t.Setenv("T3_STEWARD_FRICTION", "1")

	code, stdout, stderr := captureSpoolStd(t, args)
	if code != wantCode {
		t.Errorf("exit code %d with an unwritable spool, %d with a writable one", code, wantCode)
	}
	if stdout != wantStdout {
		t.Errorf("stdout changed:\n unwritable %q\n writable   %q", stdout, wantStdout)
	}
	if stderr != wantStderr {
		t.Errorf("stderr changed:\n unwritable %q\n writable   %q", stderr, wantStderr)
	}
	if entries, err := os.ReadDir(blocked); err != nil || len(entries) != 1 {
		t.Errorf("the blocked directory holds %v (err %v), want only the obstruction", entries, err)
	}
}

// The file layout and the record shape are the ones homelab-cli's toolfeedback
// reader consumes: <dir>/<source>/<host>-<YYYY-MM-DD>.jsonl, one JSON object
// per line, with the keys it reads.
func TestSpoolMatchesTheReaderShape(t *testing.T) {
	dir := useSpool(t)
	captureSpoolStd(t, []string{"wait", "add", "--help"})

	paths := spoolPaths(t, dir)
	if len(paths) != 1 {
		t.Fatalf("%d spool files, want 1: %v", len(paths), paths)
	}
	if source := filepath.Base(filepath.Dir(paths[0])); source != spoolSource {
		t.Errorf("the spool directory is %q, want %q", source, spoolSource)
	}
	// SPOOL_NAME_RE in homelab-cli/bin/toolfeedback.
	name := regexp.MustCompile(`^(?P<host>.+)-(?P<date>\d{4}-\d{2}-\d{2})\.jsonl$`)
	if !name.MatchString(filepath.Base(paths[0])) {
		t.Errorf("%q is not a spool file name the reader recognises", filepath.Base(paths[0]))
	}

	record := spoolLines(t, dir)[0]
	// event_time in the reader parses this after replacing Z with +00:00.
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", recordString(t, record, "ts")); err != nil {
		t.Errorf("ts %q is not the timestamp format the reader parses: %v", record["ts"], err)
	}
	for key, want := range map[string]string{
		"host": "string", "session": "string", "proc": "string", "tool": "string",
		"seq": "number", "ms": "number", "ok": "bool",
	} {
		value, present := record[key]
		if !present {
			t.Errorf("record has no %q, which the reader reads", key)
			continue
		}
		var got string
		switch value.(type) {
		case string:
			got = "string"
		case float64:
			got = "number"
		case bool:
			got = "bool"
		default:
			got = "other"
		}
		if got != want {
			t.Errorf("record %q is a %s, the reader wants a %s", key, got, want)
		}
	}
}

// Every exit code this program documents maps onto one of the five classes an
// agent experiences, and the mapping is the one the help pages describe.
func TestSpoolOutcomeMapsTheDocumentedExitCodes(t *testing.T) {
	for _, testCase := range []struct {
		code    int
		class   string
		outcome string
	}{
		{0, "", spoolOK},
		{2, "verdict", spoolState},
		{3, "client-configuration", spoolUsage},
		{4, "authentication", spoolUsage},
		{5, "unavailable", spoolTransport},
		{6, "timeout", spoolTransport},
		{7, "protocol", spoolTransport},
		{8, "rejected", spoolState},
		// Exit 1 is everything this program did not number, so the class it
		// gets has to say that and nothing more.
		{1, spoolUnclassified, spoolUnclassified},
	} {
		var err error
		if testCase.code != 0 {
			err = errSpoolTest
		}
		if class := spoolErrClass(testCase.code, err); class != testCase.class {
			t.Errorf("exit %d classes as %q, want %q", testCase.code, class, testCase.class)
		}
		if outcome := spoolOutcome(testCase.class); outcome != testCase.outcome {
			t.Errorf("class %q is outcome %q, want %q", testCase.class, outcome, testCase.outcome)
		}
	}
}

// A path the caller named that cannot be opened is not an unclassified
// failure. The exit code says nothing - this program answers 1 for a missing
// file, a usage refusal and a crash alike - but the standard library's
// sentinels do, and errors.Is reads them without any part of the message
// reaching the record, which is what keeps this split inside the rule the rest
// of the classifier follows.
func TestSpoolSplitsALocalIOFailureOutOfTheUnclassifiedBucket(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{"a path that is not there", fmt.Errorf("open %s: %w", spoolSecret, fs.ErrNotExist)},
		{"a path that cannot be read", fmt.Errorf("open %s: %w", spoolSecret, fs.ErrPermission)},
	} {
		class := spoolErrClass(1, testCase.err)
		if class != "local-io" {
			t.Errorf("%s classes as %q, want %q", testCase.name, class, "local-io")
		}
		if outcome := spoolOutcome(class); outcome != spoolState {
			t.Errorf("%s is outcome %q, want %q", testCase.name, outcome, spoolState)
		}
		// The class is a literal in this file; the path the caller named is
		// in the message and stays there.
		if strings.Contains(class, spoolSecret) {
			t.Errorf("the class carries the path from the message: %q", class)
		}
	}
	// An error with no sentinel and no recognisable wording is still the
	// bucket, because nothing about it says more than that.
	if class := spoolErrClass(1, errSpoolTest); class != spoolUnclassified {
		t.Errorf("an unrecognisable failure classes as %q, want %q", class, spoolUnclassified)
	}
}

// The spool is off unless it was asked for, in the same way as the MCP
// producers that share the directory.
func TestSpoolIsOffUnlessAskedFor(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TOOLFEEDBACK_DIR", dir)
	t.Setenv("T3_STEWARD_FRICTION", "")
	t.Setenv("TOOLFEEDBACK", "")
	if spoolEnabled() {
		t.Fatal("the spool is on in a directory with no enabled marker")
	}
	captureSpoolStd(t, []string{"wait", "add", "--help"})
	if paths := spoolPaths(t, dir); len(paths) != 0 {
		t.Fatalf("the spool wrote %v while off", paths)
	}

	if err := os.WriteFile(filepath.Join(dir, "enabled"), nil, 0o600); err != nil {
		t.Fatalf("write the enabled marker: %v", err)
	}
	if !spoolEnabled() {
		t.Fatal("the enabled marker did not turn the spool on")
	}
	t.Setenv("TOOLFEEDBACK", "0")
	if spoolEnabled() {
		t.Fatal("TOOLFEEDBACK=0 did not turn the spool off")
	}
}

var errSpoolTest = errSpoolTestError{}

type errSpoolTestError struct{}

func (errSpoolTestError) Error() string { return "a failure with no recognisable wording" }
