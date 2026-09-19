package main

// Per-invocation instrumentation spool: one JSON line per run of this binary,
// appended to the same tool-feedback spool the MCP servers on this fleet
// already write. Until now every claim about how this CLI behaves for an agent
// rested on a scheduled probe; this records what actually happened, one line
// per call, on every host and from every harness.
//
// Three properties decide whether the spool is worth shipping, and each one is
// enforced here rather than left to the caller.
//
// It carries no argument values. The record holds the verb path, the flag keys
// and an outcome class, and nothing else that came from the command line. A
// value can be a prompt, a document, a path or a token, and these records are
// shipped to another host, so the safe set is the set of names: the verb path
// is built by matching words against the help-page registry, so only a word
// that already names a documented verb is ever written, and the flag keys are
// the names with their values and everything after a bare "--" cut away. An
// error is recorded as a class drawn from a closed vocabulary rather than as
// its message, because a message quotes what the caller typed.
//
// It never changes what the caller sees. Every failure in here - no directory,
// a full disk, a read-only home - is dropped in silence. Losing an event is
// better than turning a working command into a failing one, and a warning on
// standard error would corrupt the output of every verb an agent parses.
//
// Its cost is bounded. One O_APPEND write of a short line, no fsync, no
// network, no retry, and a record that exceeds spoolRecordLimit is dropped.

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// spoolSource names this tool's directory in the spool. Every tool that writes
// the spool has its own, so one report can rank them side by side.
const spoolSource = "t3-steward"

const (
	// spoolMaxArgs bounds the flag keys recorded for one call. A command line
	// with more distinct options than this is not a shape worth resolving
	// further, and the bound is what keeps the line short.
	spoolMaxArgs = 24
	// spoolMaxKeyLen bounds one flag key. A longer word is not a flag this
	// program declares, so it is dropped rather than truncated.
	spoolMaxKeyLen = 40
	// spoolMaxIdentLen bounds a session or client identifier that crossed a
	// process boundary, because it is written into the line.
	spoolMaxIdentLen = 128
	// spoolRecordLimit bounds the encoded record. A line over it is dropped:
	// the spool is a measurement, not a log, and an unbounded line would let a
	// pathological command line cost the caller real time.
	spoolRecordLimit = 4096
)

// spoolRecord is one line of the spool. The field names and their order are
// the shape the tool-feedback reader already consumes from the MCP producers;
// exit is this producer's addition, because a CLI has an exit code and an MCP
// server does not.
type spoolRecord struct {
	TS      string   `json:"ts"`
	Host    string   `json:"host"`
	Session string   `json:"session"`
	Proc    string   `json:"proc"`
	Client  string   `json:"client,omitempty"`
	Ver     string   `json:"ver,omitempty"`
	Seq     int      `json:"seq"`
	Tool    string   `json:"tool"`
	Args    []string `json:"args,omitempty"`
	Outcome string   `json:"outcome"`
	OK      bool     `json:"ok"`
	Err     string   `json:"err,omitempty"`
	Exit    int      `json:"exit"`
	Ms      int64    `json:"ms"`
}

// The five outcome classes an agent actually experiences. They are mapped from
// the exit codes this program already documents rather than from a taxonomy
// invented for the spool, so a report and the help page agree about what
// happened.
const (
	// spoolOK is exit 0.
	spoolOK = "ok"
	// spoolUsage is a refusal the caller can fix on its own side: the verb or
	// the flag does not exist, or this host cannot form a request at all.
	spoolUsage = "usage"
	// spoolState is a refusal about the world: the coordinator looked and said
	// no, or a verb that owns its verdict reported a run that did not succeed.
	// Retyping the command does not change the answer.
	spoolState = "state"
	// spoolTransport is a failure to get an answer at all.
	spoolTransport = "transport"
	// spoolInternal is an error this program never classified. See
	// spoolErrClass for why this class is wider here than it should be.
	spoolInternal = "internal"
)

// spoolEnabled reports whether invocations are recorded. Off unless someone
// asked for it, in the same way and by the same switches as the MCP producers
// that share this spool: create the file "enabled" in the spool directory, or
// set T3_STEWARD_FRICTION=1, or set TOOLFEEDBACK=1 to move every tool
// together. Either of the variables set to 0 turns it off again.
func spoolEnabled() bool {
	for _, name := range []string{"T3_STEWARD_FRICTION", "TOOLFEEDBACK"} {
		switch os.Getenv(name) {
		case "1":
			return true
		case "0":
			return false
		}
	}
	dir := spoolDir()
	if dir == "" {
		return false
	}
	_, err := os.Stat(filepath.Join(dir, "enabled"))
	return err == nil
}

// spoolDir is where the spool files live, one directory per tool and one file
// per host per day. The precedence is the one the reader and the other
// producers use, so that pointing TOOLFEEDBACK_DIR at a scratch directory
// moves the whole spool at once - which is how the tests keep out of the
// operator's real one.
func spoolDir() string {
	for _, name := range []string{"T3_STEWARD_FRICTION_DIR", "TOOLFEEDBACK_DIR"} {
		if dir := os.Getenv(name); dir != "" {
			return dir
		}
	}
	if dir := os.Getenv("XDG_DATA_HOME"); dir != "" {
		return filepath.Join(dir, "toolfeedback")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".local", "share", "toolfeedback")
}

// spoolIdent keeps an identifier that came from the environment to the
// characters an identifier is made of, so that a spool line stays one line
// whatever an environment held, and bounds its length.
func spoolIdent(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > spoolMaxIdentLen {
		value = value[:spoolMaxIdentLen]
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			return r
		default:
			return -1
		}
	}, value)
}

// spoolSession is the agent session this invocation belongs to, as the harness
// exported it. It is what lets a report line a refusal up with what the agent
// did next. A harness that exports none leaves the field empty rather than
// costing the record: an invocation that cannot be attributed still counts
// towards the per-verb rates, which is most of what the spool is for.
func spoolSession() string {
	for _, name := range []string{
		"TOOLFEEDBACK_SESSION",
		"T3_STEWARD_SESSION_ID",
		"CLAUDE_CODE_SESSION_ID",
		"CODEX_SESSION_ID",
		"OPENCODE_SESSION_ID",
	} {
		if value := spoolIdent(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

// spoolClient is the harness that ran this command. The reason the spool is
// written by the binary rather than derived from Claude Code's transcripts is
// that a call made from Codex or OpenCode leaves no transcript to derive it
// from, and those are exactly the harnesses that read the most stale
// documentation, so a transcript-derived report would under-count them.
func spoolClient() string {
	if value := spoolIdent(os.Getenv("TOOLFEEDBACK_CLIENT")); value != "" {
		return value
	}
	for _, probe := range []struct {
		name     string
		variable []string
	}{
		{"claude-code", []string{"CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT"}},
		{"codex", []string{"CODEX_HOME", "CODEX_SANDBOX"}},
		{"opencode", []string{"OPENCODE", "OPENCODE_BIN_PATH"}},
	} {
		for _, variable := range probe.variable {
			if os.Getenv(variable) != "" {
				return probe.name
			}
		}
	}
	return ""
}

// spoolHost is this machine's short name.
func spoolHost() string {
	host, err := os.Hostname()
	if err != nil {
		return ""
	}
	if index := strings.IndexByte(host, '.'); index > 0 {
		host = host[:index]
	}
	return host
}

// spoolProc tells two invocations apart when they share a session and a
// timestamp, which is what a shell pipeline of two calls produces.
func spoolProc() string {
	buffer := make([]byte, 6)
	if _, err := rand.Read(buffer); err != nil {
		return ""
	}
	return hex.EncodeToString(buffer)
}

// spoolVersion is which build made the call, so that a failure rate before a
// fix can be compared with the rate after it.
func spoolVersion() string {
	if commit == "" || commit == "none" {
		return version
	}
	short := commit
	if len(short) > 12 {
		short = short[:12]
	}
	return version + "/" + short
}

// spoolVerbPath is the verb this invocation ran, as a full path:
// "t3-steward task run", not "t3-steward".
//
// A word extends the path only when the words so far already name a registered
// help page, which makes the help-page registry the spool's whole vocabulary.
// That is the property that keeps operands out: "backlog show run-7f3a" stops
// after "backlog show", because no page is registered for the run id, and
// "diagnose /home/igor/secret" stops after "diagnose". It also means an
// undocumented verb records as the truncated path of its family, which is a
// signal rather than a loss: a verb that agents call and no page documents is
// the thing a help-contract audit is looking for.
func spoolVerbPath(args []string) string {
	path := []string{spoolSource}
	verbs := []string{}
	for _, word := range args {
		// A bare "--" hands the rest to the caller's own command, and a flag
		// ends the verb path. Neither can extend it.
		if word == "--" || strings.HasPrefix(word, "-") {
			break
		}
		candidate := append(append([]string{}, verbs...), word)
		if _, registered := helpPageFor(strings.Join(candidate, " ")); !registered {
			break
		}
		verbs = candidate
	}
	return strings.Join(append(path, verbs...), " ")
}

// spoolUndeclaredFlag stands for a word that was shaped like a flag and is not
// one this program declares. Recording the word itself would be recording
// whatever the caller put after a dash, so the fact is recorded and the word
// is not: a call that passed an option the program does not have is the shape
// this spool exists to count, and which option it was is a question for the
// transcript, not for a record that is shipped to another host.
const spoolUndeclaredFlag = "?"

// spoolArgKeys is the ordered set of flag names this command line carried,
// with every value removed. The names are the part worth keeping: they say
// which way of addressing the work the agent reached for, and a flag this
// program does not declare is exactly the shape of the refusals this spool
// exists to count.
//
// Scanning stops at a bare "--", because everything after it belongs to the
// caller's own command or prompt. "t3-steward task run --model M -- ./deploy
// --token abc" records one key, not two.
func spoolArgKeys(args []string) []string {
	var keys []string
	seen := map[string]bool{}
	for _, word := range args {
		if word == "--" {
			break
		}
		if !strings.HasPrefix(word, "-") || word == "-" {
			continue
		}
		key := strings.TrimLeft(word, "-")
		// "--model=sonnet" carries its value in the same word; only the name
		// before the first "=" is a name.
		if index := strings.IndexByte(key, '='); index >= 0 {
			key = key[:index]
		}
		key = spoolFlagKey(key)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		keys = append(keys, key)
		if len(keys) == spoolMaxArgs {
			break
		}
	}
	return keys
}

// spoolFlagKey admits a flag name only if this program declares it, and
// answers spoolUndeclaredFlag for every other word that stood where a flag
// should. Shape is not enough: "--sk-live-0xDEADBEEF" is shaped exactly like a
// flag, and a caller that mistypes a value into flag position would otherwise
// put it in a record that is shipped. An allowlist of names this program
// already documents is the only construction under which that cannot happen.
func spoolFlagKey(key string) string {
	if key == "" || len(key) > spoolMaxKeyLen {
		return spoolUndeclaredFlag
	}
	if _, declared := spoolDeclaredFlags()[key]; !declared {
		return spoolUndeclaredFlag
	}
	return key
}

// spoolFlagNameRe finds an option name in this program's own help text.
var spoolFlagNameRe = regexp.MustCompile(`--?[A-Za-z][A-Za-z0-9_-]*`)

// spoolGlobalFlags are the options the dispatcher declares for every verb it
// parses itself, plus the three spellings of a help request. They are named
// here because registerGlobalFlags declares them in code rather than on a
// page.
var spoolGlobalFlags = []string{"config", "dry-run", "no-dry-run", "log-level", "help", "h", "v"}

// spoolDeclaredFlags is every option name this program documents, read out of
// the same help-page registry that answers --help. Deriving it rather than
// writing a second list is what keeps it true: an existing contract test
// already fails the build when a parser accepts a flag no page names, so a
// flag cannot be added to this program without entering this set.
//
// It is computed at most once per process, and only when a command line
// carried a flag at all.
var spoolDeclaredFlags = sync.OnceValue(func() map[string]struct{} {
	declared := make(map[string]struct{}, 256)
	add := func(text string) {
		for _, name := range spoolFlagNameRe.FindAllString(text, -1) {
			if key := strings.TrimLeft(name, "-"); key != "" {
				declared[key] = struct{}{}
			}
		}
	}
	for _, name := range spoolGlobalFlags {
		declared[name] = struct{}{}
	}
	for _, page := range helpPages {
		// A page states its options either as structured flags or, when it
		// carries a reference this package already holds in full, inside that
		// reference. Both are this program's own text, compiled in; neither
		// comes from a caller.
		add(page.Body)
		for _, line := range page.Usage {
			add(line)
		}
		for _, option := range page.Flags {
			add(option.Name)
		}
		for _, option := range page.Undocumented {
			add(option)
		}
	}
	return declared
})

// spoolErrClass names why an invocation failed, from a closed vocabulary. It
// is deliberately not the error message: a message quotes the file, the run id
// or the flag value that caused it, and the record is shipped to another host.
// The class is what a report clusters on anyway, and the verb path already
// says which verb produced it.
//
// The codes are this program's own, as its help pages document them: 3 client
// configuration, 4 authentication, 5 unavailable, 6 timeout, 7 protocol, 8
// rejected by the coordinator, 2 a verdict a verb owns outright, 1 anything
// else.
//
// "error" is the honest name for that last bucket, and it is wide: this
// program collapses a refusal it could have classified - "replay needs exactly
// one file argument" - and a genuine internal failure into the same exit 1.
// The sentinels and the parser's own wording recover the usage refusals that
// can be recognised without guessing; what is left is recorded as unclassified
// rather than as something more specific than the evidence supports. How big
// that bucket turns out to be is itself a measurement worth having.
func spoolErrClass(code int, err error) string {
	if err == nil && code == 0 {
		return ""
	}
	switch code {
	case 2:
		return "verdict"
	case 3:
		return "client-configuration"
	case 4:
		return "authentication"
	case 5:
		return "unavailable"
	case 6:
		return "timeout"
	case 7:
		return "protocol"
	case 8:
		return "rejected"
	}
	switch {
	case errors.Is(err, errUnknownCommand), errors.Is(err, errUnknownTaskCommand):
		return "unknown-command"
	case errors.Is(err, flag.ErrHelp):
		return "help"
	case spoolIsUnknownVerb(err):
		return "unknown-verb"
	case spoolIsFlagError(err):
		return "flag"
	}
	return "error"
}

// spoolFlagRefusals are the standard library flag parser's fixed wordings. A
// refusal that begins with one of them is a flag the caller got wrong, which
// is a different thing for an agent than a command that ran and failed.
var spoolFlagRefusals = []string{
	"flag provided but not defined:",
	"flag needs an argument:",
	"bad flag syntax:",
	"invalid boolean value",
	"invalid value ",
}

func spoolIsFlagError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	for _, refusal := range spoolFlagRefusals {
		if strings.Contains(message, refusal) {
			return true
		}
	}
	return false
}

// spoolIsUnknownVerb recognises the refusal a command family makes when a word
// stands where one of its verbs should: "unknown backlog command "shwo"; the
// backlog commands are ...". It is matched on the fixed part of that wording
// only, and nothing from the message reaches the record.
func spoolIsUnknownVerb(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.HasPrefix(message, "unknown ") && strings.Contains(message, " command \"")
}

// spoolOutcome maps an error class onto the five classes an agent experiences.
func spoolOutcome(class string) string {
	switch class {
	case "":
		return spoolOK
	case "unknown-command", "unknown-verb", "flag", "help",
		"client-configuration", "authentication":
		return spoolUsage
	case "verdict", "rejected":
		return spoolState
	case "unavailable", "timeout", "protocol":
		return spoolTransport
	default:
		return spoolInternal
	}
}

// newSpoolRecord builds the record for one finished invocation. It is separate
// from writing it so that a test can assert what a command line becomes
// without going near a file.
func newSpoolRecord(args []string, code int, err error, elapsed time.Duration) spoolRecord {
	class := spoolErrClass(code, err)
	return spoolRecord{
		TS:      time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Host:    spoolHost(),
		Session: spoolSession(),
		Proc:    spoolProc(),
		Client:  spoolClient(),
		Ver:     spoolVersion(),
		// One process is one call, so the sequence is always the first. The
		// field is kept because the reader orders on it and every other
		// producer writes it.
		Seq:     1,
		Tool:    spoolVerbPath(args),
		Args:    spoolArgKeys(args),
		Outcome: spoolOutcome(class),
		OK:      class == "",
		Err:     class,
		Exit:    code,
		Ms:      elapsed.Milliseconds(),
	}
}

// writeSpoolRecord appends one record. Every failure is dropped in silence;
// see the file comment for why that is the correct behaviour here and nowhere
// else in this program.
func writeSpoolRecord(record spoolRecord) {
	dir := spoolDir()
	if dir == "" {
		return
	}
	line, err := json.Marshal(record)
	if err != nil || len(line) > spoolRecordLimit {
		return
	}
	host := record.Host
	if host == "" {
		host = "unknown"
	}
	// One directory per tool, because a host name contains hyphens and so does
	// a tool name: with both in one file name there is no way to tell where
	// the first ends and the second begins.
	dir = filepath.Join(dir, spoolSource)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return
	}
	path := filepath.Join(dir, host+"-"+time.Now().UTC().Format("2006-01-02")+".jsonl")
	// One O_APPEND write of a short line, so that several processes spooling
	// at once interleave whole lines rather than fragments. No fsync: a
	// measurement is not worth a disk flush on every command.
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = file.Write(append(line, '\n'))
	_ = file.Close()
}

// spoolInvocation records one finished invocation, if the spool is on.
func spoolInvocation(args []string, code int, err error, elapsed time.Duration) {
	if !spoolEnabled() {
		return
	}
	writeSpoolRecord(newSpoolRecord(args, code, err, elapsed))
}
