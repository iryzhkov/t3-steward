package workerruntime

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// LiveCommand is one command an attempt started that was still running when
// its turn ended. Command is the whole command line, its arguments joined by
// single spaces and neither shortened nor otherwise changed, so that the
// secret scan sees every credential it holds whole; displayCommand bounds it
// for the nudge, the note and the failure.
type LiveCommand struct {
	PID     int    `json:"pid"`
	Command string `json:"command"`
}

// LiveCommandReport is the answer to "is anything the attempt started still
// running". Unsupported, when set, says why this host cannot answer; the
// caller then behaves as it did before the check existed and says so, rather
// than reading the silence as a clean turn end or failing a task it cannot
// judge.
type LiveCommandReport struct {
	Unsupported string
	Commands    []LiveCommand
}

// maxLiveCommandText bounds the command line shown for one live command, so a
// long gate invocation cannot crowd the nudge, the note or the failure.
const maxLiveCommandText = 200

// displayCommand is a command line as the nudge, the note and the failure show
// it: runs of white space collapsed and the text bounded. A command line that
// leaves the worker is redacted before it is shown, since shortening first
// could cut a credential so that the scanner no longer matches it.
func displayCommand(command string) string {
	return truncateText(strings.Join(strings.Fields(command), " "), maxLiveCommandText)
}

// scanLiveCommands looks for the commands an attempt left running in its
// workspace on this host.
func scanLiveCommands(workspace string) (LiveCommandReport, error) {
	return scanLiveCommandsIn(runtime.GOOS, "/proc", os.Getpid(), workspace)
}

type procEntry struct {
	parent  int
	session int
	inside  bool
	zombie  bool
}

// processTable is one reading of /proc. Command lines are read only for the
// processes a decision needs, and at most once each.
type processTable struct {
	procRoot  string
	self      int
	entries   map[int]procEntry
	ancestors map[int]bool
	argv      map[int][]string
}

// scanLiveCommandsIn finds the commands an attempt left running inside its
// workspace, by reading /proc.
//
// The mechanism is the working directory, combined with where a process
// hangs in the process tree, because neither alone is reliable on Linux:
//
//   - The provider session's process tree does not hold every command that
//     matters. An agent that backgrounds a gate with nohup or "&" starts it
//     from a shell that exits at once, and the kernel hands the orphan to the
//     nearest subreaper (systemd --user on a fleet host), which is the
//     worker's own ancestor and not the provider's descendant.
//   - The working directory alone over-reports. The provider process, its MCP
//     servers and their language servers all run in the workspace for the
//     whole session, and so do the worker's own verification commands.
//
// So, for a process whose working directory is inside the workspace, its
// parents are followed while they too are inside, up to the topmost of them,
// and the process outside the workspace that holds that subtree decides:
//
//   - pid 1 or one of the worker's own ancestors: the subtree is detached,
//     where an orphan lands, and its topmost process is reported.
//   - a command shell (sh -c and the like): a command started the subtree
//     from outside the workspace, and its topmost process is reported.
//   - any other live process, such as the T3 server: the topmost process is
//     that process's session, the provider. Only a tool execution the
//     provider itself started is the agent's: a command tool call the
//     provider still tracks, such as a Claude Code run_in_background command
//     or a Codex command. The provider's MCP servers stay in the provider's
//     session and are started directly, and their language servers are not
//     the provider's children, so a child of the provider is a tool execution
//     when it leads a session of its own, which Claude Code and Codex give
//     every command they run and which survives the shell replacing itself
//     with the command (bash -lc 'exec make check'), or when it is a command
//     shell. The command it runs is reported, below its shells and sandbox
//     wrappers, or the shell when it runs nothing else.
//
// Anything descending from the worker itself is excluded, as are zombies,
// processes whose parent this worker cannot see, and git's file-system
// monitor. One command per subtree is reported.
//
// A provider launched through a command shell would make its whole session
// look like a command; the providers T3 runs are started directly. A command
// the provider runs in the provider's own session, without a shell or a
// sandbox wrapper, cannot be told apart from an MCP server; neither provider
// runs commands that way.
func scanLiveCommandsIn(goos, procRoot string, self int, workspace string) (LiveCommandReport, error) {
	if goos != "linux" {
		return LiveCommandReport{Unsupported: "process inspection is unavailable on " + goos}, nil
	}
	if info, err := os.Stat(procRoot); err != nil || !info.IsDir() {
		return LiveCommandReport{Unsupported: "process inspection is unavailable: " + procRoot + " is not mounted"}, nil
	}
	root, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return LiveCommandReport{}, fmt.Errorf("resolve workspace: %w", err)
	}
	root = filepath.Clean(root)
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return LiveCommandReport{}, fmt.Errorf("read %s: %w", procRoot, err)
	}
	table := processTable{procRoot: procRoot, self: self, entries: make(map[int]procEntry, len(entries)), ancestors: make(map[int]bool), argv: make(map[int][]string)}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(procRoot, entry.Name(), "stat"))
		if err != nil {
			// The process exited between the listing and the read.
			continue
		}
		stat, ok := parseProcStat(raw)
		if !ok {
			continue
		}
		// An unreadable working directory belongs to another user, or to a
		// process that has exited, and is treated as outside the workspace.
		cwd, _ := os.Readlink(filepath.Join(procRoot, entry.Name(), "cwd"))
		table.entries[pid] = procEntry{parent: stat.parent, session: stat.session, inside: withinDirectory(cwd, root), zombie: stat.state == "Z"}
	}
	for pid := self; pid > 0 && !table.ancestors[pid]; {
		table.ancestors[pid] = true
		entry, ok := table.entries[pid]
		if !ok {
			break
		}
		pid = entry.parent
	}
	roots := make(map[int]bool)
	for pid, entry := range table.entries {
		if !entry.inside || entry.zombie || pid == self {
			continue
		}
		if command, found := table.liveCommandRoot(pid); found {
			roots[command] = true
		}
	}
	// A command shell is reported only when it runs nothing that is reported
	// itself: the command it runs names the work better.
	runsReported := make(map[int]bool)
	for pid := range roots {
		runsReported[table.entries[pid].parent] = true
	}
	pids := make([]int, 0, len(roots))
	for pid := range roots {
		if !runsReported[pid] {
			pids = append(pids, pid)
		}
	}
	sort.Ints(pids)
	var report LiveCommandReport
	for _, pid := range pids {
		command := strings.Join(table.args(pid), " ")
		if strings.TrimSpace(command) == "" {
			// The command line reads empty while a process is replacing its
			// program; the process still runs, under its command name.
			name, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "comm"))
			if err != nil || strings.TrimSpace(string(name)) == "" {
				continue
			}
			command = "[" + strings.TrimSpace(string(name)) + "]"
		}
		// Git's file-system monitor daemonizes inside a repository on its
		// own whenever it is configured; it is not a command the task ran.
		if strings.Contains(command, "fsmonitor--daemon") {
			continue
		}
		report.Commands = append(report.Commands, LiveCommand{PID: pid, Command: command})
	}
	return report, nil
}

// liveCommandRoot follows pid's parents while they are inside the workspace
// and decides, from the process outside the workspace that holds the topmost
// of them, whether pid belongs to a command the attempt left running, and
// which process names that command.
func (t *processTable) liveCommandRoot(pid int) (int, bool) {
	path := []int{pid}
	current := pid
	for steps := 0; steps <= len(t.entries); steps++ {
		parent := t.entries[current].parent
		if parent == t.self {
			// The worker's own work, such as a verification command.
			return 0, false
		}
		entry, visible := t.entries[parent]
		if !visible {
			// A parent this worker cannot see proves nothing either way, and a
			// false report would cost the task a nudge or its result.
			return 0, false
		}
		if entry.inside && !entry.zombie {
			current = parent
			path = append(path, parent)
			continue
		}
		switch {
		case parent == 1 || t.ancestors[parent]:
			return current, true
		case t.descendsFromSelf(parent):
			return 0, false
		case t.commandShell(parent):
			return current, true
		case len(path) >= 2 && t.toolExecution(path[len(path)-2]):
			// current is the provider and the path runs through a tool
			// execution it started. Its command shells, the subshells they
			// forked for a pipeline or a group, which carry the shell's own
			// command line, and its sandbox wrappers name nothing, so the
			// command below them is reported where there is one.
			command := len(path) - 2
			for command > 0 && (t.commandShell(path[command]) || t.sandboxWrapper(path[command])) {
				command--
			}
			return path[command], true
		}
		return 0, false
	}
	return 0, false
}

// toolExecution reports whether pid, a child of the provider, is a command
// the provider runs for the agent rather than one of its MCP servers.
func (t *processTable) toolExecution(pid int) bool {
	return t.entries[pid].session == pid || t.commandShell(pid) || t.sandboxWrapper(pid)
}

// sandboxWrappers are the programs a provider runs a tool command under that
// fork the command rather than becoming it.
var sandboxWrappers = map[string]bool{"bwrap": true, "codex-linux-sandbox": true, "firejail": true, "nsjail": true, "timeout": true}

func (t *processTable) sandboxWrapper(pid int) bool {
	args := t.args(pid)
	return len(args) > 0 && sandboxWrappers[filepath.Base(args[0])]
}

// descendsFromSelf reports whether pid is the worker or one of its
// descendants, whose processes are the worker's own work.
func (t *processTable) descendsFromSelf(pid int) bool {
	for steps := 0; pid > 0 && steps <= len(t.entries); steps++ {
		if pid == t.self {
			return true
		}
		entry, ok := t.entries[pid]
		if !ok {
			return false
		}
		pid = entry.parent
	}
	return false
}

func (t *processTable) args(pid int) []string {
	if args, ok := t.argv[pid]; ok {
		return args
	}
	var args []string
	if raw, err := os.ReadFile(filepath.Join(t.procRoot, strconv.Itoa(pid), "cmdline")); err == nil {
		args = strings.Split(strings.TrimRight(string(raw), "\x00"), "\x00")
		if len(args) == 1 && args[0] == "" {
			args = nil
		}
	}
	t.argv[pid] = args
	return args
}

// commandShells are the shells whose -c runs a command line.
var commandShells = map[string]bool{"sh": true, "bash": true, "dash": true, "zsh": true, "ksh": true, "mksh": true, "ash": true, "fish": true}

// commandShell reports whether pid is a shell running a command string, the
// way every provider's command tool and nohup-style launchers run commands,
// as opposed to an interactive shell or a script.
func (t *processTable) commandShell(pid int) bool {
	args := t.args(pid)
	if len(args) < 2 || !commandShells[strings.TrimPrefix(filepath.Base(args[0]), "-")] {
		return false
	}
	for index := 1; index < len(args); index++ {
		arg := args[index]
		switch {
		case arg == "-o" || arg == "+o":
			// The option name that follows is not an operand.
			index++
		case arg == "--" || len(arg) < 2 || (arg[0] != '-' && arg[0] != '+'):
			// The first operand ends the options: a script and its arguments.
			return false
		case arg[0] == '-' && arg[1] != '-' && strings.Contains(arg, "c"):
			return true
		}
	}
	return false
}

type procStat struct {
	state   string
	parent  int
	session int
}

// parseProcStat reads the state, the parent pid and the session id from
// /proc/<pid>/stat. The command name is in parentheses and may itself contain
// spaces and parentheses, so the fields are read after the last closing one.
func parseProcStat(raw []byte) (procStat, bool) {
	end := bytes.LastIndexByte(raw, ')')
	if end < 0 {
		return procStat{}, false
	}
	fields := strings.Fields(string(raw[end+1:]))
	if len(fields) < 4 {
		return procStat{}, false
	}
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		return procStat{}, false
	}
	session, err := strconv.Atoi(fields[3])
	if err != nil {
		return procStat{}, false
	}
	return procStat{state: fields[0], parent: parent, session: session}, true
}

func withinDirectory(path, root string) bool {
	if path == "" {
		return false
	}
	return path == root || strings.HasPrefix(path, root+string(filepath.Separator))
}

// truncateText shortens text to at most limit bytes on a rune boundary.
func truncateText(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	cut := limit - len("...")
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + "..."
}
