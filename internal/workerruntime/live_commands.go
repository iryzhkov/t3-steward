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
// its turn ended.
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

// maxLiveCommandText bounds the command line kept for one live command, so a
// long gate invocation cannot crowd the nudge, the note or the failure.
const maxLiveCommandText = 200

// scanLiveCommands looks for the commands an attempt left running in its
// workspace on this host.
func scanLiveCommands(workspace string) (LiveCommandReport, error) {
	return scanLiveCommandsIn(runtime.GOOS, "/proc", os.Getpid(), workspace)
}

type procEntry struct {
	parent int
	inside bool
	zombie bool
}

// scanLiveCommandsIn finds the commands an attempt detached inside its
// workspace and left running, by reading /proc.
//
// The mechanism is the working directory, combined with where a process
// hangs in the process tree, because neither alone is reliable on Linux:
//
//   - The provider session's process tree does not hold the commands that
//     matter. An agent that backgrounds a gate with nohup or "&" starts it
//     from a shell that exits at once, and the kernel hands the orphan to the
//     nearest subreaper (systemd --user on a fleet host), which is the
//     worker's own ancestor and not the provider's descendant.
//   - The working directory alone over-reports. The provider process, its MCP
//     servers and their language servers all run in the workspace for the
//     whole session, and so do the worker's own verification commands.
//
// So a process counts when its working directory is inside the workspace and
// it is detached: following its parents while they too are inside the
// workspace, the first parent outside it is pid 1 or one of the worker's own
// ancestors, which is where an orphan lands. A process whose chain reaches a
// live process outside the workspace that is not the worker's ancestor (the
// T3 server, through the provider session) belongs to that process and is
// excluded, as is anything descending from the worker itself. One command per
// detached subtree is reported: its topmost process, which is the command
// line the agent wrote.
//
// A command a provider still tracks as its own background task stays attached
// to the session and is therefore not reported; it cannot be told apart from
// the session's MCP and language servers by the process table alone.
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
	processes := make(map[int]procEntry, len(entries))
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
		parent, state, ok := parseProcStat(raw)
		if !ok {
			continue
		}
		// An unreadable working directory belongs to another user, or to a
		// process that has exited, and is treated as outside the workspace.
		cwd, _ := os.Readlink(filepath.Join(procRoot, entry.Name(), "cwd"))
		processes[pid] = procEntry{parent: parent, inside: withinDirectory(cwd, root), zombie: state == "Z"}
	}
	ancestors := make(map[int]bool)
	for pid := self; pid > 0 && !ancestors[pid]; {
		ancestors[pid] = true
		entry, ok := processes[pid]
		if !ok {
			break
		}
		pid = entry.parent
	}
	roots := make(map[int]bool)
	for pid, entry := range processes {
		if !entry.inside || entry.zombie || pid == self {
			continue
		}
		if top, detached := detachedRoot(pid, processes, self, ancestors); detached {
			roots[top] = true
		}
	}
	pids := make([]int, 0, len(roots))
	for pid := range roots {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	var report LiveCommandReport
	for _, pid := range pids {
		command := processCommand(procRoot, pid)
		if command == "" {
			continue
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

// detachedRoot follows pid's parents while they are inside the workspace and
// reports the topmost of them, and whether the process outside the workspace
// that holds that subtree is pid 1 or an ancestor of the worker.
func detachedRoot(pid int, processes map[int]procEntry, self int, ancestors map[int]bool) (int, bool) {
	current := pid
	for steps := 0; steps <= len(processes); steps++ {
		parent := processes[current].parent
		if parent == self {
			// The worker's own work, such as a verification command.
			return 0, false
		}
		entry, visible := processes[parent]
		if !visible {
			// A parent this worker cannot see proves nothing either way, and a
			// false report would cost the task a nudge or its result.
			return 0, false
		}
		if entry.inside && !entry.zombie {
			current = parent
			continue
		}
		return current, parent == 1 || ancestors[parent]
	}
	return 0, false
}

// parseProcStat reads the parent pid and the state from /proc/<pid>/stat. The
// command name is in parentheses and may itself contain spaces and
// parentheses, so the fields are read after the last closing one.
func parseProcStat(raw []byte) (int, string, bool) {
	end := bytes.LastIndexByte(raw, ')')
	if end < 0 {
		return 0, "", false
	}
	fields := strings.Fields(string(raw[end+1:]))
	if len(fields) < 2 {
		return 0, "", false
	}
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, "", false
	}
	return parent, fields[0], true
}

func processCommand(procRoot string, pid int) string {
	raw, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return ""
	}
	command := strings.Join(strings.Fields(strings.ReplaceAll(string(raw), "\x00", " ")), " ")
	return truncateText(command, maxLiveCommandText)
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
