//go:build linux && amd64

package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/iryzhkov/t3-steward/internal/testtiming"
	"golang.org/x/sys/unix"
)

// The guard is installed only in a disposable test child, before exec. A
// network attempt or filesystem write kills it, including instrumentation
// failures an ordinary CLI would silently swallow. No live service is needed.
func TestConfigValidateEffectsGuardHelper(t *testing.T) {
	if os.Getenv("STAGED_VALIDATOR_GUARD") != "1" {
		return
	}
	if err := os.Setenv("PATH", os.Getenv("STAGED_VALIDATOR_PATH")); err != nil {
		t.Fatal(err)
	}
	runtime.LockOSThread()
	if err := unix.Setrlimit(unix.RLIMIT_CORE, &unix.Rlimit{}); err != nil {
		t.Fatal(err)
	}
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	filter := []unix.SockFilter{{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0}}
	deny := func(number uint32) {
		filter = append(filter,
			unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: number, Jf: 1},
			unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_KILL_PROCESS})
	}
	for _, nr := range []uint32{unix.SYS_SOCKET, unix.SYS_CONNECT, unix.SYS_BIND, unix.SYS_LISTEN,
		unix.SYS_SENDTO, unix.SYS_SENDMSG, unix.SYS_MKDIR, unix.SYS_MKDIRAT, unix.SYS_UNLINK, unix.SYS_UNLINKAT,
		unix.SYS_RENAME, unix.SYS_RENAMEAT, unix.SYS_RENAMEAT2, unix.SYS_LINK, unix.SYS_LINKAT,
		unix.SYS_SYMLINK, unix.SYS_SYMLINKAT, unix.SYS_TRUNCATE, unix.SYS_FTRUNCATE,
		unix.SYS_CHMOD, unix.SYS_FCHMOD, unix.SYS_FCHMODAT, unix.SYS_CREAT, unix.SYS_FORK, unix.SYS_VFORK} {
		deny(nr)
	}
	// open/openat must be read-only and cannot create or truncate anything.
	for _, open := range []struct{ nr, offset uint32 }{{unix.SYS_OPEN, 24}, {unix.SYS_OPENAT, 32}} {
		filter = append(filter,
			unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: open.nr, Jf: 3},
			unix.SockFilter{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: open.offset},
			unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JSET | unix.BPF_K, K: unix.O_ACCMODE | unix.O_CREAT | unix.O_TRUNC | unix.O_APPEND, Jf: 1},
			unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_KILL_PROCESS},
			unix.SockFilter{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0})
	}
	filter = append(filter, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW})
	program := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	if err := unix.Prctl(unix.PR_SET_SECCOMP, unix.SECCOMP_MODE_FILTER, uintptr(unsafe.Pointer(&program)), 0, 0); err != nil {
		t.Fatal(err)
	}
	runtime.KeepAlive(filter)
	for i, arg := range os.Args {
		if arg == "--" && i+1 < len(os.Args) {
			if err := unix.Exec(os.Args[i+1], os.Args[i+1:], os.Environ()); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Fatal("missing guarded binary")
}

func TestConfigValidateActualBinaryEffectsGuard(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "t3-steward")
	build := exec.Command("go", "build", "-o", binary, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %v %s", err, out)
	}
	root, path, raw := configValidationFixture(t)
	call := func(args []string) (int, string, string) {
		ctx, cancel := context.WithTimeout(context.Background(), testtiming.Bound(10*time.Second))
		defer cancel()
		argv := append([]string{"-test.run=^TestConfigValidateEffectsGuardHelper$", "--", binary}, args...)
		cmd := exec.CommandContext(ctx, os.Args[0], argv...)
		cmd.Dir = root
		cmd.Env = append(configValidationEnvironment(root), "STAGED_VALIDATOR_GUARD=1", "STAGED_VALIDATOR_PATH="+filepath.Join(root, "tools"), scratchHomeMarker+"="+filepath.Join(root, "home"))
		for i, e := range cmd.Env {
			if strings.HasPrefix(e, "PATH=") {
				cmd.Env[i] = "PATH=" + os.Getenv("PATH")
			}
		}
		var out, stderr bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err := cmd.Run()
		if ctx.Err() != nil {
			t.Fatal("guarded validator hung")
		}
		if err == nil {
			return 0, out.String(), stderr.String()
		}
		if e, ok := err.(*exec.ExitError); ok {
			return e.ExitCode(), out.String(), stderr.String()
		}
		t.Fatal(err)
		return -1, "", ""
	}
	code, out, stderr := call([]string{"config", "validate", "--file", path, "--json"})
	assertConfigValidationResult(t, code, out, stderr, raw)
	for _, args := range configValidationBadArgs(path) {
		code, out, stderr = call(args)
		assertConfigValidationRefusal(t, code, out, stderr)
	}
	if err := os.WriteFile(path, []byte("SECRET_UNKNOWN: SECRET_VALUE\n"), 0600); err != nil {
		t.Fatal(err)
	}
	code, out, stderr = call([]string{"config", "validate", "--file", path, "--json"})
	assertConfigValidationRefusal(t, code, out, stderr)
	configValidationProjectionRefusal(t, root, path, raw, call)
	// Positive control: an ordinary version invocation tries to append its
	// spool and is killed. Thus silent instrumentation cannot evade this test.
	code, _, _ = call([]string{"version"})
	if code != -1 {
		t.Fatalf("guard failed to kill ordinary spool writer: %d", code)
	}
}
