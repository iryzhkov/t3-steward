//go:build unix

package backlogadmin

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// shortControlRoot is a temporary runtime directory short enough for a socket
// path on every platform; macOS's default temporary directory is not.
func shortControlRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "t3-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func controlTestConfig(controlDir string) SSHClientConfig {
	return SSHClientConfig{
		CoordinatorID: testCoordinatorID, Address: "normandy-steward-admin", RemoteCommand: "t3-steward",
		Credentials: testAdminCredentials(), RequestTimeout: time.Second,
		MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
		ControlDir: controlDir,
	}
}

func optionValue(t *testing.T, argv []string, name string) (string, bool) {
	t.Helper()
	for _, argument := range argv {
		if argument == "--" {
			break
		}
		if value, ok := strings.CutPrefix(argument, "-o"+name+"="); ok {
			return value, true
		}
	}
	return "", false
}

// Every request to the same destination must name the same master socket, so
// the daemon's polling and interactive commands share one TCP connection and
// a coordinator's rate limit on new SSH connections never sees them.
func TestSSHClientSharesOneMasterPerDestination(t *testing.T) {
	dir, err := PrepareControlDir(shortControlRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewSSHClient(controlTestConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, operation := range Operations() {
		argv, err := client.arguments(operation)
		if err != nil {
			t.Fatal(err)
		}
		if master, _ := optionValue(t, argv, "ControlMaster"); master != "auto" {
			t.Fatalf("ControlMaster = %q in %v", master, argv)
		}
		if persist, _ := optionValue(t, argv, "ControlPersist"); persist != "60" {
			t.Fatalf("ControlPersist = %q in %v", persist, argv)
		}
		if _, ok := optionValue(t, argv, "ServerAliveInterval"); !ok {
			t.Fatalf("no keepalive bounds a dead master: %v", argv)
		}
		path, _ := optionValue(t, argv, "ControlPath")
		if filepath.Dir(path) != dir {
			t.Fatalf("ControlPath %q is outside the private directory %q", path, dir)
		}
		// ssh appends a temporary suffix while it binds; sun_path is 104-108 bytes.
		if len(path) > maxControlPath {
			t.Fatalf("ControlPath %q is too long for a Unix socket", path)
		}
		if !sshTokenPattern.MatchString(path) {
			t.Fatalf("ControlPath %q fails the argv gate", path)
		}
		// The options precede the destination, so none can be read as the
		// remote command.
		if separator := slices.Index(argv, "--"); separator < 0 || argv[separator+1] != "normandy-steward-admin" {
			t.Fatalf("destination is not right after the option terminator: %v", argv)
		}
		paths = append(paths, path)
	}
	for _, path := range paths {
		if path != paths[0] {
			t.Fatalf("operations use different masters: %v", paths)
		}
	}

	other := controlTestConfig(dir)
	other.Address = "homelab-steward-admin"
	otherClient, err := NewSSHClient(other)
	if err != nil {
		t.Fatal(err)
	}
	if otherClient.controlPath() == client.controlPath() {
		t.Fatal("two destinations share one master socket")
	}
}

func TestSSHClientWithoutControlDirOpensAConnectionPerRequest(t *testing.T) {
	client, err := NewSSHClient(controlTestConfig(""))
	if err != nil {
		t.Fatal(err)
	}
	argv, err := client.arguments("query")
	if err != nil {
		t.Fatal(err)
	}
	for _, argument := range argv {
		if strings.HasPrefix(argument, "-oControl") {
			t.Fatalf("multiplexing option %q without a control directory", argument)
		}
	}
}

func TestSSHClientRefusesUnsafeControlDir(t *testing.T) {
	for _, dir := range []string{"relative/ssh", "/tmp/with space", "/tmp/%C"} {
		if _, err := NewSSHClient(controlTestConfig(dir)); ClassOf(err) != ClassClientConfiguration {
			t.Fatalf("control dir %q: err = %v", dir, err)
		}
	}
}

func TestPrepareControlDirIsPrivate(t *testing.T) {
	if dir, err := PrepareControlDir(""); dir != "" || err != nil {
		t.Fatalf("empty runtime dir = %q, %v", dir, err)
	}
	root := shortControlRoot(t)
	dir, err := PrepareControlDir(root)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("mode = %o", info.Mode().Perm())
	}
	if again, err := PrepareControlDir(root); err != nil || again != dir {
		t.Fatalf("second call = %q, %v", again, err)
	}

	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareControlDir(root); err == nil {
		t.Fatal("accepted a directory other users can read")
	}

	linked := shortControlRoot(t)
	if err := os.Mkdir(filepath.Join(linked, "t3-steward"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(linked, "t3-steward", "ssh")); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareControlDir(linked); err == nil {
		t.Fatal("accepted a symlinked control directory")
	}

	long := filepath.Join(shortControlRoot(t), strings.Repeat("d", 60))
	if err := os.Mkdir(long, 0o700); err != nil {
		t.Fatal(err)
	}
	if dir, err := PrepareControlDir(long); err == nil {
		t.Fatalf("accepted %q, too long for a socket path", dir)
	}
}

// A connection ssh never established, such as a firewall that rejects a burst
// of new connections, is an outage (exit 5), not a protocol mismatch (exit 7).
func TestSSHConnectFailureIsUnavailable(t *testing.T) {
	for _, message := range []string{
		"ssh: connect to host 192.168.70.234 port 22: Connection refused",
		"ssh: connect to host normandy port 22: Connection timed out",
		"ssh: Could not resolve hostname normandy: Temporary failure in name resolution",
		"kex_exchange_identification: Connection closed by remote host",
	} {
		config := controlTestConfig("")
		config.Factory = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
			return exec.CommandContext(ctx, "sh", "-c", `printf '%s\n' "$1" >&2; exit 255`, "sh", message)
		}
		client, err := NewSSHClient(config)
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Query(context.Background(), Query{Version: Version, Kind: QueryStatus})
		if ClassOf(err) != ClassUnavailable || ExitCodeFor(err) != 5 {
			t.Fatalf("%q: class = %q, exit = %d (%v)", message, ClassOf(err), ExitCodeFor(err), err)
		}
		if !strings.Contains(err.Error(), message) {
			t.Fatalf("%q: the ssh message is not in the error: %v", message, err)
		}
	}
}
