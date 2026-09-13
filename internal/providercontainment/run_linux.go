//go:build linux

package providercontainment

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"golang.org/x/sys/unix"
)

func run(ctx context.Context, spec Spec, streams Streams) error {
	if spec.WorkerID == "" || len(spec.Command) == 0 || !filepath.IsAbs(spec.Command[0]) {
		return errors.New("containment requires worker identity and absolute sandbox command")
	}
	if len(spec.Directories) > 32 || len(spec.RuntimePaths) > 16 || len(spec.ProviderHosts) > 32 {
		return errors.New("containment mount or endpoint limit exceeded")
	}
	for _, host := range spec.ProviderHosts {
		if host == "" || strings.ToLower(host) != host || strings.ContainsAny(host, "/:@* \t\r\n") || strings.HasSuffix(host, ".") {
			return errors.New("containment requires exact provider DNS names")
		}
	}
	if err := directoryresource.ValidateCatalog(spec.Directories); err != nil {
		return err
	}
	owned := []directoryresource.Identity{spec.Home, spec.Workspace}
	var ownedBindings []directoryresource.Binding
	for _, identity := range owned {
		if identity.Registration.WorkerID != spec.WorkerID {
			return errors.New("owned storage worker mismatch")
		}
		binding, err := directoryresource.Bind(identity, directoryresource.ReadWrite)
		if err != nil {
			return err
		}
		ownedBindings = append(ownedBindings, binding)
	}
	if directoryresource.Conflicts(ownedBindings[0], ownedBindings[1]) {
		return errors.New("home and workspace storage overlap")
	}
	for _, dataset := range spec.Directories {
		if dataset.Identity.Registration.WorkerID != spec.WorkerID {
			return errors.New("dataset worker mismatch")
		}
		for _, own := range ownedBindings {
			if directoryresource.Conflicts(own, dataset) {
				return errors.New("owned storage overlaps an existing dataset")
			}
		}
	}
	cwd := spec.Cwd
	if cwd == "" {
		cwd = "/workspace"
	}
	validCwd := cwd == "/workspace"
	for i := range spec.Directories {
		if cwd == fmt.Sprintf("/data/%d", i) {
			validCwd = true
		}
	}
	if !validCwd {
		return errors.New("containment cwd must be workspace or an exact dataset mount")
	}
	args := []string{"--unshare-all", "--unshare-user", "--unshare-pid", "--unshare-net", "--unshare-ipc", "--unshare-uts", "--disable-userns", "--new-session", "--die-with-parent", "--cap-drop", "ALL", "--clearenv"}
	// Trusted OS runtime only: never expose host /home, /tmp, /run or /etc wholesale.
	for _, path := range []string{"/usr", "/bin", "/sbin", "/lib", "/lib64"} {
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			args = append(args, "--symlink", target, path)
		} else {
			args = append(args, "--ro-bind", path, path)
		}
	}
	args = append(args, "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp", "--tmpfs", "/run", "--dir", "/etc")
	if _, err := os.Stat("/etc/ssl/certs"); err == nil {
		args = append(args, "--ro-bind", "/etc/ssl/certs", "/etc/ssl/certs")
	}
	var files []*os.File
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()
	mount := func(f *os.File, destination string, writable bool) {
		files = append(files, f)
		flag := "--ro-bind-fd"
		if writable {
			flag = "--bind-fd"
		}
		args = append(args, flag, strconv.Itoa(2+len(files)), destination)
	}
	for i, identity := range owned {
		f, err := directoryresource.Reopen(identity, identity.Registration)
		if err != nil {
			return err
		}
		mount(f, []string{"/home/agent", "/workspace"}[i], true)
	}
	for i, binding := range spec.Directories {
		f, err := directoryresource.Reopen(binding.Identity, binding.Identity.Registration)
		if err != nil {
			return err
		}
		mount(f, fmt.Sprintf("/data/%d", i), binding.Access == directoryresource.ReadWrite)
	}
	for i, path := range spec.RuntimePaths {
		f, identity, err := directoryresource.Open(directoryresource.Registration{WorkerID: spec.WorkerID, ResourceID: fmt.Sprintf("runtime-%d", i), Revision: "launch", Path: path})
		if err != nil {
			return err
		}
		// Runtime roots must not expose owned files or a second alias of source data.
		for _, binding := range append(append([]directoryresource.Binding(nil), spec.Directories...), ownedBindings...) {
			runtimeBinding := directoryresource.Binding{Identity: identity, Access: directoryresource.ReadWrite}
			if directoryresource.Conflicts(runtimeBinding, binding) {
				_ = f.Close()
				return errors.New("runtime root overlaps data or owned storage")
			}
		}
		mount(f, fmt.Sprintf("/runtime/%d", i), false)
	}
	environment := []string{"HOME=/home/agent", "USER=agent", "LOGNAME=agent", "PATH=/usr/bin:/bin", "LANG=C.UTF-8", "XDG_CONFIG_HOME=/home/agent/.config", "XDG_DATA_HOME=/home/agent/.local/share", "XDG_STATE_HOME=/home/agent/.local/state", "XDG_CACHE_HOME=/home/agent/.cache", "TMPDIR=/tmp", "T3_STEWARD_OUTPUT_DIR=/workspace"}
	command := append([]string(nil), spec.Command...)
	if len(spec.ProviderHosts) > 0 {
		gatewayCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		root, err := os.MkdirTemp("", "steward-egress-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(root)
		socket := filepath.Join(root, "gateway.sock")
		listener, err := net.Listen("unix", socket)
		if err != nil {
			return err
		}
		server := &http.Server{Handler: Egress{Hosts: append([]string(nil), spec.ProviderHosts...)}, ReadHeaderTimeout: 5 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return gatewayCtx }}
		defer server.Close()
		go func() { _ = server.Serve(listener) }()
		fd, err := unix.Open(socket, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		mount(os.NewFile(uintptr(fd), socket), "/run/provider-egress.sock", false)
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		f, err := os.Open(executable)
		if err != nil {
			return err
		}
		mount(f, "/steward", false)
		command = append([]string{"/steward", "worker", "contained-child", "--"}, command...)
		environment = append(environment, "HTTP_PROXY=http://127.0.0.1:18080", "HTTPS_PROXY=http://127.0.0.1:18080", "http_proxy=http://127.0.0.1:18080", "https_proxy=http://127.0.0.1:18080", "NO_PROXY=localhost,127.0.0.1,::1", "no_proxy=localhost,127.0.0.1,::1")
	}
	for _, entry := range environment {
		key, value, _ := strings.Cut(entry, "=")
		args = append(args, "--setenv", key, value)
	}
	args = append(args, "--chdir", cwd, "--remount-ro", "/", "--")
	args = append(args, command...)
	bwrap, err := exec.LookPath("bwrap")
	if err != nil {
		return errors.New("provider containment requires bubblewrap")
	}
	cmd := exec.CommandContext(ctx, bwrap, args...)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	cmd.ExtraFiles = files
	cmd.Stdin = streams.Stdin
	cmd.Stdout = streams.Stdout
	cmd.Stderr = streams.Stderr
	// This context belongs to the dedicated process supervisor, never a lease.
	// The future worker binding must preserve the supervisor across reconnects.
	return cmd.Run()
}
