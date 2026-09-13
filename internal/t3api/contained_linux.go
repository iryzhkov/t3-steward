//go:build linux

package t3api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"golang.org/x/sys/unix"
)

// NewContained reopens the recorded control directory for each token read and
// connection. The socket inode stays pinned until connect completes, so a
// provider-controlled rename or symlink cannot redirect a host-side connection.
// The caller must also fence the supervisor invocation and T3 environment.
func NewContained(control directoryresource.Identity, timeout time.Duration) (*Client, error) {
	file, err := reopenControl(control)
	if err != nil {
		return nil, err
	}
	file.Close()
	client, err := NewUnix(control.Registration.Path+"/api.sock", containedToken{control}, timeout)
	if err != nil {
		return nil, err
	}
	client.HTTP.Transport = &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			directory, err := reopenControl(control)
			if err != nil {
				return nil, err
			}
			defer directory.Close()
			socket, err := pinControlSocket(directory)
			if err != nil {
				return nil, err
			}
			defer socket.Close()
			return (&net.Dialer{Timeout: client.HTTP.Timeout}).DialContext(ctx, "unix", fmt.Sprintf("/proc/self/fd/%d", socket.Fd()))
		},
	}
	return client, nil
}

func reopenControl(identity directoryresource.Identity) (*os.File, error) {
	file, err := directoryresource.Reopen(identity, identity.Registration)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.Mode().Perm() != 0700 {
		file.Close()
		return nil, errors.New("contained control directory must be private")
	}
	return file, nil
}

func pinControlSocket(directory *os.File) (*os.File, error) {
	fd, err := unix.Openat(int(directory.Fd()), "api.sock", unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "contained-api.sock")
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0600 {
		file.Close()
		return nil, errors.New("contained API must be a private socket, not a link or regular file")
	}
	return file, nil
}

type containedToken struct{ control directoryresource.Identity }

func (s containedToken) Token(ctx context.Context) (string, error) {
	directory, err := reopenControl(s.control)
	if err != nil {
		return "", err
	}
	defer directory.Close()
	// Retain the reopened descriptor until the token has been completely read.
	// SocketTokenFile's OpenRoot also fences token symlinks beneath this inode.
	return (SocketTokenFile{Control: fmt.Sprintf("/proc/self/fd/%d", directory.Fd())}).Token(ctx)
}
func (containedToken) Invalidate() {}
