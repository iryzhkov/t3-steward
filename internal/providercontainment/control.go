package providercontainment

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
)

// ListenControl is called inside the namespace in a dedicated owned mount.
// Never unlink an existing socket: it may belong to an existing execution.
func ListenControl(socket string) (net.Listener, error) {
	if !filepath.IsAbs(socket) || filepath.Clean(socket) != socket {
		return nil, errors.New("control socket must be absolute and canonical")
	}
	if err := privateDirectory(filepath.Dir(socket)); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(socket); err == nil {
		return nil, errors.New("control socket already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(socket, 0600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}

// ControlBridge runs INSIDE the private network namespace. Its sole destination
// is that namespace's T3 API; no host TCP address or caller-chosen destination
// is accepted. Host control connects through the mounted Unix socket.
func ControlBridge(ctx context.Context, listener net.Listener, port int) error {
	if port < 1 || port > 65535 || port == 18080 {
		return errors.New("invalid scoped API port")
	}
	return bridgeTo(ctx, listener, "tcp4", "127.0.0.1:"+strconv.Itoa(port))
}
