//go:build darwin

package backlogadmin

import (
	"errors"
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// localPeerPrincipal authenticates the connected admin peer through
// LOCAL_PEERCRED, macOS's equivalent of SO_PEERCRED. Only the UID is used.
func localPeerPrincipal(conn *net.UnixConn, allowedUID uint32) (Principal, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return Principal{}, fmt.Errorf("inspect admin peer: %w", err)
	}
	var credential *unix.Xucred
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		credential, socketErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	}); err != nil {
		return Principal{}, fmt.Errorf("inspect admin peer: %w", err)
	}
	if socketErr != nil {
		return Principal{}, fmt.Errorf("authenticate admin peer: %w", socketErr)
	}
	if credential == nil || credential.Uid != allowedUID {
		return Principal{}, errors.New("admin peer uid is not authorized")
	}
	return Principal{ID: fmt.Sprintf("local:%d", credential.Uid), Roles: []string{"local-admin"}}, nil
}
