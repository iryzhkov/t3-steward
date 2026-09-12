//go:build linux

package backlogadmin

import (
	"errors"
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// localPeerPrincipal authenticates the connected admin peer by its socket
// credentials. SO_PEERCRED is a Linux facility, which is why the function has
// its own file; the local admin transport is only ever served on Linux hosts.
func localPeerPrincipal(conn *net.UnixConn, allowedUID uint32) (Principal, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return Principal{}, fmt.Errorf("inspect admin peer: %w", err)
	}
	var credential *unix.Ucred
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		credential, socketErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
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
