//go:build !linux && !darwin

package backlogadmin

import (
	"errors"
	"net"
)

// localPeerPrincipal refuses every peer on platforms without SO_PEERCRED. The
// binary still builds there so the quota watchdog can run; only the local admin
// transport is unavailable.
func localPeerPrincipal(*net.UnixConn, uint32) (Principal, error) {
	return Principal{}, errors.New("local admin transport requires Linux peer credentials")
}
