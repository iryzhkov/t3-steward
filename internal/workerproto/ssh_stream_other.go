//go:build !unix

package workerproto

import "errors"

func SSHStreamDialer(SSHConfig) (StreamDialer, error) {
	return nil, errors.New("persistent SSH worker transport requires Unix")
}
