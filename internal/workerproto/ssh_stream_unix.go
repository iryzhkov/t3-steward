//go:build unix

package workerproto

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// SSHStreamDialer starts a fixed remote bridge. SSH remains connected across
// exchanges; the remote worker process is independent of this connection.
func SSHStreamDialer(config SSHConfig) (StreamDialer, error) {
	validated, err := NewSSHTransport(config)
	if err != nil {
		return nil, err
	}
	config = validated.config
	return func(ctx context.Context) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
		if err != nil {
			return nil, err
		}
		parent := os.NewFile(uintptr(pair[0]), "worker-parent")
		child := os.NewFile(uintptr(pair[1]), "worker-child")
		defer child.Close()
		conn, err := net.FileConn(parent)
		parent.Close()
		if err != nil {
			return nil, err
		}
		seconds := max(int(config.ConnectTimeout.Round(time.Second)/time.Second), 1)
		args := []string{"-T", "-oBatchMode=yes", "-oStrictHostKeyChecking=yes", "-oConnectTimeout=" + strconv.Itoa(seconds), "-oServerAliveInterval=15", "-oServerAliveCountMax=2", "--", config.Address, config.RemoteCommand}
		args = append(args, config.RemoteArguments...)
		command := config.Factory(context.Background(), "ssh", args...)
		command.Stdin = child
		command.Stdout = child
		command.Stderr = &boundedBuffer{limit: config.MaxStderrBytes}
		if err = command.Start(); err != nil {
			conn.Close()
			return nil, err
		}
		c := &sshStreamConn{Conn: conn, stop: func() { _ = command.Process.Kill() }, done: make(chan struct{})}
		go func() { _ = command.Wait(); conn.Close(); close(c.done) }()
		if ctx.Err() != nil {
			c.Close()
			return nil, ctx.Err()
		}
		return c, nil
	}, nil
}

type sshStreamConn struct {
	net.Conn
	once sync.Once
	stop func()
	done chan struct{}
}

func (c *sshStreamConn) Close() error {
	var err error
	c.once.Do(func() { err = c.Conn.Close(); c.stop(); <-c.done })
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
