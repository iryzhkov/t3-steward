package t3api

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"time"
)

// NewUnix addresses only one contained T3 server. Neither proxy environment,
// redirects nor URL hostnames can route this client to a shared host service.
// The caller owns the private control directory and this execution's token.
func NewUnix(socket string, tokens TokenSource, timeout time.Duration) (*Client, error) {
	if !filepath.IsAbs(socket) || filepath.Clean(socket) != socket {
		return nil, errors.New("T3 control socket must be absolute and canonical")
	}
	client := New("http://contained-t3", tokens, timeout)
	client.HTTP.Transport = &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: client.HTTP.Timeout}).DialContext(ctx, "unix", socket)
		},
		DisableKeepAlives: true,
	}
	client.HTTP.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return client, nil
}
