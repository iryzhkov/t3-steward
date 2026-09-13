// Package providercontainment supplies an isolated process boundary for a
// dedicated T3 server and all its provider subprocesses. It does not release
// coordinator ownership or infer that a disconnected execution has stopped.
package providercontainment

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"
)

// Egress allows CONNECT only to explicitly approved provider DNS names on 443.
// DNS answers are checked and the validated numeric address is dialed directly,
// preventing a second DNS lookup from retargeting the connection to a host broker.
type Egress struct {
	Hosts  []string
	lookup func(context.Context, string) ([]net.IPAddr, error)
	dial   func(context.Context, string, string) (net.Conn, error)
}

func (e Egress) target(ctx context.Context, authority string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(authority)
	if err != nil || port != "443" || host != strings.ToLower(host) || strings.HasSuffix(host, ".") {
		return nil, errors.New("provider destination is not allowed")
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return nil, errors.New("numeric provider destination is not allowed")
	}
	allowed := false
	for _, name := range e.Hosts {
		if host == name {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, errors.New("provider hostname is not allowed")
	}
	lookup := e.lookup
	if lookup == nil {
		lookup = net.DefaultResolver.LookupIPAddr
	}
	addresses, err := lookup(ctx, host)
	if err != nil || len(addresses) == 0 {
		return nil, errors.New("provider DNS resolution failed")
	}
	for _, entry := range addresses {
		address, ok := netip.AddrFromSlice(entry.IP)
		if !ok || entry.Zone != "" || !publicAddress(address) {
			return nil, errors.New("provider DNS returned a non-public address")
		}
	}
	dial := e.dial
	if dial == nil {
		dial = (&net.Dialer{Timeout: 10 * time.Second}).DialContext
	}
	// Fail without retrying another address; callers own bounded retry policy.
	return dial(ctx, "tcp", net.JoinHostPort(addresses[0].IP.String(), "443"))
}

func publicAddress(address netip.Addr) bool {
	address = address.Unmap()
	if !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() {
		return false
	}
	// Exclude special-use ranges, including translation/transition mechanisms
	// that could route an apparently global address back to a private endpoint.
	for _, raw := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001::/23", "2001:db8::/32", "2002::/16"} {
		if netip.MustParsePrefix(raw).Contains(address) {
			return false
		}
	}
	return true
}

func (e Egress) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect || r.URL.User != nil || r.Host != r.URL.Host {
		http.Error(w, "CONNECT to an approved provider is required", http.StatusForbidden)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	upstream, err := e.target(ctx, r.Host)
	cancel()
	if err != nil {
		http.Error(w, "provider destination refused", http.StatusForbidden)
		return
	}
	defer upstream.Close()
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "tunneling unavailable", http.StatusInternalServerError)
		return
	}
	downstream, buffered, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer downstream.Close()
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-r.Context().Done():
			_ = upstream.Close()
			_ = downstream.Close()
		case <-finished:
		}
	}()
	if _, err := fmt.Fprint(buffered, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := buffered.Flush(); err != nil {
		return
	}
	done := make(chan struct{})
	go func() { _, _ = io.Copy(upstream, buffered); _ = upstream.Close(); _ = downstream.Close(); close(done) }()
	_, _ = io.Copy(downstream, upstream)
	_ = upstream.Close()
	_ = downstream.Close()
	<-done
}

// Bridge makes the host-side constrained Unix gateway available on loopback in
// the private network namespace. It never dials a host TCP service directly.
func Bridge(ctx context.Context, listener net.Listener, socket string) error {
	return bridgeTo(ctx, listener, "unix", socket)
}

func bridgeTo(ctx context.Context, listener net.Listener, network, address string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { <-ctx.Done(); _ = listener.Close() }()
	for {
		local, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go func() {
			defer local.Close()
			remote, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, address)
			if err != nil {
				return
			}
			defer remote.Close()
			finished := make(chan struct{})
			defer close(finished)
			go func() {
				select {
				case <-ctx.Done():
					_ = local.Close()
					_ = remote.Close()
				case <-finished:
				}
			}()
			done := make(chan struct{})
			go func() { _, _ = io.Copy(remote, local); _ = remote.Close(); _ = local.Close(); close(done) }()
			_, _ = io.Copy(local, remote)
			_ = local.Close()
			_ = remote.Close()
			<-done
		}()
	}
}
