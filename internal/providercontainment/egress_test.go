package providercontainment

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestEgressRejectsBrokerDestinationsBeforeDial(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.90.212", "169.254.169.254", "100.64.0.1", "0.0.0.1", "192.0.2.1", "198.18.0.1", "240.0.0.1", "::1", "::ffff:127.0.0.1", "fc00::1", "fe80::1", "64:ff9b::a00:1", "2002:7f00:1::1", "2001:db8::1"} {
		t.Run(address, func(t *testing.T) {
			dialed := false
			e := Egress{Hosts: []string{"provider.example"}, lookup: func(context.Context, string) ([]net.IPAddr, error) {
				return []net.IPAddr{{IP: net.ParseIP(address)}}, nil
			}, dial: func(context.Context, string, string) (net.Conn, error) {
				dialed = true
				return nil, errors.New("dialed")
			}}
			if _, err := e.target(context.Background(), "provider.example:443"); err == nil || dialed {
				t.Fatalf("unsafe address dialed: %s", address)
			}
		})
	}
	for _, authority := range []string{"127.0.0.1:443", "localhost:443", "provider.example:80", "provider.example.:443", "PROVIDER.EXAMPLE:443", "other.example:443", "user@provider.example:443"} {
		e := Egress{Hosts: []string{"provider.example"}, lookup: func(context.Context, string) ([]net.IPAddr, error) {
			t.Fatal("invalid authority resolved")
			return nil, nil
		}}
		if _, err := e.target(context.Background(), authority); err == nil {
			t.Fatalf("authority accepted: %s", authority)
		}
	}
	for _, address := range []string{"1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"} {
		if !publicAddress(netip.MustParseAddr(address)) {
			t.Fatalf("public address rejected: %s", address)
		}
	}
}

func TestEgressPinsResolvedAddressAndPreservesBufferedTunnelBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	e := Egress{Hosts: []string{"provider.example"},
		lookup: func(context.Context, string) ([]net.IPAddr, error) {
			calls++
			return []net.IPAddr{{IP: net.ParseIP("1.1.1.1")}}, nil
		},
		dial: func(_ context.Context, network, address string) (net.Conn, error) {
			if network != "tcp" || address != "1.1.1.1:443" {
				t.Errorf("not pinned to resolved address: %s %s", network, address)
			}
			local, remote := net.Pipe()
			go func() {
				defer remote.Close()
				body := make([]byte, 4)
				if _, err := io.ReadFull(remote, body); err == nil {
					_, _ = remote.Write(body)
				}
			}()
			return local, nil
		},
	}
	server := httptest.NewUnstartedServer(e)
	server.Config.BaseContext = func(net.Listener) context.Context { return ctx }
	server.Start()
	defer server.Close()
	c, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	// Send tunnel bytes with the headers, so they may already be buffered at Hijack.
	_, err = io.WriteString(c, "CONNECT provider.example:443 HTTP/1.1\r\nHost: provider.example:443\r\n\r\nping")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(c)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("CONNECT response: %v %v", response, err)
	}
	body := make([]byte, 4)
	if _, err := io.ReadFull(reader, body); err != nil || string(body) != "ping" {
		t.Fatalf("buffered data lost: %q %v", body, err)
	}
	if calls != 1 {
		t.Fatalf("DNS lookup count %d", calls)
	}
}

func TestEgressRejectsOrdinaryHTTPAndMixedDNS(t *testing.T) {
	e := Egress{Hosts: []string{"provider.example"}, lookup: func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("1.1.1.1")}, {IP: net.ParseIP("127.0.0.1")}}, nil
	}}
	response := httptest.NewRecorder()
	e.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://provider.example/", strings.NewReader("")))
	if response.Code != http.StatusForbidden {
		t.Fatal("ordinary HTTP accepted")
	}
	if _, err := e.target(context.Background(), "provider.example:443"); err == nil {
		t.Fatal("mixed private/public DNS accepted")
	}
}
