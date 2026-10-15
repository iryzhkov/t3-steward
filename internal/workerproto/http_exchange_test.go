package workerproto

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPExchange(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "http1", true: "http2"}[h2], func(t *testing.T) {
			codec := FrameCodec{MaxBytes: 128}
			request, reply := []byte("signed request\n\x00artifact"), []byte("signed reply\n\xffartifact")
			callbackDone := make(chan struct{})
			h := HTTPExchange{Enabled: true, Workers: map[string]string{"w": "e"}, MaxMessageBytes: 128, MaxArtifactBytes: 128, MaxExchangeBytes: 2048, Timeout: 5 * time.Second}
			h.Callback = func(ctx context.Context, p ExchangePreface, dial StreamDialer) error {
				defer close(callbackDone)
				conn, err := dial(ctx)
				if err != nil {
					return err
				}
				if _, err := dial(ctx); err == nil {
					return errors.New("second dial accepted")
				}
				if err = codec.Write(conn, request); err != nil {
					return err
				}
				got, err := codec.Read(conn)
				if err == nil && !bytes.Equal(got, reply) {
					return errors.New("reply changed")
				}
				return err
			}
			server := httptest.NewUnstartedServer(h)
			server.EnableHTTP2 = h2
			server.StartTLS()
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			upload, writer := io.Pipe()
			defer writer.Close()
			req, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/worker/exchange", upload)
			result := make(chan *http.Response, 1)
			failure := make(chan error, 1)
			go func() {
				res, err := server.Client().Do(req)
				if err != nil {
					failure <- err
				} else {
					result <- res
				}
			}()
			if !h2 {
				_, _ = writer.Write([]byte{0}) // An open upload must be rejected without reading routing.
				select {
				case res := <-result:
					defer res.Body.Close()
					if res.StatusCode != http.StatusHTTPVersionNotSupported {
						t.Fatal(res.StatusCode)
					}
				case err := <-failure:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatal("H1 rejection waited for EOF")
				}
				select {
				case <-callbackDone:
					t.Fatal("H1 callback ran")
				default:
				}
				return
			}
			prefix, _ := json.Marshal(ExchangePreface{Version, "w", "e"})
			if err := codec.Write(writer, prefix); err != nil {
				t.Fatal(err)
			}
			var res *http.Response
			select {
			case res = <-result:
			case err := <-failure:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal("response buffered until upload EOF")
			}
			defer res.Body.Close()
			if h2 && res.ProtoMajor != 2 {
				t.Fatal("HTTP/2 not negotiated")
			}
			got, err := codec.Read(res.Body)
			if err != nil || !bytes.Equal(got, request) {
				t.Fatalf("request changed: %q %v", got, err)
			}
			if err = codec.Write(writer, reply); err != nil {
				t.Fatal(err)
			}
			if _, err = io.ReadAll(ExchangeResponseReader{res}); err != nil {
				t.Fatal(err)
			}
			if res.Trailer.Get(ExchangeStatusTrailer) != "ok" {
				t.Fatal("completion waited for upload EOF")
			}
			writer.Close()
			<-callbackDone
		})
	}
}

func TestHTTPExchangeFailures(t *testing.T) {
	prefix := func(p ExchangePreface) []byte {
		raw, _ := json.Marshal(p)
		var b bytes.Buffer
		_ = (FrameCodec{MaxBytes: 1024}).Write(&b, raw)
		return b.Bytes()
	}
	good := prefix(ExchangePreface{Version, "w", "e"})
	prefaceRaw := func(raw string) []byte {
		var b bytes.Buffer
		_ = (FrameCodec{MaxBytes: 1024}).Write(&b, []byte(raw))
		return b.Bytes()
	}
	framed := func(raw []byte) []byte {
		var b bytes.Buffer
		_ = (FrameCodec{MaxBytes: 2048}).Write(&b, raw)
		return append(append([]byte{}, good...), b.Bytes()...)
	}
	var oversized [4]byte
	binary.BigEndian.PutUint32(oversized[:], 1025)
	for _, tc := range []struct {
		name string
		body []byte
		fail bool
	}{
		{"unknown", prefix(ExchangePreface{Version, "other", "e"}), false},
		{"version", prefix(ExchangePreface{Version + 1, "w", "e"}), false},
		{"zero frame", append(append([]byte{}, good...), 0, 0, 0, 0), true},
		{"separator", framed([]byte("no separator")), true},
		{"epoch", prefix(ExchangePreface{Version, "w", "old"}), false},
		{"unknown field", prefaceRaw(`{"version":1,"workerId":"w","workerEpoch":"e","extra":true}`), false},
		{"extra value", append(append([]byte{}, good...), 0), true},
		{"preface extra value", prefaceRaw(`{"version":1,"workerId":"w","workerEpoch":"e"} {}`), false},
		{"zero prefix", []byte{0, 0, 0, 0}, false},
		{"output budget", good, true},
		{"large prefix", oversized[:], false}, {"short prefix", good[:5], false},
		{"short frame", append(append([]byte{}, good...), 0, 0, 0, 2, 1), true},
		{"large frame", append(append([]byte{}, good...), 0, 0, 1, 0), true},
		{"callback", good, true},
		{"budget", framed(append(bytes.Repeat([]byte{1}, 1023), '\n')), true},
		{"message", framed(append(bytes.Repeat([]byte{1}, 65), '\n')), true},
		{"artifact", framed(append([]byte("x\n"), bytes.Repeat([]byte{1}, 65)...)), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := make(chan struct{}, 1)
			h := HTTPExchange{Enabled: true, Workers: map[string]string{"w": "e"}, MaxMessageBytes: 64, MaxExchangeBytes: 2048, Timeout: time.Second,
				Callback: func(ctx context.Context, p ExchangePreface, dial StreamDialer) error {
					called <- struct{}{}
					conn, _ := dial(ctx)
					if tc.name == "output budget" {
						return (FrameCodec{MaxBytes: 2048}).Write(conn, append(bytes.Repeat([]byte{'x'}, 1000), '\n'))
					}
					if tc.name == "callback" {
						return errors.New("private detail")
					}
					_, err := (FrameCodec{MaxBytes: 2048}).Read(conn)
					return err
				}}
			if tc.name == "message" || tc.name == "artifact" {
				h.MaxArtifactBytes = 64
			}
			if tc.name == "budget" || tc.name == "output budget" {
				h.MaxMessageBytes = 2048
				h.MaxExchangeBytes = 1025
			}
			s := newExchangeH2Server(h)
			defer s.Close()
			res, err := s.Client().Post(s.URL+"/worker/exchange", "application/octet-stream", bytes.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			_, err = io.ReadAll(ExchangeResponseReader{res})
			if tc.fail {
				if res.StatusCode != 200 || err == nil {
					t.Fatal("failed exchange accepted")
				}
				<-called
				if tc.name == "callback" && res.Trailer.Get(ExchangeStatusTrailer) != "error" {
					t.Fatal("callback failure trailer missing")
				}
			} else {
				if res.StatusCode != 400 || len(called) != 0 {
					t.Fatal("bad route dispatched")
				}
			}
		})
	}
	h := HTTPExchange{Monitoring: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })}
	for _, path := range []string{"/healthz", "/alerts", "/worker/exchange", "/admin/exchange", "/worker/exchange/"} {
		for _, method := range []string{"GET", "HEAD"} {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
			want := 404
			if path == "/healthz" || path == "/alerts" {
				want = 204
			}
			if w.Code != want {
				t.Fatalf("%s %s: %d", method, path, w.Code)
			}
		}
	}
	h.Enabled = true
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/worker/exchange", nil))
	if w.Code != 405 {
		t.Fatal(w.Code)
	}
	h.Callback = func(context.Context, ExchangePreface, StreamDialer) error {
		t.Fatal("unsupported duplex invoked callback")
		return nil
	}
	h.MaxMessageBytes = 64
	h.MaxExchangeBytes = 2048
	h.Timeout = time.Second
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/worker/exchange", bytes.NewReader(good)))
	if w.Code != 505 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/worker/exchange", bytes.NewReader(good))
	r.ProtoMajor = 2
	h.ServeHTTP(exchangeNoDeadlines{w}, r)
	if w.Code != 503 {
		t.Fatal("unavailable deadlines dispatched", w.Code)
	}
}

func TestHTTPExchangeStall(t *testing.T) {
	for _, mode := range []string{"reply", "prefix", "disconnect", "connectiondisconnect", "output", "outputfailure", "externaloutput", "flush", "flushfailure"} {
		t.Run(mode, func(t *testing.T) {
			done, blocked := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			h := HTTPExchange{Enabled: true, Lifetime: ctx, Workers: map[string]string{"w": "e"}, MaxMessageBytes: 64, MaxExchangeBytes: 2048, Timeout: 5 * time.Second,
				Monitoring: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }),
				Callback: func(ctx context.Context, p ExchangePreface, dial StreamDialer) error {
					conn, _ := dial(ctx)
					if mode == "output" || mode == "outputfailure" || mode == "externaloutput" {
						return (FrameCodec{MaxBytes: 2048}).Write(conn, []byte("response\n"))
					}
					_, err := (FrameCodec{MaxBytes: 2048}).Read(conn)
					return err
				}}
			s := newExchangeH2Server(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

				if r.URL.Path == "/worker/exchange" {
					if mode == "output" || mode == "outputfailure" || mode == "externaloutput" || mode == "flush" || mode == "flushfailure" {
						w = &exchangeBlockedOutput{ResponseWriter: w, ctx: ctx, blocked: blocked, fail: mode == "outputfailure" || mode == "flushfailure", flush: mode == "flush" || mode == "flushfailure"}
					}
					r.Body = &exchangeBlockingRead{ReadCloser: r.Body, blocked: blocked, at: map[string]int{"prefix": 2, "reply": 3, "disconnect": 3, "connectiondisconnect": 3}[mode]}
					defer close(done)
				}
				h.ServeHTTP(w, r)
			}))
			defer s.Close()
			peer := make(chan net.Conn, 1)
			if mode == "connectiondisconnect" {
				s.Client().Transport.(*http.Transport).DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
					conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
					if err == nil {
						peer <- conn
					}
					return conn, err
				}
			}
			body, writer := io.Pipe()
			defer writer.Close()
			req, _ := http.NewRequest("POST", s.URL+"/worker/exchange", body)
			result := make(chan error, 1)
			go func() {
				res, err := s.Client().Do(req)
				if err == nil {
					if mode == "connectiondisconnect" {
						<-blocked // The real body read is pending before peer TCP close.
						_ = (<-peer).Close()
					}
					if mode == "disconnect" {
						res.Body.Close()
						writer.CloseWithError(errors.New("disconnect"))
					} else {
						_, err = io.ReadAll(ExchangeResponseReader{res})
						res.Body.Close()
					}
				}
				result <- err
			}()
			if mode == "prefix" {
				if _, err := writer.Write([]byte{0}); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "prefix" {
				raw, _ := json.Marshal(ExchangePreface{Version, "w", "e"})
				if err := (FrameCodec{MaxBytes: 1024}).Write(writer, raw); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "disconnect" {
				select {
				case <-blocked:
				case <-time.After(10 * time.Second):
					t.Fatal("source read did not block")
				}
			}
			if mode == "externaloutput" {
				cancel()
			}
			monitorClient := s.Client()
			if mode == "connectiondisconnect" {
				transport := s.Client().Transport.(*http.Transport).Clone()
				defer transport.CloseIdleConnections()
				monitorClient = &http.Client{Transport: transport}
			}
			res, err := monitorClient.Get(s.URL + "/healthz")
			if err != nil {
				t.Fatal(err)
			}
			res.Body.Close()
			if res.StatusCode != 204 {
				t.Fatal("health blocked")
			}
			if mode == "prefix" {
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("prefix handler did not terminate")
				}
				writer.Close()
			}
			select {
			case err := <-result:
				if err == nil && mode != "disconnect" {
					t.Fatal("stall accepted")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("stall did not terminate")
			}
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("handler did not join bridges")
			}
		})
	}
}
func TestExchangeBudgetEOF(t *testing.T) {
	for _, raw := range []string{"ab", "abcde"} {
		input := bytes.NewReader([]byte(raw))
		var remaining atomic.Int64
		remaining.Store(2)
		got, err := io.ReadAll(exchangeBudgetReader{input, &remaining})
		if string(got) != "ab" || (err == nil) != (raw == "ab") || len(raw)-input.Len() > 3 {
			t.Fatalf("budget completion %q: %q %v, consumed %d", raw, got, err, len(raw)-input.Len())
		}
	}
}
func TestExchangeCompletionTrailer(t *testing.T) {
	for _, status := range []string{"", "error", "ok"} {
		res := &http.Response{Body: io.NopCloser(bytes.NewReader(nil)), Trailer: make(http.Header)}
		res.Trailer.Set(ExchangeStatusTrailer, status)
		_, err := io.ReadAll(ExchangeResponseReader{res})
		if (err == nil) != (status == "ok") {
			t.Fatalf("completion %q: %v", status, err)
		}
	}
}

// TLS fixture proves stream semantics; coordinator tests separately prove the plain same-listener backend.
func newExchangeH2Server(h http.Handler) *httptest.Server {
	s := httptest.NewUnstartedServer(h)
	s.EnableHTTP2 = true
	s.StartTLS()
	return s
}
