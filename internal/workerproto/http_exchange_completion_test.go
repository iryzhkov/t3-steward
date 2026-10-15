package workerproto

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

// Delay a real source EOF until successful callback shutdown has begun.
// Closing or canceling later cannot change the source's already observed error.
type heldExchangeEOF struct {
	io.ReadCloser
	observed, release chan struct{}
	once              sync.Once
}

func (b *heldExchangeEOF) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && n == 0 {
		b.once.Do(func() { close(b.observed); <-b.release })
	}
	return n, err
}

type exchangeDeadlineObserver struct {
	http.ResponseWriter
	shutdown chan struct{}
	once     sync.Once
}

func (w *exchangeDeadlineObserver) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *exchangeDeadlineObserver) SetReadDeadline(d time.Time) error {
	err := http.NewResponseController(w.ResponseWriter).SetReadDeadline(d)
	if !d.After(time.Now()) {
		w.once.Do(func() { close(w.shutdown) })
	}
	return err
}

func TestHTTPExchangeRetainsTrailingSourceError(t *testing.T) {
	for _, tail := range [][]byte{{0, 0}, {0, 0, 0, 2, 1}, {0, 0, 0, 2}} {
		t.Run(string([]byte{byte(len(tail)) + '0'}), func(t *testing.T) {
			observed, release, shutdown, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			codec := FrameCodec{MaxBytes: 128}
			h := HTTPExchange{Enabled: true, Workers: map[string]string{"w": "e"}, MaxMessageBytes: 128, MaxExchangeBytes: 2048, Timeout: 5 * time.Second}
			h.Callback = func(ctx context.Context, _ ExchangePreface, dial StreamDialer) error {
				conn, err := dial(ctx)
				if err != nil {
					return err
				}
				_, err = codec.Read(conn)
				<-observed // Source error is fixed before callback returns success.
				return err
			}
			s := newExchangeH2Server(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				r.Body = &heldExchangeEOF{ReadCloser: r.Body, observed: observed, release: release}
				h.ServeHTTP(&exchangeDeadlineObserver{ResponseWriter: w, shutdown: shutdown}, r)
			}))
			defer func() { releaseOnce.Do(func() { close(release) }); s.Close() }()
			var body bytes.Buffer
			_ = codec.Write(&body, []byte(`{"version":1,"workerId":"w","workerEpoch":"e"}`))
			_ = codec.Write(&body, []byte("reply\n"))
			body.Write(tail)
			res, err := s.Client().Post(s.URL+"/worker/exchange", "application/octet-stream", &body)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			select {
			case <-shutdown:
			case <-time.After(10 * time.Second):
				t.Fatal("local shutdown not reached")
			}
			select {
			case <-done:
				t.Fatal("handler did not join held source read")
			default:
			}
			releaseOnce.Do(func() { close(release) })
			if _, err := io.ReadAll(ExchangeResponseReader{res}); err == nil || res.Trailer.Get(ExchangeStatusTrailer) != "error" {
				t.Fatalf("observed truncation lost: %v %v", err, res.Trailer)
			}
			<-done
		})
	}
}

type exchangePayloadEOF struct{}

func (exchangePayloadEOF) Read(p []byte) (int, error) { return copy(p, "payload"), io.EOF }
func TestExchangePayloadAndEOF(t *testing.T) {
	for _, status := range []string{"", "error", "ok"} {
		res := &http.Response{Body: io.NopCloser(exchangePayloadEOF{}), Trailer: make(http.Header)}
		res.Trailer.Set(ExchangeStatusTrailer, status)
		buf := make([]byte, 16)
		n, err := (ExchangeResponseReader{res}).Read(buf)
		if string(buf[:n]) != "payload" || (err == io.EOF) != (status == "ok") {
			t.Fatalf("%q: %q %v", status, buf[:n], err)
		}
	}
	for _, result := range []exchangeFrameResult{
		{err: io.ErrUnexpectedEOF, stage: "read", bytes: 2},
		{err: io.EOF, stage: "read", bytes: 4},
		{err: errors.New("limit"), stage: "limits"},
	} {
		if !result.failed() {
			t.Fatal("source evidence suppressed", result)
		}
	}
}

func exchangeWait(t *testing.T, done <-chan struct{}, event string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal(event)
	}
}

func TestHTTPExchangeHeldEOFLifetime(t *testing.T) {
	for _, mode := range []string{"control", "lifetime", "middleware", "earlier deadline", "total deadline", "reset", "source+cancel"} {
		t.Run(mode, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			if mode == "earlier deadline" {
				cancel()
				parent, cancel = context.WithTimeout(context.Background(), 2*time.Second)
			}
			defer cancel()
			observed, release, shutdown, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			codec := FrameCodec{MaxBytes: 128}
			h := HTTPExchange{Enabled: true, Lifetime: parent, Workers: map[string]string{"w": "e"}, MaxMessageBytes: 128, MaxExchangeBytes: 2048, Timeout: 5 * time.Second}
			if mode == "middleware" {
				h.Lifetime = nil
				h.RequestLifetime = func(*http.Request) context.Context { return parent }
			}
			if mode == "total deadline" {
				h.Timeout = 2 * time.Second
			}
			h.Callback = func(ctx context.Context, _ ExchangePreface, dial StreamDialer) error {
				conn, err := dial(ctx)
				if err != nil {
					return err
				}
				_, err = codec.Read(conn)
				<-observed
				return err
			}
			deadlineAbort := make(chan struct{})
			var abortOnce sync.Once
			h.observe = func(event string) {
				if event == "observer" {
					abortOnce.Do(func() { close(deadlineAbort) })
				}
			}
			rawOwner := make(chan context.Context, 1)
			s := newExchangeH2Server(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				rawOwner <- r.Context()
				r.Body = &heldExchangeEOF{ReadCloser: r.Body, observed: observed, release: release}
				h.ServeHTTP(&exchangeDeadlineObserver{ResponseWriter: w, shutdown: shutdown}, r)
			}))
			defer func() { unblock(); cancel(); s.Close() }()
			var body bytes.Buffer
			_ = codec.Write(&body, []byte(`{"version":1,"workerId":"w","workerEpoch":"e"}`))
			_ = codec.Write(&body, []byte("reply\n"))
			if mode == "source+cancel" {
				body.Write([]byte{0, 0})
			}
			clientCtx, reset := context.WithCancel(context.Background())
			defer reset()
			req, _ := http.NewRequestWithContext(clientCtx, "POST", s.URL+"/worker/exchange", &body)
			res, err := s.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			if res.ProtoMajor != 2 {
				t.Fatal("H2 not negotiated")
			}
			exchangeWait(t, shutdown, "local stop absent")
			select {
			case <-done:
				t.Fatal("input join bypassed")
			default:
			}
			t.Log("empty source EOF fixed; callback nil; local stop; input join held")
			switch mode {
			case "lifetime", "middleware", "source+cancel":
				cancel()
			case "earlier deadline":
				exchangeWait(t, parent.Done(), "earlier deadline absent")
			case "total deadline": // Observe actual raw stream/body abort through the deadline callback.
				// The configured deadline remains armed while the actual input join is held.
				exchangeWait(t, deadlineAbort, "total deadline absent")
			case "reset":
				reset()
				exchangeWait(t, (<-rawOwner).Done(), "raw H2 reset absent")
			}
			unblock()
			_, err = io.ReadAll(ExchangeResponseReader{res})
			exchangeWait(t, done, "handler not joined")
			t.Logf("input/body/watchers/handler joined; parent=%v status=%q reader=%v", parent.Err(), res.Trailer.Get(ExchangeStatusTrailer), err)
			if (err == nil) != (mode == "control") {
				t.Fatalf("late lifetime outcome: %v %v", err, res.Trailer)
			}
		})
	}
}

func TestHTTPExchangeCancellationAtFinalizationBoundaries(t *testing.T) {
	for _, boundary := range []string{"callbackDone", "outputJoin", "inputJoin", "bodyClose", "watcherJoin", "beforeF", "afterF", "delayed observer"} {
		t.Run(boundary, func(t *testing.T) {
			owner, cancel := context.WithCancel(context.Background())
			defer cancel()
			hit, done := make(chan struct{}), make(chan struct{})
			observer, waiting, releaseObserver := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var observedOnce, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(releaseObserver) }) }
			var once sync.Once
			h := HTTPExchange{Enabled: true, Lifetime: owner, Workers: map[string]string{"w": "e"}, MaxMessageBytes: 128, MaxExchangeBytes: 2048, Timeout: 5 * time.Second}
			h.observe = func(event string) {
				if boundary == "delayed observer" {
					switch event {
					case "bodyClose":
						cancel()
					case "observer":
						observedOnce.Do(func() { close(observer) })
						<-releaseObserver
					case "watcherJoin":
						close(waiting)
					}
				}
				if event == boundary {
					once.Do(func() { cancel(); close(hit) })
				}
			}
			h.Callback = func(ctx context.Context, _ ExchangePreface, dial StreamDialer) error {
				conn, err := dial(ctx)
				if err == nil {
					_, err = (FrameCodec{MaxBytes: 128}).Read(conn)
				}
				return err
			}
			s := newExchangeH2Server(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(done); h.ServeHTTP(w, r) }))
			defer s.Close()
			defer unblock()
			var body bytes.Buffer
			codec := FrameCodec{MaxBytes: 128}
			_ = codec.Write(&body, []byte(`{"version":1,"workerId":"w","workerEpoch":"e"}`))
			_ = codec.Write(&body, []byte("reply\n"))
			res, err := s.Client().Post(s.URL+"/worker/exchange", "application/octet-stream", &body)
			if boundary == "delayed observer" {
				exchangeWait(t, observer, "observer not held")
				exchangeWait(t, waiting, "watcher join not pending")
				select {
				case <-done:
					t.Fatal("observer join bypassed")
				default:
				}
				unblock()
				close(hit)
			}
			if err == nil {
				_, err = io.ReadAll(ExchangeResponseReader{res})
				res.Body.Close()
			}
			exchangeWait(t, hit, "boundary not reached")
			exchangeWait(t, done, "boundary handler not joined")
			if (err == nil) != (boundary == "afterF") {
				t.Fatal("wrong cancellation linearization at", boundary, err)
			}
			t.Log("owner cancellation at", boundary, "checked against F after all joins; reader:", err)
		})
	}
}
