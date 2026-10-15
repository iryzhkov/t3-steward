package workerproto

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

type exchangeNoDeadlines struct{ http.ResponseWriter }

func (exchangeNoDeadlines) EnableFullDuplex() error { return nil }

type exchangeBlockedOutput struct {
	http.ResponseWriter
	ctx     context.Context
	blocked chan struct{}
	fail    bool
	flush   bool
}

func (w *exchangeBlockedOutput) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *exchangeBlockedOutput) Write([]byte) (int, error) {
	close(w.blocked)
	if w.fail {
		return 0, io.ErrClosedPipe
	}
	<-w.ctx.Done()
	return 0, w.ctx.Err()
}

func (w *exchangeBlockedOutput) FlushError() error {
	if !w.flush {
		return http.NewResponseController(w.ResponseWriter).Flush()
	}
	close(w.blocked)
	if w.fail {
		return io.ErrClosedPipe
	}
	<-w.ctx.Done()
	return w.ctx.Err()
}

type exchangeBlockingRead struct {
	io.ReadCloser
	blocked   chan struct{}
	reads, at int
}

func (r *exchangeBlockingRead) Read(p []byte) (int, error) {
	r.reads++
	if r.reads == r.at {
		close(r.blocked)
	}
	return r.ReadCloser.Read(p)
}

func TestHTTPExchangeExternalCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, done := make(chan struct{}), make(chan struct{})
	h := HTTPExchange{Enabled: true, Lifetime: ctx, Workers: map[string]string{"w": "e"}, MaxMessageBytes: 128, MaxExchangeBytes: 2048, Timeout: 5 * time.Second,
		Callback: func(ctx context.Context, _ ExchangePreface, _ StreamDialer) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}}
	s := newExchangeH2Server(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(done); h.ServeHTTP(w, r) }))
	defer s.Close()
	body, writer := io.Pipe()
	defer writer.Close()
	result := make(chan error, 1)
	go func() {
		res, err := s.Client().Post(s.URL+"/worker/exchange", "application/octet-stream", body)
		if err == nil {
			_, err = io.ReadAll(ExchangeResponseReader{res})
			res.Body.Close()
		}
		result <- err
	}()
	_ = (FrameCodec{MaxBytes: 1024}).Write(writer, []byte(`{"version":1,"workerId":"w","workerEpoch":"e"}`))
	<-entered
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("external cancellation did not join")
	}
	writer.Close()
	if err := <-result; err == nil {
		t.Fatal("external cancellation accepted")
	}
}
