package workerproto

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const ExchangeStatusTrailer = "X-Steward-Exchange-Status"
const ExchangePrefaceLimit int64 = 1024

// ExchangePreface is an untrusted routing claim, not an authenticated identity.
// The callback must still verify signed envelopes, epochs and replay protection.
type ExchangePreface struct {
	Version     int    `json:"version"`
	WorkerID    string `json:"workerId"`
	WorkerEpoch string `json:"workerEpoch"`
}

// ExchangeCallback must return on context cancellation and join any work it starts.
// Its one-use dialer adapts the exchange to the existing signed StreamTransport.
type ExchangeCallback func(context.Context, ExchangePreface, StreamDialer) error

// HTTPExchange shares the monitoring listener. Workers contains only configured
// HTTP-selected workers. Limits apply to both directions combined.
type HTTPExchange struct {
	Enabled                                             bool
	Monitoring                                          http.Handler
	Workers                                             map[string]string
	MaxMessageBytes, MaxArtifactBytes, MaxExchangeBytes int64
	Timeout                                             time.Duration
	Callback                                            ExchangeCallback
	// Lifetime and RequestLifetime are independent owners supplied by the listener/middleware.
	// The raw HTTP/2 stream context remains authoritative for disconnect/reset.
	Lifetime        context.Context
	RequestLifetime func(*http.Request) context.Context
	observe         func(string)
}

func (h HTTPExchange) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor == 1 && r.URL.Path == "/worker/exchange" {
		w.Header().Set("Connection", "close")
	}
	if r.URL.Path != "/worker/exchange" || !h.Enabled {
		if h.Monitoring != nil && (r.URL.Path == "/healthz" || r.URL.Path == "/alerts" || strings.HasPrefix(r.URL.Path, "/alerts/")) {
			h.Monitoring.ServeHTTP(w, r)
		} else {
			_ = http.NewResponseController(w).SetReadDeadline(time.Now())
			_ = r.Body.Close()
			http.NotFound(w, r)
		}
		return
	}
	rc := http.NewResponseController(w)
	closeRejectedBody := func() { _ = rc.SetReadDeadline(time.Now()); _ = r.Body.Close() }
	if r.Method != http.MethodPost {
		closeRejectedBody()
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if h.Callback == nil || h.MaxMessageBytes <= 0 || h.MaxMessageBytes > 8<<20 || h.MaxArtifactBytes < 0 || h.MaxArtifactBytes > StreamArtifactLimit || h.MaxExchangeBytes <= ExchangePrefaceLimit || h.MaxExchangeBytes > 64<<20 || h.Timeout <= 0 || h.Timeout > 2*time.Minute {
		closeRejectedBody()
		http.Error(w, "exchange unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.ProtoMajor != 2 {
		closeRejectedBody()
		http.Error(w, "HTTP/2 required", http.StatusHTTPVersionNotSupported)
		return
	}
	event := func(name string) {
		if h.observe != nil {
			h.observe(name)
		}
	}
	owners := []context.Context{r.Context()}
	if h.Lifetime != nil {
		owners = append(owners, h.Lifetime)
	}
	if h.RequestLifetime != nil {
		if owner := h.RequestLifetime(r); owner != nil {
			owners = append(owners, owner)
		}
	}
	deadline := time.Now().Add(h.Timeout)
	for _, owner := range owners {
		if d, ok := owner.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
	}
	if rc.SetReadDeadline(deadline) != nil || rc.SetWriteDeadline(deadline) != nil {
		closeRejectedBody()
		http.Error(w, "exchange deadlines unavailable", http.StatusServiceUnavailable)
		return
	}
	external, stopDeadline := context.WithDeadline(r.Context(), deadline)
	defer stopDeadline()
	ctx, cancel := context.WithCancel(external)
	defer cancel()
	local, bridge := net.Pipe()
	defer local.Close()
	defer bridge.Close()
	abort := func() {
		cancel()
		_ = local.Close()
		_ = bridge.Close()
		_ = rc.SetReadDeadline(time.Now())
	}
	owners = append(owners, external)
	var observers []func()
	for _, owner := range owners {
		done := make(chan struct{})
		stop := context.AfterFunc(owner, func() {
			defer close(done)
			event("observer")
			abort()
			_ = rc.SetWriteDeadline(time.Now())
		})
		observers = append(observers, sync.OnceFunc(func() {
			if !stop() {
				<-done
			}
		}))
	}
	joinObservers := sync.OnceFunc(func() {
		for _, join := range observers {
			join()
		}
	})
	cleanup := func() {
		abort()
		_ = r.Body.Close()
		joinObservers()
	}
	defer cleanup()
	var remaining atomic.Int64
	remaining.Store(h.MaxExchangeBytes)
	body := exchangeBudgetReader{r.Body, &remaining}
	raw, err := (FrameCodec{MaxBytes: ExchangePrefaceLimit}).Read(body)
	var preface ExchangePreface
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err == nil {
		err = decoder.Decode(&preface)
	}
	var extra any
	if err == nil && decoder.Decode(&extra) != io.EOF {
		err = errors.New("invalid routing preface")
	}
	epoch, known := h.Workers[preface.WorkerID]
	if err != nil || preface.Version != Version || !known || epoch == "" || epoch != preface.WorkerEpoch {
		http.Error(w, "invalid exchange route", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Trailer", ExchangeStatusTrailer)
	w.WriteHeader(http.StatusOK)
	if rc.Flush() != nil {
		return
	}
	codec := FrameCodec{MaxBytes: h.MaxMessageBytes + h.MaxArtifactBytes}
	var localShutdown atomic.Bool
	copyFrames := func(dst io.Writer, src io.Reader, flush bool) exchangeFrameResult {
		progress := &exchangeProgressReader{Reader: src}
		for {
			progress.bytes = 0
			frame, err := codec.Read(progress)
			if err != nil {
				// EOF after any frame bytes is truncation, including an empty payload read.
				if err == io.EOF && progress.bytes > 0 {
					err = io.ErrUnexpectedEOF
				}
				var timeout net.Error
				induced := progress.bytes == 0 && localShutdown.Load() && time.Now().Before(deadline) && errors.As(err, &timeout) && timeout.Timeout()
				return exchangeFrameResult{err: err, stage: "read", bytes: progress.bytes, local: induced}
			}
			header, suffix, found := bytes.Cut(frame, []byte{'\n'})
			if !found || int64(len(header)) > h.MaxMessageBytes || int64(len(suffix)) > h.MaxArtifactBytes {
				return exchangeFrameResult{err: errors.New("exchange frame limits"), stage: "limits"}
			}
			if flush && reserveExchangeBytes(&remaining, int64(len(frame))+4) != int64(len(frame))+4 {
				return exchangeFrameResult{err: errors.New("exchange byte limit"), stage: "budget"}
			}
			if err = codec.Write(dst, frame); err != nil {
				return exchangeFrameResult{err: err, stage: "write"}
			}
			if flush {
				if err = rc.Flush(); err != nil {
					return exchangeFrameResult{err: err, stage: "flush"}
				}
			}
		}
	}
	inputDone, outputDone := make(chan exchangeFrameResult, 1), make(chan exchangeFrameResult, 1)
	go func() {
		result := copyFrames(bridge, body, false)
		if result.failed() && !(result.stage == "write" && errors.Is(result.err, io.ErrClosedPipe)) {
			abort()
		}
		inputDone <- result
	}()
	go func() {
		result := copyFrames(w, bridge, true)
		if result.failed() {
			abort()
		}
		outputDone <- result
	}()
	var dialed atomic.Bool
	err = h.Callback(ctx, preface, func(context.Context) (net.Conn, error) {
		if !dialed.CompareAndSwap(false, true) {
			return nil, errors.New("exchange stream already dialed")
		}
		return local, nil
	})
	event("callbackDone")
	_ = local.Close()
	event("outputJoin")
	outputResult := <-outputDone
	localShutdown.Store(true)
	_ = rc.SetReadDeadline(time.Now())
	event("inputJoin")
	inputResult := <-inputDone
	_ = bridge.Close()
	event("bodyClose")
	_ = r.Body.Close()
	event("watcherJoin")
	joinObservers()
	event("beforeF")
	status := "ok"
	inputFailed := inputResult.failed() && !(inputResult.stage == "write" && errors.Is(inputResult.err, io.ErrClosedPipe))
	if err != nil || inputFailed || outputResult.failed() || !time.Now().Before(deadline) {
		status = "error"
	}
	// This is F: sample the actual independent owners after every owned join.
	// Stopping a watcher must not discard a cancellation that is already effective.
	for _, owner := range owners {
		if owner.Err() != nil {
			status = "error"
		}
	}
	event("afterF")
	w.Header().Set(ExchangeStatusTrailer, status)
}

// Source progress and stage survive cancellation. Only a deadline induced by
// local upload shutdown is ignored; actual EOF/truncation and limits never are.
type exchangeFrameResult struct {
	err   error
	stage string
	bytes int64
	local bool
}

func (r exchangeFrameResult) failed() bool {
	return r.err != nil && !r.local && !(r.err == io.EOF && r.bytes == 0 && r.stage == "read")
}

type exchangeProgressReader struct {
	io.Reader
	bytes int64
}

func (r *exchangeProgressReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += int64(n)
	return n, err
}

// Reserve without holding a lock while waiting for bytes. Pending reads count
// conservatively against the same budget as complete outgoing frames.
func reserveExchangeBytes(remaining *atomic.Int64, wanted int64) int64 {
	for {
		left := remaining.Load()
		n := min(left, wanted)
		if remaining.CompareAndSwap(left, left-n) {
			return n
		}
	}
}

type exchangeBudgetReader struct {
	io.Reader
	remaining *atomic.Int64
}

func (r exchangeBudgetReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	reserved := reserveExchangeBytes(r.remaining, int64(len(p)))
	if reserved == 0 {
		// As with http.MaxBytesReader, one byte distinguishes exact EOF from overflow.
		var probe [1]byte
		n, err := r.Reader.Read(probe[:])
		if n > 0 {
			return 0, errors.New("exchange byte limit")
		}
		return 0, err
	}
	n, err := r.Reader.Read(p[:reserved])
	r.remaining.Add(reserved - int64(n))
	return n, err
}

// ExchangeResponseReader checks completion at EOF. Proxies must preserve the
// declared trailer; an absent/error trailer cannot turn a partial exchange into success.
type ExchangeResponseReader struct{ Response *http.Response }

func (r ExchangeResponseReader) Read(p []byte) (int, error) {
	n, err := r.Response.Body.Read(p)
	if err == io.EOF && r.Response.Trailer.Get(ExchangeStatusTrailer) != "ok" {
		err = errors.New("HTTP exchange failed or completion trailer missing")
	}
	return n, err
}
