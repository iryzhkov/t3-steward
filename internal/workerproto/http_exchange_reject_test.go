package workerproto

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPExchangeEarlyRejection(t *testing.T) {
	for _, method := range []string{"POST", "PUT"} {
		t.Run(method, func(t *testing.T) {
			done := make(chan struct{})
			h := HTTPExchange{Enabled: true} // No callback yet; reject without waiting for upload.
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(done); h.ServeHTTP(w, r) }))
			defer s.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			body, writer := io.Pipe()
			defer writer.Close()
			req, _ := http.NewRequestWithContext(ctx, method, s.URL+"/worker/exchange", body)
			result := make(chan error, 1)
			go func() {
				res, err := s.Client().Do(req)
				if err == nil {
					res.Body.Close()
				}
				result <- err
			}()
			_, _ = writer.Write([]byte{0}) // Flush client headers without finishing the body.
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("rejection waited for upload EOF")
			}
			writer.Close()
			<-result
		})
	}
}
