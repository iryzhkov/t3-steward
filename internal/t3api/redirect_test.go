package t3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

// R-21: an authenticated request never follows a redirect, wherever it points,
// so the T3 credential cannot be delivered to another service. This covers the
// audit's TestAuditRedirectCredentialBoundary (a cross-port redirect).
func TestAuthenticatedRequestsDoNotFollowRedirects(t *testing.T) {
	var sinkRequests, sinkAuthorized atomic.Int32
	record := func(w http.ResponseWriter, r *http.Request) {
		sinkRequests.Add(1)
		if r.Header.Get("Authorization") != "" {
			sinkAuthorized.Add(1)
		}
		_, _ = w.Write([]byte("{}"))
	}
	sink := httptest.NewServer(http.HandlerFunc(record))
	defer sink.Close()
	tlsSink := httptest.NewTLSServer(http.HandlerFunc(record))
	defer tlsSink.Close()
	sinkURL, err := url.Parse(sink.URL)
	if err != nil {
		t.Fatal(err)
	}

	var location string
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/same-origin-target" {
			record(w, r)
			return
		}
		http.Redirect(w, r, location, http.StatusFound)
	}))
	defer origin.Close()

	for _, test := range []struct {
		name     string
		location string
	}{
		{"same origin", origin.URL + "/same-origin-target"},
		{"different port", sink.URL + "/api"},
		{"different host", "http://localhost:" + sinkURL.Port() + "/api"},
		{"different scheme", tlsSink.URL + "/api"},
	} {
		t.Run(test.name, func(t *testing.T) {
			sinkRequests.Store(0)
			sinkAuthorized.Store(0)
			location = test.location
			client := New(origin.URL, StaticToken("redirect marker"), testtiming.Bound(time.Second))
			_, err := client.ShellSnapshot(context.Background())
			if sinkAuthorized.Load() != 0 {
				t.Fatalf("redirect to %s delivered the authorization header", test.location)
			}
			if sinkRequests.Load() != 0 {
				t.Fatalf("redirect to %s was followed", test.location)
			}
			if err == nil || !strings.Contains(err.Error(), "redirect") {
				t.Fatalf("error = %v, want a refused redirect", err)
			}
			if strings.Contains(err.Error(), "redirect marker") {
				t.Fatalf("error leaks the credential: %v", err)
			}
		})
	}
}

// Discovery is unauthenticated and unchanged: the environment descriptor
// still follows a redirect, and nothing it follows carries a credential.
func TestUnauthenticatedDiscoveryStillFollowsRedirects(t *testing.T) {
	var authorized atomic.Bool
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			authorized.Store(true)
		}
		if r.URL.Path == "/.well-known/t3/environment" {
			http.Redirect(w, r, "/moved-descriptor", http.StatusFound)
			return
		}
		_, _ = w.Write([]byte(`{"environmentId":"env-1"}`))
	}))
	defer origin.Close()
	client := New(origin.URL, StaticToken("redirect marker"), testtiming.Bound(time.Second))
	if _, err := client.Descriptor(context.Background()); err != nil {
		t.Fatalf("descriptor through a redirect: %v", err)
	}
	if authorized.Load() {
		t.Fatal("the unauthenticated descriptor request carried authorization")
	}
}
