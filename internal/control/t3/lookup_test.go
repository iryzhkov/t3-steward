package t3

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/testtiming"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// lookupServer answers the shell snapshot and the full index with the given
// thread lists.
func lookupServer(t *testing.T, shell, index []map[string]any) *Control {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/orchestration/shell":
			_ = json.NewEncoder(w).Encode(map[string]any{"threads": shell})
		case "/api/orchestration/snapshot":
			_ = json.NewEncoder(w).Encode(map[string]any{"threads": index})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return New(t3api.New(server.URL, t3api.StaticToken("token"), testtiming.Bound(time.Second)), nil, false)
}

// LookupThread says which of four things T3's answer means for a thread, and
// treats an answer that holds no thread at all as no answer: a T3 still
// loading its read model answers with an empty list, and that must never read
// as "this thread is gone".
func TestLookupThreadTellsDeletedArchivedAbsentAndEmptyApart(t *testing.T) {
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	other := map[string]any{"id": "other"}
	for _, tc := range []struct {
		name         string
		shell, index []map[string]any
		want         domain.ThreadPresence
		wantErr      bool
	}{
		{name: "live", shell: []map[string]any{other, {"id": "thread"}}, index: []map[string]any{other}, want: domain.ThreadLive},
		{name: "deleted in the shell", shell: []map[string]any{other, {"id": "thread", "deletedAt": stamp}}, want: domain.ThreadDeleted},
		{name: "archived, only in the index", shell: []map[string]any{other},
			index: []map[string]any{other, {"id": "thread", "archivedAt": stamp}}, want: domain.ThreadArchived},
		{name: "deleted in the index", shell: []map[string]any{other},
			index: []map[string]any{other, {"id": "thread", "deletedAt": stamp}}, want: domain.ThreadDeleted},
		{name: "absent from both", shell: []map[string]any{other}, index: []map[string]any{other}, want: domain.ThreadAbsent},
		{name: "empty shell", shell: []map[string]any{}, index: []map[string]any{other}, wantErr: true},
		{name: "empty index", shell: []map[string]any{other}, index: []map[string]any{}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, presence, err := lookupServer(t, tc.shell, tc.index).LookupThread(context.Background(), "thread")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			if !tc.wantErr && presence != tc.want {
				t.Fatalf("presence = %q, want %q", presence, tc.want)
			}
		})
	}
}
