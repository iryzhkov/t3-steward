package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

func TestHostInventoryManagedProjectReadiness(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "unavailable"}[unavailable], func(t *testing.T) {
			posts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					posts++
					http.Error(w, "unexpected mutation", http.StatusBadRequest)
					return
				}
				if unavailable {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				_ = json.NewEncoder(w).Encode(t3api.ShellSnapshot{Projects: []t3api.ProjectShell{{ID: "existing-id", Title: "existing-title"}}})
			}))
			defer server.Close()
			control := t3control.New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)), false)
			settings := config.Default().BacklogV2
			settings.Projects = map[string]config.V2Project{
				"managed":          {},
				"explicit-id":      {T3Project: "existing-id"},
				"explicit-title":   {T3Project: "existing-title"},
				"missing-explicit": {T3Project: "missing"},
			}
			names := []string{"managed", "explicit-id", "explicit-title", "missing-explicit", "unregistered"}
			wanted := domain.WorkerInventory{AcceptBacklog: true}
			for _, name := range names {
				wanted.Projects = append(wanted.Projects, domain.WorkerProjectInventory{Name: name})
			}
			result, err := observeHostInventory(control, t.TempDir())(context.Background(), settings, wanted)
			if err != nil {
				t.Fatal(err)
			}
			for i, p := range result.Projects {
				want := !unavailable && i < 3
				if p.Available != want {
					t.Errorf("%s available=%v want=%v", p.Name, p.Available, want)
				}
			}
			if posts != 0 {
				t.Fatalf("inventory created projects: %d", posts)
			}
		})
	}
}
