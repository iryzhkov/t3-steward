package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func TestCampaignProgress(t *testing.T) {
	for _, args := range [][]string{{"progress"}, {"progress", "run", "--owner", "thread", "--since", "2026-10-06T12:00:00Z", "--json"}} {
		var out bytes.Buffer
		calls := 0
		c := campaignCLI{stdout: &out, query: func(_ context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
			calls++
			if q.Kind != backlogadmin.QueryWorkflows || q.ProgressMirror == nil {
				t.Fatalf("query: %+v", q)
			}
			if len(args) > 1 && (q.ProgressMirror.Owner != "thread" || len(q.ProgressMirror.RunIDs) != 1 || q.ProgressMirror.Since != time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)) {
				t.Fatalf("filter: %+v", q.ProgressMirror)
			}
			return backlogadmin.Response{ProgressMirror: &backlog.ProgressDocument{SchemaVersion: 1, Runs: []backlog.ProgressRun{}}}, nil
		}}
		if err := c.run(context.Background(), args); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatal(calls)
		}
		if len(args) == 1 && out.String() != "No campaign runs match.\n" {
			t.Fatal(out.String())
		}
		if len(args) > 1 && !strings.Contains(out.String(), "\"runs\": []") {
			t.Fatal(out.String())
		}
	}
}

func TestCampaignProgressOldCoordinatorProtocolError(t *testing.T) {
	expected := &backlogadmin.TransportError{Class: backlogadmin.ClassProtocol, Operation: "query", Err: errors.New("decode local admin frame: json: unknown field \"progressMirror\"")}
	c := campaignCLI{stdout: io.Discard, query: func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
		return backlogadmin.Response{}, expected
	}}
	err := c.runProgress(context.Background(), nil)
	if !errors.Is(err, expected) || !strings.Contains(err.Error(), "upgrade") {
		t.Fatalf("old coordinator needs upgrade guidance while preserving transport error: %v", err)
	}
}

func TestCampaignProgressErrors(t *testing.T) {
	for _, args := range [][]string{{"--since", "yesterday"}, {"--since"}, {"--owner"}, {"--owner", ""}, {"--json", "--json"}, {"--owner", "a", "--owner", "b"}, {"--limit", "1"}, {"run/task"}, {""}, {"--since", "-1"}} {
		calls := 0
		c := campaignCLI{stdout: io.Discard, query: func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
			calls++
			return backlogadmin.Response{}, nil
		}}
		if err := c.runProgress(context.Background(), args); err == nil || calls != 0 {
			t.Fatalf("%q: err=%v calls=%d", args, err, calls)
		}
	}
	c := campaignCLI{stdout: io.Discard}
	if err := c.runProgress(context.Background(), nil); err == nil {
		t.Fatal("missing transport accepted")
	}
	expected := errors.New("query failed")
	c.query = func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
		return backlogadmin.Response{}, expected
	}
	if err := c.runProgress(context.Background(), nil); !errors.Is(err, expected) {
		t.Fatal(err)
	}
	c.query = func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
		return backlogadmin.Response{}, nil
	}
	if err := c.runProgress(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "upgrade") {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if ok, err := admitCampaignHelp(&out, []string{"progress", "--help", "full"}); err != nil || !ok || !strings.Contains(out.String(), "--owner") {
		t.Fatalf("help: %v %v %s", ok, err, out.String())
	}
}
