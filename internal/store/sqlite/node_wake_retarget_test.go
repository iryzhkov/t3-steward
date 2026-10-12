package sqlite

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func retargetFixture(t *testing.T, states ...string) (*Store, domain.NodeWakeRetargetRequest, time.Time) {
	t.Helper()
	s, _, now := sinkStoreFixture(t)
	if err := s.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Workflows: []domain.Workflow{{ID: "w", Project: "project"}}}); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, state := range states {
		r := domain.NodeWaitRequest{ID: state, ThreadID: "old", Target: domain.NodeRef{RunID: "r", TaskID: domain.SinkTaskName}, Timeout: time.Hour}
		w := domain.NodeWait{Registration: r, Request: r, Actor: "operator", Host: "host", Deadline: now.Add(time.Hour), SettledAt: &now, Delivery: state, DeliveryID: "wake:" + state}
		if err := saveNodeWaitTx(context.Background(), tx, w); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return s, domain.NodeWakeRetargetRequest{RunIDs: []string{"r", "r"}, ThreadID: "new", Host: "successor", Actor: "operator", Reason: "handoff", Key: "key"}, now
}

func TestRetargetSelectorsReplayAndRegistration(t *testing.T) {
	for _, selector := range []string{"runs", "project", "thread"} {
		t.Run(selector, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s, req, now := retargetFixture(t, "pending", "held", "offline", "busy", "delivered", "rejected", "cancelled")
			if selector == "project" {
				req.RunIDs, req.Project = nil, "project"
			}
			if selector == "thread" {
				req.RunIDs, req.SourceThreadID, req.SourceHost = nil, "old", "host"
			}
			before, _ := s.LoadAuditEvents(ctx, "")
			result, err := s.RetargetNodeWakes(ctx, req, now)
			if err != nil || len(result.Moved) != 4 || len(result.Skipped) != 3 {
				t.Fatal(result, err)
			}
			after, _ := s.LoadAuditEvents(ctx, "")
			if len(after)-len(before) != 2 || after[len(before)].WorkflowRunID != "r" || after[len(before)].Actor != req.Actor || after[len(before)].Reason != req.Reason {
				t.Fatal(after)
			}
			path := s.path
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = openMigratedFixture(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			replay, err := s.RetargetNodeWakes(ctx, req, now.Add(time.Hour))
			if err != nil || !reflect.DeepEqual(replay, result) {
				t.Fatal(replay, err)
			}
			audits, _ := s.LoadAuditEvents(ctx, "")
			if len(audits) != len(after) {
				t.Fatal("replay appended audit")
			}
			waits, _ := s.ListNodeWaits(ctx)
			for _, w := range waits {
				if w.RecipientGeneration == 0 {
					continue
				}
				var indexed string
				if err := s.db.QueryRow("SELECT thread_id FROM coordinator_node_waits WHERE id=?", w.Request.ID).Scan(&indexed); err != nil || indexed != "new" {
					t.Fatal(indexed, err)
				}
				original, err := s.RegisterNodeWait(ctx, w.Registration, w.Actor, "host", now)
				if err != nil || !reflect.DeepEqual(original, w) || w.Host != "successor" || w.Deadline != now.Add(time.Hour) {
					t.Fatal(original, err)
				}
				if _, err := s.RegisterNodeWait(ctx, w.Request, w.Actor, w.Host, now); err == nil {
					t.Fatal("changed registration accepted")
				}
			}
			req.Reason = "changed"
			if _, err := s.RetargetNodeWakes(ctx, req, now); err == nil {
				t.Fatal("key conflict accepted")
			}
			req.Key, req.RunIDs, req.Project, req.SourceThreadID, req.SourceHost = "noop", []string{"r"}, "", "", ""
			noop, err := s.RetargetNodeWakes(ctx, req, now)
			if err != nil || len(noop.Unchanged) != 4 || len(noop.Moved) != 0 {
				t.Fatal(noop, err)
			}
		})
	}
}

func TestRetargetClaimOrderingAndRollback(t *testing.T) {
	for _, mode := range []string{"individual-first", "group-first", "retarget-first", "frozen", "uncertain", "audit-failure", "wrong-host"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s, req, now := retargetFixture(t, "pending", "held")
			claim := func(group bool) bool {
				t.Helper()
				var ok bool
				var err error
				if group {
					ok, err = s.ClaimNodeWakeGroup(ctx, "pending", "pending", "group", "payload", []string{"held", "pending"}, now)
				} else {
					ok, err = s.TransitionNodeWake(ctx, "pending", "pending", "sending", now)
				}
				if err != nil {
					t.Fatal(err)
				}
				return ok
			}
			switch mode {
			case "individual-first":
				if !claim(false) {
					t.Fatal("claim failed")
				}
			case "group-first":
				if !claim(true) {
					t.Fatal("claim failed")
				}
			case "frozen":
				_, _ = s.db.Exec("UPDATE coordinator_node_waits SET record=json_set(record,'$.deliveryPayload','frozen') WHERE id='pending'")
			case "uncertain":
				_, _ = s.db.Exec("UPDATE coordinator_node_waits SET record=json_set(record,'$.delivery','recovery-required') WHERE id='pending'")
			case "audit-failure":
				_, err := s.db.Exec("CREATE TRIGGER fail_retarget_audit BEFORE INSERT ON coordinator_audit_events WHEN NEW.kind='node-wake-retarget-receipt' BEGIN SELECT RAISE(ABORT,'test'); END")
				if err != nil {
					t.Fatal(err)
				}
			case "wrong-host":
				req.RunIDs, req.SourceThreadID, req.SourceHost = nil, "old", "other-host"
			}
			before, _ := s.ListNodeWaits(ctx)
			audits, _ := s.LoadAuditEvents(ctx, "")
			result, err := s.RetargetNodeWakes(ctx, req, now)
			if mode == "retarget-first" {
				if err != nil || len(result.Moved) != 2 || claim(false) || claim(true) {
					t.Fatal(result, err)
				}
				ok, err := s.ClaimNodeWakeGroupGenerations(ctx, "pending", "pending", "group", "payload", []string{"held", "pending"}, now, map[string]uint64{"held": 1})
				if err != nil || ok {
					t.Fatal("partial generation claim", ok, err)
				}
				waits, _ := s.ListNodeWaits(ctx)
				for _, w := range waits {
					if w.Delivery == "sending" {
						t.Fatal("partial claim committed")
					}
				}
				ok, err = s.TransitionNodeWakeGeneration(ctx, "pending", "pending", "sending", now, 1)
				if err != nil || !ok {
					t.Fatal("current generation refused", ok, err)
				}
			} else {
				if mode == "wrong-host" {
					if err != nil || len(result.Moved) != 0 {
						t.Fatal(result, err)
					}
				} else if err == nil {
					t.Fatal("unsafe retarget accepted")
				}
				waits, _ := s.ListNodeWaits(ctx)
				after, _ := s.LoadAuditEvents(ctx, "")
				if !reflect.DeepEqual(waits, before) || mode != "wrong-host" && len(after) != len(audits) {
					t.Fatal("partial mutation or audit")
				}
			}
		})
	}
}
