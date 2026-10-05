package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"reflect"
	"testing"
)

func TestIndependentRepairConfiguredMembershipReplay(t *testing.T) {
	for _, phase := range []string{"initial", "stored"} {
		for _, kind := range []string{"healthy", "missing", "unrelated-duplicate", "empty-entry", "project", "activation"} {
			t.Run(phase+"/"+kind, func(t *testing.T) {
				ctx := context.Background()
				_, db, entry, records, _ := independentDeclaredProduction(t)
				task, run, parent := records.Tasks[0], records.WorkflowRuns[0], records.Attempts[0]
				workflow := records.Workflows[0]
				parent.Progress = domain.ProgressActive
				parent.Control = domain.ControlRunning
				parent.ThreadID = "repair-thread"
				parent.AssignmentID = "repair-assignment"
				parent.Revision = 1
				as := domain.Assignment{ID: parent.AssignmentID, DispatchToken: "repair-token", AttemptID: parent.ID, Epoch: 1, State: domain.AssignmentClaimed, ThreadID: parent.ThreadID, Project: workflow.Project, TaskDigest: domain.TaskDigest(task), TaskRevision: task.DefinitionRevision, GraphRevision: run.GraphRevision, Route: domain.ProviderRoute{ProviderInstanceID: "t3-primary", Model: "opus"}}
				if err := db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{parent}, Assignments: []domain.Assignment{as}}); err != nil {
					t.Fatal(err)
				}
				req := backlog.DeclaredAdmissionRequest{RunID: run.ID, TaskID: task.ID, AttemptID: parent.ID}
				original, err := entry.service.ResolveDeclared(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				if phase == "stored" {
					got, e := entry.FreezeDeclared(ctx, req)
					if e != nil || !reflect.DeepEqual(got, original) {
						t.Fatal("initial valid freeze", e)
					}
					// The stored replay must reuse its original provenance without catalog admission.
					entry.service.Catalog = nil
				}
				var before sqlite.CoordinatorRecords
				var auditBefore []domain.AuditEvent
				fired := false
				entry.service.Store = independentDeclaredProductionInterleave{db, func() {
					fired = true
					switch kind {
					case "missing":
						workflow.TaskIDs = nil
					case "unrelated-duplicate":
						workflow.TaskIDs = []string{task.ID, "other", "other"}
					case "empty-entry":
						workflow.TaskIDs = []string{task.ID, ""}
					case "project":
						as.Project = "foreign"
					case "activation":
						as.ActivationID = "foreign"
					}
					if e := db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Workflows: []domain.Workflow{workflow}, Assignments: []domain.Assignment{as}}); e != nil {
						t.Fatal(e)
					}
					var e error
					before, e = db.LoadCoordinatorRecords(ctx)
					if e != nil {
						t.Fatal(e)
					}
					auditBefore, e = db.LoadAuditEvents(ctx, "")
					if e != nil {
						t.Fatal(e)
					}
				}}
				got, err := entry.FreezeDeclared(ctx, req)
				if !fired {
					t.Fatal("did not reach owning writer")
				}
				if kind == "healthy" {
					if err != nil || !reflect.DeepEqual(got, original) {
						t.Fatal("healthy original replay", err)
					}
					return
				}
				if err == nil {
					t.Fatal("configured writer admitted corrupt current custody")
				}
				t.Logf("configured %s %s refusal: %v", phase, kind, err)
				after, e := db.LoadCoordinatorRecords(ctx)
				if e != nil {
					t.Fatal(e)
				}
				auditAfter, e := db.LoadAuditEvents(ctx, "")
				if e != nil {
					t.Fatal(e)
				}
				if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(auditBefore, auditAfter) {
					t.Fatal("refusal changed coordinator records/native audit")
				}
				stored, found, e := db.GetFrozenReviewAuthority(ctx, run.ID, task.ID)
				if e != nil || found != (phase == "stored") {
					t.Fatal("authority presence changed", found, e)
				}
				if found && !reflect.DeepEqual(stored, original.Authority) {
					t.Fatal("original authority changed")
				}
				entry.service.Store = db
				if _, e = entry.FreezeDeclared(ctx, req); e == nil {
					t.Fatal("subsequent identity-only freeze admitted same corruption")
				}
				// Restore valid custody; exact original snapshot must succeed, even with replay catalog absent.
				workflow.TaskIDs = []string{task.ID}
				as.Project = workflow.Project
				as.ActivationID = ""
				if e = db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Workflows: []domain.Workflow{workflow}, Assignments: []domain.Assignment{as}}); e != nil {
					t.Fatal(e)
				}
				restored, e := entry.FreezeDeclared(ctx, req)
				if e != nil || !reflect.DeepEqual(original, restored) {
					t.Fatal("restored custody lost original issuance", e)
				}
			})
		}
	}
}
