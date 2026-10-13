package main

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

type backgroundArtifactConnector struct {
	driver.Connector
	count *int
}

func (c backgroundArtifactConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &backgroundArtifactConn{tracingConn: &tracingConn{Conn: conn, metrics: &sqliteTraceMetrics{}}, count: c.count}, nil
}

type backgroundArtifactConn struct {
	*tracingConn
	count *int
}

func (c *backgroundArtifactConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.tracingConn.QueryContext(ctx, query, args)
	if err == nil && strings.HasPrefix(query, "SELECT record FROM coordinator_artifacts") {
		return &backgroundArtifactRows{Rows: rows, count: c.count}, nil
	}
	return rows, err
}

type backgroundArtifactRows struct {
	driver.Rows
	count *int
}

func (r *backgroundArtifactRows) Next(values []driver.Value) error {
	err := r.Rows.Next(values)
	if err == nil {
		*r.count++
	}
	return err
}

func TestCoordinatorBackgroundArtifactsBoundedByLiveRuns(t *testing.T) {
	for _, reader := range []string{"recovery", "schedule"} {
		t.Run(reader, func(t *testing.T) {
			var small int
			for _, history := range []int{4, 500} {
				path := t.TempDir() + "/state.db"
				if err := sqlitetest.Seed(path); err != nil {
					t.Fatal(err)
				}
				count := 0
				store, err := sqlite.OpenMigratedInstrumented(path, func(c driver.Connector) driver.Connector {
					return backgroundArtifactConnector{Connector: c, count: &count}
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := store.Close(); err != nil {
						t.Error(err)
					}
				})
				ctx := context.Background()
				live := domain.WorkflowRun{ID: "live", Progress: domain.ProgressActive, Supervision: &domain.SupervisionRecord{Config: recoveryTestSupervisionConfig()}}
				records := sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{live}, Artifacts: []domain.Artifact{{ID: "live-artifact", WorkflowRunID: live.ID}}}
				for i := 0; i < history; i++ {
					id := fmt.Sprintf("old-%03d", i)
					records.WorkflowRuns = append(records.WorkflowRuns, domain.WorkflowRun{ID: id, Sink: &domain.SinkTask{Progress: domain.ProgressSucceeded}})
					records.Artifacts = append(records.Artifacts, domain.Artifact{ID: id, WorkflowRunID: id})
				}
				if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
					t.Fatal(err)
				}
				count = 0
				if reader == "recovery" {
					c := coordinatorSupervision{store: backlog.CoordinatorSupervisionStore{Store: store}}
					if err := c.observeRecoveryFailures(ctx, live, time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)); err != nil {
						t.Fatal(err)
					}
				} else {
					_, err := (coordinatorRoleScheduleStore{Store: store}).ResolveScheduleTrigger(ctx, domain.ScheduleTriggerRequest{ScheduleID: "missing"})
					if err == nil || !strings.Contains(err.Error(), "current template missing") {
						t.Fatalf("unexpected resolution: %v", err)
					}
				}
				t.Logf("history=%d background artifact rows=%d", history, count)
				if history == 4 {
					small = count
				} else if count != small {
					t.Errorf("fixed active set artifact decodes changed with history: %d -> %d", small, count)
				}
			}
		})
	}
}
