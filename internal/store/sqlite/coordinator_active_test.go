package sqlite

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type activeCountingConnector struct{ attentionStopConnector }

func (c activeCountingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.attentionStopConnector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &activeCountingConn{conn.(*attentionStopConn)}, nil
}

type activeCountingConn struct{ *attentionStopConn }

func (c *activeCountingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func TestActiveCoordinatorArtifacts(t *testing.T) {
	ctx := context.Background()
	var previousFull, previousActive, previousQueries int
	for _, history := range []int{4, 500} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			reads := &attentionStopReads{}
			reads.reset()
			store, err := OpenMigratedInstrumented(copyFixtureSchema(t), func(c driver.Connector) driver.Connector {
				return activeCountingConnector{attentionStopConnector{Connector: c, reads: reads}}
			})
			requireAttentionStopOK(t, err)
			t.Cleanup(func() { _ = store.Close() })
			records := CoordinatorRecords{
				WorkflowRuns: []domain.WorkflowRun{{ID: "live", Progress: domain.ProgressSucceeded, Sink: &domain.SinkTask{Progress: domain.ProgressActive}}, {ID: "unresolved", Progress: domain.ProgressSucceeded}},
				Artifacts: []domain.Artifact{{ID: "live", WorkflowRunID: "live"}, {ID: "unknown", WorkflowRunID: "unknown"},
					{ID: "definition"}, {ID: "unresolved", WorkflowRunID: "unresolved"}},
			}
			wantActive := sortedActiveTestArtifacts(records.Artifacts)
			for i := 0; i < history; i++ {
				id := fmt.Sprintf("old-%03d", i)
				records.WorkflowRuns = append(records.WorkflowRuns, domain.WorkflowRun{ID: id, Sink: &domain.SinkTask{Progress: domain.ProgressSucceeded}})
				records.Artifacts = append(records.Artifacts, domain.Artifact{ID: id, WorkflowRunID: id})
			}
			requireAttentionStopOK(t, store.SaveCoordinatorRecords(ctx, records))
			reads.reset()
			active, err := store.LoadActiveCoordinatorRecords(ctx)
			requireAttentionStopOK(t, err)
			query := strings.Join(strings.Fields(activeArtifactQuery), " ")
			activeRows, queries := reads.rows[query], reads.queries[query]
			if !reflect.DeepEqual(active.Artifacts, wantActive) {
				t.Fatal("active load lost unresolved, run-less or unknown artifacts")
			}
			reads.reset()
			full, err := store.LoadCoordinatorRecords(ctx)
			requireAttentionStopOK(t, err)
			fullRows := reads.rows["SELECT record FROM coordinator_artifacts ORDER BY id"]
			if activeRows >= fullRows || !reflect.DeepEqual(full.Artifacts, sortedActiveTestArtifacts(records.Artifacts)) {
				t.Fatalf("active rows=%d full rows=%d; full history changed", activeRows, fullRows)
			}
			if previousFull > 0 && (fullRows <= previousFull || activeRows != previousActive || queries != previousQueries) {
				t.Fatalf("history changed active decode/statement work: rows=%d/%d queries=%d/%d", activeRows, previousActive, queries, previousQueries)
			}
			previousFull, previousActive, previousQueries = fullRows, activeRows, queries
			tickCtx, snapshot := WithCoordinatorRecordsSnapshot(ctx)
			defer snapshot.Finish()
			tick, err := store.LoadCoordinatorRecords(tickCtx)
			requireAttentionStopOK(t, err)
			if !reflect.DeepEqual(tick.Artifacts, active.Artifacts) {
				t.Fatal("tick and non-tick active artifacts differ")
			}
			// Future run narrowing must not prevent historical hydration.
			active.WorkflowRuns = nil
			hydrated, err := active.WithRunArtifacts(ctx, "old-000", "old-000")
			requireAttentionStopOK(t, err)
			hydrated, err = hydrated.WithArtifactIDs(ctx, "old-001", "old-000", "old-001")
			requireAttentionStopOK(t, err)
			if len(hydrated.Artifacts) != len(active.Artifacts)+2 || !reflect.DeepEqual(hydrated.Artifacts, sortedActiveTestArtifacts(hydrated.Artifacts)) {
				t.Fatal("targeted hydration lost deduplication or sorting")
			}
			isolated, err := store.LoadCoordinatorRecords(tickCtx)
			requireAttentionStopOK(t, err)
			if !reflect.DeepEqual(isolated.Artifacts, tick.Artifacts) || len(active.Artifacts) != len(tick.Artifacts) {
				t.Fatal("hydration modified caller or cached records")
			}
			plan, err := store.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+activeArtifactQuery)
			requireAttentionStopOK(t, err)
			var details []string
			for plan.Next() {
				var id, parent, unused int
				var detail string
				requireAttentionStopOK(t, plan.Scan(&id, &parent, &unused, &detail))
				details = append(details, detail)
			}
			requireAttentionStopOK(t, plan.Err())
			requireAttentionStopOK(t, plan.Close())
			explain := strings.Join(details, "\n")
			if !strings.Contains(explain, "SEARCH coordinator_artifacts USING INDEX coordinator_artifacts_run") ||
				!strings.Contains(explain, "SCAN coordinator_artifacts USING COVERING INDEX coordinator_artifacts_run") || strings.Contains(explain, "MATERIALIZE") {
				t.Fatalf("artifact payload access is not run-key driven:\n%s", explain)
			}
			external, err := OpenMigrated(store.path)
			requireAttentionStopOK(t, err)
			requireAttentionStopOK(t, external.SaveCoordinatorRecords(ctx, CoordinatorRecords{Artifacts: []domain.Artifact{{ID: "late", WorkflowRunID: "live"}}}))
			requireAttentionStopOK(t, external.Close())
			refreshed, err := store.LoadCoordinatorRecords(tickCtx)
			requireAttentionStopOK(t, err)
			if len(refreshed.Artifacts) <= len(tick.Artifacts) {
				t.Fatal("external artifact write did not invalidate snapshot")
			}
		})
	}
}

func sortedActiveTestArtifacts(artifacts []domain.Artifact) []domain.Artifact {
	result := append([]domain.Artifact(nil), artifacts...)
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
