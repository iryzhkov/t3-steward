package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/coordinatormetrics"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"time"
)

// LoadMetricsSnapshot performs only SELECTs in one read transaction. It does
// not load workflow/task payloads, initialize schema, or reconcile admission.
// HTTP requests never call it; the opt-in producer refreshes a cached snapshot.
func (s *QueryOnlyStore) LoadMetricsSnapshot(ctx context.Context) (coordinatormetrics.Snapshot, error) {
	out := coordinatormetrics.Snapshot{}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	count := func(query string) (map[string]int64, error) {
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		result := map[string]int64{}
		for rows.Next() {
			var key string
			var n int64
			if err := rows.Scan(&key, &n); err != nil {
				return nil, err
			}
			result[key] = n
		}
		return result, rows.Err()
	}
	if out.Tasks, err = count(`SELECT COALESCE(json_extract(a.record,'$.progress'),'unknown'),count(*) FROM coordinator_attempts a
 WHERE a.task_id<>'' AND COALESCE(json_extract(a.record,'$.supervisionActivationId'),'')=''
 AND a.number=(SELECT max(b.number) FROM coordinator_attempts b WHERE b.workflow_run_id=a.workflow_run_id AND b.task_id=a.task_id AND COALESCE(json_extract(b.record,'$.supervisionActivationId'),'')='')
 GROUP BY 1`); err != nil {
		return out, err
	}
	if out.Assignments, err = count(`SELECT assignment_state,count(*) FROM coordinator_assignments GROUP BY 1`); err != nil {
		return out, err
	}
	if out.Admissions, err = count(`SELECT COALESCE(json_extract(record,'$.admission'),'unknown'),count(*) FROM coordinator_quota_admissions GROUP BY 1`); err != nil {
		return out, err
	}
	// Evidence coverage is explicitly bounded to the latest rows, not fleet
	// consumption. Only presence metadata is read; no raw token values.
	usageRows, err := tx.QueryContext(ctx, `SELECT worker_id,event_id,provider,thread_id,kind,field_presence,observed_at
 FROM usage_samples ORDER BY observed_at DESC LIMIT 10001`)
	if err != nil {
		return out, err
	}
	type evidenceKey struct{ event, provider, thread string }
	type evidenceRow struct {
		worker, kind string
		key          evidenceKey
		presence     int64
		at           string
	}
	var evidence []evidenceRow
	for usageRows.Next() {
		var r evidenceRow
		if err = usageRows.Scan(&r.worker, &r.key.event, &r.key.provider, &r.key.thread, &r.kind, &r.presence, &r.at); err != nil {
			usageRows.Close()
			return out, err
		}
		evidence = append(evidence, r)
	}
	err = usageRows.Err()
	usageRows.Close()
	if err != nil {
		return out, err
	}
	if len(evidence) > 10000 {
		out.UsageEvidenceTruncated = true
		evidence = evidence[:10000]
	}
	forwarded := map[evidenceKey]bool{}
	for _, r := range evidence {
		if r.worker != "" {
			forwarded[r.key] = true
		}
	}
	out.UsageEvidence = map[string]int64{}
	out.UsageFields = map[string]int64{}
	for _, r := range evidence {
		if r.worker == "" && forwarded[r.key] {
			continue
		}
		out.UsageEvidence[r.kind]++
		if r.kind == "call" || r.kind == "turn" {
			coverage := "partial"
			if r.presence == 15 {
				coverage = "complete"
			} else if r.presence == 0 {
				coverage = "absent"
			}
			out.UsageFields[coverage]++
		}
		at, parseErr := time.Parse(time.RFC3339Nano, r.at)
		if parseErr == nil && (out.UsageEvidenceOldest.IsZero() || at.Before(out.UsageEvidenceOldest)) {
			out.UsageEvidenceOldest = at
		}
	}
	// Decode only the observation projection, never worker inventory, paths or
	// journal excerpts. The bound rejects the entire refresh, preserving cache.
	rows, err := tx.QueryContext(ctx, `SELECT json_object('workerId',worker_id,'connected',json(CASE WHEN connected=1 THEN 'true' ELSE 'false' END),'observedAt',json_extract(record,'$.observedAt'),'validUntil',valid_until,'quotaObservations',json(COALESCE(json_extract(record,'$.quotaObservations'),'[]'))) FROM coordinator_worker_snapshots ORDER BY worker_id LIMIT 10001`)
	if err != nil {
		return out, err
	}
	quotaCount := 0
	for rows.Next() {
		var raw string
		var w domain.WorkerSnapshot
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal([]byte(raw), &w)
		}
		if err != nil {
			rows.Close()
			return out, err
		}
		quotaCount += len(w.QuotaObservations)
		if quotaCount > 10000 {
			rows.Close()
			return out, fmt.Errorf("worker quota metric snapshot bound exceeded")
		}
		out.Workers = append(out.Workers, w)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	if len(out.Workers) > 10000 {
		return out, fmt.Errorf("worker metric snapshot bound exceeded")
	}
	rows, err = tx.QueryContext(ctx, `SELECT state FROM buckets ORDER BY key LIMIT 10001`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var raw string
		var v domain.BucketState
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal([]byte(raw), &v)
		}
		if err != nil {
			rows.Close()
			return out, err
		}
		out.Buckets = append(out.Buckets, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	if len(out.Buckets) > 10000 {
		return out, fmt.Errorf("bucket metric snapshot bound exceeded")
	}
	return out, tx.Commit()
}
