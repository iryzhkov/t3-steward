package backlog

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"path/filepath"
	"time"
)

// A workflow record is written atomically with its tasks only after its immutable
// files are published. Completion can be retried without reingesting or consulting
// the current policy; the first durable receipt remains authoritative.
func (s *SubmissionService) completeDurableRoleSubmission(ctx context.Context, record *domain.SubmissionRecord, finalDir, contentDigest string) (bool, error) {
	reader, ok := s.Store.(interface {
		LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	})
	if !ok {
		return false, nil
	}
	records, err := reader.LoadCoordinatorRecords(ctx)
	if err != nil {
		return false, fmt.Errorf("inspect pending role submission: %w", err)
	}
	workflowExists := false
	for _, workflow := range records.Workflows {
		if workflow.ID == record.WorkflowID {
			workflowExists = true
			break
		}
	}
	if !workflowExists {
		return false, nil
	}
	hasRole := false
	for _, task := range records.Tasks {
		if task.WorkflowID == record.WorkflowID && task.Role != "" {
			hasRole = true
		}
	}
	if !hasRole {
		return false, nil
	}
	if !record.RegisterOnly {
		runExists := false
		for _, run := range records.WorkflowRuns {
			if run.ID == record.RunID && run.WorkflowID == record.WorkflowID {
				runExists = true
				break
			}
		}
		if !runExists {
			return false, fmt.Errorf("pending role submission %s has workflow records but no run; repair coordinator storage", record.Key)
		}
	}
	publishedDigest, err := directorySubmissionDigest(ctx, filepath.Join(finalDir, "files"), s.MaxBytes, s.MaxFiles)
	if err != nil {
		return false, fmt.Errorf("verify durable pending role submission files: %w", err)
	}
	if publishedDigest != contentDigest {
		return false, fmt.Errorf("durable pending role submission files do not match reserved content")
	}
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now().UTC()
	}
	completed, _, err := s.Store.CompleteSubmission(ctx, record.Key, record.Digest, now)
	if err != nil {
		return false, err
	}
	*record = completed
	return true, nil
}
