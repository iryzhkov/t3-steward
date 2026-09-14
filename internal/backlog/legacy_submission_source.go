package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type SingleTaskSubmitter interface {
	SubmitSingleTask(context.Context, SingleTaskSubmission) (SubmissionResult, error)
}

type LegacySubmissionReport struct {
	Accepted []SubmissionResult
	Skipped  []string
	// Quarantined names the submissions skipped because their exact content was
	// already reported as impossible. They are silent on purpose.
	Quarantined []string
	Errors      []error
}

// LegacySubmissionQuarantine durably records intake content that can never be
// accepted. The drop directory is read-only to the coordinator, so the marker
// cannot live beside the file it describes.
type LegacySubmissionQuarantine interface {
	// QuarantineSubmission records the conflict and reports whether this exact
	// content was already recorded.
	QuarantineSubmission(ctx context.Context, key, digest, reason string, at time.Time) (domain.SubmissionRecord, bool, error)
	LoadSubmissionQuarantine(ctx context.Context, key string) (domain.SubmissionRecord, bool, error)
	ReleaseSubmissionQuarantine(ctx context.Context, key string) error
}

// ErrPermanentIntake marks an intake failure that the same content will always
// produce. Retrying it cannot succeed; only different content can.
var ErrPermanentIntake = errors.New("legacy submission content can never be accepted")

// LegacySubmissionSource adapts the unchanged owner-controlled Markdown drop
// directory into durable immutable v2 submissions.
type LegacySubmissionSource struct {
	Dir            string
	Submitter      SingleTaskSubmitter
	ProjectAliases map[string]string
	MaxBytes       int64
	MaxFiles       int
	AllowedUID     uint32
	// Quarantine makes a permanent conflict a durable fact reported once. The
	// source re-reads the drop directory every cycle and never drains it, so
	// without a quarantine the same impossible file is reported forever.
	Quarantine LegacySubmissionQuarantine
	Now        func() time.Time
}

func (s LegacySubmissionSource) Tick(ctx context.Context) LegacySubmissionReport {
	var report LegacySubmissionReport
	if strings.TrimSpace(s.Dir) != s.Dir || s.Dir == "" {
		report.Errors = append(report.Errors, fmt.Errorf("legacy submission directory is required"))
		return report
	}
	if s.Submitter == nil {
		report.Errors = append(report.Errors, fmt.Errorf("legacy submission submitter is required"))
		return report
	}
	if err := authenticateLegacySubmissionDir(s.Dir, s.AllowedUID); err != nil {
		report.Errors = append(report.Errors, err)
		return report
	}
	files, loadErrors := loadLegacyTasksBounded(s.Dir, s.MaxBytes, s.MaxFiles)
	report.Errors = append(report.Errors, loadErrors...)
	sort.Slice(files, func(i, j int) bool {
		return files[i].task.ID < files[j].task.ID
	})
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			report.Errors = append(report.Errors, err)
			return report
		}
		task := file.task
		if !task.IsEnabled() {
			report.Skipped = append(report.Skipped, task.ID)
			continue
		}
		key := LegacySubmissionKey(task.ID)
		quarantined, err := s.quarantinedDigest(ctx, key)
		if err != nil {
			report.Errors = append(report.Errors, err)
			continue
		}
		if quarantined == file.digest {
			report.Quarantined = append(report.Quarantined, task.ID)
			continue
		}
		if quarantined != "" {
			// The content changed, so the reason it was quarantined may no
			// longer hold. Try it again and report what it does now.
			if err := s.Quarantine.ReleaseSubmissionQuarantine(ctx, key); err != nil {
				report.Errors = append(report.Errors, err)
				continue
			}
		}
		project, ok := s.ProjectAliases[task.Project]
		if !ok {
			s.recordConflict(ctx, &report, task.ID, key, file.digest,
				fmt.Errorf("legacy submission %q references unmapped project %q: %w", task.ID, task.Project, ErrPermanentIntake))
			continue
		}
		task.Project = project
		result, err := s.Submitter.SubmitSingleTask(ctx, SingleTaskSubmission{
			IdempotencyKey: key,
			Task:           task,
		})
		if err != nil {
			submitErr := fmt.Errorf("submit legacy task %q: %w", task.ID, err)
			if errors.Is(err, domain.ErrSubmissionConflict) {
				s.recordConflict(ctx, &report, task.ID, key, file.digest, submitErr)
				continue
			}
			report.Errors = append(report.Errors, submitErr)
			continue
		}
		report.Accepted = append(report.Accepted, result)
	}
	return report
}

// LegacySubmissionKey is the durable idempotency key of one legacy task file.
func LegacySubmissionKey(taskID string) string {
	sum := sha256.Sum256([]byte(taskID))
	return fmt.Sprintf("legacy-%x", sum[:])
}

func (s LegacySubmissionSource) quarantinedDigest(ctx context.Context, key string) (string, error) {
	if s.Quarantine == nil {
		return "", nil
	}
	record, found, err := s.Quarantine.LoadSubmissionQuarantine(ctx, key)
	if err != nil {
		return "", fmt.Errorf("load legacy submission quarantine %q: %w", key, err)
	}
	if !found {
		return "", nil
	}
	return record.Digest, nil
}

// recordConflict reports a permanent conflict exactly once. Without a durable
// quarantine there is nowhere to record that it was reported, so the caller
// keeps hearing it on every cycle, which is the behavior this replaces.
func (s LegacySubmissionSource) recordConflict(
	ctx context.Context,
	report *LegacySubmissionReport,
	taskID, key, digest string,
	cause error,
) {
	if s.Quarantine == nil {
		report.Errors = append(report.Errors, cause)
		return
	}
	at := time.Now().UTC()
	if s.Now != nil {
		at = s.Now().UTC()
	}
	_, reported, err := s.Quarantine.QuarantineSubmission(ctx, key, digest, cause.Error(), at)
	if err != nil {
		report.Errors = append(report.Errors, cause, fmt.Errorf("quarantine legacy submission %q: %w", taskID, err))
		return
	}
	if reported {
		report.Quarantined = append(report.Quarantined, taskID)
		return
	}
	report.Errors = append(report.Errors, cause)
	report.Quarantined = append(report.Quarantined, taskID)
}

func authenticateLegacySubmissionDir(dir string, allowedUID uint32) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("inspect legacy submission directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("legacy submission path is not a real directory")
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != allowedUID {
		return fmt.Errorf("legacy submission directory owner is not authorized")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("legacy submission directory must not be group/world writable")
	}
	return nil
}

// legacySubmissionFile is one parsed drop-directory file and the digest of the
// exact bytes it was parsed from. The digest decides whether a quarantined key
// is still describing the same impossible content.
type legacySubmissionFile struct {
	task   Task
	digest string
}

func loadLegacyTasksBounded(dir string, maxBytes int64, maxFiles int) ([]legacySubmissionFile, []error) {
	if maxBytes <= 0 || maxFiles <= 0 {
		return nil, []error{fmt.Errorf("positive legacy submission byte and file limits are required")}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, []error{fmt.Errorf("read legacy submission directory: %w", err)}
	}
	var tasks []legacySubmissionFile
	var errs []error
	remaining := maxBytes
	files := 0
	for _, entry := range entries {
		if strings.ToLower(filepath.Ext(entry.Name())) != ".md" {
			continue
		}
		files++
		if files > maxFiles {
			return nil, append(errs, fmt.Errorf("legacy submission directory exceeds %d files", maxFiles))
		}
		path := filepath.Join(dir, entry.Name())
		if entry.Type()&os.ModeSymlink != 0 {
			errs = append(errs, fmt.Errorf("%s: legacy submission is not a regular file", path))
			continue
		}
		file, err := os.Open(path)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", path, err))
			continue
		}
		info, statErr := file.Stat()
		if statErr != nil || !info.Mode().IsRegular() {
			file.Close()
			if statErr != nil {
				errs = append(errs, fmt.Errorf("%s: %w", path, statErr))
			} else {
				errs = append(errs, fmt.Errorf("%s: legacy submission is not a regular file", path))
			}
			continue
		}
		raw, readErr := io.ReadAll(io.LimitReader(file, remaining+1))
		closeErr := file.Close()
		if readErr != nil {
			errs = append(errs, fmt.Errorf("%s: %w", path, readErr))
			continue
		}
		if closeErr != nil {
			errs = append(errs, fmt.Errorf("%s: %w", path, closeErr))
			continue
		}
		if int64(len(raw)) > remaining {
			return nil, append(errs, fmt.Errorf("legacy submissions exceed %d bytes", maxBytes))
		}
		remaining -= int64(len(raw))
		task, parseErr := parseFile(path, raw, info.ModTime())
		if parseErr != nil {
			errs = append(errs, parseErr)
			continue
		}
		sum := sha256.Sum256(raw)
		tasks = append(tasks, legacySubmissionFile{task: task, digest: hex.EncodeToString(sum[:])})
	}
	return tasks, errs
}

// LegacyProjectAliases accepts either a v2 logical project name or its unique
// configured T3 project title/id. Managed projects have only their logical alias.
func LegacyProjectAliases(projects map[string]string) (map[string]string, error) {
	aliases := make(map[string]string, len(projects)*2)
	for name, t3Project := range projects {
		if strings.TrimSpace(name) != name || name == "" ||
			strings.TrimSpace(t3Project) != t3Project {
			return nil, fmt.Errorf("legacy project aliases require trimmed names and T3 projects")
		}
		aliases[name] = name
	}
	for name, t3Project := range projects {
		if t3Project == "" {
			continue
		}
		if existing, ok := aliases[t3Project]; ok && existing != name {
			return nil, fmt.Errorf("T3 project %q maps to multiple v2 projects", t3Project)
		}
		aliases[t3Project] = name
	}
	return aliases, nil
}
