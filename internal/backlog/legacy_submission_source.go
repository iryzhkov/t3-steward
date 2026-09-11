package backlog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

type SingleTaskSubmitter interface {
	SubmitSingleTask(context.Context, SingleTaskSubmission) (SubmissionResult, error)
}

type LegacySubmissionReport struct {
	Accepted []SubmissionResult
	Skipped  []string
	Errors   []error
}

// LegacySubmissionSource adapts the unchanged owner-controlled Markdown drop
// directory into durable immutable v2 submissions.
type LegacySubmissionSource struct {
	Dir            string
	Submitter      SingleTaskSubmitter
	ProjectAliases map[string]string
	MaxBytes       int64
	MaxFiles       int
	AllowedUID     uint32
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
	tasks, loadErrors := loadLegacyTasksBounded(s.Dir, s.MaxBytes, s.MaxFiles)
	report.Errors = append(report.Errors, loadErrors...)
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID < tasks[j].ID
	})
	for _, task := range tasks {
		if err := ctx.Err(); err != nil {
			report.Errors = append(report.Errors, err)
			return report
		}
		if !task.IsEnabled() {
			report.Skipped = append(report.Skipped, task.ID)
			continue
		}
		project, ok := s.ProjectAliases[task.Project]
		if !ok {
			report.Errors = append(report.Errors,
				fmt.Errorf("legacy submission %q references unmapped project %q", task.ID, task.Project))
			continue
		}
		task.Project = project
		sum := sha256.Sum256([]byte(task.ID))
		result, err := s.Submitter.SubmitSingleTask(ctx, SingleTaskSubmission{
			IdempotencyKey: fmt.Sprintf("legacy-%x", sum[:]),
			Task:           task,
		})
		if err != nil {
			report.Errors = append(report.Errors, fmt.Errorf("submit legacy task %q: %w", task.ID, err))
			continue
		}
		report.Accepted = append(report.Accepted, result)
	}
	return report
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

func loadLegacyTasksBounded(dir string, maxBytes int64, maxFiles int) ([]Task, []error) {
	if maxBytes <= 0 || maxFiles <= 0 {
		return nil, []error{fmt.Errorf("positive legacy submission byte and file limits are required")}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, []error{fmt.Errorf("read legacy submission directory: %w", err)}
	}
	var tasks []Task
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
		tasks = append(tasks, task)
	}
	return tasks, errs
}

// LegacyProjectAliases accepts either a v2 logical project name or its unique
// configured T3 project title/id.
func LegacyProjectAliases(projects map[string]string) (map[string]string, error) {
	aliases := make(map[string]string, len(projects)*2)
	for name, t3Project := range projects {
		if strings.TrimSpace(name) != name || name == "" ||
			strings.TrimSpace(t3Project) != t3Project || t3Project == "" {
			return nil, fmt.Errorf("legacy project aliases require trimmed names and T3 projects")
		}
		aliases[name] = name
		if existing, ok := aliases[t3Project]; ok && existing != name {
			return nil, fmt.Errorf("T3 project %q maps to multiple v2 projects", t3Project)
		}
		aliases[t3Project] = name
	}
	return aliases, nil
}
