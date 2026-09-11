package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func (c backlogAdminCLI) runSubmission(ctx context.Context, args []string) error {
	if c.submissions == nil {
		return errors.New("coordinator submission transport is unavailable")
	}
	archivePath, key, asJSON, err := parseSubmissionArgs(args)
	if err != nil {
		return err
	}
	archive, size, err := openSubmissionArchive(archivePath)
	if err != nil {
		return err
	}
	defer archive.Close()

	response, err := c.submissions.SubmitArchive(ctx, backlogadmin.LocalSubmissionRequest{
		IdempotencyKey: key,
	}, archive, size)
	if err != nil {
		return err
	}
	if asJSON {
		encoder := json.NewEncoder(c.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(response)
	}
	_, err = fmt.Fprintf(
		c.stdout,
		"submission %s: workflow=%s run=%s state=%s replay=%t digest=%s\n",
		response.Key, response.WorkflowID, response.RunID, response.State, response.Replay, response.Digest,
	)
	return err
}

func parseSubmissionArgs(args []string) (archivePath, key string, asJSON bool, err error) {
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--idempotency-key":
			if key != "" || index+1 >= len(args) || args[index+1] == "" {
				return "", "", false, errors.New("--idempotency-key needs one nonempty value")
			}
			index++
			key = args[index]
		case "--json":
			if asJSON {
				return "", "", false, errors.New("--json may be supplied only once")
			}
			asJSON = true
		default:
			if len(args[index]) > 0 && args[index][0] == '-' {
				return "", "", false, fmt.Errorf("unknown submission option %q", args[index])
			}
			if archivePath != "" {
				return "", "", false, errors.New("backlog submit accepts exactly one archive path")
			}
			archivePath = args[index]
		}
	}
	if archivePath == "" {
		return "", "", false, errors.New("backlog submit needs a tar archive path")
	}
	return archivePath, key, asJSON, nil
}

func openSubmissionArchive(path string) (*os.File, int64, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, 0, fmt.Errorf("inspect submission archive: %w", err)
	}
	if !before.Mode().IsRegular() {
		return nil, 0, errors.New("submission archive must be a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open submission archive: %w", err)
	}
	after, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, 0, fmt.Errorf("inspect opened submission archive: %w", err)
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() <= 0 {
		file.Close()
		return nil, 0, errors.New("submission archive changed or is empty")
	}
	return file, after.Size(), nil
}
