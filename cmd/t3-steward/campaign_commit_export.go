package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

const campaignCommitExportUsage = "Usage: t3-steward campaign commit export <run>/<task>/<commit-name> --bundle FILE [--branch NAME]\n"

func exportCLIHash(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

func parseCampaignCommitExportArgs(args []string) (backlogadmin.CommitExportRequest, string, error) {
	var r backlogadmin.CommitExportRequest
	path := ""
	var positional []string
	branchSet := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--bundle", "--branch":
			flag := args[i]
			if i+1 >= len(args) || args[i+1] == "" {
				return r, path, fmt.Errorf("%s requires a value", flag)
			}
			i++
			if flag == "--bundle" {
				if path != "" {
					return r, path, errors.New("duplicate --bundle")
				}
				path = args[i]
			} else {
				if branchSet {
					return r, path, errors.New("duplicate --branch")
				}
				r.Branch = args[i]
				branchSet = true
			}
		default:
			if strings.HasPrefix(args[i], "-") {
				return r, path, fmt.Errorf("unknown commit export option %q", args[i])
			}
			positional = append(positional, args[i])
		}
	}
	if len(positional) != 1 || path == "" {
		return r, path, fmt.Errorf("invalid commit export arguments: %s", strings.TrimSpace(campaignCommitExportUsage))
	}
	parts := strings.Split(positional[0], "/")
	if len(parts) != 3 {
		return r, path, errors.New("commit export requires <run>/<task>/<commit-name>")
	}
	for _, s := range parts {
		if s == "" || s == "." || s == ".." || len(s) > 1024 || strings.TrimSpace(s) != s {
			return r, path, errors.New("invalid declared commit reference")
		}
	}
	r.RunID, r.TaskID, r.Name = parts[0], parts[1], parts[2]
	if !branchSet {
		r.Branch = r.Name
	}
	if err := backlog.ValidateExportBranch(r.Branch); err != nil {
		return r, path, err
	}
	return r, path, nil
}

func (c campaignCLI) runCommit(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "export" {
		return fmt.Errorf("invalid commit export arguments: %s", strings.TrimSpace(campaignCommitExportUsage))
	}
	r, path, err := parseCampaignCommitExportArgs(args[1:])
	if err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("commit export: destination %s already exists", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if c.exportCommit == nil {
		return errors.New("coordinator commit export is unavailable; upgrade the coordinator")
	}
	result, err := c.exportCommit(ctx, r)
	if err != nil {
		return err
	}
	if result.Content == nil {
		return errors.New("commit export: missing bundle content")
	}
	// Close exactly once, before publishing, so a failed transport close cannot
	// leave a destination that appears successful.
	closed := false
	defer func() {
		if !closed {
			_ = result.Content.Close()
		}
	}()
	if result.Provenance == nil || result.Provenance.WorkflowRunID != r.RunID || result.Provenance.Name != r.Name ||
		result.Provenance.Commit == "" || result.Provenance.Base == "" || result.Metadata.Size <= 0 || result.Metadata.Size > backlog.DefaultCommitBundleMaxBytes || len(result.Metadata.SHA256) != 64 {
		return errors.New("commit export: invalid coordinator metadata")
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".t3-commit-export-")
	if err != nil {
		return err
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(result.Content, result.Metadata.Size+1))
	syncErr := file.Sync()
	closeErr := file.Close()
	sourceErr := result.Content.Close()
	closed = true
	if err := errors.Join(copyErr, syncErr, closeErr, sourceErr); err != nil {
		return fmt.Errorf("commit export: receive bundle: %w", err)
	}
	digest := fmt.Sprintf("%x", hash.Sum(nil))
	if size != result.Metadata.Size || !strings.EqualFold(digest, result.Metadata.SHA256) {
		return errors.New("commit export: bundle digest or size mismatch")
	}
	// Linking an already-complete private file publishes atomically without
	// replacing an existing destination, including a concurrently created one.
	if err := os.Link(tmp, path); err != nil {
		return fmt.Errorf("commit export: publish bundle: %w", err)
	}
	_, err = fmt.Fprintf(c.stdout, "commit %s\nbase %s\nbundle %s\nsha256 %s\n", result.Provenance.Commit, result.Provenance.Base, path, digest)
	return err
}
