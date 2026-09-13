package t3

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// ManagedProject identifies worker-owned project metadata. Key must remain stable
// across attempts and restarts; WorkspaceRoot is owned by the worker, not a dataset.
type ManagedProject struct {
	Key           string
	Title         string
	WorkspaceRoot string
}

// EnsureProject provisions project metadata without requiring a Git repository.
// It never adopts a project by title. Both the project and command IDs are stable
// so an ambiguous response can be reconciled without creating a second project.
func (c *Control) EnsureProject(ctx context.Context, in ManagedProject) (string, error) {
	if strings.TrimSpace(in.Key) != in.Key || in.Key == "" ||
		strings.TrimSpace(in.Title) != in.Title || in.Title == "" ||
		strings.ContainsFunc(in.Key+in.Title+in.WorkspaceRoot, unicode.IsControl) ||
		!filepath.IsAbs(in.WorkspaceRoot) || filepath.Clean(in.WorkspaceRoot) != in.WorkspaceRoot ||
		in.WorkspaceRoot == string(filepath.Separator) {
		return "", errors.New("ensure T3 project: stable key, title and clean absolute owned workspace root are required")
	}
	id := deterministicID(in.Key, "steward.project")
	match := func(snapshot *t3api.ShellSnapshot) (bool, error) {
		for _, p := range snapshot.Projects {
			if p.ID != id {
				continue
			}
			if p.Title != in.Title || p.WorkspaceRoot != in.WorkspaceRoot {
				return false, fmt.Errorf("ensure T3 project %s: existing metadata differs from owned project identity", id)
			}
			return true, nil
		}
		return false, nil
	}
	snapshot, err := c.client.ShellSnapshot(ctx)
	if err != nil {
		return "", fmt.Errorf("ensure T3 project: observe before creation: %w", err)
	}
	found, err := match(snapshot)
	if err != nil || found {
		return id, err
	}
	if c.DryRun {
		c.log.Info("dry-run: would create managed project", "project", id, "workspace", in.WorkspaceRoot)
		return id, nil
	}
	_, dispatchErr := c.client.Dispatch(ctx, map[string]any{
		"type": "project.create", "commandId": deterministicID(in.Key, "steward.project.create"),
		"projectId": id, "title": in.Title, "workspaceRoot": in.WorkspaceRoot,
		"createWorkspaceRootIfMissing": true, "createdAt": now(),
	})
	// Observe after both success and failure. A lost response is not evidence that
	// creation failed, and an acknowledgement alone is not matching metadata.
	snapshot, err = c.client.ShellSnapshot(ctx)
	if err != nil {
		return id, fmt.Errorf("ensure T3 project: observe creation: %w", errors.Join(dispatchErr, err))
	}
	found, err = match(snapshot)
	if err != nil {
		return id, err
	}
	if found {
		return id, nil
	}
	if dispatchErr != nil {
		return id, fmt.Errorf("ensure T3 project: create: %w", dispatchErr)
	}
	return id, errors.New("ensure T3 project: creation not yet visible; reconcile the same identity")
}
