package t3

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
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
//
// It identifies its project by the owned workspace root rather than by the ID
// it would derive, and it never adopts a project by title. The root is the
// identity T3 itself keeps unique -- it refuses a second active project for one
// workspace root -- and the root of a managed project is a directory this
// worker owns, so a project found there is this project whatever ID it carries.
//
// That is also what makes a managed project deletable. T3 keeps a deleted
// project's record and refuses to create the same ID twice, so an identity that
// is only ever derived from the key is spent the first time anything removes
// the project, and every later dispatch for that key would fail. Recreating
// under a fresh ID at the same root is safe precisely because the root, not the
// ID, is what the next call looks for.
//
// The first creation for a key still uses the derived project and command IDs,
// so an ambiguous response to it is reconciled without creating a second
// project; after that the root does the reconciling.
func (c *Control) EnsureProject(ctx context.Context, in ManagedProject) (string, error) {
	if strings.TrimSpace(in.Key) != in.Key || in.Key == "" ||
		strings.TrimSpace(in.Title) != in.Title || in.Title == "" ||
		strings.ContainsFunc(in.Key+in.Title+in.WorkspaceRoot, unicode.IsControl) ||
		!filepath.IsAbs(in.WorkspaceRoot) || filepath.Clean(in.WorkspaceRoot) != in.WorkspaceRoot ||
		in.WorkspaceRoot == string(filepath.Separator) {
		return "", errors.New("ensure T3 project: stable key, title and clean absolute owned workspace root are required")
	}
	id := deterministicID(in.Key, "steward.project")
	// The snapshot carries active projects only, so a project that is absent
	// here is either not created yet or deleted, and the two are the same
	// question to this function: does the owned root hold a project now.
	adopted := func(snapshot *t3api.ShellSnapshot) (string, error) {
		for _, p := range snapshot.Projects {
			if p.WorkspaceRoot != in.WorkspaceRoot {
				continue
			}
			if p.Title != in.Title {
				return "", fmt.Errorf("ensure T3 project: project %s already holds owned workspace root %s under the title %q rather than %q",
					p.ID, in.WorkspaceRoot, p.Title, in.Title)
			}
			return p.ID, nil
		}
		return "", nil
	}
	snapshot, err := c.client.ShellSnapshot(ctx)
	if err != nil {
		return "", fmt.Errorf("ensure T3 project: observe before creation: %w", err)
	}
	if existing, err := adopted(snapshot); err != nil || existing != "" {
		return existing, err
	}
	if c.DryRun {
		c.log.Info("dry-run: would create managed project", "project", id, "workspace", in.WorkspaceRoot)
		return id, nil
	}
	create := func(projectID, commandToken string) error {
		_, err := c.client.Dispatch(ctx, map[string]any{
			"type": "project.create", "commandId": deterministicID(commandToken, "steward.project.create"),
			"projectId": projectID, "title": in.Title, "workspaceRoot": in.WorkspaceRoot,
			"createWorkspaceRootIfMissing": true, "createdAt": now(),
		})
		return err
	}
	dispatchErr := create(id, in.Key)
	if dispatchErr != nil && refusedForASpentProjectID(dispatchErr) {
		// This key's derived identity has been used and deleted. The key keeps
		// its meaning, the root keeps the reconciling, and the project gets an
		// identity that has not been spent. The command ID travels with it so
		// two creations are never one command.
		c.log.Info("managed project identity is spent; recreating the owned root under a fresh identity",
			"spent", id, "workspace", in.WorkspaceRoot)
		id = newID()
		dispatchErr = create(id, id)
	}
	// Observe after both success and failure. A lost response is not evidence that
	// creation failed, and an acknowledgement alone is not matching metadata.
	// A newly accepted project can be absent from the first shell projection.
	// Reconcile only this same owned root, within a deadline, without dispatching
	// another create command or attaching a thread to unverified metadata.
	const visibilityDeadline = 5 * time.Second
	const visibilityInterval = 100 * time.Millisecond
	reconcileCtx, cancel := context.WithTimeout(ctx, visibilityDeadline)
	defer cancel()
	ticker := time.NewTicker(visibilityInterval)
	defer ticker.Stop()
	observations := 0
	var observeErr error
	for {
		observations++
		snapshot, observeErr = c.client.ShellSnapshot(reconcileCtx)
		if observeErr == nil {
			found, matchErr := adopted(snapshot)
			if matchErr != nil {
				return id, matchErr
			}
			if found != "" {
				c.log.Info("managed project visible at owned workspace root",
					"project", found, "workspace", in.WorkspaceRoot, "observations", observations)
				return found, nil
			}
		}
		select {
		case <-reconcileCtx.Done():
			return id, fmt.Errorf("ensure T3 project: creation not yet visible at owned workspace root %s after %d observations: %w",
				in.WorkspaceRoot, observations, errors.Join(dispatchErr, observeErr, reconcileCtx.Err()))
		case <-ticker.C:
		}
	}
}

// refusedForASpentProjectID reports T3's own refusal to create a project under
// an ID it has already recorded, which is what a deleted project leaves behind:
// the record survives the deletion and the identity cannot be used again.
//
// The refusal crosses the HTTP boundary as its text, so the text is what there
// is to read. It is matched loosely, on the part of T3's invariant message that
// states the cause, and a message this does not recognise is returned to the
// caller unchanged rather than retried under a new identity -- creating a
// second project is not the safe answer to a refusal nobody understood. T3's
// control protocol is internal, which is why the watchdog pins a tested version
// range; see internal/compat and docs/t3-protocol.md.
func refusedForASpentProjectID(err error) bool {
	return err != nil && strings.Contains(err.Error(), "cannot be created twice")
}
