package t3

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
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
//
// A spent identity is replaced by one fenced on a root-absent snapshot rather
// than by one chained from the identity before it. A chain must replay every
// retained generation to reach the next one, so a root that has been created
// and swept often enough could never be provisioned within a bounded call. The
// fenced identity is derived from the sequence of a snapshot in which the root
// is absent: every project created under it is accepted after that snapshot,
// so its receipt is newer than the fence, and a retry, restart or concurrent
// caller that observes the same snapshot dispatches the same command. However
// many deleted generations the root retains, one fenced identity skips them
// all, and the call is bounded by a dispatch count and a wall-clock budget.
func (c *Control) EnsureProject(ctx context.Context, in ManagedProject) (string, error) {
	if strings.TrimSpace(in.Key) != in.Key || in.Key == "" ||
		strings.TrimSpace(in.Title) != in.Title || in.Title == "" ||
		strings.ContainsFunc(in.Key+in.Title+in.WorkspaceRoot, unicode.IsControl) ||
		!filepath.IsAbs(in.WorkspaceRoot) || filepath.Clean(in.WorkspaceRoot) != in.WorkspaceRoot ||
		in.WorkspaceRoot == string(filepath.Separator) {
		return "", errors.New("ensure T3 project: stable key, title and clean absolute owned workspace root are required")
	}
	id := deterministicID(in.Key, "steward.project")
	// The snapshot carries active projects only. Its sequence also fences a
	// create receipt: an accepted command older than a root-absent snapshot
	// cannot make that project active by replaying the same command ID.
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
	// The budget bounds the whole creation, every dispatch and observation
	// included, so no history of spent identities can hold the caller.
	const maxCreateDispatches = 4
	const creationBudget = 10 * time.Second
	const visibilityInterval = 100 * time.Millisecond
	budgetCtx, cancel := context.WithTimeout(ctx, creationBudget)
	defer cancel()
	ticker := time.NewTicker(visibilityInterval)
	defer ticker.Stop()
	commandToken := in.Key
	tried := map[string]bool{}
	observations := 0
	for dispatches := 0; ; dispatches++ {
		if tried[commandToken] {
			// Only a fence can repeat, and only when no newer root-absent
			// snapshot exists; dispatching it again would change nothing.
			return id, fmt.Errorf("ensure T3 project: creation identity %s at owned workspace root %s is spent and no root-absent snapshot after sequence %d exists to fence a replacement",
				deterministicID(commandToken, "steward.project"), in.WorkspaceRoot, snapshot.SnapshotSequence)
		}
		if dispatches == maxCreateDispatches {
			return id, fmt.Errorf("ensure T3 project: exhausted %d creation dispatches at owned workspace root %s",
				maxCreateDispatches, in.WorkspaceRoot)
		}
		tried[commandToken] = true
		id = deterministicID(commandToken, "steward.project")
		result, dispatchErr := c.client.Dispatch(budgetCtx, map[string]any{
			"type": "project.create", "commandId": deterministicID(commandToken, "steward.project.create"),
			"projectId": id, "title": in.Title, "workspaceRoot": in.WorkspaceRoot,
			"createWorkspaceRootIfMissing": true, "createdAt": now(),
		})
		if refusedForASpentProjectID(dispatchErr) {
			// The refusal postdates the snapshot in hand, so observe again:
			// the fence must come from a snapshot taken after it.
			c.log.Info("managed project identity is spent; fencing a replacement on a fresh snapshot",
				"spent", id, "workspace", in.WorkspaceRoot, "dispatch", dispatches)
			next, err := c.client.ShellSnapshot(budgetCtx)
			if err != nil {
				return id, fmt.Errorf("ensure T3 project: observe after spent identity %s: %w", id, err)
			}
			snapshot = next
			if existing, err := adopted(snapshot); err != nil || existing != "" {
				return existing, err
			}
			commandToken = fencedCreationToken(in.Key, snapshot.SnapshotSequence)
			continue
		}
		var receipt int64
		if dispatchErr == nil && result != nil {
			receipt = result.Sequence
		}
		if receipt > 0 && snapshot.SnapshotSequence >= receipt {
			// T3 acknowledged a previous invocation of this command. The
			// root was absent from a snapshot at or after that receipt, so
			// replaying it cannot recreate a project that was later deleted.
			c.log.Info("managed project create receipt predates root-absent snapshot; fencing a replacement",
				"project", id, "receipt_sequence", receipt,
				"snapshot_sequence", snapshot.SnapshotSequence, "dispatch", dispatches)
			commandToken = fencedCreationToken(in.Key, snapshot.SnapshotSequence)
			continue
		}
		// Observe after both success and failure. A lost response is not
		// evidence that creation failed, and an acknowledgement alone is not
		// matching metadata. Reconcile only this same owned root. A refusal
		// nobody understood is never retried under another identity; it is
		// only observed, in case the root is held by a creation still to be
		// projected.
		var observeErr error
		for {
			observations++
			next, err := c.client.ShellSnapshot(budgetCtx)
			observeErr = err
			if err == nil {
				snapshot = next
				found, matchErr := adopted(snapshot)
				if matchErr != nil {
					return id, matchErr
				}
				if found != "" {
					c.log.Info("managed project visible at owned workspace root",
						"project", found, "workspace", in.WorkspaceRoot, "observations", observations)
					return found, nil
				}
				if receipt > 0 && snapshot.SnapshotSequence >= receipt {
					// The projection has reached the acknowledged creation
					// and the root is absent: the project was deleted before
					// this call saw it, and its identity is spent.
					c.log.Info("managed project was removed before it was observed; fencing a replacement",
						"project", id, "receipt_sequence", receipt,
						"snapshot_sequence", snapshot.SnapshotSequence, "dispatch", dispatches)
					break
				}
			}
			select {
			case <-budgetCtx.Done():
				return id, fmt.Errorf("ensure T3 project: creation not yet visible at owned workspace root %s after %d observations: %w",
					in.WorkspaceRoot, observations, errors.Join(dispatchErr, observeErr, budgetCtx.Err()))
			case <-ticker.C:
			}
		}
		commandToken = fencedCreationToken(in.Key, snapshot.SnapshotSequence)
	}
}

// fencedCreationToken is the command token of the creation that replaces a
// spent identity once the owned root has been observed absent at sequence. It
// depends on the key and the fence alone, never on which identities were spent
// before, so reaching it needs no replay of the retained history.
func fencedCreationToken(key string, sequence int64) string {
	return key + "\x00fence\x00" + strconv.FormatInt(sequence, 10)
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
