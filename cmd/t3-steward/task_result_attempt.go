package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// ofSelectedAttempt reports whether artifact was produced by the attempt a task
// result is collected for.
//
// The coordinator lists every artifact of the task, from every attempt. After a
// retry, taking them all presented the earlier attempt's final message and
// outputs as the new attempt's, hid that the new attempt had no final message,
// and let an earlier artifact listed later overwrite a newer file of the same
// name. A task with no attempt has produced nothing to collect.
func ofSelectedAttempt(task backlogadmin.TaskDetail, artifact backlogadmin.Artifact) bool {
	return task.Attempt != nil && artifact.Metadata.AttemptID == task.Attempt.ID
}

// resultDirectoryName refuses a task name that is not one plain path element.
// The task's result directory is replaced wholesale, so a name such as "" or
// ".." would replace the run's directory or the one above it, and a hidden name
// could be taken for another collection's staging directory and removed. The
// coordinator validates task names, so this only stops a name it should never
// send.
func resultDirectoryName(name string) error {
	if name == "" || strings.HasPrefix(name, ".") || strings.ContainsAny(name, `/\`) || filepath.Base(name) != name {
		return fmt.Errorf("task name %q cannot name a result directory", name)
	}
	return nil
}

// stageResultDirectory makes an empty directory beside directory for one
// collection to write into. The caller removes it if the collection fails, so
// a failed collection leaves the previous one untouched.
func stageResultDirectory(directory string) (string, error) {
	parent := filepath.Dir(directory)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", err
	}
	return os.MkdirTemp(parent, resultStagingPrefix+"*")
}

// resultStagingPrefix begins the name of every directory a collection keeps
// beside a task's directory: its staging directory and the earlier collections
// it moves aside. No task name begins with it.
const resultStagingPrefix = ".task-result-"

// interruptedCollectionAge is how long a hidden staging directory beside a
// task's directory must have been left untouched before a later collection
// takes it for one a crash interrupted. No collection runs this long.
const interruptedCollectionAge = 24 * time.Hour

// replaceResultDirectoryRounds bounds how often a collection moves aside a
// directory that another collection of the same task put in place meanwhile.
const replaceResultDirectoryRounds = 16

// swapDirectories is exchangeDirectories; tests replace it to take the path a
// filesystem without an atomic exchange takes.
var swapDirectories = exchangeDirectories

// replaceResultDirectory puts a finished collection in place of whatever an
// earlier collection left at directory, so no file of an earlier attempt
// survives next to the selected attempt's results. A collection that wrote
// nothing removes the earlier one and leaves no directory, as a task that
// never produced anything always has.
//
// A directory cannot be renamed over a non-empty one, so the new collection is
// exchanged with the earlier one in a single rename: a reader, or a crash, sees
// either the whole earlier collection or the whole new one at directory, never
// no directory. When there is no earlier collection a plain rename publishes
// the new one; if another collection of the same task got there first, the new
// one is exchanged with it, and the last collection to finish wins. Once the new
// collection is in place, removing the earlier one, now at staged, is cleanup: a
// removal that fails does not undo the collection, and the directory left
// behind is returned for the caller to report. So is removing what a collection
// interrupted by a crash left beside directory.
func replaceResultDirectory(staged, directory string) ([]string, error) {
	left, err := publishResultDirectory(staged, directory)
	if err != nil {
		return nil, err
	}
	return append(left, removeInterruptedCollections(staged, directory, time.Now())...), nil
}

// removeInterruptedCollections removes the staging directories and moved-aside
// collections beside directory that no collection has touched for
// interruptedCollectionAge, which only a collection a crash interrupted leaves.
// A recent one may belong to a collection of the same task still running, so
// it is kept. It returns those it could not remove.
func removeInterruptedCollections(staged, directory string, now time.Time) []string {
	parent := filepath.Dir(directory)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return nil
	}
	var left []string
	for _, entry := range entries {
		path := filepath.Join(parent, entry.Name())
		if !strings.HasPrefix(entry.Name(), resultStagingPrefix) || path == staged || strings.HasPrefix(path, staged+"-") {
			continue
		}
		info, err := os.Lstat(path)
		if err != nil || now.Sub(info.ModTime()) < interruptedCollectionAge {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			left = append(left, path)
		}
	}
	return left
}

// publishResultDirectory puts staged in place of directory; see
// replaceResultDirectory.
func publishResultDirectory(staged, directory string) ([]string, error) {
	entries, err := os.ReadDir(staged)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return moveAsideResultDirectory(staged, directory, entries)
	}
	for round := 0; round < replaceResultDirectoryRounds; round++ {
		err := swapDirectories(staged, directory)
		switch {
		case err == nil:
			if err := os.RemoveAll(staged); err != nil {
				return []string{staged}, nil
			}
			return nil, nil
		case errors.Is(err, errExchangeUnsupported):
			// A plain rename can publish to an absent destination atomically.
			// It cannot overwrite a nonempty collection, even if a concurrent
			// publisher created that collection after the failed exchange.
			// Never move that collection aside to make this rename succeed.
			if renameErr := os.Rename(staged, directory); renameErr != nil {
				return nil, fmt.Errorf("cannot publish collection at %s: %w; use a filesystem that supports atomic directory exchange (rename: %w)", directory, err, renameErr)
			}
			return nil, nil
		case !errors.Is(err, fs.ErrNotExist):
			return nil, err
		}
		err = os.Rename(staged, directory)
		if err == nil {
			return nil, nil
		}
		// ErrExist covers ENOTEMPTY: a concurrent collection got there first.
		if !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("another collection kept replacing %s", directory)
}

// moveAsideResultDirectory replaces directory in two renames: what is there is
// moved aside, then the new collection is renamed in. Between the two there is
// no directory, so it is used only to remove an earlier collection when the new
// one is empty. Nonempty replacements must exchange atomically or refuse.
//
// Another collection of the same task may put its own directory in place in
// between; that one is complete too, so it is moved aside as well and the last
// collection to finish wins.
func moveAsideResultDirectory(staged, directory string, entries []os.DirEntry) ([]string, error) {
	var retired []string
	placed := false
	for round := 0; round < replaceResultDirectoryRounds && !placed; round++ {
		aside := fmt.Sprintf("%s-previous-%d", staged, round)
		if err := os.Rename(directory, aside); err == nil {
			retired = append(retired, aside)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, restoreResultDirectory(retired, directory, err)
		}
		if len(entries) == 0 {
			if err := os.Remove(staged); err != nil {
				return nil, restoreResultDirectory(retired, directory, err)
			}
			placed = true
			break
		}
		err := os.Rename(staged, directory)
		if err == nil {
			placed = true
			break
		}
		// ErrExist covers ENOTEMPTY: a concurrent collection got there first.
		if !errors.Is(err, fs.ErrExist) {
			return nil, restoreResultDirectory(retired, directory, err)
		}
	}
	if !placed {
		return nil, restoreResultDirectory(retired, directory,
			fmt.Errorf("another collection kept replacing %s", directory))
	}
	var left []string
	for _, path := range retired {
		if err := os.RemoveAll(path); err != nil {
			left = append(left, path)
		}
	}
	return left, nil
}

// restoreResultDirectory puts the collection that was in place before back, when
// the new one could not take its place, and says where it is if it cannot.
func restoreResultDirectory(retired []string, directory string, cause error) error {
	if len(retired) == 0 {
		return cause
	}
	original := retired[0]
	if err := os.Rename(original, directory); err != nil {
		return fmt.Errorf("%w; the previous collection is kept at %s", cause, original)
	}
	for _, path := range retired[1:] {
		_ = os.RemoveAll(path)
	}
	return cause
}
