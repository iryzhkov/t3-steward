package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

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
// ".." would replace the run's directory or the one above it. The coordinator
// validates task names, so this only stops a name it should never send.
func resultDirectoryName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) || filepath.Base(name) != name {
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
	return os.MkdirTemp(parent, ".task-result-*")
}

// replaceResultDirectoryRounds bounds how often a collection moves aside a
// directory that another collection of the same task put in place meanwhile.
const replaceResultDirectoryRounds = 16

// replaceResultDirectory puts a finished collection in place of whatever an
// earlier collection left at directory, so no file of an earlier attempt
// survives next to the selected attempt's results. A collection that wrote
// nothing removes the earlier one and leaves no directory, as a task that
// never produced anything always has.
//
// A directory cannot be renamed over a non-empty one, so what is there is first
// moved aside. Another collection of the same task may put its own directory in
// place in between; that one is complete too, so it is moved aside as well and
// the last collection to finish wins. Once the new collection is in place,
// removing the old ones is cleanup: a removal that fails does not undo the
// collection, and the directories left behind are returned for the caller to
// report.
func replaceResultDirectory(staged, directory string) ([]string, error) {
	entries, err := os.ReadDir(staged)
	if err != nil {
		return nil, err
	}
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
