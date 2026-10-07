package main

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"

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

// replaceResultDirectory puts a finished collection in place of whatever an
// earlier collection left at directory, so no file of an earlier attempt
// survives next to the selected attempt's results. A collection that wrote
// nothing removes the earlier one and leaves no directory, as a task that
// never produced anything always has.
//
// A directory cannot be renamed over a non-empty one, so the earlier
// collection is first moved aside, and restored if the new one cannot take its
// place.
func replaceResultDirectory(staged, directory string) error {
	entries, err := os.ReadDir(staged)
	if err != nil {
		return err
	}
	retired := ""
	if _, err := os.Lstat(directory); err == nil {
		retired = staged + "-previous"
		if err := os.Rename(directory, retired); err != nil {
			return err
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if len(entries) == 0 {
		if err := os.Remove(staged); err != nil {
			return err
		}
	} else if err := os.Rename(staged, directory); err != nil {
		if retired != "" {
			_ = os.Rename(retired, directory)
		}
		return err
	}
	if retired != "" {
		return os.RemoveAll(retired)
	}
	return nil
}
