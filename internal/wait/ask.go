package wait

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// prepareAskAnswerFile makes the workspace's ask-answer.json say exactly what
// this wake says: it removes the file a previous ask may have left, and writes
// the new answer when there is one. An unanswered ask leaves no file.
func prepareAskAnswerFile(workspace string, answer *domain.AskAnswer) error {
	if workspace == "" || !filepath.IsAbs(workspace) {
		return fmt.Errorf("workspace %q is not an absolute path", workspace)
	}
	target := filepath.Join(workspace, domain.AskAnswerFile)
	if existing, err := os.Lstat(target); err == nil {
		if !existing.Mode().IsRegular() {
			return fmt.Errorf("%s exists and is not a regular file; refusing to replace it", target)
		}
		if err := os.Remove(target); err != nil {
			return fmt.Errorf("remove the previous %s: %w", domain.AskAnswerFile, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if answer == nil {
		return nil
	}
	return writeAskAnswerFile(workspace, *answer)
}

// askAnswerFileNote is appended to a wake whose answer file could not be
// prepared, so the task does not read a missing or stale file as the answer.
func askAnswerFileNote(err error) string {
	return fmt.Sprintf("\nNote: %s could not be prepared in the workspace (%v). Do not read that file; "+
		"use only the document in this message.\n", domain.AskAnswerFile, err)
}

// writeAskAnswerFile puts the ask-answer/v1 document at the root of the task's
// workspace before the wake that resumes the task is sent, so the resumed turn
// can read the answer as a file as well as in its message.
//
// The workspace is the path the attempt's own worker reported. It must be an
// absolute directory that is not a symlink, and an existing answer file is
// replaced only when it is a regular file: a link planted there would
// otherwise redirect the write. The file is written beside its final name and
// renamed, so the task never reads half a document. It is excluded from the
// repository's own git exclude list when the workspace has a .git directory,
// so a task that commits everything does not commit its answer.
func writeAskAnswerFile(workspace string, answer domain.AskAnswer) error {
	if workspace == "" || !filepath.IsAbs(workspace) {
		return fmt.Errorf("workspace %q is not an absolute path", workspace)
	}
	info, err := os.Lstat(workspace)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("workspace %s is not a directory", workspace)
	}
	target := filepath.Join(workspace, domain.AskAnswerFile)
	if existing, err := os.Lstat(target); err == nil && !existing.Mode().IsRegular() {
		return fmt.Errorf("%s exists and is not a regular file; refusing to replace it", target)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	raw, err := json.MarshalIndent(answer, "", "  ")
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(workspace, ".ask-answer-*.json")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if _, err := temporary.Write(append(raw, '\n')); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, target); err != nil {
		return err
	}
	excludeAskAnswerFromGit(workspace)
	return nil
}

// excludeAskAnswerFromGit adds the answer file to .git/info/exclude when the
// workspace is a plain checkout. It is best effort: a failure leaves a file
// the task may commit, which is visible, never a lost answer.
func excludeAskAnswerFromGit(workspace string) {
	excludeFromGit(workspace, "/"+domain.AskAnswerFile)
}
