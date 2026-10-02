package wait

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

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
	gitDir := filepath.Join(workspace, ".git")
	if info, err := os.Lstat(gitDir); err != nil || !info.IsDir() {
		return
	}
	info := filepath.Join(gitDir, "info")
	if err := os.MkdirAll(info, 0o700); err != nil {
		return
	}
	exclude := filepath.Join(info, "exclude")
	line := "/" + domain.AskAnswerFile
	current, err := os.ReadFile(exclude)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return
	}
	for _, existing := range strings.Split(string(current), "\n") {
		if strings.TrimSpace(existing) == line {
			return
		}
	}
	updated := string(current)
	if updated != "" && !strings.HasSuffix(updated, "\n") {
		updated += "\n"
	}
	_ = os.WriteFile(exclude, []byte(updated+line+"\n"), 0o600)
}
