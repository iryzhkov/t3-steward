package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"golang.org/x/sys/unix"
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

// Retired names have a separate namespace and a fixed-size task identity so
// even a long task name fits in a filesystem path element.
const resultRetiredPrefix = resultStagingPrefix + "retired-"

// Five seconds covers ordinary raw-path file opens; keeping the newest sixteen
// generations also protects readers across a burst of re-collections.
const resultRetirementGrace = 5 * time.Second
const resultRetainedGenerations = 16

func retiredResultPrefix(directory string) string {
	return fmt.Sprintf("%s%x-", resultRetiredPrefix, sha256.Sum256([]byte(filepath.Base(directory))))
}

// lockResultParent serializes publications and sweeps across processes without
// leaving a lock file beside the collections. Independent opens also serialize
// goroutines in one process. Closing the descriptor (including on crash) releases
// the flock. Sweeps use a nonblocking lock and defer cleanup to the next sweep
// when a publisher owns the parent.
func lockResultParent(directory string, wait bool) (*os.File, error) {
	parent, err := os.Open(filepath.Dir(directory))
	if err != nil {
		return nil, err
	}
	flags := unix.LOCK_EX
	if !wait {
		flags |= unix.LOCK_NB
	}
	for {
		err = unix.Flock(int(parent.Fd()), flags)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	if err != nil {
		_ = parent.Close()
		return nil, err
	}
	return parent, nil
}

// prepareResultExchange reserves an order while holding the parent lock and
// moves staging into the retirement namespace BEFORE exchange. The incoming
// inode in the name distinguishes a crash before exchange (unpublished staging)
// from a crash after exchange (a real retirement), without a second rename.
// Advancing beyond every reserved timestamp prevents clock rollback or equal
// timestamps from changing publication order.
func prepareResultExchange(staged, directory string) (prepared, retired string, err error) {
	entries, err := os.ReadDir(filepath.Dir(directory))
	if err != nil {
		return "", "", err
	}
	prefix := retiredResultPrefix(directory)
	order := time.Now().UnixNano()
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		stamp, _, _ := strings.Cut(strings.TrimPrefix(entry.Name(), prefix), "-")
		nanos, parseErr := strconv.ParseInt(stamp, 10, 64)
		if parseErr == nil && nanos >= order {
			if nanos == 1<<63-1 {
				return "", "", errors.New("result retirement order exhausted")
			}
			order = nanos + 1
		}
	}
	var incoming unix.Stat_t
	if err := unix.Lstat(staged, &incoming); err != nil {
		return "", "", err
	}
	retired = filepath.Join(filepath.Dir(directory), prefix+
		fmt.Sprintf("%020d-%s", order, strings.TrimPrefix(filepath.Base(staged), resultStagingPrefix)))
	prepared = fmt.Sprintf("%s-staged-%d", retired, incoming.Ino)
	if err := os.Rename(staged, prepared); err != nil {
		return "", "", err
	}
	return prepared, retired, nil
}

// unpublishedResult reports whether a protected exchange path still contains
// its incoming inode. Once exchange succeeds it contains the previous public
// inode, so even a failed final retirement rename uses the age AND count gates.
func unpublishedResult(path string) (bool, error) {
	_, inode, ok := strings.Cut(filepath.Base(path), "-staged-")
	if !ok {
		return false, nil
	}
	want, err := strconv.ParseUint(inode, 10, 64)
	if err != nil {
		return false, err
	}
	var current unix.Stat_t
	if err := unix.Lstat(path, &current); err != nil {
		return false, err
	}
	return current.Ino == want, nil
}

// removeRetiredResults keeps the newest K and every generation within the grace
// window. Rapid publishers can temporarily retain more than K; after the window,
// the next publish or sweep reduces retention to K. Retirement time comes from
// the name, never from the old collection's potentially ancient mtime.
// The caller must hold the parent-directory lock.
func removeRetiredResults(directory string, now time.Time) []string {
	parent := filepath.Dir(directory)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return nil
	}
	prefix := retiredResultPrefix(directory)
	type retired struct {
		name string
		when time.Time
	}
	var generations []retired
	var left []string
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		stamp, _, ok := strings.Cut(strings.TrimPrefix(entry.Name(), prefix), "-")
		nanos, err := strconv.ParseInt(stamp, 10, 64)
		if !ok || err != nil {
			continue
		}
		path := filepath.Join(parent, entry.Name())
		unpublished, err := unpublishedResult(path)
		if err != nil {
			// Uncertain identity must never cause deletion of a reader's files.
			left = append(left, path)
			continue
		}
		if unpublished {
			// Crashed before exchange: it is not a publication and must not
			// displace any of the newest K actual retirements.
			if now.Sub(time.Unix(0, nanos)) >= interruptedCollectionAge {
				if err := os.RemoveAll(path); err != nil {
					left = append(left, path)
				}
			}
			continue
		}
		generations = append(generations, retired{entry.Name(), time.Unix(0, nanos)})
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i].name > generations[j].name })
	for i, generation := range generations {
		if i < resultRetainedGenerations || now.Sub(generation.when) < resultRetirementGrace {
			continue
		}
		path := filepath.Join(parent, generation.name)
		if err := os.RemoveAll(path); err != nil {
			left = append(left, path)
		}
	}
	return left
}

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
// collection is in place, retiring the earlier one and reclaiming eligible
// generations is cleanup: a failure does not undo publication, and the path
// left behind is returned for the caller to report. So is removing what a
// collection interrupted by a crash left beside directory.
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
// it is kept. Retired generations of this task use their separate age and count
// gates instead. It returns those it could not remove.
func removeInterruptedCollections(staged, directory string, now time.Time) []string {
	lock, err := lockResultParent(directory, false)
	if errors.Is(err, unix.EWOULDBLOCK) {
		return nil
	}
	if err != nil {
		return []string{filepath.Dir(directory)}
	}
	defer lock.Close()
	parent := filepath.Dir(directory)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return nil
	}
	left := removeRetiredResults(directory, now)
	for _, entry := range entries {
		path := filepath.Join(parent, entry.Name())
		if !strings.HasPrefix(entry.Name(), resultStagingPrefix) || strings.HasPrefix(entry.Name(), resultRetiredPrefix) || path == staged || strings.HasPrefix(path, staged+"-") {
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

// publishResultDirectory atomically publishes a nonempty collection and renames
// the exchanged-out generation to a hidden, task-specific retired sibling.
// A parent-directory flock orders exchange, retirement naming and reclamation
// across processes. Before exchange, staging moves to a protected retirement
// name recording the incoming inode; a crash or failed final rename cannot
// expose an exchanged generation to the abandoned-staging sweep. The inode
// distinguishes unpublished preparations, which are cleaned up after 24 hours
// and do not count toward the newest K. Retirement order is reserved under the
// lock and is monotonic even if the wall clock moves backwards.
// A raw-path reader completing before K (resultRetainedGenerations) further
// publications sees a complete file. Reclamation requires BOTH retirement age
// >= resultRetirementGrace and exclusion from the newest K retired generations.
// Thus retention is K plus generations within the grace window, not a hard
// disk-space bound during bursts. Cleanup runs on successful publication and
// the leftover sweep; failures are returned in left without undoing publication.
// Empty collections and unsupported exchange retain their existing semantics.
func publishResultDirectory(staged, directory string) (left []string, err error) {
	lock, err := lockResultParent(directory, true)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	defer func() {
		if err == nil {
			left = append(left, removeRetiredResults(directory, time.Now())...)
		}
	}()
	entries, err := os.ReadDir(staged)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return moveAsideResultDirectory(staged, directory, entries)
	}
	prepared, retired, err := prepareResultExchange(staged, directory)
	if err != nil {
		return nil, err
	}
	// Before publication fails, restore the caller's staging path so its existing
	// deferred cleanup still removes the unselected collection.
	defer func() {
		if err != nil {
			if restoreErr := os.Rename(prepared, staged); restoreErr != nil {
				err = fmt.Errorf("%w; unpublished collection kept at %s: %v", err, prepared, restoreErr)
			}
		}
	}()
	for round := 0; round < replaceResultDirectoryRounds; round++ {
		exchangeErr := swapDirectories(prepared, directory)
		switch {
		case exchangeErr == nil:
			if err := os.Rename(prepared, retired); err != nil {
				return []string{prepared}, nil
			}
			return nil, nil
		case errors.Is(exchangeErr, errExchangeUnsupported):
			// A plain rename can publish to an absent destination atomically.
			// It cannot overwrite a nonempty collection, even if a concurrent
			// publisher created that collection after the failed exchange.
			// Never move that collection aside to make this rename succeed.
			if renameErr := os.Rename(prepared, directory); renameErr != nil {
				return nil, fmt.Errorf("cannot publish collection at %s: %w; use a filesystem that supports atomic directory exchange (rename: %w)", directory, exchangeErr, renameErr)
			}
			return nil, nil
		case !errors.Is(exchangeErr, fs.ErrNotExist):
			return nil, exchangeErr
		}
		err = os.Rename(prepared, directory)
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
