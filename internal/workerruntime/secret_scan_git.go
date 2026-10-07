package workerruntime

import (
	"context"
	"errors"
	"io"
	"strings"
)

type scanCountingReader struct {
	reader io.Reader
	bytes  int64
}

func (r *scanCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes += int64(n)
	return n, err
}

// Include endpoint diff blobs even when an added/modified path reuses an object
// reachable at Base. Both sides cover removed lines as well as new content;
// rev-list additionally covers intermediate commit history and tree names.
func scanCommitListing(ctx context.Context, repo, base, commit string) ([]byte, error) {
	history, err := scanGitOutput(ctx, repo, "rev-list", "--objects", base+".."+commit, "--")
	if err != nil {
		return nil, err
	}
	diff, err := scanGitOutput(ctx, repo, "diff-tree", "-r", "--no-commit-id", "--raw", "-z", "--no-abbrev", "--no-renames", base, commit, "--")
	if err != nil {
		return nil, err
	}
	parts := strings.Split(string(diff), "\x00")
	var listing strings.Builder
	listing.Write(history)
	for i := 0; i < len(parts)-1; i += 2 {
		fields := strings.Fields(parts[i])
		if len(fields) != 5 || !strings.HasPrefix(fields[0], ":") {
			return nil, errors.New("invalid git diff listing")
		}
		for _, id := range fields[2:4] {
			if strings.Trim(id, "0") == "" {
				continue
			}
			if !gitObjectID.MatchString(id) {
				return nil, errors.New("invalid git diff object")
			}
			// Newline names are still scanned directly; the listing uses the hash for
			// its line-safe display location in that case.
			path := parts[i+1]
			if strings.ContainsAny(path, "\r\n") {
				path = id
			}
			listing.WriteString(id + " " + path + "\n")
		}
	}
	return []byte(listing.String()), nil
}
