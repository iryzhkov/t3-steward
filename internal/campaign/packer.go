package campaign

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// archiveMode is the mode every archive entry carries. The mode a file happens
// to have on the author's machine is not part of the campaign: ingestion
// rewrites permissions when it stores the file, so inheriting them would only
// make two identical campaigns produce two different archives.
const archiveMode = 0o444

// archiveModTime is the timestamp every archive entry carries. A campaign has
// no meaningful modification time of its own, and inheriting one would make
// the digest depend on when a file was copied rather than on what it contains.
var archiveModTime = time.Unix(0, 0).UTC()

// Bundle is one campaign packaged for submission.
type Bundle struct {
	// Campaign is the loaded campaign these bytes were built from.
	Campaign *Campaign
	// Archive is the uncompressed tar stream to submit, exactly as is.
	Archive []byte
	// ArchiveSHA256 identifies those bytes. Two campaign directories with
	// identical content produce the same value on any filesystem and in any
	// creation order.
	ArchiveSHA256 string
	// ContentDigest is the digest the coordinator will record for this
	// submission. It is derived from the same inventory and the same bytes,
	// so an authoring command can show before submission the digest the
	// submission response will return.
	ContentDigest string
}

// Prepare loads a campaign directory and packs it in one step.
func Prepare(reference string, limits Limits) (Bundle, error) {
	loaded, err := Load(reference, limits)
	if err != nil {
		return Bundle{}, err
	}
	return loaded.Pack()
}

// Pack builds the deterministic uncompressed tar stream for a loaded campaign.
//
// Determinism is a correctness property rather than a tidiness one: the
// idempotency guarantee is that the same key with the same content returns the
// same run and that the same key with different content is refused, and
// neither survives if one directory can produce two different digests. Entry
// order is the inventory order, each path is spelled once, and mode, ownership
// and timestamps are fixed instead of inherited.
//
// The archive is built in memory and never written to a temporary file. It
// cannot outgrow the coordinator's configured archive limit, which is small
// enough to hold, and nothing is left behind when packing fails because
// nothing was created outside the process.
func (c *Campaign) Pack() (Bundle, error) {
	if c == nil {
		return Bundle{}, errors.New("campaign: a loaded campaign is required")
	}
	if err := c.Limits.validate(); err != nil {
		return Bundle{}, err
	}
	if len(c.Files) == 0 {
		return Bundle{}, errors.New("campaign: nothing to pack")
	}
	buffer := bytes.NewBuffer(make([]byte, 0, estimateArchiveSize(c.Files)))
	writer := tar.NewWriter(buffer)
	content := backlog.NewSubmissionDigest()
	for _, file := range c.Files {
		header := &tar.Header{
			Typeflag: tar.TypeReg,
			Name:     file.Path,
			Size:     file.Size,
			Mode:     archiveMode,
			ModTime:  archiveModTime,
			Uid:      0,
			Gid:      0,
			Uname:    "",
			Gname:    "",
			Format:   tar.FormatPAX,
		}
		if err := writer.WriteHeader(header); err != nil {
			return Bundle{}, fmt.Errorf("campaign: write archive entry %q: %w", file.Path, err)
		}
		// The digest is framed by the coordinator's own implementation, so a
		// predicted digest cannot drift from the recorded one. The content is
		// streamed into it while it streams into the archive, so no file is
		// read twice.
		digestWriter, endEntry := content.File(file.Path)
		if err := c.writeEntry(writer, digestWriter, file); err != nil {
			return Bundle{}, fmt.Errorf("campaign: file %q: %w", file.Path, err)
		}
		endEntry()
	}
	if err := writer.Close(); err != nil {
		return Bundle{}, fmt.Errorf("campaign: finish archive: %w", err)
	}
	archive := buffer.Bytes()
	if int64(len(archive)) > c.Limits.MaxBytes {
		return Bundle{}, fmt.Errorf("campaign: archive of %d bytes exceeds the limit of %d", len(archive), c.Limits.MaxBytes)
	}
	// Check the finished bytes with the validator the coordinator runs on
	// receipt, so an archive this package can build but the coordinator would
	// refuse fails here instead of on submission.
	limits := workerproto.ArchiveLimits{MaxEntries: c.Limits.MaxFiles, MaxBytes: c.Limits.MaxBytes}
	if err := workerproto.ValidateTarArchive(bytes.NewReader(archive), int64(len(archive)), limits); err != nil {
		return Bundle{}, fmt.Errorf("campaign: %w", err)
	}
	sum := sha256.Sum256(archive)
	return Bundle{
		Campaign:      c,
		Archive:       archive,
		ArchiveSHA256: hex.EncodeToString(sum[:]),
		ContentDigest: content.Sum(),
	}, nil
}

// writeEntry streams one file into the archive and refuses to pack content
// that no longer matches what validation accepted. A file that was rewritten,
// truncated, extended or replaced between loading and packing is reported;
// none of it reaches the archive, because the archive is only returned when
// every entry was written whole.
func (c *Campaign) writeEntry(archive io.Writer, content io.Writer, file File) error {
	if err := refuseSymbolicLinks(c.Root, file.Path); err != nil {
		return err
	}
	handle, err := os.Open(filepath.Join(c.Root, filepath.FromSlash(file.Path)))
	if err != nil {
		return err
	}
	defer handle.Close()
	info, err := handle.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("referenced path is not a regular file")
	}
	digest := sha256.New()
	written, err := io.Copy(io.MultiWriter(archive, content, digest), io.LimitReader(handle, file.Size))
	if err != nil {
		return err
	}
	if written != file.Size {
		return fmt.Errorf("file changed after validation: %d of %d bytes", written, file.Size)
	}
	var extra [1]byte
	if read, _ := handle.Read(extra[:]); read != 0 {
		return fmt.Errorf("file grew after validation: more than %d bytes", file.Size)
	}
	if hex.EncodeToString(digest.Sum(nil)) != file.SHA256 {
		return errors.New("file content changed after validation")
	}
	return nil
}

// estimateArchiveSize sizes the buffer for one tar header per file, the
// padding tar adds to each entry, and the two empty blocks that end a stream.
func estimateArchiveSize(files []File) int {
	const block = 512
	total := 2 * block
	for _, file := range files {
		total += block + int((file.Size+block-1)/block)*block
	}
	return total
}
