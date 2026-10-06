package backlog

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"
)

// packEntry is one object of a hand-built pack: a whole object, or a
// REF_DELTA (type 7) naming its base by object ID.
type packEntry struct {
	kind int
	base []byte
	data []byte
}

func buildPack(t *testing.T, entries []packEntry) []byte {
	t.Helper()
	var pack bytes.Buffer
	pack.WriteString("PACK")
	_ = binary.Write(&pack, binary.BigEndian, uint32(2))
	_ = binary.Write(&pack, binary.BigEndian, uint32(len(entries)))
	for _, entry := range entries {
		size := len(entry.data)
		header := byte(entry.kind<<4) | byte(size&0x0f)
		size >>= 4
		for size > 0 {
			pack.WriteByte(header | 0x80)
			header = byte(size & 0x7f)
			size >>= 7
		}
		pack.WriteByte(header)
		pack.Write(entry.base)
		compressed := zlib.NewWriter(&pack)
		if _, err := compressed.Write(entry.data); err != nil {
			t.Fatal(err)
		}
		if err := compressed.Close(); err != nil {
			t.Fatal(err)
		}
	}
	sum := sha1.Sum(pack.Bytes())
	pack.Write(sum[:])
	return pack.Bytes()
}

func gitObjectID(kind string, data []byte) []byte {
	sum := sha1.Sum(append([]byte(fmt.Sprintf("%s %d\x00", kind, len(data))), data...))
	return sum[:]
}

// A delta whose base is in the pack can be checked without any prerequisite,
// so a broken one must be refused even though the pack is otherwise intact
// and holds the declared commit.
func TestExportCommitBundleRefusesBrokenInPackDelta(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	p := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{})
	gitRun(t, repository, "fetch", "--quiet", storage+"/"+p.bundle.StoragePath, p.provenance.Ref+":refs/heads/declared")
	commit := []byte(gitOutput(t, repository, "cat-file", "commit", p.commit) + "\n")
	if hex.EncodeToString(gitObjectID("commit", commit)) != p.commit {
		t.Fatal("commit fixture does not hash to the declared commit")
	}
	base := []byte("short\n")
	// Delta: base size 6, result size 10, then copy 10 bytes from offset 200.
	broken := []byte{6, 10, 0x80 | 0x01 | 0x10, 200, 10}
	valid := []byte{6, 6, 0x80 | 0x10, 6}
	header := fmt.Sprintf("# v2 git bundle\n-%s campaign base\n%s %s\n\n", p.base, p.commit, p.provenance.Ref)
	provenance := p.provenance
	provenance.Bundle = nil
	for name, delta := range map[string][]byte{"valid": valid, "broken": broken} {
		t.Run(name, func(t *testing.T) {
			pack := buildPack(t, []packEntry{{kind: 1, data: commit}, {kind: 3, data: base}, {kind: 7, base: gitObjectID("blob", base), data: delta}})
			b, err := ExportCommitBundle(context.Background(), provenance, "delta", bytes.NewReader(append([]byte(header), pack...)), DefaultCommitBundleMaxBytes)
			if name == "valid" {
				if err != nil {
					t.Fatalf("refused a pack with a valid in-pack delta: %v", err)
				}
				b.Close()
				return
			}
			if err == nil {
				b.Close()
				t.Fatal("accepted a pack whose in-pack delta cannot be applied")
			}
		})
	}
}
