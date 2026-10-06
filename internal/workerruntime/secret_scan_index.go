package workerruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// canaryIndex finds every canary variant and history signature in one pass,
// so the cost of a scan does not grow with the number of credentials. Each
// candidate is keyed by its first four bytes; a bitset of hashed keys rejects
// almost every position before the candidate map is consulted.
type canaryIndex struct {
	bits       []uint64
	candidates map[uint32][]canaryCandidate
	size       int
}

type canaryCandidate struct {
	value       []byte // exact current variant; nil for a history signature
	length      int
	digest      [32]byte
	fingerprint string
	base64      bool
}

const canaryIndexBits = 20

func canaryKeyHash(key uint32) uint32 { return (key * 0x9E3779B1) >> (32 - canaryIndexBits) }

func (s *resultScanner) canaryIndex() *canaryIndex {
	size := len(s.canaries) + len(s.history)
	if s.index != nil && s.index.size == size {
		return s.index
	}
	index := &canaryIndex{bits: make([]uint64, 1<<canaryIndexBits/64), candidates: map[uint32][]canaryCandidate{}, size: size}
	add := func(prefix []byte, candidate canaryCandidate) {
		key := binary.LittleEndian.Uint32(prefix)
		h := canaryKeyHash(key)
		index.bits[h/64] |= 1 << (h % 64)
		index.candidates[key] = append(index.candidates[key], candidate)
	}
	for _, c := range s.canaries {
		add(c.value, canaryCandidate{value: c.value, length: len(c.value), fingerprint: c.fingerprint, base64: c.base64})
	}
	for _, c := range s.history {
		candidate := canaryCandidate{length: c.Length, fingerprint: c.Fingerprint, base64: c.Base64}
		if digest, err := hex.DecodeString(c.SHA256); err == nil && len(digest) == 32 && len(c.Prefix) == 4 {
			copy(candidate.digest[:], digest)
			add(c.Prefix, candidate)
		}
	}
	s.index = index
	return index
}

// rawCanaryMatch finds the earliest exact canary; base64Only restricts it to
// encoded variants, for the view with line breaks removed.
func (s *resultScanner) rawCanaryMatch(data []byte, base64Only bool) (int, int, string) {
	index := s.canaryIndex()
	if index.size == 0 {
		return -1, 0, ""
	}
	for i := 0; i+4 <= len(data); i++ {
		key := binary.LittleEndian.Uint32(data[i:])
		h := canaryKeyHash(key)
		if index.bits[h/64]&(1<<(h%64)) == 0 {
			continue
		}
		for _, c := range index.candidates[key] {
			if base64Only && !c.base64 || i+c.length > len(data) {
				continue
			}
			if c.value != nil {
				if bytes.Equal(data[i:i+c.length], c.value) {
					return i, i + c.length, c.fingerprint
				}
			} else if sha256.Sum256(data[i:i+c.length]) == c.digest {
				return i, i + c.length, c.fingerprint
			}
		}
	}
	return -1, 0, ""
}
