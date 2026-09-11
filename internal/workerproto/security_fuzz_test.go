package workerproto

import (
	"bytes"
	"testing"
)

func FuzzProtocolCodecStrictBoundedDecode(f *testing.F) {
	f.Add([]byte(`{"version":"backlog.worker/v1"}`))
	f.Add([]byte(`{} {}`))
	f.Add(bytes.Repeat([]byte("x"), 257))
	f.Fuzz(func(t *testing.T, raw []byte) {
		var envelope Envelope
		_ = (Codec{MaxBytes: 256}).Decode(bytes.NewReader(raw), &envelope)
	})
}

func FuzzArtifactTarValidationIsBounded(f *testing.F) {
	f.Add([]byte("not a tar archive"))
	f.Add(make([]byte, 1024))
	f.Fuzz(func(t *testing.T, raw []byte) {
		_ = ValidateTarArchive(bytes.NewReader(raw), int64(len(raw)), ArchiveLimits{MaxEntries: 8, MaxBytes: 2048})
	})
}
