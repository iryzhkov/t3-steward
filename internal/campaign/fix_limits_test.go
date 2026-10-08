package campaign

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestWriteFixChainUsesCallerLimits(t *testing.T) {
	source, options := fixFixture()
	options.Brief = append(options.Brief, CompiledFile{Path: "inputs/fix/brief/inputs/large.txt", Content: bytes.Repeat([]byte("x"), int(DefaultLimits.MaxBytes)+1)})
	unit, err := GenerateFixChain(source, options)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "chain")
	limits := Limits{MaxFiles: DefaultLimits.MaxFiles, MaxBytes: 2 * DefaultLimits.MaxBytes}
	if _, err := WriteFixChain(target, unit, limits); err != nil {
		t.Fatal(err)
	}
	if _, err := Prepare(target, limits); err != nil {
		t.Fatal(err)
	}
}
