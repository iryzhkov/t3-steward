package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func verifyAssets(dir string) error {
	manifest, err := os.Open(filepath.Join(dir, "checksums.txt"))
	if err != nil {
		return err
	}
	defer manifest.Close()
	expected := map[string]string{}
	scanner := bufio.NewScanner(manifest)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) < 67 || line[64:66] != "  " {
			return fmt.Errorf("invalid checksum line %q", line)
		}
		digest, name := line[:64], line[66:]
		bytes, err := hex.DecodeString(digest)
		if err != nil || len(bytes) != sha256.Size || name == "." || name == ".." || name == "checksums.txt" || strings.ContainsAny(name, "/\\") || expected[name] != "" {
			return fmt.Errorf("invalid checksum entry %q", name)
		}
		expected[name] = strings.ToLower(digest)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(expected) == 0 {
		return fmt.Errorf("empty checksums.txt")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "checksums.txt" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("asset %q is not a regular file", entry.Name())
		}
		want, ok := expected[entry.Name()]
		if !ok {
			return fmt.Errorf("asset %q missing from checksums.txt", entry.Name())
		}
		f, err := os.Open(filepath.Join(dir, entry.Name()))
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, err = io.Copy(hash, f)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if hex.EncodeToString(hash.Sum(nil)) != want {
			return fmt.Errorf("asset %q checksum differs", entry.Name())
		}
		delete(expected, entry.Name())
	}
	for name := range expected {
		return fmt.Errorf("asset %q missing from release", name)
	}
	return nil
}
