package wait

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type logEntry struct {
	name    string
	path    string
	modTime time.Time
	age     time.Duration
}

func readDir(dir string) ([]logEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	var out []logEntry
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "events.") || !strings.Contains(name, ".log") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, logEntry{name: name, path: filepath.Join(dir, name), modTime: info.ModTime(), age: now.Sub(info.ModTime())})
	}
	return out, nil
}

// fileContains scans a file line by line for any of the needles.
func fileContains(path string, needles ...[]byte) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	for {
		line, err := r.ReadSlice('\n')
		for _, n := range needles {
			if bytes.Contains(line, n) {
				return true
			}
		}
		if err != nil {
			return false
		}
	}
}
