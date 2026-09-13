package wait

import (
	"bufio"
	"bytes"
	"encoding/json"
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

// Match structured identity fields, never session IDs quoted inside tool output.
func fileContainsSession(path, session string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		start := bytes.IndexByte(line, '{')
		if start < 0 {
			continue
		}
		var event struct {
			Provider         string `json:"provider"`
			ProviderThreadID string `json:"providerThreadId"`
			SessionID        string `json:"session_id"`
			Payload          struct {
				ProviderThreadID string `json:"providerThreadId"`
				SessionID        string `json:"session_id"`
				ThreadID         string `json:"threadId"`
			} `json:"payload"`
		}
		if json.Unmarshal(line[start:], &event) != nil {
			continue
		}
		if event.ProviderThreadID == session || event.SessionID == session ||
			event.Payload.ProviderThreadID == session || event.Payload.SessionID == session ||
			(event.Provider == "codex" && event.Payload.ThreadID == session) {
			return true
		}
	}
	return false
}
