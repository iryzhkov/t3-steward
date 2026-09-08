package t3api

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RuntimeState mirrors userdata/server-runtime.json, which the T3 server
// writes on start and removes on a clean shutdown.
type RuntimeState struct {
	Version   int    `json:"version"`
	PID       int    `json:"pid"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Origin    string `json:"origin"`
	DevURL    string `json:"devUrl"`
	StartedAt string `json:"startedAt"`
}

// UserDataDir returns <dataDir>/userdata.
func UserDataDir(dataDir string) string {
	return filepath.Join(dataDir, "userdata")
}

// ProviderLogDir returns the directory T3 writes provider event logs to.
func ProviderLogDir(dataDir string) string {
	return filepath.Join(UserDataDir(dataDir), "logs", "provider")
}

// RuntimeStatePath returns the server-runtime.json path.
func RuntimeStatePath(dataDir string) string {
	return filepath.Join(UserDataDir(dataDir), "server-runtime.json")
}

// ReadRuntimeState reads server-runtime.json. It returns os.ErrNotExist
// when the server has not written one.
func ReadRuntimeState(dataDir string) (*RuntimeState, error) {
	raw, err := os.ReadFile(RuntimeStatePath(dataDir))
	if err != nil {
		return nil, err
	}
	var st RuntimeState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("decode %s: %w", RuntimeStatePath(dataDir), err)
	}
	if st.Origin == "" {
		return nil, errors.New("server-runtime.json has no origin")
	}
	return &st, nil
}

// DiscoverURL resolves the server base URL: the explicit URL when given,
// otherwise the origin recorded in server-runtime.json.
func DiscoverURL(explicit, dataDir string) (string, error) {
	if strings.TrimSpace(explicit) != "" {
		return strings.TrimRight(strings.TrimSpace(explicit), "/"), nil
	}
	st, err := ReadRuntimeState(dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("no T3 server URL configured and %s does not exist (is the T3 server running?)", RuntimeStatePath(dataDir))
		}
		return "", err
	}
	origin := strings.TrimRight(st.Origin, "/")
	// A wildcard bind is recorded as 127.0.0.1 already; keep whatever the
	// server wrote so that a custom host works too.
	return origin, nil
}
