package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/privatefile"
)

// CoordinatorClientBootstrapPath is where UpKeeper writes this host's
// coordinator client settings, relative to the home directory.
const CoordinatorClientBootstrapPath = ".config/t3-steward/coordinator-client.json"

// coordinatorClientBootstrapLimit bounds the file. It carries a handful of
// scalars; anything larger is a sign that something else was written there.
const coordinatorClientBootstrapLimit = 64 * 1024

var coordinatorBootstrapName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var coordinatorBootstrapAddress = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._@:/-]{0,252}$`)
var coordinatorBootstrapCommand = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,252}$`)

// CoordinatorClientBootstrap is the UpKeeper-owned projection of this host's
// coordinator client. It exists so that adding a client to a host does not give
// the operator's own config.yaml a second author, which is the drift the fleet
// configuration component exists to remove.
//
// It confers no authority by itself: credential_ref is a reference whose value
// is resolved elsewhere, and an inline secret cannot arrive here because
// unknown fields are refused.
type CoordinatorClientBootstrap struct {
	SchemaVersion  int                               `json:"schema_version"`
	CoordinatorID  string                            `json:"coordinator_id"`
	Address        string                            `json:"address"`
	Connection     string                            `json:"connection"`
	RemoteCommand  string                            `json:"remote_command"`
	CredentialRef  string                            `json:"credential_ref"`
	RequestTimeout string                            `json:"request_timeout"`
	MessageLimits  *CoordinatorClientBootstrapLimits `json:"message_limits,omitempty"`
}

// CoordinatorClientBootstrapLimits are the two bounds the client needs. The
// file carries no file-count limit: that one follows the local configuration.
type CoordinatorClientBootstrapLimits struct {
	MaxBytes         int64 `json:"max_bytes"`
	MaxArtifactBytes int64 `json:"max_artifact_bytes"`
}

// DecodeCoordinatorClientBootstrap parses and validates the document. Unknown
// fields, trailing content and anything outside the admin credential namespace
// are refused, so neither a catalog nor an inline secret can arrive this way.
func DecodeCoordinatorClientBootstrap(raw []byte) (CoordinatorClientBootstrap, error) {
	var bootstrap CoordinatorClientBootstrap
	if len(raw) > coordinatorClientBootstrapLimit {
		return bootstrap, fmt.Errorf("coordinator client bootstrap exceeds %d KiB", coordinatorClientBootstrapLimit/1024)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bootstrap); err != nil {
		return bootstrap, fmt.Errorf("invalid coordinator client bootstrap: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return bootstrap, errors.New("coordinator client bootstrap has trailing content")
	}
	if bootstrap.SchemaVersion != 1 {
		return bootstrap, fmt.Errorf("coordinator client bootstrap schema_version must be 1 (got %d)", bootstrap.SchemaVersion)
	}
	if !coordinatorBootstrapName.MatchString(bootstrap.CoordinatorID) {
		return bootstrap, errors.New("coordinator client bootstrap coordinator_id is invalid")
	}
	if !coordinatorBootstrapAddress.MatchString(bootstrap.Address) {
		return bootstrap, errors.New("coordinator client bootstrap address is invalid")
	}
	if bootstrap.Connection != "ssh" {
		return bootstrap, fmt.Errorf("coordinator client bootstrap connection must be ssh (got %q)", bootstrap.Connection)
	}
	if !coordinatorBootstrapCommand.MatchString(bootstrap.RemoteCommand) {
		return bootstrap, errors.New("coordinator client bootstrap remote_command is invalid")
	}
	if err := validateAdminCredentialReference(bootstrap.CredentialRef); err != nil {
		return bootstrap, fmt.Errorf("coordinator client bootstrap: %w", err)
	}
	if _, err := time.ParseDuration(bootstrap.RequestTimeout); err != nil {
		return bootstrap, errors.New("coordinator client bootstrap request_timeout must be a duration such as 30s")
	}
	if bootstrap.MessageLimits != nil &&
		(bootstrap.MessageLimits.MaxBytes <= 0 || bootstrap.MessageLimits.MaxArtifactBytes <= 0) {
		return bootstrap, errors.New("coordinator client bootstrap message limits must be positive")
	}
	return bootstrap, nil
}

// Settings converts a decoded bootstrap into the same block an operator would
// have written in config.yaml, so that exactly one shape reaches validation.
func (b CoordinatorClientBootstrap) Settings() V2CoordinatorClient {
	timeout, _ := time.ParseDuration(b.RequestTimeout)
	client := V2CoordinatorClient{
		CoordinatorID:  b.CoordinatorID,
		Address:        b.Address,
		Connection:     b.Connection,
		RemoteCommand:  b.RemoteCommand,
		Credential:     b.CredentialRef,
		RequestTimeout: Duration(timeout),
	}
	if b.MessageLimits != nil {
		client.MessageLimits.MaxBytes = b.MessageLimits.MaxBytes
		client.MessageLimits.MaxArtifactBytes = b.MessageLimits.MaxArtifactBytes
	}
	return client
}

// LoadCoordinatorClientBootstrap reads the UpKeeper-owned file under home. It
// reports false when the file is absent, and an error when it exists but is not
// an owner-only 0600 regular file or does not validate.
func LoadCoordinatorClientBootstrap(home string) (CoordinatorClientBootstrap, bool, error) {
	path := filepath.Join(home, CoordinatorClientBootstrapPath)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return CoordinatorClientBootstrap{}, false, nil
	}
	raw, err := privatefile.Read(path, coordinatorClientBootstrapLimit)
	if err != nil {
		return CoordinatorClientBootstrap{}, false, fmt.Errorf("coordinator client bootstrap at %s requires a nonempty owner-only regular 0600 file: %w", path, err)
	}
	bootstrap, err := DecodeCoordinatorClientBootstrap(raw)
	if err != nil {
		return CoordinatorClientBootstrap{}, false, err
	}
	return bootstrap, true, nil
}

// applyCoordinatorClientBootstrap fills the client block from the UpKeeper-owned
// file when config.yaml declares none. An explicit block in config.yaml is an
// operator override and always wins, and the file is not even read then.
func (c *Config) applyCoordinatorClientBootstrap(home string) error {
	if c.BacklogV2.CoordinatorClient.Configured() || strings.TrimSpace(home) == "" {
		return nil
	}
	bootstrap, found, err := LoadCoordinatorClientBootstrap(home)
	if err != nil || !found {
		return err
	}
	c.BacklogV2.CoordinatorClient = bootstrap.Settings()
	return nil
}

// coordinatorClientHome is the home directory the loader consults. Tests
// replace it; nothing else does.
var coordinatorClientHome = func() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return home
}
