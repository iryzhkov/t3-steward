package workerruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// WorkerBootstrap is the U0 host projection. It confers no enrollment authority.
type WorkerBootstrap struct {
	SchemaVersion  int      `json:"schema_version"`
	WorkerID       string   `json:"worker_id"`
	CoordinatorID  string   `json:"coordinator_id"`
	Transport      string   `json:"transport"`
	Capabilities   []string `json:"capabilities"`
	ProviderRoutes []string `json:"provider_routes"`
	CredentialRef  string   `json:"credential_ref"`
}

var bootstrapName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var bootstrapItem = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*$`)

func DecodeWorkerBootstrap(raw []byte) (WorkerBootstrap, string, error) {
	var b WorkerBootstrap
	if len(raw) > 64*1024 {
		return b, "", errors.New("worker bootstrap exceeds 64 KiB")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&b); err != nil {
		return b, "", errors.New("invalid worker bootstrap")
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		return b, "", errors.New("worker bootstrap has trailing content")
	}
	if b.SchemaVersion != 1 || !bootstrapName.MatchString(b.WorkerID) || !bootstrapName.MatchString(b.CoordinatorID) || b.Transport != "ssh" || b.CredentialRef != "secretref:f02-protocol/"+b.WorkerID {
		return b, "", errors.New("worker bootstrap identity or transport is invalid")
	}
	for _, values := range [][]string{b.Capabilities, b.ProviderRoutes} {
		if values == nil || !slices.IsSorted(values) {
			return b, "", errors.New("worker bootstrap lists must be sorted arrays")
		}
		for i, v := range values {
			if !bootstrapItem.MatchString(v) || (i > 0 && values[i-1] == v) {
				return b, "", errors.New("worker bootstrap list is invalid")
			}
		}
	}
	if !slices.Contains(b.Capabilities, "git") || !slices.Contains(b.Capabilities, "huyang") {
		return b, "", errors.New("worker bootstrap requires git and huyang")
	}
	// Match the U0 canonical JSON key order, independent of Go struct layout.
	canonical, _ := json.MarshalIndent(map[string]any{"schema_version": b.SchemaVersion, "worker_id": b.WorkerID, "coordinator_id": b.CoordinatorID, "transport": b.Transport, "capabilities": b.Capabilities, "provider_routes": b.ProviderRoutes, "credential_ref": b.CredentialRef}, "", "  ")
	canonical = append(canonical, '\n')
	sum := sha256.Sum256(canonical)
	return b, hex.EncodeToString(sum[:]), nil
}

func LoadWorkerBootstrap(home string) (WorkerBootstrap, string, error) {
	path := filepath.Join(home, ".config/t3-steward/worker-bootstrap.json")
	raw, err := readPrivateFile(path, 64*1024)
	if err != nil {
		return WorkerBootstrap{}, "", fmt.Errorf("worker bootstrap unavailable: %w", err)
	}
	return DecodeWorkerBootstrap(raw)
}

// FileProtocolCredentialResolver reads only a target-local UpKeeper reference.
// Values and hashes never leave the authentication boundary.
type FileProtocolCredentialResolver struct{ Home string }

func (r FileProtocolCredentialResolver) ResolveProtocol(ctx context.Context, ref string) (ProtocolCredentials, error) {
	if err := ctx.Err(); err != nil {
		return ProtocolCredentials{}, err
	}
	const prefix = "secretref:f02-protocol/"
	if !strings.HasPrefix(ref, prefix) || !bootstrapName.MatchString(strings.TrimPrefix(ref, prefix)) {
		return ProtocolCredentials{}, errors.New("invalid protocol secret reference")
	}
	home := r.Home
	if home == "" {
		var err error
		home, err = os.UserHomeDir()
		if err != nil {
			return ProtocolCredentials{}, err
		}
	}
	root := filepath.Join(home, ".config/upkeeper/secrets")
	path := filepath.Join(root, strings.TrimPrefix(ref, "secretref:"))
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return ProtocolCredentials{}, errors.New("protocol secret store unavailable")
	}
	resolved, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return ProtocolCredentials{}, errors.New("protocol secret directory unavailable")
	}
	rel, err := filepath.Rel(resolvedRoot, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return ProtocolCredentials{}, errors.New("protocol secret escapes store")
	}
	raw, err := readPrivateFile(path, 16*1024)
	if err != nil {
		return ProtocolCredentials{}, errors.New("protocol secret requires a nonempty owner-only regular 0600 file")
	}
	resolver := EnvironmentProtocolCredentialResolver{Lookup: func(string) (string, bool) { return string(raw), true }}
	credentials, err := resolver.ResolveProtocol(ctx, ref)
	if err != nil {
		return ProtocolCredentials{}, err
	}
	if credentials.WorkerPrincipal != "ssh:"+strings.TrimPrefix(ref, prefix) {
		return ProtocolCredentials{}, errors.New("protocol secret worker principal mismatch")
	}
	return credentials, nil
}

// ProtocolResolver preserves the original restricted-environment seam for
// legacy workers while selecting the U0 resolver only for explicit references.
type ProtocolResolver struct{ Home string }

func (r ProtocolResolver) ResolveProtocol(ctx context.Context, ref string) (ProtocolCredentials, error) {
	if strings.HasPrefix(ref, "secretref:") {
		return FileProtocolCredentialResolver(r).ResolveProtocol(ctx, ref)
	}
	return (EnvironmentProtocolCredentialResolver{}).ResolveProtocol(ctx, ref)
}
