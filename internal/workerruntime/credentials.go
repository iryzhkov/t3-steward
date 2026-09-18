package workerruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"
)

// CredentialFileSuffix is appended to a credential variable's name to name a
// file that holds the value instead: T3_STEWARD_CREDENTIAL_<REF>_FILE=<path>.
// The inline variable wins when both are set. The file is read at use, one
// trailing newline is trimmed, and it is refused when missing or world-readable
// with an error that names the variable and the path but never the content.
const CredentialFileSuffix = "_FILE"

// ResolveCredentialVariable returns the value behind one credential variable:
// the variable itself when it is set and non-empty, otherwise the content of
// the file its _FILE companion names. found is false when neither is set.
func ResolveCredentialVariable(lookup func(string) (string, bool), name string) (value string, found bool, err error) {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	if inline, ok := lookup(name); ok && inline != "" {
		return inline, true, nil
	}
	variable := name + CredentialFileSuffix
	path, ok := lookup(variable)
	if !ok || path == "" {
		return "", false, nil
	}
	value, err = readCredentialFile(variable, path)
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

// readCredentialFile reads a credential file named by variable. It refuses a
// missing file, a symbolic link, anything but a regular file, and a file that
// is readable by others; the error names the variable and the path only.
func readCredentialFile(variable, path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("%s names %s, which does not exist", variable, path)
		}
		return "", fmt.Errorf("%s names %s, which cannot be inspected: %w", variable, path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%s names %s, which is a symbolic link; name the file itself", variable, path)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s names %s, which is not a regular file", variable, path)
	}
	if info.Mode().Perm()&0o004 != 0 {
		return "", fmt.Errorf("%s names %s, which is world-readable (mode %04o); chmod 0600 it", variable, path, info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("%s names %s, which cannot be read: %w", variable, path, err)
	}
	value := string(raw)
	value = strings.TrimSuffix(value, "\n")
	value = strings.TrimSuffix(value, "\r")
	if value == "" {
		return "", fmt.Errorf("%s names %s, which is empty", variable, path)
	}
	return value, nil
}

// CredentialChecker resolves credential references only for availability. Secret
// values never enter execution packages, journals, errors, or worker snapshots.
type CredentialChecker interface {
	Require(context.Context, []string) error
}

// EnvironmentCredentialChecker resolves references from a restricted worker
// environment. The prefix defaults to T3_STEWARD_CREDENTIAL_.
type EnvironmentCredentialChecker struct {
	Prefix string
	Lookup func(string) (string, bool)
}

func (c EnvironmentCredentialChecker) Require(ctx context.Context, references []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prefix := c.Prefix
	if prefix == "" {
		prefix = "T3_STEWARD_CREDENTIAL_"
	}
	lookup := c.Lookup
	if lookup == nil {
		lookup = os.LookupEnv
	}
	seen := make(map[string]struct{}, len(references))
	for _, reference := range references {
		if reference == "" {
			return errors.New("resolve credentials: empty reference")
		}
		if _, exists := seen[reference]; exists {
			continue
		}
		seen[reference] = struct{}{}
		name := prefix + CredentialEnvironmentName(reference)
		value, ok, err := ResolveCredentialVariable(lookup, name)
		if err != nil {
			return fmt.Errorf("resolve credentials: reference %q: %w", reference, err)
		}
		if !ok || value == "" {
			return fmt.Errorf("resolve credentials: reference %q is unavailable", reference)
		}
	}
	return nil
}

// ProtocolCredentials is the mutually authenticated identity material behind
// one configured worker credential reference.
type ProtocolCredentials struct {
	CoordinatorPrincipal string `json:"coordinatorPrincipal"`
	CoordinatorKeyID     string `json:"coordinatorKeyId"`
	CoordinatorSecret    []byte `json:"coordinatorSecret"`
	WorkerPrincipal      string `json:"workerPrincipal"`
	WorkerKeyID          string `json:"workerKeyId"`
	WorkerSecret         []byte `json:"workerSecret"`
}

// ProtocolCredentialResolver resolves a worker credential reference without
// placing its value in configuration, execution packages, or durable state.
type ProtocolCredentialResolver interface {
	ResolveProtocol(context.Context, string) (ProtocolCredentials, error)
}

// EnvironmentProtocolCredentialResolver expects a base64-bearing JSON bundle
// in the same restricted environment namespace as project credential checks.
type EnvironmentProtocolCredentialResolver struct {
	Prefix string
	Lookup func(string) (string, bool)
}

func (r EnvironmentProtocolCredentialResolver) ResolveProtocol(ctx context.Context, reference string) (ProtocolCredentials, error) {
	if err := ctx.Err(); err != nil {
		return ProtocolCredentials{}, err
	}
	if reference == "" {
		return ProtocolCredentials{}, errors.New("resolve protocol credentials: empty reference")
	}
	prefix := r.Prefix
	if prefix == "" {
		prefix = "T3_STEWARD_CREDENTIAL_"
	}
	lookup := r.Lookup
	if lookup == nil {
		lookup = os.LookupEnv
	}
	raw, ok, err := ResolveCredentialVariable(lookup, prefix+CredentialEnvironmentName(reference))
	if err != nil {
		return ProtocolCredentials{}, fmt.Errorf("resolve protocol credentials: reference %q: %w", reference, err)
	}
	if !ok || raw == "" {
		return ProtocolCredentials{}, fmt.Errorf("resolve protocol credentials: reference %q is unavailable", reference)
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var credentials ProtocolCredentials
	if err := decoder.Decode(&credentials); err != nil {
		return ProtocolCredentials{}, fmt.Errorf("resolve protocol credentials: reference %q is malformed", reference)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ProtocolCredentials{}, fmt.Errorf("resolve protocol credentials: reference %q has trailing content", reference)
	}
	if credentials.CoordinatorPrincipal == "" || credentials.CoordinatorKeyID == "" ||
		len(credentials.CoordinatorSecret) < 16 || credentials.WorkerPrincipal == "" ||
		credentials.WorkerKeyID == "" || len(credentials.WorkerSecret) < 16 {
		return ProtocolCredentials{}, fmt.Errorf("resolve protocol credentials: reference %q is incomplete", reference)
	}
	return credentials, nil
}

// CredentialEnvironmentName maps a credential reference onto the variable
// name that carries it under the T3_STEWARD_CREDENTIAL_ prefix: letters and
// digits upper-cased, everything else an underscore.
func CredentialEnvironmentName(reference string) string {
	var result strings.Builder
	for _, char := range reference {
		if unicode.IsLetter(char) || unicode.IsDigit(char) {
			result.WriteRune(unicode.ToUpper(char))
		} else {
			result.WriteByte('_')
		}
	}
	return result.String()
}
