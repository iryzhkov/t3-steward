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
		name := prefix + credentialEnvironmentName(reference)
		value, ok := lookup(name)
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
	raw, ok := lookup(prefix + credentialEnvironmentName(reference))
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

func credentialEnvironmentName(reference string) string {
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
