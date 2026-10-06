package workerruntime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/providercontainment"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type secretValueResolver interface {
	SecretValues(context.Context, []string) ([]string, error)
}

// SecretValues resolves only the declared execution references. Values stay in
// process memory and are used solely as exact-match scan canaries.
func (c EnvironmentCredentialChecker) SecretValues(ctx context.Context, references []string) ([]string, error) {
	if err := c.Require(ctx, references); err != nil {
		return nil, err
	}
	prefix := c.Prefix
	if prefix == "" {
		prefix = "T3_STEWARD_CREDENTIAL_"
	}
	var values []string
	for _, ref := range references {
		value, found, err := ResolveCredentialVariable(c.Lookup, prefix+CredentialEnvironmentName(ref))
		if err != nil || !found {
			return nil, errors.New("secret scan credential unavailable")
		}
		values = append(values, value)
	}
	return values, nil
}
func modelLoginCanaries(paths []string) ([]string, error) {
	var values []string
	var visit func(any)
	visit = func(v any) {
		switch item := v.(type) {
		case map[string]any:
			for key, value := range item {
				name := strings.ToLower(key)
				if str, ok := value.(string); ok && str != "" && (strings.Contains(name, "token") || strings.Contains(name, "key") || strings.Contains(name, "secret") || strings.Contains(name, "password") || name == "access" || name == "refresh") {
					values = append(values, str)
				} else {
					visit(value)
				}
			}
		case []any:
			for _, value := range item {
				visit(value)
			}
		}
	}
	for _, path := range paths {
		raw, err := readScanBounded(path, 1<<20)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, errors.New("secret scan model login read failed")
		}
		var login any
		if json.Unmarshal(raw, &login) != nil {
			return nil, errors.New("secret scan model login malformed")
		}
		visit(login)
	}
	return values, nil
}
func loginPaths(home string) []string {
	if home == "" {
		return nil
	}
	return []string{filepath.Join(home, ".codex", "auth.json"), filepath.Join(home, ".claude", ".credentials.json"), filepath.Join(home, ".local", "share", "opencode", "auth.json")}
}
func serviceScanCanaries(options WorkerServiceOptions, credentials ProtocolCredentials, journalRoot string) func(context.Context, workerproto.ExecutionPackage) ([]string, error) {
	return func(ctx context.Context, pkg workerproto.ExecutionPackage) ([]string, error) {
		values := []string{string(credentials.CoordinatorSecret), string(credentials.WorkerSecret)}
		if len(pkg.Environment.RequiredCredentials) > 0 {
			resolver, ok := options.ProjectCredentials.(secretValueResolver)
			if !ok {
				return nil, errors.New("secret scan credential resolver does not expose canaries")
			}
			resolved, err := resolver.SecretValues(ctx, pkg.Environment.RequiredCredentials)
			if err != nil {
				return nil, err
			}
			values = append(values, resolved...)
		}
		paths := options.ModelLoginFiles
		if paths == nil {
			home, _ := os.UserHomeDir()
			// Contained providers hold login material only in their assigned home.
			if len(pkg.Environment.DirectoryBindings) > 0 {
				manager := ContainedT3{Supervisor: providercontainment.Supervisor{Root: filepath.Join(journalRoot, "contained")}}
				record, err := manager.load(pkg)
				if err != nil {
					return nil, errors.New("secret scan contained home unavailable")
				}
				home = record.Launch.Spec.Home.Registration.Path
			}
			paths = loginPaths(home)
		}
		model, err := modelLoginCanaries(paths)
		if err != nil {
			return nil, err
		}
		values = append(values, model...)
		for _, name := range []string{"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN", "CLOUDFLARE_API_TOKEN", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
			if value, ok := os.LookupEnv(name); ok && value != "" {
				values = append(values, value)
			}
		}
		return values, nil
	}
}
