package backlogadmin

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/privatefile"
)

// clientName is the shape of the client part of an admin credential
// reference. It is the same shape the worker bootstrap accepts, so that one
// secret store cannot hold two spellings of the same host.
var clientName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// FileAdminCredentialResolver reads an admin credential from the host's own
// secret store, the same 0600 store under ~/.config/upkeeper/secrets that the
// worker protocol credential comes from.
//
// This is how an admin client is actually provisioned on these machines:
// UpKeeper distributes the opaque reference and the host secret authority puts
// the value in the store. Requiring the value in the environment instead would
// mean every `t3-steward campaign submit` had to carry a secret in its
// invocation, which is how secrets end up in shell profiles and process lists.
type FileAdminCredentialResolver struct{ Home string }

func (r FileAdminCredentialResolver) ResolveAdmin(reference string) (AdminCredentials, error) {
	if err := ValidateAdminCredentialReference(reference); err != nil {
		return AdminCredentials{}, err
	}
	client := strings.TrimPrefix(reference, AdminCredentialPrefix)
	if !clientName.MatchString(client) {
		return AdminCredentials{}, errors.New("invalid admin credential reference")
	}
	home := r.Home
	if home == "" {
		var err error
		home, err = os.UserHomeDir()
		if err != nil {
			return AdminCredentials{}, err
		}
	}
	root := filepath.Join(home, ".config/upkeeper/secrets")
	path := filepath.Join(root, strings.TrimPrefix(reference, "secretref:"))
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return AdminCredentials{}, errors.New("admin secret store unavailable")
	}
	resolved, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return AdminCredentials{}, errors.New("admin secret directory unavailable")
	}
	relative, err := filepath.Rel(resolvedRoot, resolved)
	if err != nil || relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return AdminCredentials{}, errors.New("admin secret escapes store")
	}
	raw, err := privatefile.Read(path, 16*1024)
	if err != nil {
		return AdminCredentials{}, errors.New(
			"admin secret requires a nonempty owner-only regular 0600 file")
	}
	decoder := EnvironmentAdminCredentialResolver{
		Lookup: func(string) (string, bool) { return string(raw), true },
	}
	return decoder.ResolveAdmin(reference)
}

// AdminResolver selects the secret store for an explicit reference and keeps
// the restricted-environment seam for anything else, mirroring the worker
// protocol resolver so that the two credential paths cannot diverge in how
// they treat a reference.
type AdminResolver struct{ Home string }

func (r AdminResolver) ResolveAdmin(reference string) (AdminCredentials, error) {
	if strings.HasPrefix(reference, "secretref:") {
		return FileAdminCredentialResolver(r).ResolveAdmin(reference)
	}
	return EnvironmentAdminCredentialResolver{}.ResolveAdmin(reference)
}
