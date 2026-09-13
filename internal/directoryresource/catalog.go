package directoryresource

import (
	"fmt"
	"reflect"
)

// ValidateCatalog rejects ambiguous registrations before taking a detached copy.
func ValidateCatalog(approved []Binding) error {
	seen := map[string]bool{}
	for _, binding := range approved {
		if err := ValidateBinding(binding); err != nil {
			return err
		}
		key := binding.Identity.Registration.WorkerID + "\x00" + binding.Identity.Registration.ResourceID
		if seen[key] {
			return fmt.Errorf("duplicate directory registration")
		}
		seen[key] = true
	}
	return nil
}

// Authorize checks exact inspected identity and catalog revision, permitting only
// an access downgrade from an approved writer to a reader. Raw paths are never
// sufficient evidence of authorization.
func Authorize(approved, requested []Binding) error {
	if err := ValidateCatalog(approved); err != nil {
		return err
	}
	if err := ValidateCatalog(requested); err != nil {
		return err
	}
	for _, request := range requested {
		found := false
		for _, allowed := range approved {
			if reflect.DeepEqual(allowed.Identity, request.Identity) && (allowed.Access == ReadWrite || request.Access == ReadOnly) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("directory %q is not authorized by this catalog", request.Identity.Registration.ResourceID)
		}
	}
	return nil
}
