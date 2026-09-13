package directoryresource

import (
	"fmt"
	"strings"
)

// Request names operator evidence, never a path supplied by a capsule.
type Request struct {
	WorkerID   string `yaml:"worker" json:"worker"`
	ResourceID string `yaml:"resource" json:"resource"`
	Revision   string `yaml:"revision" json:"revision"`
	Access     Access `yaml:"access,omitempty" json:"access,omitempty"`
}

func ValidateRequests(requests []Request) error {
	seen := map[string]bool{}
	worker := ""
	for _, r := range requests {
		for _, id := range []string{r.WorkerID, r.ResourceID, r.Revision} {
			if id == "" || strings.TrimSpace(id) != id || strings.ContainsAny(id, "\x00\r\n") {
				return fmt.Errorf("directory request requires trimmed worker, resource and revision")
			}
		}
		if r.Access != "" && r.Access != ReadOnly && r.Access != ReadWrite {
			return fmt.Errorf("invalid directory request access %q", r.Access)
		}
		if worker != "" && worker != r.WorkerID {
			return fmt.Errorf("one task cannot use directories on different workers")
		}
		worker = r.WorkerID
		if seen[r.ResourceID] {
			return fmt.Errorf("duplicate directory request %q", r.ResourceID)
		}
		seen[r.ResourceID] = true
	}
	return nil
}

// Resolve copies exact approved identities, with read-only as the default.
// Authorization is repeated during package construction against the worker catalog.
func Resolve(approved []Binding, requests []Request) ([]Binding, error) {
	if err := ValidateCatalog(approved); err != nil {
		return nil, err
	}
	if err := ValidateRequests(requests); err != nil {
		return nil, err
	}
	var result []Binding
	for _, r := range requests {
		found := false
		for _, b := range approved {
			reg := b.Identity.Registration
			if reg.WorkerID != r.WorkerID || reg.ResourceID != r.ResourceID || reg.Revision != r.Revision {
				continue
			}
			resolved, err := Bind(b.Identity, r.Access)
			if err != nil {
				return nil, err
			}
			if err := Authorize([]Binding{b}, []Binding{resolved}); err != nil {
				return nil, err
			}
			result = append(result, resolved)
			found = true
			break
		}
		if !found {
			return nil, fmt.Errorf("directory %q on worker %q at revision %q is not registered for this project", r.ResourceID, r.WorkerID, r.Revision)
		}
	}
	return result, nil
}
