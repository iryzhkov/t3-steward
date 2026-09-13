// Package directoryresource defines fenced host directory identities.
// It does not grant provider access or make existing-directory capsules runnable.
package directoryresource

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

type Access string

const (
	ReadOnly  Access = "read-only"
	ReadWrite Access = "read-write"
)

// Registration is operator-owned. Paths must already be canonical; inspection
// never creates, chmods or otherwise mutates source directories.
type Registration struct {
	WorkerID   string `json:"workerId"`
	ResourceID string `json:"resourceId"`
	Revision   string `json:"revision"`
	Path       string `json:"path"`
	Writable   bool   `json:"writable,omitempty"`
}

// Object identifies an inode independently of bind-mount aliases. Birth time
// prevents an inode reused after deletion from matching an old registration.
type Object struct {
	Device       uint64 `json:"device"`
	Inode        uint64 `json:"inode"`
	BirthSeconds int64  `json:"birthSeconds"`
	BirthNanos   uint32 `json:"birthNanos"`
}

type Identity struct {
	Registration Registration `json:"registration"`
	Object       Object       `json:"object"`
	MountID      uint64       `json:"mountId"`
	Ancestors    []Object     `json:"ancestors"`
}

type Binding struct {
	Identity Identity `json:"identity"`
	Access   Access   `json:"access"`
}

func (r Registration) Validate() error {
	if strings.TrimSpace(r.WorkerID) == "" || strings.TrimSpace(r.ResourceID) == "" || strings.TrimSpace(r.Revision) == "" {
		return errors.New("directory resource requires worker, resource and revision")
	}
	if !filepath.IsAbs(r.Path) || filepath.Clean(r.Path) != r.Path || r.Path == "/" {
		return errors.New("directory resource requires a canonical absolute non-root path")
	}
	return nil
}

func Bind(identity Identity, requested Access) (Binding, error) {
	if err := identity.Registration.Validate(); err != nil {
		return Binding{}, err
	}
	if identity.Object.Inode == 0 || identity.MountID == 0 || len(identity.Ancestors) == 0 {
		return Binding{}, errors.New("directory resource identity evidence is incomplete")
	}
	if requested == "" {
		requested = ReadOnly
	}
	if requested != ReadOnly && requested != ReadWrite {
		return Binding{}, fmt.Errorf("invalid directory access %q", requested)
	}
	if requested == ReadWrite && !identity.Registration.Writable {
		return Binding{}, errors.New("registered directory does not permit writable access")
	}
	identity.Ancestors = append([]Object(nil), identity.Ancestors...)
	return Binding{Identity: identity, Access: requested}, nil
}

// Conflicts is conservative for invalid access. Callers must validate bindings
// before admission. Readers coexist; any writer excludes overlapping readers
// and writers on the same host. Ancestors detect aliases of containing roots.
func Conflicts(a, b Binding) bool {
	if a.Identity.Registration.WorkerID != b.Identity.Registration.WorkerID {
		return false
	}
	if a.Access == ReadOnly && b.Access == ReadOnly {
		return false
	}
	ap, bp := a.Identity.Registration.Path, b.Identity.Registration.Path
	if ap == bp || strings.HasPrefix(ap, bp+"/") || strings.HasPrefix(bp, ap+"/") {
		return true
	}
	if a.Identity.Object == b.Identity.Object {
		return true
	}
	for _, p := range a.Identity.Ancestors {
		if p == b.Identity.Object {
			return true
		}
	}
	for _, p := range b.Identity.Ancestors {
		if p == a.Identity.Object {
			return true
		}
	}
	return false
}
