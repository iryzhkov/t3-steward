package providercontainment

import (
	"context"
	"io"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
)

// Spec is an operator-owned launch contract, never a capsule-provided command.
// Home and Workspace are pre-provisioned owned directories; dataset identities
// are resolved from the approved catalog. Runtime roots are read-only at /runtime/N.
type Spec struct {
	WorkerID      string                      `json:"workerId"`
	Home          directoryresource.Identity  `json:"home"`
	Workspace     directoryresource.Identity  `json:"workspace"`
	Directories   []directoryresource.Binding `json:"directories,omitempty"`
	RuntimePaths  []string                    `json:"runtimePaths,omitempty"`
	ProviderHosts []string                    `json:"providerHosts,omitempty"`
	Command       []string                    `json:"command"`
	// Cwd is /workspace or exactly /data/N for direct existing-directory use.
	Cwd string `json:"cwd,omitempty"`
}

type Streams struct {
	Stdin          io.Reader
	Stdout, Stderr io.Writer
}

func Run(ctx context.Context, spec Spec, streams Streams) error { return run(ctx, spec, streams) }
