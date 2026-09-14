package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"time"

	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
	"github.com/iryzhkov/t3-steward/internal/t3api"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// ContainedAttachment is persisted by preparation only after the dedicated
// server is authenticated. It is stored in the supervisor journal, outside all
// provider mounts. No token is stored here.
type ContainedAttachment struct {
	WorkerID      string                        `json:"workerId"`
	Identity      workerproto.ExecutionIdentity `json:"identity"`
	Launch        providercontainment.Launch    `json:"launch"`
	InvocationID  string                        `json:"invocationId"`
	EnvironmentID string                        `json:"environmentId"`
}

// ContainedT3 attaches to durable, already-running execution supervisors.
// There is intentionally no Start callback: incomplete preparation, missing
// services and uncertain identities never authorize a replacement execution.
type ContainedT3 struct {
	Profile    *ContainedProfile
	Supervisor providercontainment.Supervisor
	Timeout    time.Duration
	observe    func(context.Context, providercontainment.Launch) (providercontainment.SupervisorObservation, error)
}

func (p ContainedT3) observation(ctx context.Context, r ContainedAttachment) error {
	var obs providercontainment.SupervisorObservation
	var err error
	if p.observe != nil {
		obs, err = p.observe(ctx, r.Launch)
	} else {
		obs, err = p.Supervisor.Observe(ctx, r.Launch)
	}
	if err != nil {
		return err
	}
	if obs.Stopped || obs.State != "active/running" || obs.InvocationID == "" || obs.InvocationID != r.InvocationID {
		return errors.New("contained supervisor invocation is unavailable or changed")
	}
	if r.Launch.Spec.Workspace.Registration.Path != "" {
		identities := []directoryresource.Identity{r.Launch.Spec.Home, r.Launch.Spec.Workspace}
		for _, binding := range r.Launch.Spec.Directories {
			identities = append(identities, binding.Identity)
		}
		for _, identity := range []*directoryresource.Identity{r.Launch.Spec.Inputs, r.Launch.Spec.Dependencies} {
			if identity != nil {
				identities = append(identities, *identity)
			}
		}
		for _, identity := range identities {
			file, err := directoryresource.Reopen(identity, identity.Registration)
			if err != nil {
				return err
			}
			file.Close()
		}
	}
	return nil
}

func (p ContainedT3) recordPath(pkg workerproto.ExecutionPackage) (string, error) {
	root := p.Supervisor.Root
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" {
		return "", errors.New("contained attachment requires private supervisor storage")
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 || resolved != root {
		return "", errors.New("contained attachment storage must be canonical and private")
	}
	if pkg.WorkerID == "" || pkg.Identity.ThreadID == "" || pkg.Identity.AssignmentID == "" {
		return "", errors.New("contained attachment requires execution identity")
	}
	key := sha256.Sum256([]byte(pkg.WorkerID + "\x00" + pkg.Identity.ThreadID))
	return filepath.Join(root, "attachment-"+hex.EncodeToString(key[:])+".json"), nil
}

func validateAttachment(pkg workerproto.ExecutionPackage, r ContainedAttachment) error {
	if r.WorkerID != pkg.WorkerID || r.Identity != pkg.Identity ||
		r.Launch.ExecutionID != pkg.Identity.ThreadID || r.Launch.Spec.WorkerID != pkg.WorkerID ||
		r.InvocationID == "" || r.EnvironmentID == "" || r.Launch.Spec.Control == nil ||
		len(pkg.Environment.DirectoryBindings) == 0 ||
		!reflect.DeepEqual(r.Launch.Spec.Directories, pkg.Environment.DirectoryBindings) {
		return errors.New("contained attachment does not match execution package")
	}
	return nil
}

func (p ContainedT3) load(pkg workerproto.ExecutionPackage) (ContainedAttachment, error) {
	path, err := p.recordPath(pkg)
	if err != nil {
		return ContainedAttachment{}, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return ContainedAttachment{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > 1<<20 {
		return ContainedAttachment{}, errors.New("invalid contained attachment storage")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ContainedAttachment{}, err
	}
	var record ContainedAttachment
	if err := json.Unmarshal(data, &record); err != nil {
		return record, err
	}
	return record, validateAttachment(pkg, record)
}

func (p ContainedT3) client(ctx context.Context, r ContainedAttachment) (*t3api.Client, error) {
	if err := p.observation(ctx, r); err != nil {
		return nil, err
	}
	control := *r.Launch.Spec.Control
	// The transport revalidates and pins this identity for every connection and
	// token read. It never resolves a provider-controlled socket symlink.
	client, err := t3api.NewContained(control, p.Timeout)
	if err != nil {
		return nil, err
	}
	client.HTTP.Transport = attachmentTransport{next: client.HTTP.Transport, check: func(ctx context.Context) error { return p.observation(ctx, r) }}
	descriptor, err := client.Descriptor(ctx)
	if err != nil {
		return nil, err
	}
	if descriptor.EnvironmentID != r.EnvironmentID {
		return nil, errors.New("contained T3 environment identity changed")
	}
	return client, nil
}

// Remember commits a preparation receipt without overwriting another identity.
// It does not start a service or dispatch a provider turn. Authentication and
// supervisor identity must both be confirmed before publishing the receipt.
func (p ContainedT3) Remember(ctx context.Context, pkg workerproto.ExecutionPackage, r ContainedAttachment) error {
	if err := validateAttachment(pkg, r); err != nil {
		return err
	}
	path, err := p.recordPath(pkg)
	if err != nil {
		return err
	}
	// Supervisor.Observe also verifies its journal is outside all sandbox mounts.
	client, err := p.client(ctx, r)
	if err != nil {
		return err
	}
	session, err := client.SessionInfo(ctx)
	if err != nil {
		return err
	}
	if !session.Authenticated {
		return errors.New("contained T3 authentication unproven")
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return errors.New("contained attachment too large")
	}
	tmp, err := os.CreateTemp(p.Supervisor.Root, ".attachment-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, writeErr := tmp.Write(data)
	syncErr := tmp.Sync()
	closeErr := tmp.Close()
	if err = errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	// Hard-link publication is atomic and refuses replacement. A concurrent or
	// repeated identical receipt is harmless; a different one is recovery-required.
	if err = os.Link(tmp.Name(), path); errors.Is(err, os.ErrExist) {
		existing, loadErr := p.load(pkg)
		if loadErr != nil {
			return loadErr
		}
		if !reflect.DeepEqual(existing, r) {
			return errors.New("contained attachment already bound to another invocation")
		}
	} else if err != nil {
		return err
	}
	directory, err := os.Open(p.Supervisor.Root)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (p ContainedT3) Attach(ctx context.Context, pkg workerproto.ExecutionPackage) (T3Control, error) {
	_, planErr := p.preparation(pkg)
	if p.Profile != nil || !errors.Is(planErr, os.ErrNotExist) {
		if retained, err := p.stoppedControl(ctx, pkg); err != nil {
			return nil, err
		} else if retained != nil {
			return retained, nil
		}
	}
	r, err := p.load(pkg)
	if err != nil {
		return nil, fmt.Errorf("contained preparation receipt unavailable: %w", err)
	}
	client, err := p.client(ctx, r)
	if err != nil {
		return nil, err
	}
	return t3control.New(client, nil, false), nil
}

// Check around each request: a service disappearing during an effect produces
// uncertainty, never an accepted response from a different invocation.
type attachmentTransport struct {
	next  http.RoundTripper
	check func(context.Context) error
}

func (t attachmentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := t.check(req.Context()); err != nil {
		return nil, err
	}
	response, err := t.next.RoundTrip(req)
	if err != nil {
		return response, err
	}
	if err = t.check(req.Context()); err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		return nil, err
	}
	return response, nil
}
