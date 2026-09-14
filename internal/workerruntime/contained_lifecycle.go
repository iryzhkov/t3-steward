package workerruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
	"github.com/iryzhkov/t3-steward/internal/t3api"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// ContainedProfile is operator configuration, never a capsule-supplied command.
// The first integrated lane uses a fresh output workspace plus registered data.
type ContainedProfile struct {
	RuntimePaths   []string
	Node           string
	T3Entry        string
	OpenCodeBinary string
	ProviderHosts  []string
}

var ErrContainedCustody = errors.New("contained execution custody requires reconciliation")

type containedPreparation struct {
	Identity workerproto.ExecutionIdentity
	WorkerID string
	Launch   providercontainment.Launch
}

type containedCapture struct {
	Identity workerproto.ExecutionIdentity
	WorkerID string
	Thread   *domain.Thread
	Message  string
	Archive  []byte
}

// privateJSON publishes a complete record without replacement. Equivalent replay
// is permitted; a changed record under the same identity is never adopted.
func privateJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if old, err := readBoundedRegularFile(path, 16<<20); err == nil {
		if string(old) != string(data) {
			return errors.New("contained receipt identity changed")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".receipt-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, we := tmp.Write(data)
	se := tmp.Sync()
	ce := tmp.Close()
	if err = errors.Join(we, se, ce); err != nil {
		return err
	}
	if err = os.Link(tmp.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (p ContainedT3) preparation(pkg workerproto.ExecutionPackage) (containedPreparation, error) {
	path, err := p.recordPath(pkg)
	if err != nil {
		return containedPreparation{}, err
	}
	data, err := readBoundedRegularFile(path+".preparation", 1<<20)
	if err != nil {
		return containedPreparation{}, err
	}
	var plan containedPreparation
	if err = json.Unmarshal(data, &plan); err != nil {
		return plan, err
	}
	if plan.Identity != pkg.Identity || plan.WorkerID != pkg.WorkerID ||
		plan.Launch.ExecutionID != pkg.Identity.ThreadID || plan.Launch.Spec.WorkerID != pkg.WorkerID ||
		!reflect.DeepEqual(plan.Launch.Spec.Directories, pkg.Environment.DirectoryBindings) {
		return plan, errors.New("contained preparation does not match package")
	}
	return plan, nil
}

func (p ContainedT3) PrepareExecution(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) error {
	if p.Profile == nil || pkg.Environment.Type != backlog.EnvironmentFresh || pkg.Route.ProviderInstanceID != "opencode" || len(pkg.Environment.DirectoryBindings) == 0 {
		return errors.New("contained lane requires configured OpenCode and a fresh workspace with directory attachments")
	}
	if len(p.Profile.RuntimePaths) == 0 || len(p.Profile.RuntimePaths) > 16 || strings.TrimSpace(pkg.Route.Model) == "" {
		return errors.New("contained profile needs runtime roots and an explicit model")
	}
	for _, command := range []string{p.Profile.Node, p.Profile.T3Entry, p.Profile.OpenCodeBinary} {
		parts := strings.Split(command, "/")
		if len(parts) < 4 || parts[1] != "runtime" || filepath.Clean(command) != command {
			return errors.New("contained provider commands must be below a runtime mount")
		}
		index, err := strconv.Atoi(parts[2])
		if err != nil || index < 0 || index >= len(p.Profile.RuntimePaths) {
			return errors.New("contained runtime index invalid")
		}
	}
	if pkg.Limits.PrepareTimeout <= 0 {
		return errors.New("contained preparation requires a bounded timeout")
	}
	ctx, cancel := context.WithTimeout(ctx, pkg.Limits.PrepareTimeout)
	defer cancel()
	if err := ensureRealDirectory(p.Supervisor.Root); err != nil {
		return err
	}
	path, err := p.recordPath(pkg)
	if err != nil {
		return err
	}
	plan, err := p.preparation(pkg)
	if errors.Is(err, os.ErrNotExist) {
		base := filepath.Join(filepath.Dir(workspace), ".contained")
		if err = ensureRealDirectory(base); err != nil {
			return err
		}
		spec := providercontainment.Spec{WorkerID: pkg.WorkerID, Directories: directoryresource.CloneBindings(pkg.Environment.DirectoryBindings),
			RuntimePaths: append([]string(nil), p.Profile.RuntimePaths...), ProviderHosts: append([]string(nil), p.Profile.ProviderHosts...), ControlPort: 18881,
			Command: []string{"/steward", "worker", "contained-t3", "--node", p.Profile.Node, "--entry", p.Profile.T3Entry, "--port", "18881", "--opencode-binary", p.Profile.OpenCodeBinary, "--opencode-model", pkg.Route.Model}}
		for _, name := range []string{"home", "control", "workspace"} {
			dir := workspace
			if name != "workspace" {
				dir = filepath.Join(base, name)
				if err = ensureRealDirectory(dir); err != nil {
					return err
				}
			}
			file, identity, e := directoryresource.Open(directoryresource.Registration{WorkerID: pkg.WorkerID, ResourceID: name, Revision: pkg.Identity.AssignmentID, Path: dir, Writable: true})
			if e != nil {
				return e
			}
			file.Close()
			switch name {
			case "home":
				spec.Home = identity
			case "workspace":
				spec.Workspace = identity
			case "control":
				spec.Control = &identity
			}
		}
		for _, name := range []string{"inputs", "dependencies"} {
			file, identity, e := directoryresource.Open(directoryresource.Registration{WorkerID: pkg.WorkerID, ResourceID: name, Revision: pkg.Identity.AssignmentID, Path: filepath.Join(filepath.Dir(workspace), name)})
			if e != nil {
				return e
			}
			file.Close()
			if name == "inputs" {
				spec.Inputs = &identity
			} else {
				spec.Dependencies = &identity
			}
		}
		plan = containedPreparation{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Launch: providercontainment.Launch{ExecutionID: pkg.Identity.ThreadID, Spec: spec}}
		if err = privateJSON(path+".preparation", plan); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if plan.Launch.Spec.Workspace.Registration.Path != workspace {
		return errors.New("contained workspace identity changed")
	}
	// From here an error cannot authorize release: a start may have committed.
	obs, err := p.Supervisor.Start(ctx, plan.Launch)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrContainedCustody, err)
	}
	for {
		if obs.Stopped || (obs.State != "active/running" && obs.State != "activating/start") {
			return fmt.Errorf("%w: supervisor is %s", ErrContainedCustody, obs.State)
		}
		client, e := t3api.NewContained(*plan.Launch.Spec.Control, p.Timeout)
		if e == nil {
			descriptor, de := client.Descriptor(ctx)
			if de == nil {
				record := ContainedAttachment{WorkerID: pkg.WorkerID, Identity: pkg.Identity, Launch: plan.Launch, InvocationID: obs.InvocationID, EnvironmentID: descriptor.EnvironmentID}
				if e = p.Remember(ctx, pkg, record); e == nil {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ErrContainedCustody, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
		obs, err = p.Supervisor.Observe(ctx, plan.Launch)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrContainedCustody, err)
		}
	}
}

func (p ContainedT3) captured(pkg workerproto.ExecutionPackage) (containedCapture, error) {
	path, err := p.recordPath(pkg)
	if err != nil {
		return containedCapture{}, err
	}
	data, err := readBoundedRegularFile(path+".capture", 16<<20)
	if err != nil {
		return containedCapture{}, err
	}
	var result containedCapture
	if err = json.Unmarshal(data, &result); err != nil {
		return result, err
	}
	if result.Identity != pkg.Identity || result.WorkerID != pkg.WorkerID {
		return result, errors.New("contained capture identity changed")
	}
	return result, nil
}

// Quiesce snapshots the terminal T3 outcome before stopping its entire service.
// A forced stop also handles preparation that never created a thread. It never
// uses lease expiry as a reason to stop and never releases an uncertain start.
func (p ContainedT3) Quiesce(ctx context.Context, pkg workerproto.ExecutionPackage, force bool) error {
	plan, err := p.preparation(pkg)
	if errors.Is(err, os.ErrNotExist) && force {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err = p.captured(pkg); errors.Is(err, os.ErrNotExist) {
		capture := containedCapture{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Archive: []byte("{}")}
		record, e := p.load(pkg)
		if e == nil {
			client, e := p.client(ctx, record)
			if e != nil {
				return e
			}
			control := t3control.New(client, nil, false)
			thread, e := control.GetThread(ctx, pkg.Identity.ThreadID)
			if e != nil {
				return e
			}
			if thread != nil && !workerThreadTerminal(*thread) {
				if !force {
					return errors.New("contained provider turn still active")
				}
				if e = control.StopThread(ctx, *thread, t3control.StopSession); e != nil {
					return e
				}
				var stopped bool
				thread, stopped, e = control.WaitStopped(ctx, pkg.Identity.ThreadID, p.Timeout)
				if e != nil {
					return e
				}
				if !stopped {
					return errors.New("contained provider stop not confirmed")
				}
			}
			capture.Thread = thread
			if thread != nil {
				capture.Message, e = control.LastAssistantMessage(ctx, pkg.Identity.ThreadID)
				if e != nil {
					return e
				}
				capture.Archive, e = control.ExportThread(ctx, pkg.Identity.ThreadID)
				if e != nil {
					return e
				}
			}
		} else if !force || !errors.Is(e, os.ErrNotExist) {
			return e
		}
		path, e := p.recordPath(pkg)
		if e != nil {
			return e
		}
		if e = privateJSON(path+".capture", capture); e != nil {
			return e
		}
	} else if err != nil {
		return err
	}
	stopped, err := p.Supervisor.Stop(ctx, plan.Launch)
	if err != nil {
		return err
	}
	if !stopped.Stopped {
		return errors.New("contained process custody unproven")
	}
	if force {
		return p.stopVerifications(ctx, pkg)
	}
	return nil
}

func (p ContainedT3) stoppedControl(ctx context.Context, pkg workerproto.ExecutionPackage) (T3Control, error) {
	plan, err := p.preparation(pkg)
	if errors.Is(err, os.ErrNotExist) {
		return retainedT3{capture: containedCapture{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Archive: []byte("{}")}}, nil
	}
	if err != nil {
		return nil, err
	}
	obs, err := p.Supervisor.Observe(ctx, plan.Launch)
	if err != nil {
		return nil, err
	}
	if !obs.Stopped {
		return nil, nil
	}
	capture, err := p.captured(pkg)
	if err != nil {
		return nil, err
	}
	return retainedT3{capture: capture}, nil
}

type retainedT3 struct {
	T3Control
	capture containedCapture
}

func (r retainedT3) GetThread(_ context.Context, id string) (*domain.Thread, error) {
	if id != r.capture.Identity.ThreadID {
		return nil, errors.New("wrong retained thread")
	}
	return r.capture.Thread, nil
}
func (r retainedT3) ListThreads(context.Context) ([]domain.Thread, error) {
	if r.capture.Thread == nil {
		return nil, nil
	}
	return []domain.Thread{*r.capture.Thread}, nil
}
func (r retainedT3) LastAssistantMessage(ctx context.Context, id string) (string, error) {
	if _, err := r.GetThread(ctx, id); err != nil {
		return "", err
	}
	return r.capture.Message, nil
}
func (r retainedT3) ExportThread(ctx context.Context, id string) ([]byte, error) {
	if _, err := r.GetThread(ctx, id); err != nil {
		return nil, err
	}
	return append([]byte(nil), r.capture.Archive...), nil
}

// The retained control answers for exactly one finalized thread. A request that
// names another identity is a mismatch, never a silently accepted no-op.
func (r retainedT3) SettleThread(ctx context.Context, id, _ string) error {
	_, err := r.GetThread(ctx, id)
	return err
}
func (r retainedT3) StopThread(ctx context.Context, thread domain.Thread, _ t3control.StopMode) error {
	_, err := r.GetThread(ctx, thread.ID)
	return err
}
func (r retainedT3) WaitStopped(ctx context.Context, id string, _ time.Duration) (*domain.Thread, bool, error) {
	thread, err := r.GetThread(ctx, id)
	if err != nil {
		return nil, false, err
	}
	return thread, true, nil
}
func (r retainedT3) ResumeThread(context.Context, domain.Thread, string) error {
	return errors.New("contained execution already finalized")
}
func (r retainedT3) WarnThread(context.Context, domain.Thread, domain.Warning) error {
	return errors.New("contained execution already finalized")
}
func (r retainedT3) CreateAndStartThread(context.Context, t3control.NewThreadInput) (string, error) {
	return "", errors.New("contained execution already finalized")
}
func (r retainedT3) ResolveProjectID(context.Context, string) (string, error) {
	return "", errors.New("contained execution already finalized")
}
func (r retainedT3) EnsureProject(context.Context, t3control.ManagedProject) (string, error) {
	return "", errors.New("contained execution already finalized")
}
