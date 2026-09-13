package providercontainment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// Supervisor owns launch intent outside the worker's lifetime. Root is private,
// worker-owned state, never a directory mounted into the provider namespace.
// Losing a request context only interrupts the systemd client, not the service.
type Supervisor struct {
	Root       string
	Executable string
	command    func(context.Context, string, ...string) ([]byte, error)
}

type Launch struct {
	ExecutionID string `json:"executionId"`
	Spec        Spec   `json:"spec"`
}

type SupervisorObservation struct {
	Unit         string `json:"unit"`
	Digest       string `json:"digest"`
	State        string `json:"state"`
	InvocationID string `json:"invocationId,omitempty"`
	// Stopped is authoritative process custody, never provider/task success.
	Stopped bool `json:"stopped"`
}

type launchIntent struct {
	Launch     Launch `json:"launch"`
	Executable string `json:"executable"`
}

func (s Supervisor) invoke(ctx context.Context, command string, args ...string) ([]byte, error) {
	if s.command != nil {
		return s.command(ctx, command, args...)
	}
	if runtime.GOOS != "linux" {
		return nil, errors.New("durable containment requires Linux systemd")
	}
	return exec.CommandContext(ctx, command, args...).CombinedOutput()
}

func (s Supervisor) identity(launch Launch) (unit, dir, digest string, payload []byte, err error) {
	if strings.TrimSpace(launch.ExecutionID) == "" || len(launch.ExecutionID) > 512 || launch.Spec.WorkerID == "" {
		err = errors.New("supervisor requires bounded execution and worker identities")
		return
	}
	if !filepath.IsAbs(s.Root) || filepath.Clean(s.Root) != s.Root || s.Root == "/" || !filepath.IsAbs(s.Executable) {
		err = errors.New("supervisor requires absolute private state and executable paths")
		return
	}
	// The supervisor journal must not be visible or writable inside the sandbox.
	for _, identity := range append([]string{launch.Spec.Home.Registration.Path, launch.Spec.Workspace.Registration.Path}, sourcePaths(launch.Spec)...) {
		if identity == "" {
			continue
		}
		var overlap bool
		overlap, err = directoriesOverlap(s.Root, identity)
		if err != nil {
			return
		}
		if overlap {
			err = errors.New("supervisor state overlaps a sandbox mount")
			return
		}
	}
	key := sha256.Sum256([]byte(launch.Spec.WorkerID + "\x00" + launch.ExecutionID))
	unit = "t3-contained-" + hex.EncodeToString(key[:]) + ".service"
	dir = filepath.Join(s.Root, unit)
	payload, err = json.Marshal(launchIntent{Launch: launch, Executable: s.Executable})
	sum := sha256.Sum256(payload)
	digest = hex.EncodeToString(sum[:])
	return
}

func sourcePaths(spec Spec) []string {
	result := append([]string(nil), spec.RuntimePaths...)
	if spec.Control != nil {
		result = append(result, spec.Control.Registration.Path)
	}
	for _, b := range spec.Directories {
		result = append(result, b.Identity.Registration.Path)
	}
	return result
}

// Compare filesystem identities as well as ancestry so a bind mount or alias
// of a dataset cannot expose the journal under a different pathname.
func directoriesOverlap(a, b string) (bool, error) {
	for _, pair := range [][2]string{{a, b}, {b, a}} {
		target, err := os.Stat(pair[0])
		if err != nil {
			return false, err
		}
		for path := pair[1]; ; path = filepath.Dir(path) {
			info, err := os.Stat(path)
			if err != nil {
				return false, err
			}
			if os.SameFile(target, info) {
				return true, nil
			}
			if filepath.Dir(path) == path {
				break
			}
		}
	}
	return false, nil
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func writeExclusive(path string, content []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(content)
	syncErr := f.Sync()
	closeErr := f.Close()
	return errors.Join(writeErr, syncErr, closeErr)
}

func privateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.New("supervisor state must be a real private 0700 directory")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if resolved != path {
		return errors.New("supervisor state path contains a symlink")
	}
	return nil
}

// Start durably reserves an identity before sending a single start request.
// Every subsequent call only observes, including after a crash or lost response.
// An incomplete reservation is recovery-required; it never authorizes a retry.
func (s Supervisor) Start(ctx context.Context, launch Launch) (SupervisorObservation, error) {
	unit, dir, digest, payload, err := s.identity(launch)
	if err != nil {
		return SupervisorObservation{}, err
	}
	if runtime.GOOS != "linux" && s.command == nil {
		return SupervisorObservation{}, errors.New("durable containment requires Linux systemd")
	}
	if err = privateDirectory(s.Root); err != nil {
		return SupervisorObservation{}, err
	}
	if err = os.Mkdir(dir, 0700); errors.Is(err, os.ErrExist) {
		return s.Observe(ctx, launch)
	} else if err != nil {
		return SupervisorObservation{}, err
	}
	// Persist both the reservation and complete spec before any external effect.
	if err = syncDirectory(s.Root); err != nil {
		return SupervisorObservation{}, err
	}
	if err = writeExclusive(filepath.Join(dir, "intent.json"), payload); err != nil {
		return SupervisorObservation{}, err
	}
	spec, err := json.Marshal(launch.Spec)
	if err != nil {
		return SupervisorObservation{}, err
	}
	if err = writeExclusive(filepath.Join(dir, "spec.json"), spec); err != nil {
		return SupervisorObservation{}, err
	}
	if err = syncDirectory(dir); err != nil {
		return SupervisorObservation{}, err
	}
	_, err = s.invoke(ctx, "systemd-run", "--user", "--quiet", "--unit="+unit,
		"--description=t3-containment:"+digest, "--service-type=exec", "--expand-environment=no",
		"--property=Restart=no", "--property=RemainAfterExit=yes",
		"--property=KillMode=control-group", "--property=TimeoutStopSec=30s",
		"--", s.Executable, "worker", "contained-exec", "--spec", filepath.Join(dir, "spec.json"))
	if err != nil {
		return SupervisorObservation{Unit: unit, Digest: digest, State: "recovery-required"}, fmt.Errorf("supervisor start response uncertain; observe only: %w", err)
	}
	return s.Observe(ctx, launch)
}

func (s Supervisor) checkIntent(launch Launch) (string, string, string, error) {
	unit, dir, digest, payload, err := s.identity(launch)
	if err != nil {
		return "", "", "", err
	}
	if err = privateDirectory(s.Root); err != nil {
		return "", "", "", err
	}
	if err = privateDirectory(dir); err != nil {
		return "", "", "", err
	}
	got, err := os.ReadFile(filepath.Join(dir, "intent.json"))
	if err != nil {
		return "", "", "", fmt.Errorf("incomplete supervisor intent; recovery required: %w", err)
	}
	if string(got) != string(payload) {
		return "", "", "", errors.New("supervisor execution identity already bound to a different launch")
	}
	return unit, dir, digest, nil
}

func (s Supervisor) Observe(ctx context.Context, launch Launch) (SupervisorObservation, error) {
	unit, dir, digest, err := s.checkIntent(launch)
	if err != nil {
		return SupervisorObservation{}, err
	}
	observation := SupervisorObservation{Unit: unit, Digest: digest, State: "recovery-required"}
	if receipt, err := os.ReadFile(filepath.Join(dir, "stopped")); err == nil {
		if string(receipt) != digest {
			return observation, errors.New("invalid supervisor stop receipt")
		}
		observation.State, observation.Stopped = "stopped", true
		return observation, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return observation, err
	}
	data, err := s.invoke(ctx, "systemctl", "--user", "show", unit,
		"--property=LoadState,Description,ActiveState,SubState,InvocationID")
	if err != nil {
		return observation, fmt.Errorf("supervisor observation unavailable: %w", err)
	}
	props := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok {
			props[key] = value
		}
	}
	if props["LoadState"] != "loaded" {
		return observation, nil
	}
	if props["Description"] != "t3-containment:"+digest {
		return observation, errors.New("supervisor unit identity mismatch")
	}
	observation.InvocationID = props["InvocationID"]
	switch props["ActiveState"] {
	case "active", "activating", "deactivating", "failed", "inactive":
		observation.State = props["ActiveState"] + "/" + props["SubState"]
	}
	// Even active/exited does not by itself prove all descendants stopped.
	return observation, nil
}

// Stop is an explicit operator/worker cancellation or finalization effect.
// There is deliberately no lease or expiry input. systemctl stop waits for the
// control group; a durable receipt is written only after its successful reply.
// Lost replies or missing units remain uncertain and keep ownership.
func (s Supervisor) Stop(ctx context.Context, launch Launch) (SupervisorObservation, error) {
	before, err := s.Observe(ctx, launch)
	if err != nil || before.Stopped {
		return before, err
	}
	if before.State == "recovery-required" {
		return before, errors.New("cannot prove supervisor identity for stop")
	}
	if _, err = s.invoke(ctx, "systemctl", "--user", "stop", before.Unit); err != nil {
		return before, fmt.Errorf("supervisor stop unproven: %w", err)
	}
	_, dir, digest, err := s.checkIntent(launch)
	if err != nil {
		return before, err
	}
	// Publish a complete receipt atomically. A partial write never proves stop.
	tmp, err := os.CreateTemp(dir, ".stopped-")
	if err != nil {
		return before, err
	}
	name := tmp.Name()
	defer os.Remove(name)
	_, writeErr := tmp.WriteString(digest)
	syncErr := tmp.Sync()
	closeErr := tmp.Close()
	if err = errors.Join(writeErr, syncErr, closeErr); err != nil {
		return before, err
	}
	if err = os.Rename(name, filepath.Join(dir, "stopped")); err != nil {
		return before, err
	}
	if err = syncDirectory(dir); err != nil {
		return before, err
	}
	return s.Observe(ctx, launch)
}
