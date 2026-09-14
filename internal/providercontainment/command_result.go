package providercontainment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// CommandResult is a durable command exit observation. A result is usable only
// after Stop proves that every process in the invocation has stopped.
type CommandResult struct {
	Digest       string `json:"digest"`
	InvocationID string `json:"invocationId"`
	ExitCode     int    `json:"exitCode"`
}

var ErrCommandRunning = errors.New("contained command has not exited")

// Result never starts a command. It preserves an exit observation before stop,
// allowing recovery when the worker disappears between observation and cleanup.
func (s Supervisor) Result(ctx context.Context, launch Launch) (CommandResult, error) {
	unit, dir, digest, err := s.checkIntent(launch)
	if err != nil {
		return CommandResult{}, err
	}
	path := filepath.Join(dir, "command-result.json")
	var result CommandResult
	data, err := os.ReadFile(path)
	if err == nil {
		if err = json.Unmarshal(data, &result); err != nil {
			return result, err
		}
		if result.Digest != digest || result.InvocationID == "" || result.ExitCode < 0 || result.ExitCode > 255 {
			return result, errors.New("invalid contained command result")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, err
	} else {
		obs, err := s.Observe(ctx, launch)
		if err != nil {
			return result, err
		}
		if obs.Stopped {
			return result, errors.New("command stopped without an exit result")
		}
		data, err := s.invoke(ctx, "systemctl", "--user", "show", unit, "--property=LoadState,Description,ActiveState,SubState,InvocationID,ExecMainCode,ExecMainStatus")
		if err != nil {
			return result, err
		}
		props := map[string]string{}
		for _, line := range strings.Split(string(data), "\n") {
			if k, v, ok := strings.Cut(line, "="); ok {
				props[k] = v
			}
		}
		if props["LoadState"] != "loaded" || props["Description"] != "t3-containment:"+digest || props["InvocationID"] == "" || props["InvocationID"] != obs.InvocationID {
			return result, errors.New("contained command invocation unproven")
		}
		switch props["ActiveState"] + "/" + props["SubState"] {
		case "active/exited", "failed/failed":
		default:
			return result, ErrCommandRunning
		}
		code, e1 := strconv.Atoi(props["ExecMainCode"])
		status, e2 := strconv.Atoi(props["ExecMainStatus"])
		if e1 != nil || e2 != nil || code < 1 || code > 3 || status < 0 || status > 255 {
			return result, errors.New("contained command exit unproven")
		}
		if props["ActiveState"] == "failed" && status == 0 {
			status = 255
		}
		if code != 1 {
			status = 128 + status
			if status > 255 {
				status = 255
			}
		}
		result = CommandResult{Digest: digest, InvocationID: obs.InvocationID, ExitCode: status}
		encoded, err := json.Marshal(result)
		if err != nil {
			return result, err
		}
		tmp, err := os.CreateTemp(dir, ".command-result-")
		if err != nil {
			return result, err
		}
		defer os.Remove(tmp.Name())
		_, we := tmp.Write(encoded)
		se := tmp.Sync()
		ce := tmp.Close()
		if err = errors.Join(we, se, ce); err != nil {
			return result, err
		}
		if err = os.Link(tmp.Name(), path); err != nil {
			return result, fmt.Errorf("command result publication: %w", err)
		}
		if err = syncDirectory(dir); err != nil {
			return result, err
		}
	}
	current, err := s.Observe(ctx, launch)
	if err != nil {
		return result, err
	}
	if !current.Stopped && current.InvocationID != result.InvocationID {
		return result, errors.New("command invocation changed before custody confirmation")
	}
	stopped, err := s.Stop(ctx, launch)
	if err != nil {
		return result, err
	}
	if !stopped.Stopped {
		return result, errors.New("contained command process custody unproven")
	}
	return result, nil
}
