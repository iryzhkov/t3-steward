package providercontainment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// T3Spec names operator-approved runtimes already mounted inside the namespace.
// T3 state and credentials belong only to this execution's home/control mounts.
type T3Spec struct {
	Node           string
	Entry          string
	Port           int
	OpenCodeBinary string
	OpenCodeModel  string
}

func (s T3Spec) validate() error {
	for _, path := range []string{s.Node, s.Entry} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || !strings.HasPrefix(path, "/runtime/") {
			return errors.New("contained T3 requires mounted absolute runtime paths")
		}
	}
	if s.Port < 1 || s.Port > 65535 || s.Port == 18080 {
		return errors.New("invalid contained T3 port")
	}
	if (s.OpenCodeBinary == "") != (s.OpenCodeModel == "") {
		return errors.New("contained OpenCode requires both binary and model")
	}
	if s.OpenCodeBinary != "" {
		if !filepath.IsAbs(s.OpenCodeBinary) || filepath.Clean(s.OpenCodeBinary) != s.OpenCodeBinary || !strings.HasPrefix(s.OpenCodeBinary, "/runtime/") {
			return errors.New("contained OpenCode requires a mounted absolute runtime path")
		}
		if strings.TrimSpace(s.OpenCodeModel) != s.OpenCodeModel || len(s.OpenCodeModel) > 256 || strings.ContainsAny(s.OpenCodeModel, "\x00\r\n\t ") {
			return errors.New("invalid contained OpenCode model")
		}
	}
	return nil
}

func (s T3Spec) tokenArgs() []string {
	return []string{s.Entry, "auth", "session", "issue", "--base-dir", "/home/agent/t3", "--token-only", "--ttl", "60m", "--label", "steward-contained"}
}

func (s T3Spec) serverArgs(cwd string) []string {
	// Desktop mode with a cleared environment disables startup project/thread
	// creation. The worker explicitly ensures its project through the scoped API.
	return []string{s.Entry, "start", "--mode", "desktop", "--base-dir", "/home/agent/t3", "--port", fmt.Sprint(s.Port), "--host", "127.0.0.1", "--no-browser", cwd}
}

func publishToken(root string, token []byte) error {
	token = bytes.TrimSpace(token)
	if len(token) == 0 || len(token) > 16384 || bytes.ContainsAny(token, " \t\r\n") {
		return errors.New("invalid contained T3 session token")
	}
	if err := privateDirectory(root); err != nil {
		return err
	}
	file, err := os.CreateTemp(root, ".token-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	_, writeErr := file.Write(token)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err = errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(root, "token")); err != nil {
		return err
	}
	return syncDirectory(root)
}

// RunT3 runs inside contained-child. Its lifetime belongs to the durable
// namespace supervisor, never a worker request/connection or lease.
func RunT3(ctx context.Context, spec T3Spec, streams Streams) error {
	if err := spec.validate(); err != nil {
		return err
	}
	if err := privateDirectory("/control"); err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	issue := func() error {
		issueCtx, cancel := context.WithTimeout(runCtx, 30*time.Second)
		defer cancel()
		token, err := exec.CommandContext(issueCtx, spec.Node, spec.tokenArgs()...).Output()
		if err != nil {
			return fmt.Errorf("contained T3 session issuance failed: %w", err)
		}
		return publishToken("/control", token)
	}
	// A fresh dedicated server is preparation, never adoption of an unknown home.
	// Refuse partial prior startup instead of replacing provider settings/state.
	if err := os.Mkdir("/home/agent/t3", 0700); err != nil {
		return err
	}
	if err := os.Mkdir("/home/agent/t3/userdata", 0700); err != nil {
		return err
	}
	if err := prepareT3Settings("/home/agent/t3/userdata", spec); err != nil {
		return err
	}
	if err := issue(); err != nil {
		return err
	}
	// Refresh execution-local access only. Fail closed if maintenance fails;
	// neither refresh nor its failure starts/retries a provider thread.
	refreshDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(48 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				refreshDone <- nil
				return
			case <-ticker.C:
				if err := issue(); err != nil {
					cancel()
					refreshDone <- err
					return
				}
			}
		}
	}()
	command := exec.CommandContext(runCtx, spec.Node, spec.serverArgs(cwd)...)
	command.Stdin, command.Stdout, command.Stderr = streams.Stdin, streams.Stdout, streams.Stderr
	runErr := command.Run()
	cancel()
	return errors.Join(runErr, <-refreshDone)
}
