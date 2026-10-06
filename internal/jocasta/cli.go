// Package jocasta writes and reads Jocasta documents through the jocasta CLI
// configured on the local host. There is no Jocasta API client in this
// repository on purpose: the CLI owns the configuration, the credentials and
// the guard contract, and Steward only ever holds a path, a revision and the
// document bytes.
package jocasta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Code classifies a failed Jocasta operation. The values the CLI reports in
// error.code are passed through; the constants are the ones Steward acts on.
type Code string

const (
	CodeNotFound    Code = "not_found"
	CodeConflict    Code = "conflict"
	CodeAuth        Code = "auth"
	CodeUnavailable Code = "unavailable"
	CodeInvalid     Code = "invalid"
	CodeLocal       Code = "local_io"
	// CodeMissingCLI is Steward's own: the jocasta binary is not installed
	// where the coordinator looked for it.
	CodeMissingCLI Code = "cli_missing"
)

// maxMessageBytes bounds the CLI message an Error keeps. The message is the
// CLI's own JSON text, never its standard error or its configuration.
const maxMessageBytes = 256

// Error is one failed Jocasta operation.
type Error struct {
	Op       string
	Code     Code
	ExitCode int
	Message  string
}

func (e *Error) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("jocasta %s: %s", e.Op, e.Code)
	}
	return fmt.Sprintf("jocasta %s: %s: %s", e.Op, e.Code, e.Message)
}

// CodeOf returns the classification of a Jocasta error, or "" for any other
// error.
func CodeOf(err error) Code {
	var jocastaErr *Error
	if errors.As(err, &jocastaErr) {
		return jocastaErr.Code
	}
	return ""
}

// IsConflict reports whether a write was refused because its guard did not
// hold, or because --create found the path taken. Nothing was written, and the
// caller re-reads and reconciles.
func IsConflict(err error) bool {
	var jocastaErr *Error
	if !errors.As(err, &jocastaErr) {
		return false
	}
	return jocastaErr.Code == CodeConflict || jocastaErr.ExitCode == 3
}

// Document is one revision of a Jocasta document.
type Document struct {
	Path     string
	Revision int64
	Content  []byte
}

// Client is the part of Jocasta the milestone ledger uses: a full read and the
// two fenced writes. Every write carries an idempotency key, so a write whose
// outcome was lost commits at most once when it is repeated.
type Client interface {
	Get(ctx context.Context, path string) (Document, error)
	Create(ctx context.Context, path string, content []byte, requestID string) (int64, error)
	Update(ctx context.Context, path string, content []byte, ifRevision int64, requestID string) (int64, error)
}

// DefaultTimeout bounds one CLI invocation.
const DefaultTimeout = 30 * time.Second

// CLI runs the jocasta command-line client with its default configuration.
type CLI struct {
	// Binary is the jocasta executable. Empty means the first of "jocasta" on
	// PATH and ~/.local/bin/jocasta, where fleet hosts install it and which a
	// systemd user service's PATH does not always include.
	Binary string
	// TempDir holds the private files that carry document content to and from
	// the CLI. Empty means the system temporary directory.
	TempDir string
	// Timeout bounds each invocation; zero means DefaultTimeout.
	Timeout time.Duration
}

// Get reads the current revision of a document in full.
func (c CLI) Get(ctx context.Context, path string) (Document, error) {
	file, cleanup, err := c.tempFile(nil)
	if err != nil {
		return Document{}, &Error{Op: "get", Code: CodeLocal, Message: err.Error()}
	}
	defer cleanup()
	receipt, err := c.run(ctx, "get", "get", path, "--output", file, "--overwrite", "--json")
	if err != nil {
		return Document{}, err
	}
	content, err := os.ReadFile(file)
	if err != nil {
		return Document{}, &Error{Op: "get", Code: CodeLocal, Message: err.Error()}
	}
	return Document{Path: receipt.path(path), Revision: receipt.Document.Revision, Content: content}, nil
}

// Create writes a new document and fails with a conflict if the path is taken.
func (c CLI) Create(ctx context.Context, path string, content []byte, requestID string) (int64, error) {
	return c.put(ctx, path, content, "--create", requestID)
}

// Update writes a new revision, fenced on ifRevision being current.
func (c CLI) Update(ctx context.Context, path string, content []byte, ifRevision int64, requestID string) (int64, error) {
	if ifRevision <= 0 {
		return 0, &Error{Op: "put", Code: CodeInvalid, Message: "an update needs the revision it replaces"}
	}
	return c.put(ctx, path, content, "--if-revision="+strconv.FormatInt(ifRevision, 10), requestID)
}

func (c CLI) put(ctx context.Context, path string, content []byte, guard, requestID string) (int64, error) {
	if requestID == "" {
		return 0, &Error{Op: "put", Code: CodeInvalid, Message: "a write needs an idempotency key"}
	}
	file, cleanup, err := c.tempFile(content)
	if err != nil {
		return 0, &Error{Op: "put", Code: CodeLocal, Message: err.Error()}
	}
	defer cleanup()
	args := []string{"put", path, "--file", file}
	if name, value, ok := strings.Cut(guard, "="); ok {
		args = append(args, name, value)
	} else {
		args = append(args, guard)
	}
	args = append(args, "--request-id", requestID, "--json")
	receipt, err := c.run(ctx, "put", args...)
	if err != nil {
		return 0, err
	}
	if receipt.Document.Revision <= 0 {
		return 0, &Error{Op: "put", Code: CodeUnavailable, Message: "receipt names no revision"}
	}
	return receipt.Document.Revision, nil
}

// receipt is the part of the CLI's versioned JSON output Steward reads.
type receipt struct {
	Document struct {
		Path     string `json:"path"`
		Revision int64  `json:"revision"`
	} `json:"document"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (r receipt) path(requested string) string {
	if r.Document.Path != "" {
		return r.Document.Path
	}
	return requested
}

func (c CLI) run(ctx context.Context, op string, args ...string) (receipt, error) {
	binary, err := c.binary()
	if err != nil {
		return receipt{}, &Error{Op: op, Code: CodeMissingCLI, Message: err.Error()}
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var stdout bytes.Buffer
	command := exec.CommandContext(ctx, binary, args...)
	command.Stdout = &stdout
	// Standard error is discarded rather than logged: it is free text the
	// coordinator cannot vouch for, and the JSON receipt carries the answer.
	command.Stderr = nil
	runErr := command.Run()
	var parsed receipt
	decodeErr := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &parsed)
	if runErr == nil {
		if decodeErr != nil {
			return receipt{}, &Error{Op: op, Code: CodeUnavailable, Message: "unreadable receipt"}
		}
		return parsed, nil
	}
	failure := &Error{Op: op, Code: CodeUnavailable}
	var exitErr *exec.ExitError
	switch {
	case errors.As(runErr, &exitErr):
		failure.ExitCode = exitErr.ExitCode()
		failure.Code = exitCode(failure.ExitCode)
	case errors.Is(runErr, exec.ErrNotFound) || errors.Is(runErr, os.ErrNotExist):
		failure.Code = CodeMissingCLI
	}
	if ctx.Err() != nil {
		failure.Code, failure.Message = CodeUnavailable, "timed out after "+timeout.String()
		return receipt{}, failure
	}
	if decodeErr == nil && parsed.Error != nil {
		if parsed.Error.Code != "" {
			failure.Code = Code(parsed.Error.Code)
		}
		failure.Message = boundMessage(parsed.Error.Message)
	}
	return receipt{}, failure
}

// exitCode maps the CLI's documented exit statuses.
func exitCode(status int) Code {
	switch status {
	case 2:
		return CodeNotFound
	case 3:
		return CodeConflict
	case 4:
		return CodeAuth
	case 6:
		return CodeInvalid
	case 7:
		return CodeLocal
	default:
		return CodeUnavailable
	}
}

func boundMessage(message string) string {
	message = strings.Join(strings.Fields(message), " ")
	if len(message) <= maxMessageBytes {
		return message
	}
	cut := maxMessageBytes
	for cut > 0 && !utf8.RuneStart(message[cut]) {
		cut--
	}
	return message[:cut] + "..."
}

func (c CLI) binary() (string, error) {
	if c.Binary != "" {
		if _, err := os.Stat(c.Binary); err != nil {
			return "", fmt.Errorf("jocasta CLI not found at %s", c.Binary)
		}
		return c.Binary, nil
	}
	if path, err := exec.LookPath("jocasta"); err == nil {
		return path, nil
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidate := filepath.Join(home, ".local", "bin", "jocasta")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	return "", errors.New("jocasta CLI not found on PATH or in ~/.local/bin")
}

// tempFile creates a private file holding content, or an empty one for the
// CLI to fill, and returns a function that removes it.
func (c CLI) tempFile(content []byte) (string, func(), error) {
	dir := c.TempDir
	if dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", nil, err
		}
	}
	file, err := os.CreateTemp(dir, "jocasta-*.md")
	if err != nil {
		return "", nil, err
	}
	name := file.Name()
	cleanup := func() { _ = os.Remove(name) }
	if _, err := file.Write(content); err != nil {
		file.Close()
		cleanup()
		return "", nil, err
	}
	if err := file.Close(); err != nil {
		cleanup()
		return "", nil, err
	}
	return name, cleanup, nil
}
