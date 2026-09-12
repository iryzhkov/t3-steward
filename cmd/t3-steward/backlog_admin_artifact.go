package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

const maxInlineArtifactBytes = 16 << 20

type adminArtifactService interface {
	OpenArtifact(context.Context, backlogadmin.Principal, string) (backlogadmin.ArtifactContent, error)
}

func (c backlogAdminCLI) runArtifactGet(ctx context.Context, args []string) error {
	if len(args) != 1 && len(args) != 3 {
		return errors.New("artifact get usage: backlog artifact get <artifact> [--output <path>]")
	}
	artifactID := args[0]
	outputPath := ""
	if len(args) == 3 {
		if args[1] != "--output" || strings.TrimSpace(args[2]) == "" {
			return errors.New("artifact get usage: backlog artifact get <artifact> [--output <path>]")
		}
		outputPath = args[2]
	}
	if c.artifacts == nil {
		return backlogadmin.ErrArtifactContentUnavailable
	}
	content, err := c.artifacts.OpenArtifact(ctx, c.principal, artifactID)
	if err != nil {
		return err
	}
	defer content.Content.Close()

	if outputPath != "" {
		return downloadArtifact(content.Content, outputPath)
	}
	if !inlineArtifactMediaType(content.Metadata.MediaType) {
		return fmt.Errorf("artifact media type %q requires --output", content.Metadata.MediaType)
	}
	if content.Metadata.Size > maxInlineArtifactBytes {
		return fmt.Errorf("artifact is too large for terminal output; use --output")
	}
	raw, err := io.ReadAll(io.LimitReader(content.Content, maxInlineArtifactBytes+1))
	if err != nil {
		return fmt.Errorf("read artifact: %w", err)
	}
	if len(raw) > maxInlineArtifactBytes {
		return fmt.Errorf("artifact is too large for terminal output; use --output")
	}
	_, err = c.stdout.Write(safeTerminalText(raw))
	return err
}

func inlineArtifactMediaType(mediaType string) bool {
	mediaType, _, _ = strings.Cut(strings.ToLower(strings.TrimSpace(mediaType)), ";")
	switch mediaType {
	case "text/plain", "text/markdown", "text/x-diff", "text/x-patch", "application/json", "application/x-diff":
		return true
	default:
		return false
	}
}

func safeTerminalText(raw []byte) []byte {
	var safe bytes.Buffer
	for len(raw) > 0 {
		value, size := utf8.DecodeRune(raw)
		if value == utf8.RuneError && size == 1 {
			safe.WriteRune(unicode.ReplacementChar)
			raw = raw[1:]
			continue
		}
		raw = raw[size:]
		if value == '\n' || value == '\r' || value == '\t' || !unicode.IsControl(value) {
			safe.WriteRune(value)
			continue
		}
		if value <= 0xff {
			fmt.Fprintf(&safe, "\\x%02x", value)
		} else {
			fmt.Fprintf(&safe, "\\u%04x", value)
		}
	}
	return safe.Bytes()
}

// ensureRealOutputDirectory requires every component of the output path to be a
// real directory. The one exception is a symlink owned by root that nobody else
// can write, such as macOS's /var -> /private/var: an operator cannot redirect
// it, so it is not the attack this check exists for. Symlinks the user could
// have planted, anywhere below, are still refused.
func ensureRealOutputDirectory(path string) error {
	current := filepath.Clean(path)
	for {
		info, err := os.Lstat(current)
		if err != nil {
			return errors.New("artifact output directory is not a real directory")
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if !systemOwnedSymlink(info) {
				return errors.New("artifact output directory is not a real directory")
			}
		} else if !info.IsDir() {
			return errors.New("artifact output directory is not a real directory")
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
		current = parent
	}
}

func systemOwnedSymlink(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0 && info.Mode().Perm()&0o022 == 0
}

func openRealOutputDirectory(path string) (*os.Root, error) {
	if err := ensureRealOutputDirectory(path); err != nil {
		return nil, err
	}
	checked, err := os.Stat(path)
	if err != nil || !checked.IsDir() {
		return nil, errors.New("artifact output directory is not a real directory")
	}
	return openCheckedOutputDirectory(path, checked)
}

func openCheckedOutputDirectory(path string, checked os.FileInfo) (*os.Root, error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, errors.New("artifact output directory is not a real directory")
	}
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(checked, opened) {
		root.Close()
		return nil, errors.New("artifact output directory changed during validation")
	}
	return root, nil
}

func createArtifactStage(root *os.Root) (*os.File, string, error) {
	for range 100 {
		var suffix [16]byte
		if _, err := rand.Read(suffix[:]); err != nil {
			return nil, "", fmt.Errorf("generate artifact staging name: %w", err)
		}
		name := ".t3-artifact-" + hex.EncodeToString(suffix[:])
		stage, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return nil, "", fmt.Errorf("create artifact output: %w", err)
		}
		return stage, name, nil
	}
	return nil, "", errors.New("create artifact output: staging name collision")
}

func downloadArtifact(content io.Reader, destination string) error {
	absolute, err := filepath.Abs(destination)
	if err != nil {
		return fmt.Errorf("resolve artifact output: %w", err)
	}
	parent := filepath.Dir(absolute)
	outputRoot, err := openRealOutputDirectory(parent)
	if err != nil {
		return err
	}
	defer outputRoot.Close()

	outputName := filepath.Base(absolute)
	if _, err := outputRoot.Lstat(outputName); err == nil {
		return fmt.Errorf("artifact output %q already exists", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect artifact output: %w", err)
	}
	stage, stageName, err := createArtifactStage(outputRoot)
	if err != nil {
		return err
	}
	defer outputRoot.Remove(stageName)
	if _, err := io.Copy(stage, content); err != nil {
		stage.Close()
		return fmt.Errorf("write artifact output: %w", err)
	}
	if err := stage.Chmod(0o600); err != nil {
		stage.Close()
		return fmt.Errorf("protect artifact output: %w", err)
	}
	if err := stage.Sync(); err != nil {
		stage.Close()
		return fmt.Errorf("sync artifact output: %w", err)
	}
	if err := stage.Close(); err != nil {
		return fmt.Errorf("close artifact output: %w", err)
	}
	if err := outputRoot.Link(stageName, outputName); err != nil {
		return fmt.Errorf("commit artifact output: %w", err)
	}
	return nil
}
