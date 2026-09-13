package t3api

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// SocketTokenFile reads atomically rotated execution-local credentials. OpenRoot
// prevents a provider-created symlink from making the host read outside Control.
type SocketTokenFile struct{ Control string }

func (s SocketTokenFile) Token(context.Context) (string, error) {
	if !filepath.IsAbs(s.Control) || filepath.Clean(s.Control) != s.Control {
		return "", errors.New("control directory must be absolute and canonical")
	}
	root, err := os.OpenRoot(s.Control)
	if err != nil {
		return "", err
	}
	defer root.Close()
	file, err := openTokenFile(root)
	if err != nil {
		return "", errors.New("contained token is unavailable or outside control storage")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > 16384 {
		return "", errors.New("contained token must be bounded private regular storage")
	}
	data, err := io.ReadAll(io.LimitReader(file, 16385))
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(data))
	if len(data) > 16384 || token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("invalid contained token")
	}
	return token, nil
}
func (SocketTokenFile) Invalidate() {}
