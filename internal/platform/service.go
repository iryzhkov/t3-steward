// Package platform installs the watchdog as a per-user background process.
// Linux (systemd user units) is fully supported; other platforms return
// ErrUnsupported and leave everything untouched.
package platform

import (
	"errors"
	"fmt"
	"runtime"
)

// ErrUnsupported means the platform has no background-process installer yet.
var ErrUnsupported = errors.New("background-process installation is not supported on this platform yet; run `t3-steward run` in the foreground (see README, \"Manual foreground operation\")")

// InstallOptions parametrize the generated service definition.
type InstallOptions struct {
	// Binary is the absolute path to the watchdog executable.
	Binary string
	// ConfigPath is the absolute path to config.yaml.
	ConfigPath string
	// Force overwrites an existing customized definition.
	Force bool
	// T3Unit is the T3 systemd user unit to order after, empty to skip.
	T3Unit string
	// Enable starts the service now and at login.
	Enable bool
	// DryRun reports whether the configuration still has dry-run on, for
	// the installer's advice.
	DryRun bool
	// CredentialFiles are rendered as Environment=T3_STEWARD_CREDENTIAL_<REF>_FILE=<path>
	// lines, so the steward reads each credential from its file at use and
	// no wrapper script has to export it. Paths under the home directory are
	// rendered with %h.
	CredentialFiles []CredentialFile
}

// CredentialFile binds one credential reference to the file that holds its
// value on this host.
type CredentialFile struct {
	// Reference is the credential reference as configured (for example
	// F02_PROTOCOL or secretref:f02-protocol/normandy); it is mapped onto the
	// variable name the same way the runtime maps it.
	Reference string
	// Path is the absolute path of the file.
	Path string
}

// InstallResult reports what was written.
type InstallResult struct {
	Path     string
	Commands []string
	Notes    []string
}

// ServiceManager abstracts one platform's process manager.
type ServiceManager interface {
	Install(opts InstallOptions) (*InstallResult, error)
	Uninstall() (*InstallResult, error)
	StatusHint() []string
}

// Current returns the manager for this OS.
func Current() (ServiceManager, error) {
	switch runtime.GOOS {
	case "linux":
		return &systemdUser{}, nil
	default:
		return nil, fmt.Errorf("%w (%s/%s)", ErrUnsupported, runtime.GOOS, runtime.GOARCH)
	}
}
