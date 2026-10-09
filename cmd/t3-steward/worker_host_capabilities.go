package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// hostCapabilityProbe observes the host capabilities a persistent worker
// advertises in its snapshot (workerproto.HostObservedCapability). It runs on
// every snapshot. Coordinator authentication is bounded and cached; repository
// probes remain local and do not contact the remote repository.
//
// A capability is advertised only when its check passes. Silence is the
// answer for anything not observed, never a guess that it is probably fine.
type hostCapabilityProbe struct {
	cfg config.Config
	// coordinatorReach and taskWorker answer the coordinator-client and
	// ask-relay questions; tests replace them.
	coordinatorReach func(context.Context, config.Config) error
	taskWorker       func(config.Config) (string, error)
	// configHome is the XDG configuration directory Huyang reads.
	configHome func() (string, error)
	// pushCredentials reports whether push credentials for a repository are
	// present on this host.
	pushCredentials func(context.Context, string) bool
	now             func() time.Time

	mu          sync.Mutex
	push        map[string]cachedHostObservation
	coordinator *cachedHostObservation
}

// cachedHostObservation keeps a push-credential answer for hostCapabilityTTL,
// because a credential helper is a process start per project per snapshot.
type cachedHostObservation struct {
	present bool
	at      time.Time
}

const (
	hostCapabilityTTL      = 5 * time.Minute
	hostPushProbeTimeout   = 3 * time.Second
	huyangConfigMaxBytes   = 1 << 20
	gitCredentialMaxOutput = 64 << 10
)

func newHostCapabilityProbe(cfg config.Config) *hostCapabilityProbe {
	return &hostCapabilityProbe{
		cfg:              cfg,
		coordinatorReach: authenticatedHostCoordinatorReach,
		taskWorker:       resolveTaskWaitWorker,
		configHome:       xdgConfigHome,
		pushCredentials:  gitPushCredentialsPresent,
		now:              time.Now,
	}
}

// Observe returns the host capabilities this host provides now, sorted. The
// projects are the ones this worker serves; the settings are the catalog's,
// which name each project's repository and the task workspace root.
func (p *hostCapabilityProbe) Observe(ctx context.Context, settings config.BacklogV2, projects []domain.WorkerProjectInventory) []string {
	var observed []string
	if p.coordinatorPresent(ctx) {
		observed = append(observed, workerproto.CapabilityCoordinatorClient)
		// The relay thread and the wake are delivered by this host's steward
		// as the worker the task runs on, so the ask relay needs both.
		if _, err := p.taskWorker(p.cfg); err == nil {
			observed = append(observed, workerproto.CapabilityAskRelay)
		}
	}
	if home, err := p.configHome(); err == nil && huyangTrustsWorkspaces(home, settings.Storage.Workspaces) {
		observed = append(observed, workerproto.CapabilityHuyangTrusted)
	}
	for _, project := range projects {
		name := workerproto.GitPushCapability(project.Name)
		binding, ok := settings.Projects[project.Name]
		if name == "" || !ok || strings.TrimSpace(binding.Repository) == "" {
			continue
		}
		if p.pushPresent(ctx, binding.Repository) {
			observed = append(observed, name)
		}
	}
	slices.Sort(observed)
	return slices.Compact(observed)
}

func (p *hostCapabilityProbe) pushPresent(ctx context.Context, repository string) bool {
	now := p.now()
	p.mu.Lock()
	cached, ok := p.push[repository]
	p.mu.Unlock()
	if ok && now.Sub(cached.at) < hostCapabilityTTL {
		return cached.present
	}
	if ctx.Err() != nil {
		// Out of time for this snapshot: not observed, and not cached, so the
		// next snapshot asks again.
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, hostPushProbeTimeout)
	present := p.pushCredentials(probeCtx, repository)
	cancel()
	if ctx.Err() != nil && !present {
		return false
	}
	p.mu.Lock()
	if p.push == nil {
		p.push = map[string]cachedHostObservation{}
	}
	p.push[repository] = cachedHostObservation{present: present, at: now}
	p.mu.Unlock()
	return present
}

// coordinatorPresent caches both authenticated success and failure. Cancellation
// before an observation never creates evidence for a later snapshot.
func (p *hostCapabilityProbe) coordinatorPresent(ctx context.Context) bool {
	now := p.now()
	p.mu.Lock()
	cached := p.coordinator
	p.mu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	if cached != nil && now.Sub(cached.at) < hostCapabilityTTL {
		return cached.present
	}
	probeCtx, cancel := context.WithTimeout(ctx, hostPushProbeTimeout)
	present := p.coordinatorReach(probeCtx, p.cfg) == nil
	cancel()
	if ctx.Err() != nil {
		return false
	}
	p.mu.Lock()
	p.coordinator = &cachedHostObservation{present: present, at: now}
	p.mu.Unlock()
	return present
}

// hostCoordinatorReach also gates one-shot asks, without snapshot caching.
func hostCoordinatorReach(cfg config.Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), hostPushProbeTimeout)
	defer cancel()
	return authenticatedHostCoordinatorReach(ctx, cfg)
}

var hostCoordinatorTransport = newCoordinatorTransport

// The transport verifies signed remote responses (or owner-only local socket
// peers). A status query adds no authority and never changes coordinator state.
func authenticatedHostCoordinatorReach(ctx context.Context, cfg config.Config) error {
	transport, err := hostCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	response, err := transport.client.Query(ctx, backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryStatus, Principal: transport.principal,
	})
	if err != nil {
		return err
	}
	if response.Status == nil {
		return errors.New("coordinator returned no authenticated status")
	}
	return nil
}

func xdgConfigHome() (string, error) {
	if dir := os.Getenv("XDG_CONFIG_HOME"); filepath.IsAbs(dir) {
		return dir, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config"), nil
}

// huyangTrustsWorkspaces reports whether a trust root in Huyang's
// configuration (CONFIG_HOME/huyang/config.toml, [trust] roots) is the task
// workspace root or one of its ancestors, so that every task workspace under
// it is trusted. Both sides are compared after resolving symbolic links, since
// a worker's workspace root is often reached through one.
func huyangTrustsWorkspaces(configHome, workspaceRoot string) bool {
	if strings.TrimSpace(workspaceRoot) == "" || !filepath.IsAbs(workspaceRoot) {
		return false
	}
	file, err := os.Open(filepath.Join(configHome, "huyang", "config.toml"))
	if err != nil {
		return false
	}
	defer file.Close()
	raw, err := readBounded(file, huyangConfigMaxBytes)
	if err != nil {
		return false
	}
	workspace := canonicalPath(workspaceRoot)
	for _, root := range huyangTrustRoots(raw) {
		if !filepath.IsAbs(root) {
			continue
		}
		root = canonicalPath(root)
		if workspace == root || strings.HasPrefix(workspace, root+string(filepath.Separator)) || root == string(filepath.Separator) {
			return true
		}
	}
	return false
}

func canonicalPath(path string) string {
	path = filepath.Clean(path)
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return resolved
	}
	return path
}

func readBounded(file *os.File, limit int64) ([]byte, error) {
	var buffer bytes.Buffer
	n, err := buffer.ReadFrom(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if n > limit {
		return nil, errors.New("file exceeds its size limit")
	}
	return buffer.Bytes(), nil
}

// huyangTrustRoots reads the roots array of the [trust] table. It understands
// the subset of TOML that table is written in -- a single or multi-line array
// of basic or literal strings, with comments -- and returns nothing for
// anything else, so an unreadable configuration is reported as untrusted.
func huyangTrustRoots(raw []byte) []string {
	var roots []string
	section := ""
	collecting := false
	var array strings.Builder
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64<<10), huyangConfigMaxBytes)
	for scanner.Scan() {
		line := strings.TrimSpace(stripTOMLComment(scanner.Text()))
		if collecting {
			array.WriteString(line)
			if strings.Contains(line, "]") {
				roots = append(roots, tomlStrings(array.String())...)
				collecting = false
			}
			continue
		}
		if strings.HasPrefix(line, "[") {
			section = strings.TrimSpace(strings.Trim(line, "[]"))
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if !(section == "trust" && key == "roots") && !(section == "" && key == "trust.roots") {
			continue
		}
		value = strings.TrimSpace(value)
		if !strings.HasPrefix(value, "[") {
			continue
		}
		array.Reset()
		array.WriteString(value)
		if strings.Contains(value, "]") {
			roots = append(roots, tomlStrings(value)...)
		} else {
			collecting = true
		}
	}
	return roots
}

// stripTOMLComment removes a # comment that is not inside a string.
func stripTOMLComment(line string) string {
	var quote byte
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case quote != 0 && c == '\\' && quote == '"':
			i++
		case quote != 0 && c == quote:
			quote = 0
		case quote == 0 && (c == '"' || c == '\''):
			quote = c
		case quote == 0 && c == '#':
			return line[:i]
		}
	}
	return line
}

// tomlStrings returns the string elements of one array literal. A basic
// string with an escape other than \\ or \" is skipped rather than guessed at.
func tomlStrings(array string) []string {
	var values []string
	for i := 0; i < len(array); i++ {
		quote := array[i]
		if quote != '"' && quote != '\'' {
			continue
		}
		var value strings.Builder
		valid, closed := true, false
		for i++; i < len(array); i++ {
			c := array[i]
			if c == quote {
				closed = true
				break
			}
			if quote == '"' && c == '\\' && i+1 < len(array) {
				i++
				if array[i] != '\\' && array[i] != '"' {
					valid = false
				}
				value.WriteByte(array[i])
				continue
			}
			value.WriteByte(c)
		}
		if valid && closed {
			values = append(values, value.String())
		}
	}
	return values
}

// gitPushCredentialsPresent reports whether this host holds credentials Git
// would present when pushing to the repository. It proves presence, not the
// remote's permission: an SSH identity in the agent or on disk for an SSH
// remote, a credential helper answer for an HTTPS remote, a writable directory
// for a local one. The credential value is read only to see that it is
// non-empty and is never kept, logged or reported.
func gitPushCredentialsPresent(ctx context.Context, repository string) bool {
	scheme, _, path := repositoryLocation(repository)
	switch scheme {
	case "ssh":
		return sshIdentityPresent(ctx, repository)
	case "https", "http":
		return gitCredentialPresent(ctx, repository)
	case "file":
		info, err := os.Stat(path)
		return err == nil && info.IsDir() && info.Mode().Perm()&0o200 != 0
	default:
		return false
	}
}

// repositoryLocation classifies a Git repository address as ssh, https, http
// or file, with its host or local path.
func repositoryLocation(repository string) (scheme, host, path string) {
	repository = strings.TrimSpace(repository)
	if strings.Contains(repository, "://") {
		parsed, err := url.Parse(repository)
		if err != nil {
			return "", "", ""
		}
		switch parsed.Scheme {
		case "ssh", "git+ssh", "ssh+git":
			return "ssh", parsed.Hostname(), ""
		case "https", "http":
			return parsed.Scheme, parsed.Host, ""
		case "file":
			return "file", "", parsed.Path
		}
		return "", "", ""
	}
	if filepath.IsAbs(repository) {
		return "file", "", repository
	}
	// The scp-like form, [user@]host:path, before any slash.
	if colon := strings.Index(repository, ":"); colon > 0 && !strings.Contains(repository[:colon], "/") {
		host := repository[:colon]
		if at := strings.LastIndex(host, "@"); at >= 0 {
			host = host[at+1:]
		}
		return "ssh", host, ""
	}
	return "", "", ""
}

func sshIdentityPresent(ctx context.Context, repository string) bool {
	// Ask OpenSSH which identities it would select for this destination, including
	// aliases, user, port and IdentitiesOnly. -G expands config without connecting.
	destination := repository
	args := []string{"-G"}
	if parsed, err := url.Parse(repository); err == nil && parsed.Host != "" {
		destination = parsed.Hostname()
		if parsed.User != nil {
			destination = parsed.User.Username() + "@" + destination
		}
		if parsed.Port() != "" {
			args = append(args, "-p", parsed.Port())
		}
	} else if colon := strings.Index(repository, ":"); colon > 0 {
		destination = repository[:colon]
	}
	if destination == "" || strings.HasPrefix(destination, "-") {
		return false
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return false
	}
	if config := filepath.Join(home, ".ssh", "config"); fileExists(config) {
		args = append(args, "-F", config)
	}
	command := exec.CommandContext(ctx, "ssh", append(args, destination)...)
	var output boundedBuffer
	output.limit = gitCredentialMaxOutput
	command.Stdout = &output
	if command.Run() != nil {
		return false
	}
	var identities []string
	only := false
	agent := os.Getenv("SSH_AUTH_SOCK")
	for _, line := range strings.Split(output.String(), "\n") {
		key, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		switch key {
		case "identitiesonly":
			only = value == "yes"
		case "identityagent":
			if value != "SSH_AUTH_SOCK" {
				agent = value
			}
		case "identityfile":
			if strings.HasPrefix(value, "~/") {
				value = filepath.Join(home, value[2:])
			}
			identities = append(identities, value)
		}
	}
	var agentKeys string
	if agent != "" && agent != "none" {
		add := exec.CommandContext(ctx, "ssh-add", "-L")
		add.Env = append(os.Environ(), "SSH_AUTH_SOCK="+agent)
		var keys boundedBuffer
		keys.limit = gitCredentialMaxOutput
		add.Stdout = &keys
		if add.Run() == nil {
			agentKeys = keys.String()
		}
		if !only && agentKeys != "" {
			return true
		}
	}
	for _, identity := range identities {
		// An unencrypted private key is usable without prompting.
		if exec.CommandContext(ctx, "ssh-keygen", "-y", "-P", "", "-f", identity).Run() == nil {
			return true
		}
		// With IdentitiesOnly, the agent must hold a selected identity.
		if public, err := os.ReadFile(identity + ".pub"); err == nil {
			fields := strings.Fields(string(public))
			if len(fields) >= 2 && strings.Contains(agentKeys, fields[0]+" "+fields[1]+" ") {
				return true
			}
		}
	}
	return false
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

// gitCredentialPresent asks the configured credential helpers, never a person:
// terminal prompts and askpass programs are disabled.
func gitCredentialPresent(ctx context.Context, repository string) bool {
	if strings.ContainsAny(repository, "\r\n") {
		return false
	}
	command := exec.CommandContext(ctx, "git", "credential", "fill")
	command.Env = append(slices.DeleteFunc(os.Environ(), func(entry string) bool {
		return strings.HasPrefix(entry, "GIT_ASKPASS=") || strings.HasPrefix(entry, "SSH_ASKPASS=")
	}), "GIT_TERMINAL_PROMPT=0", "GCM_INTERACTIVE=never")
	command.Stdin = strings.NewReader("url=" + repository + "\n\n")
	var output boundedBuffer
	output.limit = gitCredentialMaxOutput
	command.Stdout = &output
	if err := command.Run(); err != nil {
		return false
	}
	for _, line := range strings.Split(output.String(), "\n") {
		if value, ok := strings.CutPrefix(line, "password="); ok && value != "" {
			return true
		}
	}
	return false
}

// boundedBuffer keeps at most limit bytes and discards the rest.
type boundedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if room := b.limit - b.Len(); room > 0 {
		if len(p) > room {
			b.Buffer.Write(p[:room])
		} else {
			b.Buffer.Write(p)
		}
	}
	return len(p), nil
}
