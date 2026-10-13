// Package claudequota acquires account-bound quota observations without model requests.
// The private HTTP contract is pinned to Claude Code 2.1.296.
package claudequota

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/privatefile"
)

const (
	NativeVersion    = "2.1.296"
	DefaultInterval  = 300 * time.Second
	DefaultJitter    = 15 * time.Second
	DefaultTimeout   = 10 * time.Second
	MaximumBodyBytes = 64 << 10
	apiBase          = "https://api.anthropic.com"
)

func validAccountID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value
}

// ModelBinding maps one reported bucket to an explicitly configured window.
// Models documents exact catalog IDs. It never grants authorization to run them.
type ModelBinding struct {
	DisplayName string
	SourceKey   string
	Window      string
	Label       string
	Selector    string
	Models      []string
	Optional    bool
}

// Options names the collector's existing local login and the pool it measures.
type Options struct {
	ProviderInstanceID string
	AccountID          string
	CredentialDir      string
	StatusPath         string
	Interval           time.Duration
	Jitter             time.Duration
	Timeout            time.Duration
	MaxObservationAge  time.Duration
	PipelineBudget     time.Duration
	ModelBindings      []ModelBinding
}

// Collector implements daemon.Source. Dependencies stay private to this module;
// tests replace the HTTP transport, clock and waiter without a live account.
type Collector struct {
	options        Options
	client         *http.Client
	now            func() time.Time
	wait           func(context.Context, time.Duration) error
	jitter         func(time.Duration) time.Duration
	readCredential func() (credentialSnapshot, error)
	status         Status
}

type failure struct {
	code       string
	httpStatus int
	retryAt    time.Time
}

func (e *failure) Error() string { return "claude quota: " + e.code }
func fail(code string) error     { return &failure{code: code} }

func defaults(options Options) Options {
	if options.Interval == 0 {
		options.Interval = DefaultInterval
	}
	if options.Jitter == 0 {
		options.Jitter = DefaultJitter
	}
	if options.Timeout == 0 {
		options.Timeout = DefaultTimeout
	}
	return options
}

// ValidateOptions checks a binding without reading credentials or resolving paths.
// Empty paths let configuration validation run before runtime paths are resolved.
func ValidateOptions(options Options) error {
	options = defaults(options)
	if options.ProviderInstanceID == "" || strings.TrimSpace(options.ProviderInstanceID) != options.ProviderInstanceID || !validAccountID(options.AccountID) {
		return fail("invalid-binding")
	}
	if options.Interval < DefaultInterval || options.Jitter < 0 || options.Jitter > DefaultJitter || options.Timeout <= 0 || options.Timeout > DefaultTimeout || options.PipelineBudget < 0 {
		return fail("invalid-timing")
	}
	remaining := options.MaxObservationAge
	if remaining <= 0 {
		return fail("insufficient-scoped-age")
	}
	for _, bound := range []time.Duration{options.Interval, options.Jitter, options.Timeout, options.PipelineBudget} {
		if bound > remaining {
			return fail("insufficient-scoped-age")
		}
		remaining -= bound
	}
	if options.StatusPath != "" && !filepath.IsAbs(options.StatusPath) {
		return fail("invalid-status-path")
	}
	if options.CredentialDir != "" && !filepath.IsAbs(options.CredentialDir) {
		return fail("invalid-credential-directory")
	}
	seen := map[string]bool{}
	sources := map[string]bool{}
	for _, binding := range options.ModelBindings {
		if binding.Window == "" || binding.Window == "five_hour" || binding.Window == "seven_day" || seen[binding.Window] || binding.Label == "" || binding.Selector == "" || len(binding.Models) == 0 || (binding.DisplayName == "") == (binding.SourceKey == "") {
			return fail("invalid-model-binding")
		}
		if binding.SourceKey != "" && binding.SourceKey != "seven_day_opus" && binding.SourceKey != "seven_day_sonnet" && binding.SourceKey != "seven_day_oauth_apps" {
			return fail("invalid-model-binding")
		}
		sourceKey := binding.SourceKey + strings.ToLower(binding.DisplayName)
		if sources[sourceKey] {
			return fail("duplicate-model-binding")
		}
		sources[sourceKey] = true
		seen[binding.Window] = true
		for _, model := range binding.Models {
			if model == "" || strings.TrimSpace(model) != model {
				return fail("invalid-model-binding")
			}
		}
	}
	return nil
}

// New does not read credentials, write status or contact the provider.
func New(options Options) (*Collector, error) {
	options = defaults(options)
	if err := ValidateOptions(options); err != nil {
		return nil, err
	}
	if options.StatusPath == "" {
		return nil, fail("invalid-status-path")
	}
	if options.CredentialDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fail("credential-directory-unavailable")
		}
		options.CredentialDir = filepath.Join(home, ".claude")
	}
	options.ModelBindings = append([]ModelBinding(nil), options.ModelBindings...)
	for i := range options.ModelBindings {
		options.ModelBindings[i].Models = append([]string(nil), options.ModelBindings[i].Models...)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// A fresh connection prevents hidden retries after a reused connection fails.
	transport.DisableKeepAlives = true
	// HTTP/2 can retry refused streams internally. Pin single-attempt HTTP/1.
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetHTTP1(true)
	c := &Collector{options: options, now: time.Now, wait: waitContext, jitter: randomJitter,
		client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
	c.readCredential = func() (credentialSnapshot, error) {
		return readCredentialFile(filepath.Join(options.CredentialDir, ".credentials.json"))
	}
	return c, nil
}

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func randomJitter(bound time.Duration) time.Duration {
	if bound <= 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(bound)*2+1))
	if err != nil {
		return bound
	}
	return time.Duration(n.Int64()) - bound
}

// Run preserves the provider's acquisition time. A failure is recorded locally;
// no synthetic healthy, unavailable or account-relabelled observation is emitted.
func (c *Collector) Run(ctx context.Context, output chan<- domain.QuotaSnapshot) error {
	if err := c.loadStatus(); err != nil {
		return err
	}
	if c.status.NextAttemptAfter != nil && c.status.NextAttemptAfter.After(c.now()) {
		if err := c.wait(ctx, c.status.NextAttemptAfter.Sub(c.now())); err != nil {
			return err
		}
	}
	for {
		started := c.now().UTC()
		snapshots, err := c.collect(ctx, started)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		completed := c.now().UTC()
		c.status.LastAttemptStartedAt = started
		c.status.LastAttemptCompletedAt = completed
		c.status.State = "ok"
		c.status.ErrorCode = ""
		c.status.HTTPStatus = 0
		c.status.NextAttemptAfter = nil
		if err != nil {
			c.status.State = "failed"
			c.status.ErrorCode = "acquisition-failed"
			var f *failure
			if errors.As(err, &f) {
				c.status.ErrorCode = f.code
				c.status.HTTPStatus = f.httpStatus
				if !f.retryAt.IsZero() {
					at := f.retryAt.UTC()
					c.status.NextAttemptAfter = &at
				}
			}
		} else {
			c.status.LastSuccessStartedAt = started
			c.status.LastSuccessCompletedAt = completed
			c.status.LastSuccessWindowCount = len(snapshots)
		}
		if err := c.saveStatus(); err != nil {
			return err
		}
		if err == nil {
			for _, snapshot := range snapshots {
				select {
				case output <- snapshot:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		delay := started.Add(c.options.Interval + c.jitter(c.options.Jitter)).Sub(c.now())
		if delay < 0 {
			delay = 0
		}
		if c.status.NextAttemptAfter != nil {
			if backoff := c.status.NextAttemptAfter.Sub(c.now()); backoff > delay {
				delay = backoff
			}
		}
		if err := c.wait(ctx, delay); err != nil {
			return err
		}
	}
}

type credentialSnapshot struct {
	raw       []byte
	token     string
	scopes    []string
	expiresAt *float64
	info      os.FileInfo
}

func readCredentialFile(path string) (credentialSnapshot, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return credentialSnapshot{}, fail("credential-unavailable")
	}
	// Reuse the same owner-only, no-symlink, bounded read used by Steward
	// bootstrap credentials. Never expose its path-bearing error.
	raw, err := privatefile.Read(path, MaximumBodyBytes)
	if err != nil {
		return credentialSnapshot{}, fail("credential-unavailable")
	}
	after, statErr := os.Lstat(path)
	if statErr != nil || !sameGeneration(info, after) {
		return credentialSnapshot{}, fail("credential-changed")
	}
	var body struct {
		OAuth struct {
			AccessToken string   `json:"accessToken"`
			Scopes      []string `json:"scopes"`
			ExpiresAt   *float64 `json:"expiresAt"`
		} `json:"claudeAiOauth"`
	}
	if json.Unmarshal(raw, &body) != nil || body.OAuth.AccessToken == "" {
		return credentialSnapshot{}, fail("credential-unavailable")
	}
	return credentialSnapshot{raw: raw, token: body.OAuth.AccessToken, scopes: body.OAuth.Scopes, expiresAt: body.OAuth.ExpiresAt, info: info}, nil
}
func sameGeneration(a, b os.FileInfo) bool {
	return a != nil && b != nil && os.SameFile(a, b) && a.ModTime().Equal(b.ModTime()) && a.Size() == b.Size() && a.Mode() == b.Mode()
}
func (c *Collector) collect(parent context.Context, started time.Time) ([]domain.QuotaSnapshot, error) {
	ctx, cancel := context.WithTimeout(parent, c.options.Timeout)
	defer cancel()
	if ctx.Err() != nil {
		return nil, fail("deadline")
	}
	credential, err := c.readCredential()
	if err != nil {
		return nil, err
	}
	profileScope := false
	for _, scope := range credential.scopes {
		profileScope = profileScope || scope == "user:profile"
	}
	if !profileScope {
		return nil, fail("credential-profile-scope-missing")
	}
	if credential.expiresAt == nil || math.IsInf(*credential.expiresAt, 0) || math.IsNaN(*credential.expiresAt) {
		return nil, fail("credential-expiry-invalid")
	}
	if *credential.expiresAt <= float64(c.now().UnixMilli()) {
		return nil, fail("credential-expired")
	}
	profile, err := c.get(ctx, "/api/oauth/profile", credential.token, true)
	if err != nil {
		return nil, err
	}
	var identity struct {
		Account struct {
			UUID string `json:"uuid"`
		} `json:"account"`
	}
	if json.Unmarshal(profile, &identity) != nil || !validAccountID(identity.Account.UUID) {
		return nil, fail("invalid-profile")
	}
	if identity.Account.UUID != c.options.AccountID {
		return nil, fail("account-mismatch")
	}
	usage, err := c.get(ctx, "/api/oauth/usage", credential.token, false)
	if err != nil {
		return nil, err
	}
	snapshots, err := c.decode(usage, started)
	if err != nil {
		return nil, err
	}
	after, err := c.readCredential()
	if err != nil {
		return nil, fail("credential-changed")
	}
	if subtle.ConstantTimeCompare(credential.raw, after.raw) != 1 || !sameGeneration(credential.info, after.info) {
		return nil, fail("credential-changed")
	}
	if ctx.Err() != nil {
		return nil, fail("deadline")
	}
	eventBytes := make([]byte, 16)
	if _, err := rand.Read(eventBytes); err != nil {
		return nil, fail("event-identity-unavailable")
	}
	eventID := "claude-native/" + hex.EncodeToString(eventBytes)
	for i := range snapshots {
		snapshots[i].SourceEventID = eventID
	}
	return snapshots, nil
}
func (c *Collector) get(ctx context.Context, path, token string, profile bool) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, fail("deadline")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+path, nil)
	if err != nil {
		return nil, fail("request-invalid")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("User-Agent", "claude-code/"+NativeVersion)
	request.Header.Set("Content-Type", "application/json")
	if profile {
		request.Header.Set("Cache-Control", "no-cache")
	} else {
		request.Header.Set("anthropic-beta", "oauth-2025-04-20")
	}
	response, err := c.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fail("deadline")
		}
		return nil, fail("transport")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		f := &failure{code: "http-response", httpStatus: response.StatusCode}
		switch response.StatusCode {
		case 401:
			f.code = "unauthenticated"
		case 403:
			f.code = "forbidden"
		case 429:
			f.code = "rate-limited"
		default:
			if response.StatusCode >= 300 && response.StatusCode < 400 {
				f.code = "redirect-refused"
			}
		}
		if response.StatusCode == 429 {
			f.retryAt = parseRetryAfter(response.Header.Get("Retry-After"), c.now())
		}
		return nil, f
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaximumBodyBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return nil, fail("deadline")
		}
		return nil, fail("response-read")
	}
	if len(body) > MaximumBodyBytes {
		return nil, fail("response-too-large")
	}
	return body, nil
}
func parseRetryAfter(value string, now time.Time) time.Time {
	if value == "" {
		return time.Time{}
	}
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err == nil && seconds >= 0 && strconv.FormatInt(seconds, 10) == value && seconds <= int64((365*24*time.Hour)/time.Second) {
		return now.Add(time.Duration(seconds) * time.Second)
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at
	}
	return time.Time{}
}

type wireWindow struct {
	Utilization *float64 `json:"utilization"`
	ResetsAt    *string  `json:"resets_at"`
}
type wireRow struct {
	Kind     string   `json:"kind"`
	Percent  *float64 `json:"percent"`
	ResetsAt *string  `json:"resets_at"`
	Scope    *struct {
		Model *struct {
			DisplayName string `json:"display_name"`
		} `json:"model"`
	} `json:"scope"`
}

func validWindow(raw json.RawMessage, now time.Time) (float64, time.Time, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, time.Time{}, fail("partial-window")
	}
	var window wireWindow
	if json.Unmarshal(raw, &window) != nil || window.Utilization == nil || window.ResetsAt == nil {
		return 0, time.Time{}, fail("invalid-window")
	}
	used := *window.Utilization
	if math.IsNaN(used) || math.IsInf(used, 0) || used < 0 || used > 100 {
		return 0, time.Time{}, fail("invalid-window")
	}
	reset, err := time.Parse(time.RFC3339Nano, *window.ResetsAt)
	if err != nil || !reset.After(now) {
		return 0, time.Time{}, fail("invalid-window")
	}
	return used, reset.UTC(), nil
}
func (c *Collector) decode(body []byte, started time.Time) ([]domain.QuotaSnapshot, error) {
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(body))
	if decoder.Decode(&fields) != nil || fields == nil {
		return nil, fail("invalid-usage")
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return nil, fail("invalid-usage")
	}
	now := c.now()
	snapshots := make([]domain.QuotaSnapshot, 0, 2+len(c.options.ModelBindings))
	appendWindow := func(window, label, selector string, raw json.RawMessage, duration time.Duration) error {
		used, reset, err := validWindow(raw, now)
		if err != nil {
			return err
		}
		snapshots = append(snapshots, domain.QuotaSnapshot{Key: domain.BucketKey{ProviderInstanceID: c.options.ProviderInstanceID, AccountID: c.options.AccountID, LimitID: "claude", Window: window}, LimitName: label, UsedPercent: used, ResetsAt: &reset, WindowDuration: duration, ModelSelector: selector, ObservedAt: started})
		return nil
	}
	if err := appendWindow("five_hour", "Claude 5-hour", "", fields["five_hour"], 5*time.Hour); err != nil {
		return nil, err
	}
	if err := appendWindow("seven_day", "Claude 7-day", "", fields["seven_day"], 7*24*time.Hour); err != nil {
		return nil, err
	}
	boundSources := map[string]bool{}
	boundNames := map[string]bool{}
	for _, b := range c.options.ModelBindings {
		boundSources[b.SourceKey] = true
		if b.DisplayName != "" {
			boundNames[strings.ToLower(b.DisplayName)] = true
		}
	}
	for _, key := range []string{"seven_day_oauth_apps", "seven_day_opus", "seven_day_sonnet"} {
		raw := fields[key]
		if len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			if _, _, err := validWindow(raw, now); err != nil {
				return nil, err
			}
			if !boundSources[key] {
				return nil, fail("unbound-model-window")
			}
		}
	}
	var rows []wireRow
	rawRows := fields["limits"]
	if len(rawRows) > 0 && !bytes.Equal(bytes.TrimSpace(rawRows), []byte("null")) {
		if json.Unmarshal(rawRows, &rows) != nil {
			return nil, fail("invalid-model-windows")
		}
	}
	rowByName := map[string]wireRow{}
	for _, row := range rows {
		if row.Kind != "weekly_scoped" {
			continue
		}
		if row.Scope == nil || row.Scope.Model == nil {
			return nil, fail("invalid-model-windows")
		}
		name := strings.ToLower(row.Scope.Model.DisplayName)
		if name == "" {
			return nil, fail("invalid-model-windows")
		}
		if _, duplicate := rowByName[name]; duplicate {
			return nil, fail("duplicate-model-window")
		}
		if !boundNames[name] {
			return nil, fail("unbound-model-window")
		}
		rowByName[name] = row
	}
	for _, binding := range c.options.ModelBindings {
		raw := fields[binding.SourceKey]
		if binding.DisplayName != "" {
			row, exists := rowByName[strings.ToLower(binding.DisplayName)]
			if exists {
				raw, _ = json.Marshal(wireWindow{Utilization: row.Percent, ResetsAt: row.ResetsAt})
			} else {
				raw = nil
			}
		}
		if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			if binding.Optional {
				continue
			}
			return nil, fail("partial-model-window")
		}
		if err := appendWindow(binding.Window, binding.Label, binding.Selector, raw, 7*24*time.Hour); err != nil {
			return nil, err
		}
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Key.Window < snapshots[j].Key.Window })
	return snapshots, nil
}

// Status is local diagnostic state. It is never provider evidence or a health wire record.
type Status struct {
	Schema                 string     `json:"schema"`
	NativeVersion          string     `json:"nativeVersion"`
	AccountFingerprint     string     `json:"accountFingerprint"`
	State                  string     `json:"state"`
	ErrorCode              string     `json:"errorCode,omitempty"`
	HTTPStatus             int        `json:"httpStatus,omitempty"`
	LastAttemptStartedAt   time.Time  `json:"lastAttemptStartedAt"`
	LastAttemptCompletedAt time.Time  `json:"lastAttemptCompletedAt"`
	LastSuccessStartedAt   time.Time  `json:"lastSuccessStartedAt"`
	LastSuccessCompletedAt time.Time  `json:"lastSuccessCompletedAt"`
	LastSuccessWindowCount int        `json:"lastSuccessWindowCount"`
	NextAttemptAfter       *time.Time `json:"nextAttemptAfter,omitempty"`
}

func (c *Collector) loadStatus() error {
	identity := sha256.Sum256([]byte(c.options.AccountID))
	c.status = Status{Schema: "claude-native-quota/v1", NativeVersion: NativeVersion, AccountFingerprint: hex.EncodeToString(identity[:]), State: "pending"}
	if _, err := os.Lstat(c.options.StatusPath); os.IsNotExist(err) {
		return nil
	}
	body, err := privatefile.Read(c.options.StatusPath, MaximumBodyBytes)
	if err != nil {
		return fail("status-invalid")
	}
	var previous Status
	if json.Unmarshal(body, &previous) != nil || previous.Schema != c.status.Schema || previous.NativeVersion != NativeVersion || previous.AccountFingerprint != c.status.AccountFingerprint {
		return fail("status-invalid")
	}
	c.status = previous
	return nil
}
func (c *Collector) saveStatus() error {
	directory := filepath.Dir(c.options.StatusPath)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return fail("status-unavailable")
	}
	f, err := os.CreateTemp(directory, ".claude-quota-status-*")
	if err != nil {
		return fail("status-unavailable")
	}
	name := f.Name()
	defer os.Remove(name)
	if f.Chmod(0600) != nil {
		f.Close()
		return fail("status-unavailable")
	}
	body, err := json.Marshal(c.status)
	if err != nil {
		f.Close()
		return fail("status-unavailable")
	}
	if _, err = f.Write(append(body, '\n')); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return fail("status-unavailable")
	}
	if os.Rename(name, c.options.StatusPath) != nil {
		return fail("status-unavailable")
	}
	d, err := os.Open(directory)
	if err != nil {
		return fail("status-unavailable")
	}
	syncErr := d.Sync()
	closeErr = d.Close()
	if syncErr != nil || closeErr != nil {
		return fail("status-unavailable")
	}
	return nil
}
