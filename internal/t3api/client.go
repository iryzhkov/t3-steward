// Package t3api is a minimal HTTP client for the T3 Code server's
// environment API: the well-known descriptor, the orchestration shell
// snapshot, thread detail, and command dispatch.
//
// The API is internal to T3 and may change between versions; see
// docs/t3-protocol.md for the version this package was written against.
package t3api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Descriptor is the unauthenticated environment descriptor.
type Descriptor struct {
	EnvironmentID string `json:"environmentId"`
	Label         string `json:"label"`
	Platform      struct {
		OS   string `json:"os"`
		Arch string `json:"arch"`
	} `json:"platform"`
	ServerVersion string          `json:"serverVersion"`
	Capabilities  map[string]any  `json:"capabilities"`
	Raw           json.RawMessage `json:"-"`
}

// LatestTurn mirrors OrchestrationLatestTurn.
type LatestTurn struct {
	TurnID      string  `json:"turnId"`
	State       string  `json:"state"`
	RequestedAt string  `json:"requestedAt"`
	StartedAt   *string `json:"startedAt"`
	CompletedAt *string `json:"completedAt"`
}

// Session mirrors OrchestrationSession.
type Session struct {
	ThreadID           string  `json:"threadId"`
	Status             string  `json:"status"`
	ProviderName       *string `json:"providerName"`
	ProviderInstanceID string  `json:"providerInstanceId"`
	RuntimeMode        string  `json:"runtimeMode"`
	ActiveTurnID       *string `json:"activeTurnId"`
	UpdatedAt          string  `json:"updatedAt"`
}

// ModelSelection mirrors T3's model selection.
type ModelSelection struct {
	InstanceID string          `json:"instanceId"`
	Model      string          `json:"model"`
	Options    json.RawMessage `json:"options,omitempty"`
}

// ThreadShell mirrors OrchestrationThreadShell.
type ThreadShell struct {
	ID                  string          `json:"id"`
	ProjectID           string          `json:"projectId"`
	Title               string          `json:"title"`
	ModelSelection      json.RawMessage `json:"modelSelection"`
	RuntimeMode         string          `json:"runtimeMode"`
	InteractionMode     string          `json:"interactionMode"`
	LatestTurn          *LatestTurn     `json:"latestTurn"`
	CreatedAt           string          `json:"createdAt"`
	UpdatedAt           string          `json:"updatedAt"`
	ArchivedAt          *string         `json:"archivedAt"`
	DeletedAt           *string         `json:"deletedAt"`
	Session             *Session        `json:"session"`
	LatestUserMessageAt *string         `json:"latestUserMessageAt"`
	HasPendingApprovals bool            `json:"hasPendingApprovals"`
	HasPendingUserInput bool            `json:"hasPendingUserInput"`
	BackgroundLiveness  *string         `json:"backgroundLiveness"`
}

// Model decodes the model selection's instance and model ids.
func (t ThreadShell) Model() ModelSelection {
	var m ModelSelection
	_ = json.Unmarshal(t.ModelSelection, &m)
	return m
}

// ProjectShell mirrors OrchestrationProjectShell.
type ProjectShell struct {
	ID                    string          `json:"id"`
	Title                 string          `json:"title"`
	WorkspaceRoot         string          `json:"workspaceRoot"`
	DefaultModelSelection json.RawMessage `json:"defaultModelSelection"`
}

// ShellSnapshot mirrors OrchestrationShellSnapshot.
type ShellSnapshot struct {
	SnapshotSequence int64          `json:"snapshotSequence"`
	Projects         []ProjectShell `json:"projects"`
	Threads          []ThreadShell  `json:"threads"`
	UpdatedAt        string         `json:"updatedAt"`
}

// Message is the subset of OrchestrationMessage used to detect manual
// interaction after a stop.
type Message struct {
	ID        string `json:"id"`
	Role      string `json:"role"`
	Text      string `json:"text"`
	CreatedAt string `json:"createdAt"`
}

// ThreadDetail mirrors the thread part of OrchestrationThreadDetailSnapshot.
type ThreadDetail struct {
	ID              string          `json:"id"`
	Title           string          `json:"title"`
	ModelSelection  json.RawMessage `json:"modelSelection"`
	RuntimeMode     string          `json:"runtimeMode"`
	InteractionMode string          `json:"interactionMode"`
	LatestTurn      *LatestTurn     `json:"latestTurn"`
	ArchivedAt      *string         `json:"archivedAt"`
	DeletedAt       *string         `json:"deletedAt"`
	UpdatedAt       string          `json:"updatedAt"`
	Session         *Session        `json:"session"`
	Messages        []Message       `json:"messages"`
}

// DispatchResult mirrors T3's DispatchResult.
type DispatchResult struct {
	Sequence int64 `json:"sequence"`
}

// SessionInfo is the authenticated session description.
type SessionInfo struct {
	Authenticated bool     `json:"authenticated"`
	Scopes        []string `json:"scopes"`
	Raw           json.RawMessage
}

// TokenSource supplies bearer tokens.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
	// Invalidate forces the next Token call to mint a fresh token.
	Invalidate()
}

// StaticToken is a TokenSource for a fixed token.
type StaticToken string

// Token implements TokenSource.
func (s StaticToken) Token(context.Context) (string, error) { return string(s), nil }

// Invalidate implements TokenSource.
func (StaticToken) Invalidate() {}

// APIError is a non-2xx response.
type APIError struct {
	Status int
	Body   string
	Tag    string
	Reason string
}

func (e *APIError) Error() string {
	if e.Tag != "" {
		return fmt.Sprintf("T3 API %d %s (%s)", e.Status, e.Tag, e.Reason)
	}
	return fmt.Sprintf("T3 API %d: %s", e.Status, truncate(e.Body, 200))
}

// IsAuthError reports whether the error is a 401.
func IsAuthError(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusUnauthorized
}

// Client talks to one T3 server.
type Client struct {
	BaseURL string
	HTTP    *http.Client
	Tokens  TokenSource
	// UserAgent identifies the watchdog to the server.
	UserAgent string
}

// New returns a client with the given base URL and token source.
func New(baseURL string, tokens TokenSource, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		BaseURL:   strings.TrimRight(baseURL, "/"),
		HTTP:      &http.Client{Timeout: timeout},
		Tokens:    tokens,
		UserAgent: "t3-quota-watchdog",
	}
}

// Descriptor fetches /.well-known/t3/environment without authentication.
func (c *Client) Descriptor(ctx context.Context) (*Descriptor, error) {
	var d Descriptor
	raw, err := c.do(ctx, http.MethodGet, "/.well-known/t3/environment", nil, false)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("decode environment descriptor: %w", err)
	}
	d.Raw = raw
	return &d, nil
}

// SessionInfo fetches /api/auth/session for the current token.
func (c *Client) SessionInfo(ctx context.Context) (*SessionInfo, error) {
	raw, err := c.do(ctx, http.MethodGet, "/api/auth/session", nil, true)
	if err != nil {
		return nil, err
	}
	var s SessionInfo
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("decode session: %w", err)
	}
	s.Raw = raw
	return &s, nil
}

// ShellSnapshot fetches /api/orchestration/shell.
func (c *Client) ShellSnapshot(ctx context.Context) (*ShellSnapshot, error) {
	raw, err := c.do(ctx, http.MethodGet, "/api/orchestration/shell", nil, true)
	if err != nil {
		return nil, err
	}
	var s ShellSnapshot
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("decode shell snapshot: %w", err)
	}
	return &s, nil
}

// ThreadIndex lists every thread including archived ones from the full
// orchestration read model, decoding only identity fields.
func (c *Client) ThreadIndex(ctx context.Context) ([]ThreadShell, error) {
	raw, err := c.do(ctx, http.MethodGet, "/api/orchestration/snapshot", nil, true)
	if err != nil {
		return nil, err
	}
	var s struct {
		Threads []ThreadShell `json:"threads"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	return s.Threads, nil
}

// ThreadDetail fetches one thread with its last turnLimit turns.
func (c *Client) ThreadDetail(ctx context.Context, threadID string, turnLimit int) (*ThreadDetail, error) {
	path := "/api/orchestration/threads/" + url.PathEscape(threadID)
	if turnLimit > 0 {
		path += "?turnLimit=" + fmt.Sprint(turnLimit)
	}
	raw, err := c.do(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Thread ThreadDetail `json:"thread"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return nil, fmt.Errorf("decode thread detail: %w", err)
	}
	return &wrapper.Thread, nil
}

// Dispatch posts one ClientOrchestrationCommand.
func (c *Client) Dispatch(ctx context.Context, command map[string]any) (*DispatchResult, error) {
	body, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	raw, err := c.do(ctx, http.MethodPost, "/api/orchestration/dispatch", body, true)
	if err != nil {
		return nil, err
	}
	var r DispatchResult
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("decode dispatch result: %w", err)
	}
	return &r, nil
}

func (c *Client) do(ctx context.Context, method, path string, body []byte, auth bool) ([]byte, error) {
	raw, err := c.once(ctx, method, path, body, auth)
	if auth && IsAuthError(err) && c.Tokens != nil {
		c.Tokens.Invalidate()
		raw, err = c.once(ctx, method, path, body, auth)
	}
	return raw, err
}

func (c *Client) once(ctx context.Context, method, path string, body []byte, auth bool) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth {
		if c.Tokens == nil {
			return nil, errors.New("no token source configured")
		}
		token, err := c.Tokens.Token(ctx)
		if err != nil {
			return nil, fmt.Errorf("obtain T3 token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, fmt.Errorf("%s %s: read body: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		apiErr := &APIError{Status: resp.StatusCode, Body: string(raw)}
		var tagged struct {
			Tag    string `json:"_tag"`
			Reason string `json:"reason"`
			Detail string `json:"detail"`
		}
		if json.Unmarshal(raw, &tagged) == nil {
			apiErr.Tag = tagged.Tag
			apiErr.Reason = tagged.Reason
			if apiErr.Reason == "" {
				apiErr.Reason = tagged.Detail
			}
		}
		return nil, apiErr
	}
	return raw, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ParseTime parses T3's ISO timestamps; it returns nil for empty or
// unparsable values.
func ParseTime(s *string) *time.Time {
	if s == nil || *s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, *s)
	if err != nil {
		return nil
	}
	return &t
}
