// Package t3 is the control adapter: it maps the watchdog's thread
// operations onto T3's orchestration HTTP API. Every T3-specific command
// name lives here and nowhere else.
package t3

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// StopMode selects how a thread is stopped.
type StopMode string

const (
	// StopInterrupt dispatches thread.turn.interrupt: the running turn ends,
	// the provider session stays alive for the next user message.
	StopInterrupt StopMode = "interrupt"
	// StopSession dispatches thread.session.stop: the provider process for
	// the thread is shut down.
	StopSession StopMode = "session-stop"
)

// Control drives threads through the T3 API.
type Control struct {
	client *t3api.Client
	log    *slog.Logger
	// DryRun makes every mutating call log instead of dispatching.
	DryRun bool
}

// New wraps a client.
func New(client *t3api.Client, logger *slog.Logger, dryRun bool) *Control {
	if logger == nil {
		logger = slog.Default()
	}
	return &Control{client: client, log: logger.With("component", "t3control"), DryRun: dryRun}
}

// Client exposes the underlying API client.
func (c *Control) Client() *t3api.Client { return c.client }

// ListThreads returns every live (non-archived, non-deleted) thread.
func (c *Control) ListThreads(ctx context.Context) ([]domain.Thread, error) {
	snap, err := c.client.ShellSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domain.Thread, 0, len(snap.Threads))
	for _, t := range snap.Threads {
		if t.DeletedAt != nil {
			continue
		}
		out = append(out, FromShell(t))
	}
	return out, nil
}

// FromShell converts a T3 thread shell into the domain thread.
func FromShell(t t3api.ThreadShell) domain.Thread {
	sel := t.Model()
	var selection map[string]any
	_ = json.Unmarshal(t.ModelSelection, &selection)
	d := domain.Thread{
		ID:                  t.ID,
		Title:               t.Title,
		ProjectID:           t.ProjectID,
		ProviderInstanceID:  sel.InstanceID,
		Model:               sel.Model,
		ModelSelection:      selection,
		RuntimeMode:         t.RuntimeMode,
		InteractionMode:     t.InteractionMode,
		LatestUserMessageAt: t3api.ParseTime(t.LatestUserMessageAt),
		HasPendingApprovals: t.HasPendingApprovals,
		HasPendingUserInput: t.HasPendingUserInput,
		ArchivedAt:          t3api.ParseTime(t.ArchivedAt),
		SettledAt:           t3api.ParseTime(t.SettledAt),
	}
	if up := t3api.ParseTime(&t.UpdatedAt); up != nil {
		d.UpdatedAt = *up
	}
	if t.BackgroundLiveness != nil {
		d.BackgroundWork = *t.BackgroundLiveness
	}
	if t.SettledOverride != nil {
		d.SettledOverride = *t.SettledOverride
	}
	if t.LatestTurn != nil {
		d.TurnID = t.LatestTurn.TurnID
		d.TurnState = t.LatestTurn.State
	}
	if t.Session != nil {
		d.SessionStatus = t.Session.Status
		if t.Session.ProviderInstanceID != "" && d.ProviderInstanceID == "" {
			d.ProviderInstanceID = t.Session.ProviderInstanceID
		}
	}
	d.Running = IsRunning(d)
	return d
}

// IsRunning is the watchdog's definition of a busy thread: a running turn,
// a starting or running provider session, or native background work
// (subagents, workflows) still alive after the turn settled.
func IsRunning(t domain.Thread) bool {
	if t.ArchivedAt != nil {
		return false
	}
	if t.TurnState == "running" {
		return true
	}
	switch t.SessionStatus {
	case "running", "starting":
		return true
	}
	return t.BackgroundWork == "working"
}

// GetThread fetches one thread's current shell state. It returns a nil
// thread when T3 no longer lists it (deleted).
func (c *Control) GetThread(ctx context.Context, threadID string) (*domain.Thread, error) {
	snap, err := c.client.ShellSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	for _, t := range snap.Threads {
		if t.ID == threadID {
			if t.DeletedAt != nil {
				return nil, nil
			}
			d := FromShell(t)
			return &d, nil
		}
	}
	return nil, nil
}

// LastUserMessageAt returns the newest user message time in the thread's
// recent turns, used to detect a manual message after a watchdog stop.
func (c *Control) LastUserMessageAt(ctx context.Context, threadID string) (*time.Time, error) {
	detail, err := c.client.ThreadDetail(ctx, threadID, 3)
	if err != nil {
		return nil, err
	}
	var newest *time.Time
	for _, m := range detail.Messages {
		if m.Role != "user" {
			continue
		}
		created := m.CreatedAt
		if t := t3api.ParseTime(&created); t != nil && (newest == nil || t.After(*newest)) {
			newest = t
		}
	}
	return newest, nil
}

// WarnThread delivers a message to a running thread. For Claude the T3
// server steers the live turn with it; for other providers T3 forwards a
// turn start that the provider queues or applies as it sees fit.
func (c *Control) WarnThread(ctx context.Context, thread domain.Thread, warning domain.Warning) error {
	return c.sendMessage(ctx, thread, warning.Text, string(warning.Kind))
}

// ResumeThread starts a new turn with the resume prompt.
func (c *Control) ResumeThread(ctx context.Context, thread domain.Thread, prompt string) error {
	return c.sendMessage(ctx, thread, prompt, "resume")
}

func (c *Control) sendMessage(ctx context.Context, thread domain.Thread, text, purpose string) error {
	if thread.ModelSelection == nil {
		return fmt.Errorf("thread %s has no model selection", thread.ID)
	}
	runtimeMode := thread.RuntimeMode
	if runtimeMode == "" {
		runtimeMode = "full-access"
	}
	interactionMode := thread.InteractionMode
	if interactionMode == "" {
		interactionMode = "default"
	}
	cmd := map[string]any{
		"type":      "thread.turn.start",
		"commandId": newID(),
		"threadId":  thread.ID,
		"message": map[string]any{
			"messageId":   newID(),
			"role":        "user",
			"text":        text,
			"attachments": []any{},
		},
		"modelSelection":  thread.ModelSelection,
		"runtimeMode":     runtimeMode,
		"interactionMode": interactionMode,
		"createdAt":       now(),
	}
	if c.DryRun {
		c.log.Info("dry-run: would send message", "purpose", purpose, "thread", thread.ID, "title", thread.Title)
		return nil
	}
	_, err := c.client.Dispatch(ctx, cmd)
	if err != nil {
		return fmt.Errorf("send %s message to thread %s: %w", purpose, thread.ID, err)
	}
	c.log.Info("message sent", "purpose", purpose, "thread", thread.ID, "title", thread.Title)
	return nil
}

// StopThread interrupts the running turn or stops the session.
func (c *Control) StopThread(ctx context.Context, thread domain.Thread, mode StopMode) error {
	var cmd map[string]any
	switch mode {
	case StopSession:
		cmd = map[string]any{
			"type":      "thread.session.stop",
			"commandId": newID(),
			"threadId":  thread.ID,
			"createdAt": now(),
		}
	default:
		cmd = map[string]any{
			"type":      "thread.turn.interrupt",
			"commandId": newID(),
			"threadId":  thread.ID,
			"createdAt": now(),
		}
		if thread.TurnID != "" && thread.TurnState == "running" {
			cmd["turnId"] = thread.TurnID
		}
	}
	if c.DryRun {
		c.log.Info("dry-run: would stop thread", "mode", string(mode), "thread", thread.ID, "title", thread.Title)
		return nil
	}
	if _, err := c.client.Dispatch(ctx, cmd); err != nil {
		return fmt.Errorf("stop thread %s (%s): %w", thread.ID, mode, err)
	}
	c.log.Info("stop dispatched", "mode", string(mode), "thread", thread.ID, "title", thread.Title)
	return nil
}

// WaitStopped polls until the thread is no longer running or the timeout
// passes. It returns the last observed thread.
func (c *Control) WaitStopped(ctx context.Context, threadID string, timeout time.Duration) (*domain.Thread, bool, error) {
	deadline := time.Now().Add(timeout)
	var last *domain.Thread
	for {
		t, err := c.GetThread(ctx, threadID)
		if err != nil {
			return last, false, err
		}
		if t == nil {
			return nil, true, nil
		}
		last = t
		if !t.Running {
			return t, true, nil
		}
		if time.Now().After(deadline) {
			return t, false, nil
		}
		select {
		case <-ctx.Done():
			return t, false, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// Project is a T3 project as the backlog runner needs it.
type Project struct {
	ID                    string
	Title                 string
	WorkspaceRoot         string
	DefaultModelSelection map[string]any
}

// ListProjects returns the projects known to the server.
func (c *Control) ListProjects(ctx context.Context) ([]Project, error) {
	snap, err := c.client.ShellSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Project, 0, len(snap.Projects))
	for _, p := range snap.Projects {
		proj := Project{ID: p.ID, Title: p.Title, WorkspaceRoot: p.WorkspaceRoot}
		if len(p.DefaultModelSelection) > 0 {
			_ = json.Unmarshal(p.DefaultModelSelection, &proj.DefaultModelSelection)
		}
		out = append(out, proj)
	}
	return out, nil
}

// NewThreadInput describes a thread to create and start.
type NewThreadInput struct {
	ProjectID       string
	Title           string
	ModelSelection  map[string]any
	RuntimeMode     string
	InteractionMode string
	Prompt          string
}

// CreateAndStartThread creates a thread and dispatches its first turn,
// returning the new thread id. The HTTP dispatch endpoint applies one
// command at a time, so the thread is created first.
func (c *Control) CreateAndStartThread(ctx context.Context, in NewThreadInput) (string, error) {
	threadID := newID()
	if in.RuntimeMode == "" {
		in.RuntimeMode = "full-access"
	}
	if in.InteractionMode == "" {
		in.InteractionMode = "default"
	}
	create := map[string]any{
		"type":            "thread.create",
		"commandId":       newID(),
		"threadId":        threadID,
		"projectId":       in.ProjectID,
		"title":           in.Title,
		"modelSelection":  in.ModelSelection,
		"runtimeMode":     in.RuntimeMode,
		"interactionMode": in.InteractionMode,
		"branch":          nil,
		"worktreePath":    nil,
		"createdAt":       now(),
	}
	turn := map[string]any{
		"type":      "thread.turn.start",
		"commandId": newID(),
		"threadId":  threadID,
		"message": map[string]any{
			"messageId":   newID(),
			"role":        "user",
			"text":        in.Prompt,
			"attachments": []any{},
		},
		"modelSelection":  in.ModelSelection,
		"titleSeed":       in.Title,
		"runtimeMode":     in.RuntimeMode,
		"interactionMode": in.InteractionMode,
		"createdAt":       now(),
	}
	if c.DryRun {
		c.log.Info("dry-run: would create and start thread", "title", in.Title, "project", in.ProjectID)
		return threadID, nil
	}
	if _, err := c.client.Dispatch(ctx, create); err != nil {
		return "", fmt.Errorf("create thread %q: %w", in.Title, err)
	}
	if _, err := c.client.Dispatch(ctx, turn); err != nil {
		return threadID, fmt.Errorf("start turn on thread %s: %w", threadID, err)
	}
	c.log.Info("thread created and started", "thread", threadID, "title", in.Title)
	return threadID, nil
}

// LastAssistantMessage returns the text of the newest assistant message
// in the thread's latest turn.
func (c *Control) LastAssistantMessage(ctx context.Context, threadID string) (string, error) {
	detail, err := c.client.ThreadDetail(ctx, threadID, 1)
	if err != nil {
		return "", err
	}
	text := ""
	for _, m := range detail.Messages {
		if m.Role == "assistant" && m.Text != "" {
			text = m.Text
		}
	}
	return text, nil
}

// ExportThread returns the thread's full detail as T3 serves it (all
// turns, messages, activities), for archiving.
func (c *Control) ExportThread(ctx context.Context, threadID string) ([]byte, error) {
	return c.client.ThreadDetailRaw(ctx, threadID)
}

// DeleteThread deletes a thread from T3.
func (c *Control) DeleteThread(ctx context.Context, threadID string) error {
	cmd := map[string]any{
		"type":      "thread.delete",
		"commandId": newID(),
		"threadId":  threadID,
	}
	if c.DryRun {
		c.log.Info("dry-run: would delete thread", "thread", threadID)
		return nil
	}
	if _, err := c.client.Dispatch(ctx, cmd); err != nil {
		return fmt.Errorf("delete thread %s: %w", threadID, err)
	}
	c.log.Info("thread deleted from T3", "thread", threadID)
	return nil
}

// ProjectTitle resolves a project id to its title, or returns the id.
func (c *Control) ProjectTitle(ctx context.Context, projectID string) string {
	projects, err := c.ListProjects(ctx)
	if err != nil {
		return projectID
	}
	for _, p := range projects {
		if p.ID == projectID {
			return p.Title
		}
	}
	return projectID
}

func now() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

// newID returns a random UUIDv4 string.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return strings.Join([]string{h[0:8], h[8:12], h[12:16], h[16:20], h[20:32]}, "-")
}
