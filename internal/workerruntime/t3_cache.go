package workerruntime

import (
	"context"
	"sync"
	"time"

	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// CachedT3 shares one shell snapshot within a worker reconciliation pass.
// LocalDriver.BeginObservationPass invalidates it at every pass boundary;
// mutations also invalidate it. Its lifetime may span a persistent worker's
// process, but observations must never span reconciliation passes.
type CachedT3 struct {
	Inner T3Control

	mu      sync.Mutex
	threads map[string]domain.Thread
	loaded  bool
}

func NewCachedT3(inner T3Control) *CachedT3 {
	return &CachedT3{Inner: inner}
}

func (c *CachedT3) invalidate() {
	c.mu.Lock()
	c.loaded = false
	c.threads = nil
	c.mu.Unlock()
}

func (c *CachedT3) load(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.loaded {
		return nil
	}
	threads, err := c.Inner.ListThreads(ctx)
	if err != nil {
		return err
	}
	c.threads = make(map[string]domain.Thread, len(threads))
	for _, thread := range threads {
		c.threads[thread.ID] = thread
	}
	c.loaded = true
	return nil
}

func (c *CachedT3) ListThreads(ctx context.Context) ([]domain.Thread, error) {
	if err := c.load(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]domain.Thread, 0, len(c.threads))
	for _, thread := range c.threads {
		result = append(result, thread)
	}
	return result, nil
}

func (c *CachedT3) GetThread(ctx context.Context, threadID string) (*domain.Thread, error) {
	if err := c.load(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	thread, ok := c.threads[threadID]
	if !ok {
		return nil, nil
	}
	copied := thread
	return &copied, nil
}

func (c *CachedT3) ResolveProjectID(ctx context.Context, project string) (string, error) {
	return c.Inner.ResolveProjectID(ctx, project)
}

func (c *CachedT3) CreateAndStartThread(ctx context.Context, input t3control.NewThreadInput) (string, error) {
	defer c.invalidate()
	return c.Inner.CreateAndStartThread(ctx, input)
}

func (c *CachedT3) StopThread(ctx context.Context, thread domain.Thread, mode t3control.StopMode) error {
	defer c.invalidate()
	return c.Inner.StopThread(ctx, thread, mode)
}

func (c *CachedT3) SettleThread(ctx context.Context, threadID, token string) error {
	defer c.invalidate()
	return c.Inner.SettleThread(ctx, threadID, token)
}

func (c *CachedT3) WaitStopped(ctx context.Context, threadID string, timeout time.Duration) (*domain.Thread, bool, error) {
	defer c.invalidate()
	return c.Inner.WaitStopped(ctx, threadID, timeout)
}

func (c *CachedT3) WarnThread(ctx context.Context, thread domain.Thread, warning domain.Warning) error {
	defer c.invalidate()
	return c.Inner.WarnThread(ctx, thread, warning)
}

func (c *CachedT3) ResumeThread(ctx context.Context, thread domain.Thread, prompt string) error {
	defer c.invalidate()
	return c.Inner.ResumeThread(ctx, thread, prompt)
}

func (c *CachedT3) LastAssistantMessage(ctx context.Context, threadID string) (string, error) {
	return c.Inner.LastAssistantMessage(ctx, threadID)
}

func (c *CachedT3) ExportThread(ctx context.Context, threadID string) ([]byte, error) {
	return c.Inner.ExportThread(ctx, threadID)
}

var _ T3Control = (*CachedT3)(nil)
