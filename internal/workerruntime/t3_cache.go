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
//
// The listing itself runs without the cache's lock. A mutation invalidates
// the cache by moving its generation, and a listing that was in flight across
// that move answers the call that started it but is not kept: it may predate
// the mutation. Holding the lock across the listing would make every
// mutation of the worker wait for a slow T3 to answer a listing.
type CachedT3 struct {
	Inner T3Control

	mu         sync.Mutex
	threads    map[string]domain.Thread
	loaded     bool
	generation uint64
}

func NewCachedT3(inner T3Control) *CachedT3 {
	return &CachedT3{Inner: inner}
}

func (c *CachedT3) invalidate() {
	c.mu.Lock()
	c.loaded = false
	c.threads = nil
	c.generation++
	c.mu.Unlock()
}

// load returns the pass's thread snapshot, listing T3 without the lock when
// none is cached. The returned map is never modified afterwards.
func (c *CachedT3) load(ctx context.Context) (map[string]domain.Thread, error) {
	c.mu.Lock()
	if c.loaded {
		threads := c.threads
		c.mu.Unlock()
		return threads, nil
	}
	generation := c.generation
	c.mu.Unlock()
	listed, err := c.Inner.ListThreads(ctx)
	if err != nil {
		return nil, err
	}
	threads := make(map[string]domain.Thread, len(listed))
	for _, thread := range listed {
		threads[thread.ID] = thread
	}
	c.mu.Lock()
	if c.generation == generation && !c.loaded {
		c.threads, c.loaded = threads, true
	}
	c.mu.Unlock()
	return threads, nil
}

func (c *CachedT3) ListThreads(ctx context.Context) ([]domain.Thread, error) {
	threads, err := c.load(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]domain.Thread, 0, len(threads))
	for _, thread := range threads {
		result = append(result, thread)
	}
	return result, nil
}

func (c *CachedT3) GetThread(ctx context.Context, threadID string) (*domain.Thread, error) {
	threads, err := c.load(ctx)
	if err != nil {
		return nil, err
	}
	thread, ok := threads[threadID]
	if !ok {
		return nil, nil
	}
	copied := thread
	return &copied, nil
}

func (c *CachedT3) EnsureProject(ctx context.Context, input t3control.ManagedProject) (string, error) {
	defer c.invalidate()
	return c.Inner.EnsureProject(ctx, input)
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
