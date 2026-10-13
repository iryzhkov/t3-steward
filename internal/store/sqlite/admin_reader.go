package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrAdminReaderClosed = errors.New("admin reader closed")
	ErrAdminObserver     = errors.New("admin observer invalid; close and reopen reader")
	ErrAdminCoherence    = errors.New("admin assembly changed during all three attempts")
	adminGeneration      atomic.Uint64
)

// AdminReadToken is process-local, not a durable cache promise. Compare
// DataVersion only within the same nonzero Generation, never across reopen.
type AdminReadToken struct {
	Generation  uint64
	DataVersion int64
}

// AdminReader owns two live query-only pools and one retained observer Conn.
type AdminReader struct {
	observer, load *QueryOnlyStore
	conn           *sql.Conn
	adapter        *Store
	generation     uint64
	lease, closed  chan struct{}
	closeOnce      sync.Once
	closeErr       error
	invalid        bool                                            // protected by lease
	fence          func(context.Context, *sql.Conn) (int64, error) // per-instance failure seam
	waiting        func()                                          // per-instance contended-wait proof
	admitted       func()                                          // per-instance acquired-lease proof
}

// OpenAdminReader opens an existing file, without ownership or migrations.
// Cancellation is checked around OpenQueryOnly's non-contextual Ping; it cannot
// interrupt that Ping. The returned reader must be closed before its writer.
func OpenAdminReader(ctx context.Context, path string) (*AdminReader, error) {
	return openAdminReader(ctx, path, nil)
}

func openAdminReader(ctx context.Context, path string, after func(*AdminReader) error) (r *AdminReader, err error) {
	r = &AdminReader{lease: make(chan struct{}, 1), closed: make(chan struct{}), fence: adminDataVersion}
	r.lease <- struct{}{}
	defer func() {
		if err != nil {
			err = errors.Join(err, r.Close())
			r = nil
		}
	}()
	checkpoint := func() error {
		if after != nil {
			if err := after(r); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	if err = ctx.Err(); err != nil {
		return
	}
	for _, pool := range []**QueryOnlyStore{&r.load, &r.observer} {
		if *pool, err = OpenQueryOnly(path); err != nil {
			return
		}
		if err = checkpoint(); err != nil {
			return
		}
	}
	if r.conn, err = r.observer.db.Conn(ctx); err != nil {
		return
	}
	if err = checkpoint(); err != nil {
		return
	}
	r.adapter = &Store{db: r.load.db, now: time.Now, path: path}
	r.generation = adminGeneration.Add(1)
	return
}

// Assemble runs a synchronous read-only callback, at most three times. It must
// cooperate with context, replace staged output each attempt and never retain
// the adapter or lazy capabilities beyond invocation. Only success authorizes
// publication; arbitrary side effects cannot be rolled back. Reentrant Assemble
// or Close on this reader from its callback is unsupported. No callback escapes
// into a background goroutine. Context values/deadlines survive normalization.
func (r *AdminReader) Assemble(ctx context.Context, callback func(context.Context, *Store) error) (token AdminReadToken, err error) {
	if err = ctx.Err(); err != nil {
		return
	}
	select {
	case <-r.lease:
	default:
		if r.waiting != nil {
			r.waiting()
		}
		select {
		case <-ctx.Done():
			return token, ctx.Err()
		case <-r.closed:
			return token, ErrAdminReaderClosed
		case <-r.lease:
		}
	}
	defer func() { r.lease <- struct{}{} }()
	if r.admitted != nil {
		r.admitted()
	}
	// This check linearizes admission against closing the closed channel.
	select {
	case <-r.closed:
		return token, ErrAdminReaderClosed
	default:
	}
	if r.invalid {
		// Admission can win against cancellation when the released lease is ready.
		return token, errors.Join(ErrAdminObserver, ctx.Err())
	}
	defer func() {
		if canceled := ctx.Err(); canceled != nil {
			token = AdminReadToken{}
			healthCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, healthErr := adminDataVersion(healthCtx, r.conn)
			if healthErr != nil {
				r.invalid = true
				healthErr = errors.Join(ErrAdminObserver, healthErr)
			}
			err = errors.Join(err, canceled, healthErr)
		}
	}()
	ctx = context.WithValue(WithoutCoordinatorRecordsSnapshot(ctx), activeCoordinatorRecordsKey{}, false)
	for attempt := 0; attempt < 3; attempt++ {
		if err = ctx.Err(); err != nil {
			return
		}
		before, readErr := r.version(ctx)
		if readErr != nil {
			return token, readErr
		}
		if err = ctx.Err(); err != nil {
			return
		}
		if err = callback(ctx, r.adapter); err != nil {
			return
		}
		if err = ctx.Err(); err != nil {
			return
		}
		after, readErr := r.version(ctx)
		if readErr != nil {
			return token, readErr
		}
		if before == after {
			return AdminReadToken{r.generation, after}, nil
		}
	}
	return token, ErrAdminCoherence
}

func adminDataVersion(ctx context.Context, conn *sql.Conn) (version int64, err error) {
	err = conn.QueryRowContext(ctx, "PRAGMA data_version").Scan(&version)
	return
}

func (r *AdminReader) version(ctx context.Context) (int64, error) {
	version, err := r.fence(ctx, r.conn)
	if err != nil && ctx.Err() == nil {
		r.invalid = true
		err = errors.Join(ErrAdminObserver, err)
	}
	return version, err
}

// Close rejects/wakes waiting calls before waiting for the active assembly.
// It does not cancel that callback; cooperative completion can take arbitrarily
// long. Concurrent/repeated callers wait for all cleanup and receive its result.
func (r *AdminReader) Close() error {
	r.closeOnce.Do(func() {
		close(r.closed)
		<-r.lease
		defer func() { r.lease <- struct{}{} }()
		if r.conn != nil {
			r.closeErr = r.conn.Close()
		}
		for _, pool := range []*QueryOnlyStore{r.observer, r.load} {
			if pool != nil {
				r.closeErr = errors.Join(r.closeErr, pool.Close())
			}
		}
	})
	return r.closeErr
}
