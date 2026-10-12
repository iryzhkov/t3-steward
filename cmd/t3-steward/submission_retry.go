package main

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// Submissions make at most three attempts. Backoff is 250ms then 500ms,
// each with uniform jitter of up to half the base delay. Only unavailable
// and timeout failures are retried; every attempt reuses the request and bytes.
func submitArchiveRetrying(ctx context.Context, client adminSubmissionService, request backlogadmin.LocalSubmissionRequest, archive []byte, stderr io.Writer) (backlogadmin.LocalSubmissionResponse, error) {
	return retrySubmission(ctx, stderr, submissionRetryDelay, waitSubmissionRetry, func() (backlogadmin.LocalSubmissionResponse, error) {
		return submitArchiveClaimingParent(ctx, client, request, archive)
	})
}

func submissionRetryDelay(retry int) time.Duration {
	base := 250 * time.Millisecond * time.Duration(1<<retry)
	return base + time.Duration(rand.Int64N(int64(base/2)+1))
}

func waitSubmissionRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retrySubmission(ctx context.Context, stderr io.Writer, delay func(int) time.Duration, wait func(context.Context, time.Duration) error, submit func() (backlogadmin.LocalSubmissionResponse, error)) (backlogadmin.LocalSubmissionResponse, error) {
	if stderr == nil {
		stderr = os.Stderr
	}
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return backlogadmin.LocalSubmissionResponse{}, err
		}
		response, err := submit()
		class := backlogadmin.ClassOf(err)
		if err == nil || attempt == 3 || (class != backlogadmin.ClassUnavailable && class != backlogadmin.ClassTimeout) {
			return response, err
		}
		pause := delay(attempt - 1)
		fmt.Fprintf(stderr, "submission retry %d/3 after %s: %s; waiting %s\n", attempt+1, class, strings.Join(strings.Fields(err.Error()), " "), pause)
		if err := wait(ctx, pause); err != nil {
			return backlogadmin.LocalSubmissionResponse{}, err
		}
	}
}
