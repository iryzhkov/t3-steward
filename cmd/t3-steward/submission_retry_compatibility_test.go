package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestSubmissionRetryCompatibilityBound(t *testing.T) {
	for _, class := range []backlogadmin.TransportClass{backlogadmin.ClassUnavailable, backlogadmin.ClassTimeout} {
		for _, replay := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/replay=%t", class, replay), func(t *testing.T) {
				calls := 0
				var stderr bytes.Buffer
				request := backlogadmin.LocalSubmissionRequest{IdempotencyKey: "same-key",
					Parent: &domain.SubmissionParent{RunID: "parent-run", TaskID: "parent-task", AttemptID: "parent-attempt"}}
				parentless := request
				parentless.Parent = nil
				failure := &backlogadmin.TransportError{Class: class, Err: errors.New("lost reply")}
				client := submissionFunc(func(_ context.Context, req backlogadmin.LocalSubmissionRequest, body io.Reader, size int64) (backlogadmin.LocalSubmissionResponse, error) {
					calls++
					raw, err := io.ReadAll(body)
					if err != nil || string(raw) != "same-archive" || size != int64(len(raw)) {
						t.Fatalf("archive=%q size=%d err=%v", raw, size, err)
					}
					if req.Parent != nil {
						return backlogadmin.LocalSubmissionResponse{}, &backlogadmin.TransportError{Class: backlogadmin.ClassProtocol, Err: errors.New("invalid operation envelope: json: unknown field \"parent\"")}
					}
					if !reflect.DeepEqual(req, parentless) {
						t.Fatal("negotiated request changed")
					}
					if replay && calls == 3 {
						return backlogadmin.LocalSubmissionResponse{RunID: "original-run", Replay: true}, nil
					}
					return backlogadmin.LocalSubmissionResponse{}, failure
				})
				response, err := retryArchiveSubmission(context.Background(), client, request, []byte("same-archive"), &stderr, submissionRetryDelay, func(context.Context, time.Duration) error { return nil })
				wantErr := error(failure)
				if replay {
					wantErr = nil
					if response.RunID != "original-run" || !response.Replay {
						t.Fatalf("receipt=%+v", response)
					}
				}
				if request.Parent == nil {
					t.Fatal("caller request mutated")
				}
				if calls != 3 || strings.Count(stderr.String(), "\n") != 2 || err != wantErr {
					t.Fatalf("maximum 3 transport attempts; got %d sends and %d retry lines; final error: %v", calls, strings.Count(stderr.String(), "\n"), err)
				}
			})
		}
	}
}
