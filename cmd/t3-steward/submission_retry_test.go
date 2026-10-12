package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func TestSubmissionRetryClassesAndBound(t *testing.T) {
	for _, class := range []backlogadmin.TransportClass{backlogadmin.ClassUnavailable, backlogadmin.ClassTimeout, backlogadmin.ClassClientConfiguration, backlogadmin.ClassAuthentication, backlogadmin.ClassProtocol, backlogadmin.ClassRejected, ""} {
		t.Run(string(class), func(t *testing.T) {
			failure := error(errors.New("lost answer\nremote stderr details"))
			if class != "" {
				failure = &backlogadmin.TransportError{Class: class, Err: failure}
			}
			calls := 0
			var stderr bytes.Buffer
			var pauses []time.Duration
			_, err := retrySubmission(context.Background(), &stderr, submissionRetryDelay, func(_ context.Context, d time.Duration) error {
				pauses = append(pauses, d)
				return nil
			}, func() (backlogadmin.LocalSubmissionResponse, error) {
				calls++
				return backlogadmin.LocalSubmissionResponse{}, failure
			})
			want := 1
			if class == backlogadmin.ClassUnavailable || class == backlogadmin.ClassTimeout {
				want = 3
			}
			if err != failure || calls != want || strings.Count(stderr.String(), "\n") != want-1 {
				t.Fatalf("calls=%d err=%v stderr=%q", calls, err, stderr.String())
			}
			for i, d := range pauses {
				base := 250 * time.Millisecond * time.Duration(1<<i)
				if d < base || d > base+base/2 {
					t.Fatalf("delay %d = %s", i, d)
				}
			}
		})
	}
}

func TestSubmissionRetryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	_, err := retrySubmission(ctx, io.Discard, submissionRetryDelay, func(ctx context.Context, d time.Duration) error {
		cancel()
		return waitSubmissionRetry(ctx, d)
	}, func() (backlogadmin.LocalSubmissionResponse, error) {
		calls++
		return backlogadmin.LocalSubmissionResponse{}, &backlogadmin.TransportError{Class: backlogadmin.ClassTimeout, Err: errors.New("deadline")}
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

// The delegate accepts the first request, but its reply is lost. Repeating
// the exact request must yield the delegate's existing idempotent replay.
func loseSubmissionReply(t *testing.T, delegate adminSubmissionService) adminSubmissionService {
	t.Helper()
	var first backlogadmin.LocalSubmissionRequest
	var archive []byte
	calls := 0
	return submissionFunc(func(ctx context.Context, request backlogadmin.LocalSubmissionRequest, body io.Reader, size int64) (backlogadmin.LocalSubmissionResponse, error) {
		raw, err := io.ReadAll(body)
		if err != nil || int64(len(raw)) != size {
			t.Fatalf("archive size=%d want=%d err=%v", len(raw), size, err)
		}
		calls++
		if calls == 1 {
			first, archive = request, append([]byte(nil), raw...)
		} else if !reflect.DeepEqual(first, request) || !bytes.Equal(archive, raw) {
			t.Fatal("retry changed request, key or archive")
		}
		response, err := delegate.SubmitArchive(ctx, request, bytes.NewReader(raw), size)
		if err == nil && calls == 1 {
			return backlogadmin.LocalSubmissionResponse{}, &backlogadmin.TransportError{Class: backlogadmin.ClassUnavailable, Err: errors.New("reply lost after acceptance")}
		}
		if err == nil && (!response.Replay || calls != 2) {
			t.Fatalf("response=%+v calls=%d", response, calls)
		}
		return response, err
	})
}

func assertSubmissionRetryOutput(t *testing.T, out, stderr *bytes.Buffer, asJSON bool) {
	t.Helper()
	if strings.Count(stderr.String(), "submission retry ") != 1 {
		t.Fatalf("stderr=%q", stderr.String())
	}
	if asJSON {
		var doc map[string]any
		if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
			t.Fatal(err)
		}
		if doc["replayed"] != true {
			t.Fatalf("receipt=%s", out.String())
		}
	} else if !strings.Contains(out.String(), "replayed=true") {
		t.Fatalf("receipt=%s", out.String())
	}
}

func TestSubmissionRetryLostReplyCLI(t *testing.T) {
	for _, verb := range []string{"submit", "fix", "task run"} {
		for _, asJSON := range []bool{false, true} {
			name := verb + "/text"
			if asJSON {
				name = verb + "/json"
			}
			t.Run(name, func(t *testing.T) {
				var out, stderr bytes.Buffer
				var err error
				switch verb {
				case "submit":
					f := &fixReplaySubmission{}
					cli := campaignCLI{limits: campaignTestLimits, stdout: &out, stderr: &stderr,
						submissions: func() (adminSubmissionService, error) { return loseSubmissionReply(t, f), nil },
						viability: func(_ context.Context, r backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
							return campaignReadyMatrix(r), nil
						},
					}
					args := []string{campaignFixture(t), "--idempotency-key", "retry-key", "--no-notify"}
					if asJSON {
						args = append(args, "--json")
					}
					err = cli.runSubmit(context.Background(), args)
					if len(f.archives) != 2 {
						t.Fatalf("attempts=%d", len(f.archives))
					}
				case "fix":
					cli, output := fixCommandFixture(t)
					f := armFixSubmission(t, &cli)
					cli.stderr = &stderr
					cli.submissions = func() (adminSubmissionService, error) { return loseSubmissionReply(t, f), nil }
					args := []string{"run/review", "--idempotency-key", "retry-key", "--no-notify"}
					if asJSON {
						args = append(args, "--json")
					}
					err = cli.run(context.Background(), args)
					out.Write(output.Bytes())
					if len(f.archives) != 2 {
						t.Fatalf("attempts=%d", len(f.archives))
					}
				case "task run":
					h := newTaskRunHarness()
					cli := h.cli()
					original := cli.campaign.submissions
					cli.campaign.submissions = func() (adminSubmissionService, error) {
						client, e := original()
						if e != nil {
							return nil, e
						}
						return loseSubmissionReply(t, client), nil
					}
					args := []string{"--model", "claude-haiku-4-5", "--no-notify"}
					if asJSON {
						args = append(args, "--json")
					}
					args = append(args, "--", "test lost submission reply")
					err = cli.run(context.Background(), args)
					out.Write(h.stdout.Bytes())
					stderr.Write(h.stderr.Bytes())
					if len(h.requests) != 2 || len(h.accepted) != 1 {
						t.Fatalf("requests=%d runs=%d", len(h.requests), len(h.accepted))
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				assertSubmissionRetryOutput(t, &out, &stderr, asJSON)
			})
		}
	}
}
