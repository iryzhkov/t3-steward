package ownernotify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"
)

// memoryStore is an outbox with the store's contract and none of its SQL:
// rows change only while pending, and due rows come back oldest first.
type memoryStore struct {
	mu      sync.Mutex
	rows    map[string]*memoryRow
	seq     int
	detects int
}

type memoryRow struct {
	notification Notification
	result       AttemptResult
	seq          int
}

func newMemoryStore(notifications ...Notification) *memoryStore {
	s := &memoryStore{rows: map[string]*memoryRow{}}
	for _, n := range notifications {
		s.seq++
		s.rows[n.ID] = &memoryRow{notification: n, result: AttemptResult{State: StatePending}, seq: s.seq}
	}
	return s
}

func (s *memoryStore) DetectOwnerNotifications(context.Context, string, Selection, time.Time) (Detection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.detects++
	return Detection{}, nil
}

func (s *memoryStore) DueOwnerNotifications(_ context.Context, sink string, now time.Time, limit int) ([]Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []*memoryRow
	for _, row := range s.rows {
		if row.notification.Sink == sink && row.result.State == StatePending && !row.result.NextAttemptAt.After(now) {
			due = append(due, row)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].seq < due[j].seq })
	var out []Notification
	for _, row := range due[:min(len(due), limit)] {
		n := row.notification
		n.Attempts = row.result.Attempts
		out = append(out, n)
	}
	return out, nil
}

func (s *memoryStore) RecordOwnerNotificationAttempt(_ context.Context, id string, result AttemptResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[id]
	if !ok || row.result.State != StatePending {
		return fmt.Errorf("%s is not pending", id)
	}
	row.result = result
	return nil
}

func (s *memoryStore) SyncOwnerNotificationScopes(context.Context, map[string][]Scope) error {
	return nil
}

func (s *memoryStore) PruneOwnerNotifications(context.Context, time.Time, int) (int, error) {
	return 0, nil
}

func (s *memoryStore) OwnerNotificationHolds(context.Context, Notification) (bool, error) {
	return true, nil
}

func (s *memoryStore) row(id string) AttemptResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows[id].result
}

func runFailed(id string) Notification {
	return Notification{
		ID: DiscordSinkName + "|run-failed|" + id, Sink: DiscordSinkName, Event: EventRunFailed,
		RunID: id, Campaign: "nightly", Outcome: "failed", FailedTasks: []string{"implement"},
	}
}

// webhookServer answers every request with the next status in its script and
// records what it received.
type webhookServer struct {
	mu       sync.Mutex
	statuses []int
	headers  []http.Header
	bodies   []string
	server   *httptest.Server
}

func newWebhookServer(t *testing.T, statuses ...int) *webhookServer {
	t.Helper()
	w := &webhookServer{statuses: statuses}
	w.server = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.mu.Lock()
		defer w.mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		w.bodies = append(w.bodies, string(body))
		status := http.StatusNoContent
		if index := len(w.bodies) - 1; index < len(w.statuses) {
			status = w.statuses[index]
		}
		if index := len(w.bodies) - 1; index < len(w.headers) {
			for key, values := range w.headers[index] {
				rw.Header()[key] = values
			}
		}
		rw.WriteHeader(status)
		if status == http.StatusTooManyRequests {
			_, _ = rw.Write([]byte(`{"message":"You are being rate limited.","retry_after":42.5,"global":false}`))
		}
	}))
	t.Cleanup(w.server.Close)
	return w
}

func (w *webhookServer) requests() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.bodies...)
}

const testWebhookToken = "s3cr3t-webhook-token-abcdefghijklmnop"

func (w *webhookServer) url() string {
	return w.server.URL + "/api/webhooks/123456/" + testWebhookToken
}

// clock is a settable time source.
type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func TestParseEventsDefaultsAndRefusesUnknownOrRepeatedNames(t *testing.T) {
	events, err := ParseEvents(nil)
	if err != nil || len(events) != len(DefaultEvents()) {
		t.Fatalf("ParseEvents(nil) = %v, %v", events, err)
	}
	for _, event := range events {
		if event == EventGateReview {
			t.Fatal("gate-review must be opt-in")
		}
	}
	if _, err := ParseEvents([]string{"run-failed", "run-exploded"}); err == nil ||
		!strings.Contains(err.Error(), "run-exploded") || !strings.Contains(err.Error(), "gate-review") {
		t.Fatalf("unknown event error = %v", err)
	}
	if _, err := ParseEvents([]string{"needs-input", "needs-input"}); err == nil {
		t.Fatal("a repeated event was accepted")
	}
	if events, err := ParseEvents([]string{"gate-review"}); err != nil || len(events) != 1 || events[0] != EventGateReview {
		t.Fatalf("ParseEvents(gate-review) = %v, %v", events, err)
	}
}

// A failing webhook is retried on a doubling delay and abandoned at the bound,
// with one warning line that says so.
func TestNotifierRetriesWithBackoffAndGivesUp(t *testing.T) {
	server := newWebhookServer(t, 500, 502, 503, 500)
	store := newMemoryStore(runFailed("run-1"))
	clk := &clock{now: time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)}
	var logs bytes.Buffer
	notifier := &Notifier{
		Store: store, Sinks: []Sink{NewDiscord(server.url(), Selection{Events: DefaultEvents()}, server.server.Client())},
		Logger: slog.New(slog.NewTextHandler(&logs, nil)), Now: clk.Now, MaxAttempts: 4,
	}
	id := runFailed("run-1").ID
	start := clk.now
	for attempt, wantDelay := range []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute} {
		notifier.Tick(context.Background())
		got := store.row(id)
		if got.State != StatePending || got.Attempts != attempt+1 || !got.NextAttemptAt.Equal(clk.now.Add(wantDelay)) {
			t.Fatalf("after attempt %d: %+v, want next attempt in %s", attempt+1, got, wantDelay)
		}
		// Not due yet: a pass before the delay sends nothing.
		notifier.Tick(context.Background())
		if sent := len(server.requests()); sent != attempt+1 {
			t.Fatalf("sent %d requests before the backoff elapsed, want %d", sent, attempt+1)
		}
		clk.now = got.NextAttemptAt
	}
	notifier.Tick(context.Background())
	got := store.row(id)
	if got.State != StateAbandoned || got.Attempts != 4 || !strings.Contains(got.Error, "500") {
		t.Fatalf("after the bound: %+v", got)
	}
	if !strings.Contains(logs.String(), "owner notification abandoned") {
		t.Fatalf("no give-up line in:\n%s", logs.String())
	}
	clk.now = start.Add(24 * time.Hour)
	notifier.Tick(context.Background())
	if sent := len(server.requests()); sent != 4 {
		t.Fatalf("an abandoned notification was sent again (%d requests)", sent)
	}
}

// A refusal that retrying cannot change is abandoned on the first attempt.
func TestNotifierAbandonsAPermanentRefusalAtOnce(t *testing.T) {
	server := newWebhookServer(t, http.StatusNotFound)
	store := newMemoryStore(runFailed("run-1"))
	notifier := &Notifier{Store: store, Sinks: []Sink{NewDiscord(server.url(), Selection{Events: DefaultEvents()}, server.server.Client())},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	notifier.Tick(context.Background())
	if got := store.row(runFailed("run-1").ID); got.State != StateAbandoned || got.Attempts != 1 {
		t.Fatalf("404: %+v", got)
	}
}

// A 429 waits for Retry-After instead of the ordinary backoff, and holds back
// the rest of the batch, which would go to the same rate-limited receiver.
func TestNotifierHonoursDiscordRetryAfter(t *testing.T) {
	server := newWebhookServer(t, http.StatusTooManyRequests, http.StatusNoContent, http.StatusTooManyRequests)
	server.headers = []http.Header{{"Retry-After": {"7"}}}
	store := newMemoryStore(runFailed("run-1"), runFailed("run-2"))
	clk := &clock{now: time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)}
	notifier := &Notifier{Store: store, Sinks: []Sink{NewDiscord(server.url(), Selection{Events: DefaultEvents()}, server.server.Client())},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: clk.Now}
	notifier.Tick(context.Background())
	if sent := len(server.requests()); sent != 1 {
		t.Fatalf("a rate-limited pass sent %d requests, want 1", sent)
	}
	first := store.row(runFailed("run-1").ID)
	if first.State != StatePending || !first.NextAttemptAt.Equal(clk.now.Add(7*time.Second)) {
		t.Fatalf("after 429 with Retry-After 7: %+v", first)
	}
	if second := store.row(runFailed("run-2").ID); second.Attempts != 0 {
		t.Fatalf("the second row was attempted during a rate limit: %+v", second)
	}
	// Still inside the window only run-2 is due, and it goes out.
	notifier.Tick(context.Background())
	if second := store.row(runFailed("run-2").ID); second.State != StateDelivered {
		t.Fatalf("run-2 after the window: %+v", second)
	}
	// Without the header, the body's retry_after is used.
	clk.now = first.NextAttemptAt
	notifier.Tick(context.Background())
	if again := store.row(runFailed("run-1").ID); !again.NextAttemptAt.Equal(clk.now.Add(42500 * time.Millisecond)) {
		t.Fatalf("after 429 with retry_after 42.5 in the body: %+v", again)
	}
}

// The webhook URL is a credential. It must not appear in a log line, in the
// stored error, or in the sink's formatted value, even when net/http puts it
// in the error it returns.
func TestDiscordSecretNeverAppearsInLogsOrErrors(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close() // Nothing listens there now, so every send fails.
	webhook := "http://" + address + "/api/webhooks/123456/" + testWebhookToken
	store := newMemoryStore(runFailed("run-1"))
	var logs bytes.Buffer
	sink := NewDiscord(webhook, Selection{Events: DefaultEvents()}, nil)
	notifier := &Notifier{Store: store, Sinks: []Sink{sink},
		Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})), MaxAttempts: 2}
	notifier.Tick(context.Background())
	notifier.Now = func() time.Time { return time.Now().Add(time.Hour) }
	notifier.Tick(context.Background())
	got := store.row(runFailed("run-1").ID)
	if got.State != StateAbandoned || got.Error == "" {
		t.Fatalf("unreachable webhook: %+v", got)
	}
	direct := sink.Deliver(context.Background(), runFailed("run-1"))
	for name, text := range map[string]string{
		"log":          logs.String(),
		"stored error": got.Error,
		"direct error": fmt.Sprint(direct),
		"%v":           fmt.Sprintf("%v %+v %#v %s", sink, sink, sink, sink),
	} {
		if strings.Contains(text, testWebhookToken) || strings.Contains(text, "/api/webhooks/") {
			t.Fatalf("%s contains the webhook: %s", name, text)
		}
	}
	if !strings.Contains(logs.String(), "owner notification abandoned") {
		t.Fatalf("expected a give-up line:\n%s", logs.String())
	}
}

// A delivered message carries the outcome, the failed tasks and the command to
// read the result, and never allows mentions.
func TestDiscordDeliversOneShortMessage(t *testing.T) {
	server := newWebhookServer(t)
	store := newMemoryStore(runFailed("run-1"))
	notifier := &Notifier{Store: store, Sinks: []Sink{NewDiscord(server.url(), Selection{Events: DefaultEvents()}, server.server.Client())},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	notifier.Tick(context.Background())
	notifier.Tick(context.Background())
	requests := server.requests()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want exactly 1", len(requests))
	}
	var message struct {
		Content         string `json:"content"`
		AllowedMentions struct {
			Parse []string `json:"parse"`
		} `json:"allowed_mentions"`
	}
	if err := json.Unmarshal([]byte(requests[0]), &message); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"nightly", "`run-1`", "**failed**", "`implement`", "`t3-steward task result run-1`", "`t3-steward campaign show run-1`"} {
		if !strings.Contains(message.Content, want) {
			t.Fatalf("message %q lacks %q", message.Content, want)
		}
	}
	if message.AllowedMentions.Parse == nil || len(message.AllowedMentions.Parse) != 0 {
		t.Fatalf("mentions are not disabled: %s", requests[0])
	}
}

func TestRenderDiscordStaysUnderTheContentLimit(t *testing.T) {
	failed := make([]string, 500)
	for i := range failed {
		failed[i] = fmt.Sprintf("task-with-a-long-name-%03d", i)
	}
	for _, n := range []Notification{
		{Event: EventRunFailed, RunID: "run-1", Campaign: strings.Repeat("c", 3000), Outcome: "failed", FailedTasks: failed},
		{Event: EventNeedsInput, RunID: "run-1", Task: "t", WaitID: "tw-1", Prompt: strings.Repeat("why? ", 2000)},
		{Event: EventSupervisionEscalated, RunID: "run-1", Reason: strings.Repeat("r", 5000)},
	} {
		content := RenderDiscord(n)
		if count := utf8.RuneCountInString(content); count > discordContentLimit {
			t.Fatalf("%s rendered %d characters", n.Event, count)
		}
	}
	listed := RenderDiscord(Notification{Event: EventRunFailed, RunID: "run-1", Outcome: "failed", FailedTasks: failed})
	if !strings.Contains(listed, "and 492 more") || !strings.Contains(listed, "t3-steward task result run-1") {
		t.Fatalf("long task list rendered as %q", listed)
	}
	question := RenderDiscord(Notification{Event: EventNeedsInput, RunID: "run-1", Task: "deploy", WaitID: "tw-1", Prompt: "Ship it?"})
	if !strings.Contains(question, "> Ship it?") || !strings.Contains(question, "t3-steward wait inspect tw-1") {
		t.Fatalf("needs-input rendered as %q", question)
	}
}

// cancellingSink ends the notifier's context from inside a successful send,
// which is what a reload or shutdown right after the send looks like.
type cancellingSink struct {
	cancel context.CancelFunc
	sent   int
}

func (s *cancellingSink) Name() string         { return DiscordSinkName }
func (s *cancellingSink) Selection() Selection { return Selection{Events: DefaultEvents()} }
func (s *cancellingSink) Deliver(context.Context, Notification) error {
	s.sent++
	s.cancel()
	return nil
}

// A send that succeeded is recorded even when the context ends right after
// it, so the next coordinator does not send it again.
func TestNotifierRecordsASuccessfulSendAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newMemoryStore(runFailed("run-1"))
	sink := &cancellingSink{cancel: cancel}
	notifier := &Notifier{Store: store, Sinks: []Sink{sink}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	notifier.Tick(ctx)
	if got := store.row(runFailed("run-1").ID); sink.sent != 1 || got.State != StateDelivered {
		t.Fatalf("sent %d, row %+v", sink.sent, got)
	}
}

// Free text cannot format the owner's channel: no emphasis, header, link,
// masked link, quote or new line survives from a campaign name, prompt or
// reason.
func TestRenderDiscordEscapesMarkdownInFreeText(t *testing.T) {
	hostile := "# Pwned **bold** [click](https://evil.example) _x_ ~~s~~ ||spoiler|| @everyone\n> quoted"
	for _, n := range []Notification{
		{Event: EventRunFailed, RunID: "run-1", Campaign: hostile, Outcome: "failed"},
		{Event: EventNeedsInput, RunID: "run-1", Task: "t", WaitID: "tw-1", Prompt: hostile},
		{Event: EventSupervisionEscalated, RunID: "run-1", Reason: hostile},
	} {
		content := RenderDiscord(n)
		for _, forbidden := range []string{"**bold**", "[click]", "](", "https://", "_x_", "~~s~~", "||spoiler", "\n> quoted", "\n#"} {
			if strings.Contains(content, forbidden) {
				t.Fatalf("%s: %q survives in %q", n.Event, forbidden, content)
			}
		}
		// Every header mark and mention is escaped. (Mentions are also
		// disabled in the request itself.)
		if unescaped := regexp.MustCompile(`(^|[^\\])[#@]`); unescaped.MatchString(content) {
			t.Fatalf("%s: an unescaped # or @ survives in %q", n.Event, content)
		}
		if !strings.Contains(content, `\*\*bold\*\*`) {
			t.Fatalf("%s: the text itself was lost: %q", n.Event, content)
		}
	}
}

// The program sees PATH, HOME, LANG and the variables it was given, and none
// of the rest of the coordinator's environment.
func TestCommandSinkGetsAMinimalEnvironment(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("no /bin/sh")
	}
	t.Setenv("T3_STEWARD_TEST_SECRET", "hunter2")
	t.Setenv("NOTIFY_TOPIC", "fleet")
	out := filepath.Join(t.TempDir(), "env")
	sink := NewCommand([]string{"/bin/sh", "-c", `env > "$0"`, out}, []string{"NOTIFY_TOPIC"}, Selection{Events: DefaultEvents()})
	if err := sink.Deliver(context.Background(), runFailed("run-1")); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	environment := string(raw)
	if strings.Contains(environment, "hunter2") || !strings.Contains(environment, "NOTIFY_TOPIC=fleet") ||
		!strings.Contains(environment, "PATH=") {
		t.Fatalf("environment:\n%s", environment)
	}
}

// A timeout kills the program's whole process group, including a child it
// left running in the background.
func TestCommandSinkTimeoutKillsTheProcessGroup(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("no /bin/sh")
	}
	pidFile := filepath.Join(t.TempDir(), "child")
	sink := NewCommand([]string{"/bin/sh", "-c", `sleep 60 & echo $! > "$0"; wait`, pidFile}, nil, Selection{Events: DefaultEvents()})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := sink.Deliver(ctx, runFailed("run-1")); err == nil {
		t.Fatal("a timed-out command reported delivery")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the timeout took %s", elapsed)
	}
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	var pid int
	if _, err := fmt.Sscan(strings.TrimSpace(string(raw)), &pid); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for processAlive(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("background child %d survived the timeout", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func processAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return process.Signal(syscall.Signal(0)) == nil
}

// The command sink receives the event as versioned JSON on stdin, and a
// non-zero exit is a retryable failure that quotes the program's stderr.
func TestCommandSinkWritesJSONOnStdinAndRetriesAFailure(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("no /bin/sh")
	}
	out := filepath.Join(t.TempDir(), "event.json")
	sink := NewCommand([]string{"/bin/sh", "-c", `cat > "$0"`, out}, nil, Selection{Events: DefaultEvents()})
	notification := runFailed("run-1")
	notification.Sink = CommandSinkName
	if err := sink.Deliver(context.Background(), notification); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["schemaVersion"] != float64(CommandPayloadVersion) || payload["id"] != notification.ID ||
		payload["event"] != "run-failed" || payload["runId"] != "run-1" || payload["message"] == "" {
		t.Fatalf("payload = %s", raw)
	}
	if commands, _ := payload["commands"].(map[string]any); commands["result"] != "t3-steward task result run-1" {
		t.Fatalf("payload commands = %v", payload["commands"])
	}
	failing := NewCommand([]string{"/bin/sh", "-c", "echo webhook down >&2; exit 3"}, nil, Selection{Events: DefaultEvents()})
	err = failing.Deliver(context.Background(), notification)
	var classified *DeliveryError
	if !errors.As(err, &classified) || classified.Permanent || !strings.Contains(classified.Reason, "status 3") ||
		!strings.Contains(classified.Reason, "webhook down") {
		t.Fatalf("failing command: %v", err)
	}
}
