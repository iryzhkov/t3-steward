package ownernotify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// DiscordSinkName is the Discord sink's identity in the outbox.
const DiscordSinkName = "discord"

// discordContentLimit is Discord's limit on a message's content, in
// characters. A longer message is refused with 400, which retrying cannot fix.
const discordContentLimit = 2000

// Discord posts notifications to a Discord webhook.
//
// The webhook URL is a credential: anyone holding it can post to the channel.
// It is held in an unexported field, the type prints as its name alone, and
// every error the sink returns is scrubbed of it, because net/http names the
// URL it failed to reach in its errors and those errors are logged and stored.
type Discord struct {
	webhookURL string
	selection  Selection
	client     *http.Client
}

// NewDiscord returns a Discord sink. A nil client uses a client with no
// timeout of its own: every send is bounded by the notifier's context.
func NewDiscord(webhookURL string, selection Selection, client *http.Client) *Discord {
	if client == nil {
		client = &http.Client{}
	}
	selection.Events = append([]Event(nil), selection.Events...)
	return &Discord{webhookURL: webhookURL, selection: selection, client: client}
}

// Name implements Sink.
func (d *Discord) Name() string { return DiscordSinkName }

// Selection implements Sink.
func (d *Discord) Selection() Selection { return d.selection }

// String and GoString keep the webhook URL out of any formatted value.
func (d *Discord) String() string   { return "discord webhook" }
func (d *Discord) GoString() string { return "ownernotify.Discord{webhook: [redacted]}" }

// discordMessage is the webhook body. Mentions are disabled outright: the
// content quotes campaign names and task prompts, and a prompt that contains
// @everyone must not page a whole server.
type discordMessage struct {
	Content         string                 `json:"content"`
	AllowedMentions discordAllowedMentions `json:"allowed_mentions"`
}

type discordAllowedMentions struct {
	Parse []string `json:"parse"`
}

// Deliver implements Sink.
func (d *Discord) Deliver(ctx context.Context, notification Notification) error {
	body, err := json.Marshal(discordMessage{
		Content:         RenderDiscord(notification),
		AllowedMentions: discordAllowedMentions{Parse: []string{}},
	})
	if err != nil {
		return &DeliveryError{Reason: "encode discord message: " + err.Error(), Permanent: true}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, d.webhookURL, bytes.NewReader(body))
	if err != nil {
		// The error names the URL; the configuration already validated its
		// shape, so say what failed and nothing else.
		return &DeliveryError{Reason: "discord webhook URL is not a valid request target", Permanent: true}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "t3-steward")
	response, err := d.client.Do(request)
	if err != nil {
		return &DeliveryError{Reason: "discord webhook request failed: " + d.scrub(err)}
	}
	defer response.Body.Close()
	peek, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	switch status := response.StatusCode; {
	case status >= 200 && status < 300:
		return nil
	case status == http.StatusTooManyRequests:
		return &DeliveryError{
			Reason:     "discord rate-limited the webhook (429)",
			RetryAfter: discordRetryAfter(response.Header, peek),
		}
	case status == http.StatusRequestTimeout || status >= 500:
		return &DeliveryError{Reason: "discord answered " + response.Status}
	default:
		// 400, 401, 403 and 404 mean a malformed message, a revoked token or
		// a deleted webhook. None of them changes by sending again.
		return &DeliveryError{Reason: "discord refused the webhook: " + response.Status, Permanent: true}
	}
}

// scrub renders an error without the webhook URL or its token.
func (d *Discord) scrub(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	return ScrubSecret(err.Error(), d.webhookURL)
}

// ScrubSecret removes a secret URL, and each of its path segments long enough
// to be a token, from text. It is exported so that a caller logging something
// derived from the same secret can apply the same rule.
func ScrubSecret(text, secret string) string {
	if secret == "" {
		return text
	}
	text = strings.ReplaceAll(text, secret, "[redacted]")
	if parsed, err := url.Parse(secret); err == nil {
		for _, segment := range strings.Split(parsed.Path, "/") {
			if len(segment) >= 16 {
				text = strings.ReplaceAll(text, segment, "[redacted]")
			}
		}
	}
	return text
}

// discordRetryAfter reads the delay a 429 asks for: the Retry-After header in
// seconds, which Discord may send as a fraction, or else the retry_after field
// of the JSON body. A 429 that states neither waits five seconds.
func discordRetryAfter(header http.Header, body []byte) time.Duration {
	if value := strings.TrimSpace(header.Get("Retry-After")); value != "" {
		if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 {
			return secondsDuration(seconds)
		}
		if at, err := http.ParseTime(value); err == nil {
			if delay := time.Until(at); delay > 0 {
				return delay
			}
		}
	}
	var parsed struct {
		RetryAfter float64 `json:"retry_after"`
	}
	if json.Unmarshal(body, &parsed) == nil && parsed.RetryAfter > 0 {
		return secondsDuration(parsed.RetryAfter)
	}
	return 5 * time.Second
}

func secondsDuration(seconds float64) time.Duration {
	if seconds > maxRetryAfter.Seconds() {
		return maxRetryAfter
	}
	delay := time.Duration(seconds * float64(time.Second))
	return max(delay, time.Second)
}

// RenderDiscord renders a notification as one short Discord message, always
// within Discord's content limit. The prompt and reason are the only unbounded
// parts and are shortened first; the result is cut as a last resort.
func RenderDiscord(n Notification) string {
	var b strings.Builder
	subject := "run " + code(n.RunID)
	if n.Campaign != "" {
		subject = "campaign **" + escapeMarkdown(truncate(n.Campaign, 200)) + "** " + subject
	}
	switch n.Event {
	case EventRunSucceeded, EventRunFailed, EventRunCancelled, EventRunSkipped:
		fmt.Fprintf(&b, "t3-steward: %s **%s**.", subject, escapeMarkdown(n.Outcome))
		if len(n.FailedTasks) != 0 {
			fmt.Fprintf(&b, " Failed: %s.", taskList(n.FailedTasks))
		}
		if len(n.CancelledTasks) != 0 {
			fmt.Fprintf(&b, " Cancelled: %s.", taskList(n.CancelledTasks))
		}
		fmt.Fprintf(&b, "\nResult: %s\nDetails: %s", code("t3-steward task result "+n.RunID), code("t3-steward campaign show "+n.RunID))
	case EventNeedsInput:
		fmt.Fprintf(&b, "t3-steward: %s needs input", subject)
		if n.Task != "" {
			fmt.Fprintf(&b, " on task %s", code(n.Task))
		}
		b.WriteString(".")
		if n.Prompt != "" {
			b.WriteString("\n> " + escapeMarkdown(truncate(n.Prompt, 600)))
		}
		fmt.Fprintf(&b, "\nInspect: %s", code("t3-steward wait inspect "+n.WaitID))
	case EventSupervisionEscalated:
		fmt.Fprintf(&b, "t3-steward: %s is waiting for an operator", subject)
		if n.Gate != "" {
			fmt.Fprintf(&b, " at gate %s", code(n.Gate))
		}
		b.WriteString(".")
		if n.Reason != "" {
			b.WriteString(" " + escapeMarkdown(truncate(n.Reason, 400)))
		}
		fmt.Fprintf(&b, "\nSupervision: %s", code("t3-steward campaign supervision show "+n.RunID))
	case EventGateReview:
		fmt.Fprintf(&b, "t3-steward: %s gate %s is ready for review.", subject, code(n.Gate))
		fmt.Fprintf(&b, "\nSupervision: %s", code("t3-steward campaign supervision show "+n.RunID))
	default:
		fmt.Fprintf(&b, "t3-steward: %s: %s", subject, n.Event)
	}
	return truncate(b.String(), discordContentLimit)
}

// taskList names at most eight tasks, so a run with hundreds of failures
// still fits in one message and says how many were left out.
func taskList(tasks []string) string {
	const shown = 8
	names := make([]string, 0, min(len(tasks), shown))
	for _, task := range tasks[:min(len(tasks), shown)] {
		names = append(names, code(task))
	}
	list := strings.Join(names, ", ")
	if len(tasks) > shown {
		list += fmt.Sprintf(" and %d more", len(tasks)-shown)
	}
	return list
}

// code renders an identifier as inline code. Markdown is not interpreted
// inside a code span, but a backtick would end the span early, so it is
// replaced, and the span is kept on one line.
func code(text string) string {
	return "`" + strings.ReplaceAll(inline(text), "`", "'") + "`"
}

// inline trims text and turns every control character, newlines included,
// into a space, so free text stays on its own line of the message.
func inline(text string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, strings.TrimSpace(text))
}

// markdownEscaper backslash-escapes every character Discord gives a meaning:
// emphasis, strikethrough, spoilers, code, quotes, headers, lists and masked
// links.
var markdownEscaper = strings.NewReplacer(
	`\`, `\\`, "*", `\*`, "_", `\_`, "~", `\~`, "`", "\\`", "|", `\|`, ">", `\>`,
	"#", `\#`, "-", `\-`, "[", `\[`, "]", `\]`, "(", `\(`, ")", `\)`, "<", `\<`, "@", `\@`,
	// A bare URL is linked by Discord whatever surrounds it; a zero-width
	// space after the scheme keeps it as text.
	"://", ":"+string(rune(0x200b))+"//",
)

// escapeMarkdown renders free text (campaign names, prompts, reasons) as
// plain text on one line, so nothing a campaign author or an agent wrote can
// format the owner's channel, add a link or start a header.
func escapeMarkdown(text string) string {
	return markdownEscaper.Replace(inline(text))
}

// truncate shortens text to at most limit characters, marking the cut.
func truncate(text string, limit int) string {
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	runes := []rune(text)
	return string(runes[:limit-1]) + "…"
}
