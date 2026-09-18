package wait

import (
	"sort"
	"strconv"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// TrailerPrefix opens the first line of every wake message.
const TrailerPrefix = "t3-steward-wait"

// Field is one key=value pair of the wake trailer.
type Field struct {
	Key   string
	Value string
}

// F builds a Field.
func F(key, value string) Field { return Field{Key: key, Value: value} }

// WakeTrailer renders the first line of a wake message:
//
//	t3-steward-wait kind=<kind> outcome=<outcome> wait=<id> <key>=<value> ...
//
// The three leading pairs are fixed and come first; the kind-specific pairs
// follow in key order. Pairs are space-separated, a value is quoted when it
// contains a space, a quote or a control character, and a pair with an empty
// value is left out. Readers ignore keys they do not know, so a sender may add
// pairs; the order of the extra pairs is not part of the contract.
func WakeTrailer(kind, outcome, waitID string, fields ...Field) string {
	var b strings.Builder
	b.WriteString(TrailerPrefix)
	b.WriteString(" kind=")
	b.WriteString(quoteTrailerValue(kind))
	b.WriteString(" outcome=")
	b.WriteString(quoteTrailerValue(outcome))
	b.WriteString(" wait=")
	b.WriteString(quoteTrailerValue(waitID))
	extra := make([]Field, 0, len(fields))
	for _, f := range fields {
		if f.Key == "" || f.Value == "" || f.Key == "kind" || f.Key == "outcome" || f.Key == "wait" {
			continue
		}
		extra = append(extra, f)
	}
	sort.SliceStable(extra, func(i, j int) bool { return extra[i].Key < extra[j].Key })
	for _, f := range extra {
		b.WriteString(" ")
		b.WriteString(f.Key)
		b.WriteString("=")
		b.WriteString(quoteTrailerValue(f.Value))
	}
	return b.String()
}

// FieldsOf turns a map into trailer fields.
func FieldsOf(values map[string]string) []Field {
	fields := make([]Field, 0, len(values))
	for key, value := range values {
		fields = append(fields, F(key, value))
	}
	return fields
}

// quoteTrailerValue quotes a value that would otherwise break the line into
// the wrong pairs.
func quoteTrailerValue(value string) string {
	for _, r := range value {
		if r == ' ' || r == '"' || r == '\\' || r < ' ' || r == 0x7f {
			return strconv.Quote(value)
		}
	}
	return value
}

// ParseWakeTrailer reads the trailer from the first line of a wake message.
// It returns false when the first line is not a trailer.
func ParseWakeTrailer(message string) (map[string]string, bool) {
	line, _, _ := strings.Cut(message, "\n")
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, TrailerPrefix+" ") {
		return nil, false
	}
	rest := strings.TrimPrefix(line, TrailerPrefix+" ")
	pairs := map[string]string{}
	for rest != "" {
		rest = strings.TrimLeft(rest, " ")
		if rest == "" {
			break
		}
		equals := strings.IndexByte(rest, '=')
		if equals <= 0 {
			return nil, false
		}
		key := rest[:equals]
		rest = rest[equals+1:]
		if strings.HasPrefix(rest, "\"") {
			value, tail, err := unquoteTrailerPrefix(rest)
			if err != nil {
				return nil, false
			}
			pairs[key] = value
			rest = tail
			continue
		}
		value, tail, _ := strings.Cut(rest, " ")
		pairs[key] = value
		rest = tail
	}
	if pairs["kind"] == "" || pairs["outcome"] == "" {
		return nil, false
	}
	return pairs, true
}

// unquoteTrailerPrefix reads one quoted value from the start of s.
func unquoteTrailerPrefix(s string) (string, string, error) {
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case '"':
			value, err := strconv.Unquote(s[:i+1])
			return value, s[i+1:], err
		}
	}
	return "", "", strconv.ErrSyntax
}

// outcomeName is the trailer outcome of a settled local wait.
func outcomeName(w Wait) string {
	switch w.Status {
	case StatusMet:
		return string(domain.TaskWaitMet)
	case StatusFailed:
		return string(domain.TaskWaitFailed)
	case StatusGaveUp:
		return string(domain.TaskWaitGaveUp)
	case StatusTimedOut:
		return string(domain.TaskWaitTimedOut)
	case StatusCancelled:
		return string(domain.TaskWaitCancelled)
	}
	return string(w.Status)
}

// trailerFields are the kind-specific pairs of a local wait.
func trailerFields(w Wait) []Field {
	fields := FieldsOf(w.Fields)
	if w.Kind.OrShell() == domain.WaitKindShell {
		fields = append(fields, F("exit", strconv.Itoa(w.LastExit)))
	}
	if w.Kind == domain.WaitKindGitHub && w.GitHub != nil && w.Fields["target"] == "" {
		fields = append(fields, F("target", w.GitHub.Ref()))
	}
	if w.OrTimeout && w.Status == StatusTimedOut {
		fields = append(fields, F("or-timeout", "true"))
	}
	return fields
}

// taskTrailer is the first line of the wake a task receives for one
// coordinator-owned wait.
func taskTrailer(w domain.TaskWait) string {
	outcome := ""
	var fields []Field
	if w.Result != nil {
		outcome = string(w.Result.Outcome)
		fields = FieldsOf(w.Result.Fields)
		if w.Kind.OrShell() == domain.WaitKindShell {
			fields = append(fields, F("exit", strconv.Itoa(w.Result.ExitCode)))
		}
		if w.OrTimeout && w.Result.Outcome == domain.TaskWaitTimedOut {
			fields = append(fields, F("or-timeout", "true"))
		}
	}
	return WakeTrailer(string(w.Kind.OrShell()), outcome, w.ID, fields...)
}

// nodeTrailer is the first line of a node or quota wake.
func nodeTrailer(w domain.NodeWait) string {
	kind := w.Request.Kind()
	if w.Observation == nil {
		return WakeTrailer(string(kind), "", w.Request.ID)
	}
	observation := *w.Observation
	fields := FieldsOf(observation.Fields)
	if kind == domain.WaitKindNode {
		fields = FieldsOf(domain.NodeTrailerFields(observation))
	}
	outcome := domain.NodeObservationOutcome(observation)
	if w.Request.OrTimeout && outcome == domain.TaskWaitTimedOut {
		fields = append(fields, F("or-timeout", "true"))
	}
	return WakeTrailer(string(kind), string(outcome), w.Request.ID, fields...)
}
