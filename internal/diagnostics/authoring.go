// Package diagnostics carries authoring evidence without changing parser authority.
package diagnostics

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const SchemaVersion = "diagnostics/v1"

type Document struct {
	SchemaVersion string  `json:"schemaVersion"`
	OK            bool    `json:"ok"`
	Entries       []Entry `json:"entries"`
}

type Entry struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	File     string `json:"file"`
	Line     int    `json:"line"`
	Column   int    `json:"column"`
	Path     string `json:"path"`
	Message  string `json:"message"`
	Hint     string `json:"hint"`
}

type carrier struct {
	cause    error
	document Document
}

func (e *carrier) Error() string                  { return e.cause.Error() }
func (e *carrier) Unwrap() error                  { return e.cause }
func (e *carrier) AuthoringDiagnostics() Document { return e.document }

// Attach preserves the original message and error chain.
func Attach(err error, document Document) error {
	if err == nil {
		return nil
	}
	return &carrier{err, document}
}

func Refusal(code, file string, line int, message, hint string) Document {
	return Document{SchemaVersion: SchemaVersion, Entries: []Entry{{Code: code, Severity: "error", File: file, Line: line, Message: message, Hint: hint}}}
}

// FromError collects carriers through wrapping and joins. Legacy errors have
// unknown coordinates unless the owner explicitly supplies a location.
func FromError(err error, file string) Document {
	d := Document{SchemaVersion: SchemaVersion, OK: err == nil, Entries: []Entry{}}
	var walk func(error)
	walk = func(e error) {
		if e == nil {
			return
		}
		if c, ok := e.(interface{ AuthoringDiagnostics() Document }); ok {
			for _, entry := range c.AuthoringDiagnostics().Entries {
				if entry.File == "" {
					entry.File = file
				}
				d.Entries = append(d.Entries, entry)
			}
			return
		}
		if j, ok := e.(interface{ Unwrap() []error }); ok {
			for _, child := range j.Unwrap() {
				walk(child)
			}
			return
		}
		// Preserve a wrapper's full human message for an unconverted refusal.
		var c interface {
			error
			AuthoringDiagnostics() Document
		}
		if errors.As(e, &c) {
			if wrapped, ok := e.(interface{ Unwrap() error }); ok {
				walk(wrapped.Unwrap())
				return
			}
		}
		entry := Entry{Code: "legacy", Severity: "error", File: file, Message: e.Error()}
		var location interface{ DiagnosticsLocation() (string, int) }
		if errors.As(e, &location) {
			entry.File, entry.Line = location.DiagnosticsLocation()
		}
		d.Entries = append(d.Entries, entry)
	}
	walk(err)
	sortEntries(d.Entries)
	return d
}

func sortEntries(entries []Entry) {
	sort.SliceStable(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if a.Line != b.Line {
			return a.Line < b.Line
		}
		if a.Column != b.Column {
			return a.Column < b.Column
		}
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		return a.Code < b.Code
	})
}

func Text(err error, file string) string {
	var out strings.Builder
	for _, e := range FromError(err, file).Entries {
		fmt.Fprintf(&out, "\n%s:%s:%s: %s: %s", e.File, coordinate(e.Line), coordinate(e.Column), e.Code, e.Message)
		if e.Hint != "" {
			fmt.Fprintf(&out, "\n  hint: %s", e.Hint)
		}
	}
	return out.String()
}
func coordinate(n int) string {
	if n == 0 {
		return "?"
	}
	return strconv.Itoa(n)
}

var linePattern = regexp.MustCompile(`(?:^|\s)line (\d+): (.*)$`)
var unknownPattern = regexp.MustCompile(`^field (\S+) not found in type .+$`)
var implementationTypePattern = regexp.MustCompile(` into \*?[a-z][A-Za-z0-9_]*\.[A-Z][A-Za-z0-9_]*$`)

var duplicatePattern = regexp.MustCompile(`^mapping key "(.+)" already defined at line \d+$`)

type nodeLocation struct {
	node      *yaml.Node
	path      string
	key       bool
	flowTitle bool
}

// YAML normalizes one owner decoder pass. Parsing into nodes supplies evidence
// only: it neither retries strict decoding nor makes a failed input acceptable.
// human rewrites implementation type names using the owner's vocabulary.
func YAML(err error, raw []byte, file string, human func(string) string, hints map[string]string) Document {
	var document yaml.Node
	_ = yaml.Unmarshal(raw, &document)
	var locations []nodeLocation
	var visit func(*yaml.Node, string, bool)
	visit = func(n *yaml.Node, path string, key bool) {
		locations = append(locations, nodeLocation{node: n, path: path, key: key})
		switch n.Kind {
		case yaml.DocumentNode:
			for _, child := range n.Content {
				visit(child, path, false)
			}
		case yaml.MappingNode:
			flowTitle := false
			if n.Style&yaml.FlowStyle != 0 {
				for i := 0; i+1 < len(n.Content); i += 2 {
					if n.Content[i].Value == "title" && n.Content[i+1].Style == 0 {
						flowTitle = true
					}
				}
			}
			for i := 0; i+1 < len(n.Content); i += 2 {
				k, v := n.Content[i], n.Content[i+1]
				next := k.Value
				if path != "" {
					next = path + "." + next
				}
				visit(k, next, true)
				locations[len(locations)-1].flowTitle = flowTitle
				visit(v, next, false)
			}
		case yaml.SequenceNode:
			for i, child := range n.Content {
				visit(child, fmt.Sprintf("%s[%d]", path, i), false)
			}
		}
	}
	visit(&document, "", false)
	messages := []string{err.Error()}
	var typeErr *yaml.TypeError
	if errors.As(err, &typeErr) {
		messages = typeErr.Errors
	}
	d := Document{SchemaVersion: SchemaVersion, Entries: []Entry{}}
	for _, message := range messages {
		e := Entry{Code: "yaml-syntax", Severity: "error", File: file, Message: message, Hint: "Check YAML indentation and quoting; block-style mappings can make the error easier to locate."}
		detail := strings.TrimPrefix(message, "yaml: ")
		if m := linePattern.FindStringSubmatch(detail); m != nil {
			e.Line, _ = strconv.Atoi(m[1])
			detail = m[2]
		}
		field := ""
		if m := unknownPattern.FindStringSubmatch(detail); m != nil {
			e.Code = "yaml-unknown-field"
			field = m[1]
			e.Hint = "Check this field against the supported authoring format."
		} else if m := duplicatePattern.FindStringSubmatch(detail); m != nil {
			e.Code = "yaml-duplicate-key"
			field = m[1]
			e.Hint = "Remove the duplicate key; YAML mappings require unique keys."
		} else if typeErr != nil {
			e.Code = "yaml-type"
			e.Hint = "Use the value type required by this field."
		}
		typeTag := ""
		if e.Code == "yaml-type" && strings.Contains(detail, "cannot unmarshal !!") {
			tail := strings.SplitN(detail, "cannot unmarshal ", 2)[1]
			typeTag = strings.Fields(tail)[0]
		}
		for _, loc := range locations {
			if typeTag != "" && loc.node.ShortTag() != typeTag {
				continue
			}
			if loc.node.Line != e.Line || (field != "" && (!loc.key || loc.node.Value != field)) {
				continue
			}
			// Type failures concern values. A unique scalar/value on the decoder's
			// line is enough; multiple candidates remain unknown.
			if field == "" && loc.key {
				continue
			}
			if loc.node.Kind == yaml.DocumentNode {
				continue
			}
			candidates := 0
			for _, other := range locations {
				if (typeTag == "" || other.node.ShortTag() == typeTag) && other.node.Line == e.Line && other.node.Kind != yaml.DocumentNode && ((field != "" && other.key && other.node.Value == field) || (field == "" && !other.key)) {
					candidates++
				}
			}
			if candidates == 1 {
				e.Path = loc.path
				e.Column = loc.node.Column
			}
			if field != "" && loc.flowTitle {
				e.Hint = "If the flow-style title contains a comma, quote the whole title or use a block-style mapping."
			}
		}
		if hint := hints[fmt.Sprintf("%d:%s", e.Line, field)]; hint != "" {
			e.Hint = hint
		}
		if e.Code == "yaml-syntax" {
			lines := strings.Split(string(raw), "\n")
			if e.Line > 0 && e.Line <= len(lines) {
				line := lines[e.Line-1]
				indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
				if strings.Contains(indent, "\t") && strings.Contains(detail, "token") {
					e.Hint = "Replace indentation tabs with spaces."
				}
				trimmed := strings.TrimPrefix(strings.TrimSpace(line), "- ")
				if strings.HasPrefix(trimmed, "title: ") && strings.Contains(strings.TrimPrefix(trimmed, "title: "), ": ") && !strings.ContainsAny(strings.TrimPrefix(trimmed, "title: ")[:1], "'\"|>") {
					e.Hint = "Quote a plain title containing colon-space so it remains one scalar."
				}
			}
		}
		if human != nil {
			detail = human(detail)
		}
		// Structured messages omit decoder prefixes and implementation line syntax.
		e.Message = detail
		if e.Code == "yaml-type" {
			e.Message = implementationTypePattern.ReplaceAllString(detail, " into the expected value type")
		}
		d.Entries = append(d.Entries, e)
	}
	sortEntries(d.Entries)
	return d
}
