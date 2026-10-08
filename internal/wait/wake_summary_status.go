package wait

import (
	"encoding/json"
	"strings"
)

type summaryCell struct{ value, status string }

// text returns only fixed messages or values already checked by a parser.
func (c summaryCell) text(kind, source string) string {
	switch c.status {
	case "known":
		return c.value
	case "not-run":
		return "not run"
	case "not-read":
		return "not read (summary limit)"
	case "none":
		switch kind {
		case "verdict":
			return "no review output"
		case "head":
			return "no declared commit"
		default:
			return "-"
		}
	case "unrecognized":
		switch kind {
		case "verdict":
			return verdictUnrecognized
		case "gate":
			return gateNoResult
		default:
			if strings.HasPrefix(source, "commit ") {
				return "malformed commit record"
			}
			return "no branch head in bundle"
		}
	case "missing":
		switch kind {
		case "verdict":
			return "review output missing"
		case "head":
			return "head output missing"
		default:
			return "gate.log missing"
		}
	default:
		switch kind {
		case "verdict":
			return "review output unreadable"
		case "head":
			return "head output unreadable"
		default:
			return "gate.log unreadable"
		}
	}
}

func mustVerdictJSON(verdict string) []byte {
	data, _ := json.Marshal(struct {
		Verdict string `json:"verdict"`
	}{verdict})
	return data
}

func summaryCellAnswers(status string) bool { return status == "known" || status == "unrecognized" }
