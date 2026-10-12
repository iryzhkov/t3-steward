package campaign

import (
	"regexp"
	"strings"
)

var acceptanceBullet = regexp.MustCompile(`^(?:[-+*]|[0-9]+[.)])[ \t]+(.*)$`)
var acceptanceCase = regexp.MustCompile(`^\x60[^\x60]+\x60(?:[ \t]+\(edge\))?:[[:space:]]*\S`)

// acceptanceTestsReason scans the selected brief, retaining wrapped list text.
func acceptanceTestsReason(section []byte) string {
	fence, item := "", ""
	label, edge, count := false, false, 0
	finish := func() {
		if acceptanceCase.MatchString(item) {
			count++
			edge = edge || strings.Contains(item, "(edge)")
		}
		item = ""
	}
	for _, line := range splitPlanLines(section) {
		text := planLineText(section, line)
		trimmed := strings.TrimLeft(text, " ")
		indent := len(text) - len(trimmed)
		if indent <= 3 {
			if fence != "" {
				if strings.HasPrefix(trimmed, fence) && strings.Trim(trimmed, fence[:1]+" \t") == "" {
					fence = ""
				}
				continue
			}
			if marker := fenceMarker(trimmed); marker != "" {
				fence = marker
				continue
			}
		}
		if fence != "" {
			continue
		}
		if !label {
			if text == "Acceptance tests:" {
				label = true
			}
			continue
		}
		if strings.TrimSpace(text) == "" {
			continue
		}
		if match := acceptanceBullet.FindStringSubmatch(trimmed); match != nil {
			finish()
			item = match[1]
			continue
		}
		if indent <= 3 && strings.HasPrefix(trimmed, "#") && len(planHeadings([]byte(text), 1)) > 0 {
			break
		}
		if (indent > 0 || strings.HasPrefix(text, "\t")) && item != "" {
			item += "\n" + text
			continue
		}
		break
	}
	finish()
	switch {
	case !label:
		return "missing Acceptance tests: label"
	case count == 0:
		return "Acceptance tests: has an empty list"
	case !edge:
		return "Acceptance tests: has no (edge) item"
	}
	return ""
}
