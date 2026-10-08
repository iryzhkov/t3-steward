package workerruntime

// secretBoundaryContext is the number of bytes before a candidate that the
// left-boundary rule reads: one for the preceding byte, three for a percent
// escape.
const secretBoundaryContext = 3

// eachMatch calls found with the submatch indexes, absolute in data, of every
// match of p that starts before limit and, for a prefix detector, at a left
// boundary. It is the one implementation of the rule, used by both the scan
// and redaction. before holds the bytes that preceded data[0], at most
// secretBoundaryContext of them; it is empty only when data starts the scanned
// object or text. found returns false to stop.
//
// A candidate rejected for its boundary resumes the search at the first
// boundary inside it, or at its end when it has none, so it never hides a
// token that begins inside it. Only a boundary can start a reported token, so
// skipping the other positions loses nothing, and keeps a run of overlapping
// candidates such as a repeated prefix linear rather than rematching it at
// every byte.
func (p secretPattern) eachMatch(data, before []byte, limit int, found func(match []int) bool) {
	for offset := 0; offset < len(data); {
		match := p.re.FindSubmatchIndex(data[offset:])
		if match == nil || offset+match[0] >= limit {
			return
		}
		for i := range match {
			if match[i] >= 0 {
				match[i] += offset
			}
		}
		if p.leftBoundary && !atSecretBoundary(data, before, match[0]) {
			offset = match[0] + 1
			for offset < match[1] && !atSecretBoundary(data, before, offset) {
				offset++
			}
			continue
		}
		if !found(match) {
			return
		}
		offset = max(match[1], match[0]+1)
	}
}

// secretTokenSpan is the reported token of a match: its first capture group
// when the pattern has one, otherwise the whole match.
func secretTokenSpan(match []int) (start, end int) {
	if len(match) > 2 {
		return match[2], match[3]
	}
	return match[0], match[1]
}

// atSecretBoundary reports whether a prefix detector's match may start at
// data[at]: at the start of the scanned text, after a byte that is not an
// ASCII letter, digit, underscore or hyphen (bytes 0x80 and above count as
// boundaries), or right after a percent escape such as %3D or a backslash
// escape \n, \t or \r, as in an encoded query string or a JSON string.
func atSecretBoundary(data, before []byte, at int) bool {
	// preceding returns the k-th byte before the candidate, 1 being the
	// nearest, and false when the text starts first.
	preceding := func(k int) (byte, bool) {
		if i := at - k; i >= 0 {
			return data[i], true
		}
		if i := len(before) + at - k; i >= 0 {
			return before[i], true
		}
		return 0, false
	}
	last, ok := preceding(1)
	if !ok || !secretIdentifierByte(last) {
		return true
	}
	if escape, ok := preceding(2); ok && escape == '\\' && (last == 'n' || last == 't' || last == 'r') {
		return true
	}
	percent, ok := preceding(3)
	if !ok || percent != '%' {
		return false
	}
	high, _ := preceding(2)
	return hexDigit(high) && hexDigit(last)
}

func secretIdentifierByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-'
}

func hexDigit(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// keepSecretBoundaryContext returns the last secretBoundaryContext bytes of
// before followed by consumed: the context a scan carries across a chunk cut,
// so the byte before the next window's first byte is the object's real
// previous byte rather than a start of text.
func keepSecretBoundaryContext(before, consumed []byte) []byte {
	if len(consumed) >= secretBoundaryContext {
		return append(before[:0], consumed[len(consumed)-secretBoundaryContext:]...)
	}
	joined := append(append([]byte(nil), before...), consumed...)
	return joined[max(0, len(joined)-secretBoundaryContext):]
}
