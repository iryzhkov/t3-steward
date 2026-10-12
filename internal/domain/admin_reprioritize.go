package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// DecodeReprioritizePayload validates the run reprioritization request.
// The returned importance is ready for use by submission and application.
func DecodeReprioritizePayload(payload json.RawMessage) (int, error) {
	var request struct {
		Importance *int `json:"importance"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return 0, fmt.Errorf("invalid reprioritize payload: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return 0, fmt.Errorf("invalid reprioritize payload: expected one JSON object")
	}
	if request.Importance == nil || *request.Importance < 1 || *request.Importance > 5 {
		return 0, fmt.Errorf("invalid reprioritize payload: importance must be an integer in 1-5")
	}
	return *request.Importance, nil
}
