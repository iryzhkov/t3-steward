package backlog

import (
	"encoding/json"
	"errors"
	"sort"
)

const maxMaterializedConsultations = 4

type activationMaterialization struct {
	JSON      []byte
	Selected  []string
	Omitted   []string
	Oversized []oversizedSupervisionEvent
}

type oversizedSupervisionEvent struct {
	ID   string
	Size int
}

// materializeActivationInbox is test-only evidence for the proposed bounded
// projection seam. It carries no cursor: projection is not acknowledgement.
func materializeActivationInbox(inbox ActivationInbox, byteCap int) (activationMaterialization, error) {
	if byteCap < 2 {
		return activationMaterialization{}, errors.New("supervision materialization requires a positive byte limit")
	}
	ordered := append([]SupervisionEvent(nil), inbox.Events...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Sequence != ordered[j].Sequence {
			return ordered[i].Sequence < ordered[j].Sequence
		}
		return ordered[i].ID < ordered[j].ID
	})
	result := activationMaterialization{JSON: []byte("[]")}
	selected := make([]SupervisionEvent, 0, min(maxMaterializedConsultations, len(ordered)))
	for _, event := range ordered {
		encoded, err := json.Marshal(event)
		if err != nil {
			return activationMaterialization{}, err
		}
		singleSize := len(encoded) + 2
		if singleSize > byteCap {
			result.Omitted = append(result.Omitted, event.ID)
			result.Oversized = append(result.Oversized, oversizedSupervisionEvent{ID: event.ID, Size: singleSize})
			continue
		}
		if len(selected) == maxMaterializedConsultations {
			result.Omitted = append(result.Omitted, event.ID)
			continue
		}
		candidate := append(selected, event)
		aggregate, err := json.Marshal(candidate)
		if err != nil {
			return activationMaterialization{}, err
		}
		if len(aggregate) > byteCap {
			result.Omitted = append(result.Omitted, event.ID)
			continue
		}
		selected = candidate
		result.JSON = aggregate
		result.Selected = append(result.Selected, event.ID)
	}
	return result, nil
}
