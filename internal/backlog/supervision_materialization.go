package backlog

import (
	"encoding/json"
	"errors"
	"sort"
)

// ActivationMaterialization is a bounded, read-only projection of durable
// supervision events. It deliberately carries no cursor or high-water mark:
// materializing input is not acknowledgement and cannot consume an event.
type ActivationMaterialization struct {
	JSON      []byte
	Selected  []string
	Omitted   []string
	Oversized []OversizedSupervisionEvent
}

// OversizedSupervisionEvent records an event that cannot fit even in an empty
// materialization. Recording it separately lets smaller later events proceed.
type OversizedSupervisionEvent struct {
	ID   string
	Size int
}

// MaterializeActivationInbox selects at most maxEvents events from an already
// coalesced activation inbox and serializes exactly that selection. Events that
// do not fit remain represented by ID in Omitted; an individually oversized
// event also receives an explicit Oversized outcome.
//
// This function is intentionally not wired into activation cursor advancement.
// The current scalar high-water cursor cannot acknowledge a later selected
// event while preserving an earlier omission. Integration therefore requires a
// durable per-event/per-consultation acknowledgement contract.
func MaterializeActivationInbox(inbox ActivationInbox, maxEvents, byteCap int) (ActivationMaterialization, error) {
	if maxEvents < 1 || byteCap < 2 {
		return ActivationMaterialization{}, errors.New("supervision materialization requires positive event and byte limits")
	}
	ordered := append([]SupervisionEvent(nil), inbox.Events...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Sequence != ordered[j].Sequence {
			return ordered[i].Sequence < ordered[j].Sequence
		}
		return ordered[i].ID < ordered[j].ID
	})
	result := ActivationMaterialization{JSON: []byte("[]")}
	selected := make([]SupervisionEvent, 0, min(maxEvents, len(ordered)))
	for _, event := range ordered {
		encoded, err := json.Marshal(event)
		if err != nil {
			return ActivationMaterialization{}, err
		}
		singleSize := len(encoded) + 2
		if singleSize > byteCap {
			result.Omitted = append(result.Omitted, event.ID)
			result.Oversized = append(result.Oversized, OversizedSupervisionEvent{ID: event.ID, Size: singleSize})
			continue
		}
		if len(selected) == maxEvents {
			result.Omitted = append(result.Omitted, event.ID)
			continue
		}
		candidate := append(selected, event)
		aggregate, err := json.Marshal(candidate)
		if err != nil {
			return ActivationMaterialization{}, err
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
