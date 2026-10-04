package main

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"math"
	"sort"
	"time"
)

type modelsWindow struct {
	Unknown     bool             `json:"unknown,omitempty"`
	Key         domain.BucketKey `json:"key"`
	UsedPercent float64          `json:"usedPercent"`
	Headroom    float64          `json:"headroom"`
	ResetsAt    *time.Time       `json:"resetsAt,omitempty"`
	ObservedAt  time.Time        `json:"observedAt"`
	Stale       bool             `json:"stale"`
}

func modelsPoolWindows(pool domain.QuotaPool, states []domain.BucketState, now time.Time, staleAfter time.Duration) []modelsWindow {
	matches := domain.PoolBucketMatcher(pool)
	result := []modelsWindow{}
	for _, s := range states {
		if !matches(s) {
			continue
		}
		stale := s.ObservedAt.IsZero() || now.Sub(s.ObservedAt) > staleAfter || s.ResetsAt != nil && !now.Before(*s.ResetsAt)
		result = append(result, modelsWindow{Key: s.Key, UsedPercent: s.UsedPercent, Headroom: math.Max(0, 100-s.UsedPercent), ResetsAt: s.ResetsAt, ObservedAt: s.ObservedAt, Stale: stale})
	}
	for _, key := range pool.Buckets {
		found := false
		for _, w := range result {
			if w.Key == key {
				found = true
			}
		}
		if !found {
			result = append(result, modelsWindow{Key: key, Unknown: true})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key.String() < result[j].Key.String() })
	return result
}
