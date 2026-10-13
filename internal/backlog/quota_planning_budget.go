package backlog

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func deriveQuotaPlanningWindows(
	now time.Time,
	pools []domain.QuotaPool,
	states []domain.BucketState,
	poolByProviderInstance map[string]string,
	bridge QuotaBridge,
) ([]QuotaWindowBudget, error) {
	if bridge.SafetyMargin < 0 || !finiteQuotaPlanningNumber(bridge.SafetyMargin) {
		return nil, fmt.Errorf("quota planning safety margin must be nonnegative and finite")
	}
	if bridge.FallbackForecastPerHour < 0 || !finiteQuotaPlanningNumber(bridge.FallbackForecastPerHour) {
		return nil, fmt.Errorf("quota planning fallback forecast must be nonnegative and finite")
	}
	if bridge.LongWindowCap < 0 || bridge.LongWindowCap > 100 || !finiteQuotaPlanningNumber(bridge.LongWindowCap) {
		return nil, fmt.Errorf("quota planning long-window cap must be between zero and 100")
	}
	if bridge.SurplusHorizon < 0 {
		return nil, fmt.Errorf("quota planning surplus horizon cannot be negative")
	}
	admissionByPool := make(map[string]domain.AdmissionState, len(pools))
	poolByID := make(map[string]domain.QuotaPool, len(pools))
	for _, pool := range pools {
		admissionByPool[pool.ID] = pool.Admission
		poolByID[pool.ID] = pool
	}
	// Select the newest reading before dropping expired windows. Retain an
	// all-expired pool so planning still fails closed.
	competitors := make(map[domain.BucketKey][]domain.BucketState)
	for _, state := range domain.NewestQuotaStates(states) {
		competitors[state.Key] = append(competitors[state.Key], state)
	}
	newest := make(map[domain.BucketKey]domain.BucketState)
	for key, candidates := range competitors {
		// Legacy budgets may omit phase. Admission independently closes conflicts;
		// retaining the largest usage still prevents input order granting headroom.
		state := candidates[0]
		for _, candidate := range candidates {
			if !domain.ValidQuotaPercent(candidate.UsedPercent) {
				return nil, fmt.Errorf("quota planning window %q usage must be between zero and 100", key)
			}
			if candidate.DrainDeadline != nil && (state.DrainDeadline == nil || candidate.DrainDeadline.Before(*state.DrainDeadline)) {
				state.DrainDeadline = candidate.DrainDeadline
			}
			if candidate.DrainsAt != nil && (state.DrainsAt == nil || candidate.DrainsAt.Before(*state.DrainsAt)) {
				state.DrainsAt = candidate.DrainsAt
			}
			if candidate.AppliedThresholds != nil && (state.AppliedThresholds == nil || candidate.AppliedThresholds.DrainPercent < state.AppliedThresholds.DrainPercent) {
				thresholds := *candidate.AppliedThresholds
				state.AppliedThresholds = &thresholds
			}
			if candidate.UsedPercent > state.UsedPercent {
				state.UsedPercent = candidate.UsedPercent
			}
			if candidate.RatePerMinute > state.RatePerMinute {
				state.RatePerMinute = candidate.RatePerMinute
			}
			if !domain.SameQuotaAcquisition(state.Provenance, candidate.Provenance) {
				state.Provenance = nil
			}
			if candidate.OriginValidUntil != nil && (state.OriginValidUntil == nil || candidate.OriginValidUntil.Before(*state.OriginValidUntil)) {
				state.OriginValidUntil = candidate.OriginValidUntil
			}
		}
		newest[key] = state
	}
	states = nil
	for _, state := range newest {
		states = append(states, state)
	}
	freshByPool := make(map[string]bool)
	for _, state := range newest {
		if poolID, ok := poolByProviderInstance[state.Key.ProviderInstanceID]; ok && !state.ObservedAt.After(now) && domain.QuotaReadingFresh(state, now, domain.QuotaObservationMaxAge(poolByID[poolID], state, bridge.MaxObservationAge)) {
			freshByPool[poolID] = true
		}
	}
	windows := make([]QuotaWindowBudget, 0, len(states))
	seen := make(map[string]struct{}, len(states))
	for _, state := range states {
		poolID, configured := poolByProviderInstance[state.Key.ProviderInstanceID]
		if !configured {
			continue
		}
		pool := poolByID[poolID]
		if pool.Collector != nil && state.Key.AccountID == "" && !domain.UnknownAccountRestricts(pool, state, now, bridge.MaxObservationAge) {
			continue
		}
		age := domain.QuotaObservationMaxAge(pool, state, bridge.MaxObservationAge)
		if !state.ObservedAt.Equal(newest[state.Key].ObservedAt) || freshByPool[poolID] && domain.QuotaReadingExpired(state, now, age) {
			continue
		}
		windowKey := poolID + "\x00" + state.Key.String()
		if _, duplicate := seen[windowKey]; duplicate {
			continue
		}
		seen[windowKey] = struct{}{}
		if !finiteQuotaPlanningNumber(state.UsedPercent) || state.UsedPercent < 0 || state.UsedPercent > 100 {
			return nil, fmt.Errorf("quota planning window %q usage must be between zero and 100", state.Key.String())
		}
		if state.AppliedThresholds != nil {
			drain := state.AppliedThresholds.DrainPercent
			if !finiteQuotaPlanningNumber(drain) || drain < 0 || drain > 100 {
				return nil, fmt.Errorf("quota planning window %q drain threshold must be between zero and 100", state.Key.String())
			}
		}
		forecastHorizon := age
		if state.ResetsAt != nil && state.ResetsAt.After(now) {
			forecastHorizon = state.ResetsAt.Sub(now)
		}
		forecast := bridge.FallbackForecastPerHour * forecastHorizon.Hours()
		observedForecast := state.RatePerMinute * forecastHorizon.Minutes()
		if !finiteQuotaPlanningNumber(observedForecast) || observedForecast < 0 {
			return nil, fmt.Errorf("quota planning window %q forecast must be nonnegative and finite", state.Key.String())
		}
		if observedForecast > forecast {
			forecast = observedForecast
		}
		window := QuotaWindowBudget{
			QuotaPoolID:              poolID,
			WindowID:                 state.Key.String(),
			ObservedAt:               state.ObservedAt,
			MaxObservationAge:        age,
			OriginValidUntil:         state.OriginValidUntil,
			Admission:                admissionByPool[poolID],
			Capacity:                 100,
			CurrentUsage:             state.UsedPercent,
			ForecastInteractiveUsage: forecast,
			SafetyMargin:             bridge.SafetyMargin,
		}
		if state.ResetsAt != nil {
			window.ResetsAt = state.ResetsAt.UTC()
			if state.ResetsAt.Sub(now) > 24*time.Hour {
				if capMargin := 100 - bridge.LongWindowCap; capMargin > window.SafetyMargin {
					window.SafetyMargin = capMargin
				}
			}
			if bridge.SurplusHorizon > 0 {
				window.SurplusStartsAt = state.ResetsAt.Add(-bridge.SurplusHorizon).UTC()
			}
		}
		if state.DrainDeadline != nil {
			window.DrainAt = state.DrainDeadline.UTC()
		}
		if state.DrainsAt != nil && (window.DrainAt.IsZero() || state.DrainsAt.Before(window.DrainAt)) {
			window.DrainAt = state.DrainsAt.UTC()
		}
		// Every admission path uses this budget. A supplied projection may be
		// earlier, but cannot extend the crossing implied by the same reading.
		if state.AppliedThresholds != nil && state.RatePerMinute > 0 &&
			state.UsedPercent < state.AppliedThresholds.DrainPercent {
			nanos := (state.AppliedThresholds.DrainPercent - state.UsedPercent) / state.RatePerMinute * float64(time.Minute)
			if finiteQuotaPlanningNumber(nanos) && nanos >= 0 && nanos < float64(math.MaxInt64) {
				crossing := state.ObservedAt.Add(time.Duration(nanos)).UTC()
				if window.DrainAt.IsZero() || crossing.Before(window.DrainAt) {
					window.DrainAt = crossing
				}
			}
		}
		if state.ExhaustsIn != nil {
			exhaustion := now.Add(*state.ExhaustsIn)
			if window.DrainAt.IsZero() || exhaustion.Before(window.DrainAt) {
				window.DrainAt = exhaustion
			}
		}
		if err := validateQuotaWindow(window); err != nil {
			return nil, fmt.Errorf("quota planning window %q: %w", state.Key.String(), err)
		}
		windows = append(windows, window)
	}
	sort.Slice(windows, func(i, j int) bool { return quotaWindowKey(windows[i]) < quotaWindowKey(windows[j]) })
	return windows, nil
}

func finiteQuotaPlanningNumber(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
