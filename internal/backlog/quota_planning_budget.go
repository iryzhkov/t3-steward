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
	for _, pool := range pools {
		admissionByPool[pool.ID] = pool.Admission
	}
	windows := make([]QuotaWindowBudget, 0, len(states))
	seen := make(map[string]struct{}, len(states))
	for _, state := range states {
		poolID, configured := poolByProviderInstance[state.Key.ProviderInstanceID]
		if !configured {
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
		forecastHorizon := bridge.MaxObservationAge
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
