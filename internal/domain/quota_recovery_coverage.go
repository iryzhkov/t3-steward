package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"sort"
)

// QuotaRecoveryCoverageDigest fences source/coverage changes, not credentials.
// All declared windows remain in the fence because any governing model window
// can conservatively close the shared pool. Route identity is bound separately.
func QuotaRecoveryCoverageDigest(pool QuotaPool, route ProviderRoute) string {
	if pool.Collector == nil {
		return ""
	}
	coverage := *pool.Collector
	coverage.Origins = append([]QuotaCollectorOrigin(nil), coverage.Origins...)
	sort.Slice(coverage.Origins, func(i, j int) bool {
		a, b := coverage.Origins[i], coverage.Origins[j]
		if a.WorkerID != b.WorkerID {
			return a.WorkerID < b.WorkerID
		}
		return a.ProviderInstanceID < b.ProviderInstanceID
	})
	coverage.RequiredWindows = make([]QuotaCollectorWindow, 0, len(pool.Collector.RequiredWindows))
	for _, w := range pool.Collector.RequiredWindows {
		copy := QuotaCollectorWindow{Window: w.Window, Models: append([]string(nil), w.Models...)}
		slices.Sort(copy.Models)
		coverage.RequiredWindows = append(coverage.RequiredWindows, copy)
	}
	sort.Slice(coverage.RequiredWindows, func(i, j int) bool { return coverage.RequiredWindows[i].Window < coverage.RequiredWindows[j].Window })
	catalog := append([]string(nil), pool.Models...)
	slices.Sort(catalog)
	target := struct {
		Version, Pool, Worker, Instance, Model string
		Coverage                               QuotaCollectorCoverage
		Models                                 []string
	}{
		Version: "quota-recovery-coverage/v1", Pool: pool.ID, Worker: route.WorkerID,
		Instance: route.ProviderInstanceID, Model: route.Model, Coverage: coverage, Models: catalog}
	raw, _ := json.Marshal(target)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
