// Package backlog owns persisted workflow, task, worker and scheduling authority.
package backlog

// Seeds by difficulty: percent of the short window and minutes per turn.
var (
	costSeed = map[int]float64{1: 5, 2: 10, 3: 20, 4: 35, 5: 50}
	minsSeed = map[int]float64{1: 15, 2: 30, 3: 60, 4: 90, 5: 150}
)

// SeedCost returns the difficulty's initial cost estimate.
func SeedCost(difficulty int) float64 {
	if v, ok := costSeed[difficulty]; ok {
		return v
	}
	return costSeed[3]
}

// SeedMinutes returns the difficulty's initial duration estimate.
func SeedMinutes(difficulty int) float64 {
	if v, ok := minsSeed[difficulty]; ok {
		return v
	}
	return minsSeed[3]
}
