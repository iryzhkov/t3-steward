package config

import (
	"os"
	"testing"
)

func TestUpKeeperCoordinatorProjectionFixture(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/fleet/projection-coordinator.json")
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := DecodeCoordinatorFleet(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(fleet.Workers) != 2 || len(fleet.Projects) == 0 {
		t.Fatalf("incomplete fixture: %+v", fleet)
	}
}
