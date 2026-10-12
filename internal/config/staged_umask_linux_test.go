//go:build linux

package config

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/testutil"
)

func TestPrivateStagedFixturesUnderUmasks(t *testing.T) {
	testutil.RerunWithUmasks(t, "^TestStaged(Config|AbsentInput)")
}
