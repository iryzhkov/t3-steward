//go:build linux

package main

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/testutil"
)

func TestPrivateConfigValidationUnderUmasks(t *testing.T) {
	testutil.RerunWithUmasks(t, "^TestConfigValidate")
}
