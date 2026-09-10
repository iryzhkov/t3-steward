package main

import (
	"strings"
	"testing"
)

func TestWorkerExchangeCommandAcceptsOnlyOneFixedOperation(t *testing.T) {
	if err := run([]string{"worker-exchange"}); err == nil ||
		!strings.Contains(err.Error(), "exactly one fixed operation") {
		t.Fatalf("missing operation error = %v", err)
	}
	if err := run([]string{"worker-exchange", "control", "extra"}); err == nil ||
		!strings.Contains(err.Error(), "exactly one fixed operation") {
		t.Fatalf("extra operation error = %v", err)
	}
	if err := cmdWorkerExchange(globalFlags{}, "other"); err == nil ||
		!strings.Contains(err.Error(), "operation must be") {
		t.Fatalf("unknown operation error = %v", err)
	}
}

func TestWorkerExchangeRequiresExplicitLocalConfig(t *testing.T) {
	if err := run([]string{"worker-exchange", "control"}); err == nil ||
		!strings.Contains(err.Error(), "explicit operator-controlled --config") {
		t.Fatalf("implicit config error = %v", err)
	}
}

func TestWorkerExchangeRejectsAuthorityFlags(t *testing.T) {
	for name, flags := range map[string]globalFlags{
		"dry run":    {dryRun: true},
		"no dry run": {noDryRun: true},
		"log level":  {logLevel: "debug"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := cmdWorkerExchange(flags, workerOperationControl); err == nil ||
				!strings.Contains(err.Error(), "only the local --config override") {
				t.Fatalf("authority flag error = %v", err)
			}
		})
	}
}
