package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// runQuarantineRelease clears one intake quarantine deliberately.
//
// The automatic release is bound to the file's content digest, so a submission
// refused for a reason outside the file — a project no alias mapped is the
// ordinary one — stays quarantined after the configuration is fixed, because
// the bytes did not change. This is the way out, and it asks for a reason
// because the operator, not the file, is what changed.
func (c backlogAdminCLI) runQuarantineRelease(ctx context.Context, args []string) error {
	clean, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return err
	}
	if len(clean) == 0 {
		return errors.New("quarantine release usage: backlog quarantine release <key> --reason TEXT [--json]")
	}
	request := backlogadmin.QuarantineReleaseRequest{Key: clean[0]}
	rest := clean[1:]
	for len(rest) > 0 {
		if len(rest) < 2 {
			return fmt.Errorf("%s needs a value", rest[0])
		}
		flag, value := rest[0], rest[1]
		rest = rest[2:]
		switch flag {
		case "--reason":
			if request.Reason != "" {
				return errors.New("--reason may only be specified once")
			}
			request.Reason = value
		default:
			return fmt.Errorf("unknown quarantine release flag %q", flag)
		}
	}
	if request.Reason == "" {
		return errors.New("quarantine release needs --reason TEXT")
	}
	if c.quarantine == nil {
		return errors.New("quarantine release is unavailable on this transport")
	}
	release, err := c.quarantine.ReleaseQuarantine(ctx, c.principal, request)
	if err != nil {
		return err
	}
	if asJSON {
		encoder := json.NewEncoder(c.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(release)
	}
	if !release.Released {
		fmt.Fprintf(c.stdout, "no quarantine for %s; nothing to release.\n", release.Key)
		return nil
	}
	fmt.Fprintf(c.stdout, "released %s (digest %s)\nit was refused because: %s\n"+
		"intake reads the file again on the next cycle and reports what it does now.\n",
		release.Key, release.Digest, release.Reason)
	return nil
}
