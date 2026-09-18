package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
)

const coordinatorUsage = `Usage: t3-steward coordinator <command> [flags]

Commands:
  identity [--json]   Show which coordinator this host administers and how it
                      reaches it. Read-only; it runs one status query.

"t3-steward coordinator identity" answers, before anything is submitted, the
question "where would my next submission go". It reports the coordinator id,
its owner, the release it runs, its configuration digest, its epoch, its health
and the carrier this host used to ask.

Offline or remote: local when this host runs the coordinator, remote read-only
when backlog_v2.coordinator_client is configured. It never changes anything.

Example:
  t3-steward coordinator identity --json

Configuration on a host that is not the coordinator:
  backlog_v2.coordinator_client.coordinator_id, .address and a
  .credential of the form secretref:f03-admin/<client>. The credential value is
  resolved from the environment at use and is never written to configuration.

Idempotency: none needed; the command has no effect to repeat.

Common failures:
  client-configuration (exit 3)  permanent until configuration changes: no
                                 coordinator_client block, or its credential
                                 reference does not resolve.
  authentication (exit 4)        permanent until credentials are rotated in
                                 step on both hosts.
  unavailable (exit 5)           usually temporary: the coordinator is not
                                 running, or its host is unreachable.
  timeout (exit 6)               temporary.

Recovery:
  t3-steward coordinator identity --json   Re-run; it is safe to repeat.
`

func cmdCoordinator(g globalFlags, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Print(coordinatorUsage)
		return nil
	}
	if args[0] != "identity" {
		return fmt.Errorf("unknown coordinator command %q (try \"t3-steward coordinator help\")", args[0])
	}
	asJSON := false
	for _, argument := range args[1:] {
		if argument != "--json" {
			return fmt.Errorf("unknown coordinator identity flag %q", argument)
		}
		asJSON = true
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	return runCoordinatorIdentity(context.Background(), cfg, os.Stdout, asJSON)
}

// CoordinatorIdentity is what "coordinator identity" reports. It needs no new
// request type: a status query already returns all of it.
type CoordinatorIdentity struct {
	Version             string                            `json:"version"`
	Kind                string                            `json:"kind"`
	CoordinatorID       string                            `json:"coordinatorId"`
	Owner               string                            `json:"owner"`
	Release             string                            `json:"release,omitempty"`
	ConfigurationDigest string                            `json:"configurationDigest,omitempty"`
	Epoch               int64                             `json:"epoch"`
	Health              string                            `json:"health"`
	Transport           backlogadmin.TransportDescription `json:"transport"`
}

func runCoordinatorIdentity(ctx context.Context, cfg config.Config, out io.Writer, asJSON bool) error {
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	description := transport.client.Describe()
	response, err := transport.client.Query(ctx, backlogadmin.Query{
		Version:   backlogadmin.Version,
		Kind:      backlogadmin.QueryStatus,
		Principal: transport.principal,
	})
	if err != nil {
		return err
	}
	if response.Status == nil {
		return errors.New("coordinator status query returned no status")
	}
	runtime := response.Status.Runtime
	identity := CoordinatorIdentity{
		Version:             backlogadmin.Version,
		Kind:                "coordinator-identity",
		CoordinatorID:       description.CoordinatorID,
		Owner:               runtime.Owner,
		Release:             runtime.Release,
		ConfigurationDigest: runtime.ConfigurationDigest,
		Epoch:               runtime.Epoch,
		Health:              runtime.Health,
		Transport:           description,
	}
	if asJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(identity)
	}
	fmt.Fprintf(out, "coordinator  %s\n", identity.CoordinatorID)
	fmt.Fprintf(out, "owner        %s\n", identity.Owner)
	fmt.Fprintf(out, "release      %s\n", identity.Release)
	fmt.Fprintf(out, "config       %s\n", identity.ConfigurationDigest)
	fmt.Fprintf(out, "epoch        %d\n", identity.Epoch)
	fmt.Fprintf(out, "health       %s\n", identity.Health)
	fmt.Fprintf(out, "carrier      %s via %s\n", identity.Transport.Carrier, identity.Transport.Endpoint)
	return nil
}
