package config

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ErrStagedConfig is deliberately independent of parser messages, paths and
// values. A validator may return it directly to an untrusted CLI consumer.
var ErrStagedConfig = errors.New("configuration validation refused")

func decodeFile(path string, data []byte) (Config, error) {
	c := Default()
	c.Path = path
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&c); err != nil {
		return c, fmt.Errorf("parse %s: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple YAML documents are not allowed")
		}
		return c, fmt.Errorf("parse %s: %w", path, err)
	}
	c.migrateQuotaDefaults()
	return c, nil
}

// ValidateStagedFile validates immutable staged bytes plus the applicable
// original-HOME owned projections, using LoadFile's decoder, projection and
// Validate semantics for configurations whose validity needs no credential
// resolution. Coordinator Discord configurations refuse because runtime
// validation must read their webhook credential. It never applies environment
// overrides or resolves credentials. The digest describes the staged YAML, not the effective
// projection inputs. Every observed input must remain stable until completion.
func ValidateStagedFile(path string) (string, error) {
	return validateStagedFile(path, nil)
}

// beforeCheck is a per-call fault injection seam; production always passes nil.
func validateStagedFile(path string, beforeCheck func()) (string, error) {
	refuse := func() (string, error) { return "", ErrStagedConfig }
	if strings.TrimSpace(path) == "" {
		return refuse()
	}
	staged, err := observeStagedInput(path, 4<<20, false)
	if err != nil {
		return refuse()
	}
	c, err := decodeFile(path, staged.raw)
	if err != nil {
		return refuse()
	}
	inputs := []*stagedInput{staged}
	home := coordinatorClientHome()
	if strings.TrimSpace(home) == "" {
		return refuse()
	}
	if c.BacklogV2.Mode == "coordinator" {
		fleet, err := observeStagedInput(filepath.Join(home, CoordinatorFleetPath), coordinatorFleetLimit, true)
		if err != nil {
			return refuse()
		}
		inputs = append(inputs, fleet)
		if fleet.raw != nil {
			projection, err := DecodeCoordinatorFleet(fleet.raw)
			if err != nil {
				return refuse()
			}
			if err := c.ApplyCoordinatorFleet(projection); err != nil {
				return refuse()
			}
		}
	}
	if !c.BacklogV2.CoordinatorClient.Configured() {
		client, err := observeStagedInput(filepath.Join(home, CoordinatorClientBootstrapPath), coordinatorClientBootstrapLimit, true)
		if err != nil {
			return refuse()
		}
		inputs = append(inputs, client)
		if client.raw != nil {
			projection, err := DecodeCoordinatorClientBootstrap(client.raw)
			if err != nil {
				return refuse()
			}
			defaults := c.BacklogV2.CoordinatorClient.Defaults
			c.BacklogV2.CoordinatorClient = projection.Settings()
			c.BacklogV2.CoordinatorClient.Defaults = defaults
		}
	}
	// Notifications.validate resolves the Discord webhook only in coordinator
	// mode. The pure contract authorizes no credential resolution: refuse
	// before Validate rather than skipping notification checks or asserting
	// full validity without their required external input. Runtime validation
	// remains unchanged, including its private-file and URL checks.
	if c.BacklogV2.Mode == "coordinator" && c.Notifications.Discord != nil {
		return refuse()
	}
	if err := c.Validate(); err != nil {
		return refuse()
	}
	if beforeCheck != nil {
		beforeCheck()
	}
	if coordinatorClientHome() != home {
		return refuse()
	}
	for _, input := range inputs {
		if err := input.check(); err != nil {
			return refuse()
		}
	}
	digest := sha256.Sum256(staged.raw)
	return hex.EncodeToString(digest[:]), nil
}
