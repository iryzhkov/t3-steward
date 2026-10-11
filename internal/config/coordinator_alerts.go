package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// V2CoordinatorAlerts configures the optional read-only monitoring listener.
type V2CoordinatorAlerts struct {
	Bind      string `yaml:"bind"`
	MinFreeMB int64  `yaml:"min_free_mb"`
}

func (a V2CoordinatorAlerts) validate() error {
	if a.MinFreeMB < 0 {
		return fmt.Errorf("backlog_v2.coordinator.alerts.min_free_mb must be nonnegative")
	}
	if a.Bind == "" {
		return nil
	}
	if strings.TrimSpace(a.Bind) != a.Bind {
		return fmt.Errorf("backlog_v2.coordinator.alerts.bind must be trimmed")
	}
	_, port, err := net.SplitHostPort(a.Bind)
	if err != nil {
		return fmt.Errorf("backlog_v2.coordinator.alerts.bind must be host:port")
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > 65535 {
		return fmt.Errorf("backlog_v2.coordinator.alerts.bind requires a numeric TCP port")
	}
	return nil
}
