package platform

import (
	"strings"
	"testing"
)

// F-10: every role (watchdog, worker, coordinator) runs in t3-steward.service,
// and the rendered unit pins the journal identifier so that
// "journalctl --user -t t3-steward" finds the process however it is started.
func TestRenderUnitPinsSyslogIdentifier(t *testing.T) {
	unit := RenderUnit(InstallOptions{Binary: "/usr/local/bin/t3-steward", ConfigPath: "/home/u/.config/t3-steward/config.yaml"})
	if !strings.Contains(unit, "\nSyslogIdentifier=t3-steward\n") {
		t.Fatalf("rendered unit lacks SyslogIdentifier=t3-steward:\n%s", unit)
	}
	service := strings.Index(unit, "[Service]")
	install := strings.Index(unit, "[Install]")
	identifier := strings.Index(unit, "SyslogIdentifier=")
	if !(service < identifier && identifier < install) {
		t.Fatalf("SyslogIdentifier is outside the [Service] section:\n%s", unit)
	}
}
