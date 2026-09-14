package workerproto

import "testing"

// A task whose success is defined by its outputs needs no verification command.
// Requiring one made such a package unbuildable, and the campaign that declared
// it validated, submitted, planned and assigned before its offer was withheld
// and retried forever, with the reason visible only in a coordinator warning.
// A campaign that can never run must not look accepted.
func TestValidateExecutionPackageAcceptsOutputsWithoutVerification(t *testing.T) {
	pkg := validPackage()
	pkg.Verification = nil
	if len(pkg.Outputs) == 0 {
		t.Fatal("fixture must declare an output for this case to mean anything")
	}
	if err := ValidateExecutionPackage(pkg); err != nil {
		t.Fatalf("a package with outputs and no verification was refused: %v", err)
	}
}

// Relaxing the requirement must not accept a malformed command.
func TestValidateExecutionPackageStillRefusesAnEmptyVerificationCommand(t *testing.T) {
	pkg := validPackage()
	pkg.Verification = []string{"  "}
	if err := ValidateExecutionPackage(pkg); err == nil {
		t.Fatal("a blank verification command was accepted")
	}
}
