package workerproto

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func limitedPackage() ExecutionPackage {
	pkg := validPackage()
	pkg.ResourceDemand = &domain.ResourceDemand{CPUUnits: 4, MemoryMB: 6000}
	pkg.RequiredCapabilities = []string{PackageCapabilityContainedLimits}
	return pkg
}

func TestResourceDemandWireContract(t *testing.T) {
	if !slices.Contains(SupportedPackageCapabilities(), PackageCapabilityContainedLimits) {
		t.Fatal("support not advertised")
	}
	manifest, err := BuildExecutionPackageManifest(limitedPackage())
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateExecutionPackageManifest(manifest, 1<<20); err != nil {
		t.Fatal(err)
	}
	altered := manifest
	larger := *manifest.Package.ResourceDemand
	larger.MemoryMB = 8000
	altered.Package.ResourceDemand = &larger
	if err := ValidateExecutionPackageManifest(altered, 1<<20); err == nil {
		t.Fatal("demand not content addressed")
	}
	legacy, err := BuildExecutionPackageManifest(validPackage())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(legacy)
	if strings.Contains(string(data), "resourceDemand") {
		t.Fatal("legacy package gained the demand wire field")
	}
}

func TestResourceDemandValidation(t *testing.T) {
	for name, change := range map[string]func(*ExecutionPackage){
		"missing capability": func(p *ExecutionPackage) { p.RequiredCapabilities = nil },
		"missing demand":     func(p *ExecutionPackage) { p.ResourceDemand = nil },
		"unsized demand":     func(p *ExecutionPackage) { p.ResourceDemand = &domain.ResourceDemand{} },
		"class only demand": func(p *ExecutionPackage) {
			p.ResourceDemand = &domain.ResourceDemand{MinCPUClass: domain.CPUClassMedium}
		},
		"negative demand": func(p *ExecutionPackage) { p.ResourceDemand.MemoryMB = -1 },
		"duplicate capability": func(p *ExecutionPackage) {
			p.RequiredCapabilities = append(p.RequiredCapabilities, PackageCapabilityContainedLimits)
		},
		"activation": func(p *ExecutionPackage) {
			p.Supervision = &SupervisionActivation{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			pkg := limitedPackage()
			change(&pkg)
			if err := ValidateExecutionPackage(pkg); err == nil {
				t.Fatal("invalid demand accepted")
			}
		})
	}
	if err := ValidateExecutionPackage(limitedPackage()); err != nil {
		t.Fatal(err)
	}
	memoryOnly := limitedPackage()
	memoryOnly.ResourceDemand = &domain.ResourceDemand{MemoryMB: 512}
	if err := ValidateExecutionPackage(memoryOnly); err != nil {
		t.Fatalf("memory-only demand refused: %v", err)
	}
}
