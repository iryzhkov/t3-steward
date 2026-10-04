package workerproto

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

func displayPackage() ExecutionPackage {
	pkg := validPackage()
	pkg.Display = &SessionDisplay{WorkflowName: "Release café", TaskName: "Build"}
	pkg.RequiredCapabilities = []string{PackageCapabilitySessionDisplay}
	return pkg
}
func TestSessionDisplayWireContract(t *testing.T) {
	pkg := displayPackage()
	manifest, err := BuildExecutionPackageManifest(pkg)
	if err != nil {
		t.Fatal(err)
	}
	envelope := signedTestEnvelope(t, MessageOffers, 1, "display-offer", AssignmentOffers{Offers: []AssignmentOffer{{Package: manifest}}})
	if err := VerifyEnvelopeSignature(envelope, testSecret); err != nil {
		t.Fatal(err)
	}
	var offers AssignmentOffers
	if err := DecodePayload(envelope, MessageOffers, &offers); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(offers.Offers[0].Package, manifest) {
		t.Fatal("display changed on signed wire")
	}
	if err := ValidateExecutionPackageManifest(offers.Offers[0].Package, 1<<20); err != nil {
		t.Fatal(err)
	}
	altered := manifest
	altered.Package.Display = &SessionDisplay{WorkflowName: "different", TaskName: "Build"}
	if err := ValidateExecutionPackageManifest(altered, 1<<20); err == nil {
		t.Fatal("display not content addressed")
	}
	legacy, err := BuildExecutionPackageManifest(validPackage())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(legacy)
	if strings.Contains(string(data), "\"display\"") || strings.Contains(string(data), PackageCapabilitySessionDisplay) {
		t.Fatal("legacy gained display wire fields")
	}
	// The existing JSON golden pins exact old bytes, size and content address.
	assertJSONGolden(t, "execution-package.json", legacy)
	envelope.Payload = json.RawMessage(strings.Replace(string(envelope.Payload), "\"taskName\":\"Build\"", "\"taskName\":\"Build\",\"authority\":true", 1))
	if err := DecodePayload(envelope, MessageOffers, &offers); err == nil {
		t.Fatal("unknown nested display field accepted")
	}
	if !slices.Contains(SupportedPackageCapabilities(), PackageCapabilitySessionDisplay) {
		t.Fatal("support not advertised")
	}
}
func TestSessionDisplayValidation(t *testing.T) {
	for name, change := range map[string]func(*ExecutionPackage){
		"missing capability": func(p *ExecutionPackage) { p.RequiredCapabilities = nil },
		"missing metadata":   func(p *ExecutionPackage) { p.Display = nil },
		"duplicate capability": func(p *ExecutionPackage) {
			p.RequiredCapabilities = append(p.RequiredCapabilities, PackageCapabilitySessionDisplay)
		},
		"unknown mandatory": func(p *ExecutionPackage) {
			p.RequiredCapabilities = append(p.RequiredCapabilities, "unknown-mandatory")
		},
		"control":      func(p *ExecutionPackage) { p.Display.TaskName = "bad\nname" },
		"bidi":         func(p *ExecutionPackage) { p.Display.TaskName = "bad\u202ename" },
		"whitespace":   func(p *ExecutionPackage) { p.Display.TaskName = " bad  name " },
		"rune limit":   func(p *ExecutionPackage) { p.Display.TaskName = strings.Repeat("x", 65) },
		"byte limit":   func(p *ExecutionPackage) { p.Display.TaskName = strings.Repeat("🙂", 65) },
		"invalid UTF8": func(p *ExecutionPackage) { p.Display.TaskName = string([]byte{255}) },
	} {
		t.Run(name, func(t *testing.T) {
			pkg := displayPackage()
			change(&pkg)
			if err := ValidateExecutionPackage(pkg); err == nil {
				t.Fatal("malformed display accepted")
			}
		})
	}
	pkg := displayPackage()
	pkg.Display = &SessionDisplay{}
	if err := ValidateExecutionPackage(pkg); err != nil {
		t.Fatal("empty names should permit fallback:", err)
	}
}
func TestInitialSessionTitles(t *testing.T) {
	pkg := displayPackage()
	title := InitialSessionTitle(pkg)
	for _, want := range []string{"[Steward]", pkg.Environment.Project, "Release café", "task: Build", " / run ", " / starting"} {
		if !strings.Contains(title, want) {
			t.Fatalf("title %q lacks %q", title, want)
		}
	}
	pkg.Identity.AttemptID = "retry"
	pkg.Identity.AssignmentID = "retry-assignment"
	pkg.Identity.ThreadID = "retry-thread"
	if InitialSessionTitle(pkg) != title {
		t.Fatal("retry changed title")
	}
	pkg.Identity.WorkflowRunID = "run-2"
	if InitialSessionTitle(pkg) == title {
		t.Fatal("concurrent runs indistinguishable")
	}
	pkg = validPackage()
	fallback := InitialSessionTitle(pkg)
	for _, want := range []string{"[Steward]", pkg.Environment.Project, pkg.Identity.WorkflowID, pkg.Identity.TaskID, "starting"} {
		if !strings.Contains(fallback, want) {
			t.Fatalf("fallback %q lacks %q", fallback, want)
		}
	}
	pkg = displayPackage()
	pkg.Display.TaskName = "review"
	if strings.Contains(InitialSessionTitle(pkg), "review judge:") {
		t.Fatal("name inferred review role")
	}
	pkg.Display.ReviewJudge = true
	if !strings.Contains(InitialSessionTitle(pkg), "review judge:") {
		t.Fatal("recorded judge role missing")
	}
	pkg.Supervision = &SupervisionActivation{ActivationID: "activation-1"}
	if !strings.Contains(InitialSessionTitle(pkg), "supervision: activation-1") {
		t.Fatal("activation role missing")
	}
	if err := validateSessionDisplay(pkg); err == nil {
		t.Fatal("activation claimed task judge")
	}
	pkg.Display = &SessionDisplay{WorkflowName: "Release"}
	if err := validateSessionDisplay(pkg); err != nil {
		t.Fatal(err)
	}
}
func TestSessionDisplayUnicodeBounds(t *testing.T) {
	for _, raw := range []string{" a\n\t b\x00\u202e c ", strings.Repeat("🙂", 500), strings.Repeat("界 ", 500), string([]byte{255}) + " valid", strings.Repeat("\u200b", 1000)} {
		clean := SanitizeDisplayName(raw)
		if !utf8.ValidString(clean) || len(clean) > DisplayNameMaxBytes || utf8.RuneCountInString(clean) > DisplayNameMaxRunes || SanitizeDisplayName(clean) != clean {
			t.Fatalf("unbounded/noncanonical %q", clean)
		}
		for _, r := range clean {
			if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
				t.Fatal("unsafe rune")
			}
		}
		pkg := displayPackage()
		pkg.Display.WorkflowName = clean
		pkg.Display.TaskName = clean
		pkg.Environment.Project = strings.Repeat("界", 256)
		if err := validateSessionDisplay(pkg); err != nil {
			t.Fatal(err)
		}
		title := InitialSessionTitle(pkg)
		if len(title) > InitialSessionTitleMaxBytes || !utf8.ValidString(title) || !strings.HasSuffix(title, " / starting") {
			t.Fatalf("title bounds %q", title)
		}
	}
}
