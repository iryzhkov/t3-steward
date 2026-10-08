package providercontainment

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestLimitsForSizedAndUnsizedDemand(t *testing.T) {
	for _, test := range []struct {
		name       string
		demand     domain.ResourceDemand
		want       *Limits
		properties []string
		env        []string
	}{
		{
			name: "four cpus", demand: domain.ResourceDemand{CPUUnits: 4, MemoryMB: 6000},
			want:       &Limits{CPUs: 4, MemoryMB: 6000},
			properties: []string{"CPUQuota=400%", "MemoryMax=6000M", "MemorySwapMax=0", "TasksMax=4096"},
			env:        []string{"GOMAXPROCS=4", "GOFLAGS=-p=4", "MAKEFLAGS=-j4", "CARGO_BUILD_JOBS=4"},
		},
		{
			name: "half a cpu", demand: domain.ResourceDemand{CPUUnits: 0.5, MemoryMB: 1000},
			want:       &Limits{CPUs: 1, MemoryMB: 1000},
			properties: []string{"CPUQuota=100%", "MemoryMax=1000M", "MemorySwapMax=0", "TasksMax=4096"},
			env:        []string{"GOMAXPROCS=1", "GOFLAGS=-p=1", "MAKEFLAGS=-j1", "CARGO_BUILD_JOBS=1"},
		},
		{
			name: "fractional above one rounds up", demand: domain.ResourceDemand{CPUUnits: 2.1},
			want:       &Limits{CPUs: 3},
			properties: []string{"CPUQuota=300%", "TasksMax=4096"},
			env:        []string{"GOMAXPROCS=3", "GOFLAGS=-p=3", "MAKEFLAGS=-j3", "CARGO_BUILD_JOBS=3"},
		},
		{
			name: "memory only", demand: domain.ResourceDemand{MemoryMB: 512},
			want:       &Limits{MemoryMB: 512},
			properties: []string{"MemoryMax=512M", "MemorySwapMax=0", "TasksMax=4096"},
			env:        []string{"GOMAXPROCS=1", "GOFLAGS=-p=1", "MAKEFLAGS=-j1", "CARGO_BUILD_JOBS=1"},
		},
		{name: "unsized", demand: domain.ResourceDemand{}},
		{name: "class only is unsized", demand: domain.ResourceDemand{MinCPUClass: "standard", ScratchMB: 100}},
		{name: "negative is unsized", demand: domain.ResourceDemand{CPUUnits: -2, MemoryMB: -1}},
		{name: "not a number is unsized", demand: domain.ResourceDemand{CPUUnits: math.NaN()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := LimitsFor(test.demand)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("limits %+v, want %+v", got, test.want)
			}
			if properties := got.Properties(); !reflect.DeepEqual(properties, test.properties) {
				t.Fatalf("properties %q, want %q", properties, test.properties)
			}
			if env := got.Environment(); !reflect.DeepEqual(env, test.env) {
				t.Fatalf("environment %q, want %q", env, test.env)
			}
			if err := got.Validate(); err != nil {
				t.Fatalf("derived limits do not validate: %v", err)
			}
		})
	}
}

func TestMemoryOnlyBuildEnvironment(t *testing.T) {
	demand := domain.ResourceDemand{MemoryMB: 1000}
	if err := demand.Validate(); err != nil {
		t.Fatal(err)
	}
	limits := LimitsFor(demand)
	want := []string{"GOMAXPROCS=1", "GOFLAGS=-p=1", "MAKEFLAGS=-j1", "CARGO_BUILD_JOBS=1"}
	if got := limits.Environment(); !reflect.DeepEqual(got, want) {
		t.Fatalf("memory-sized launch environment %q; want %q", got, want)
	}
}

func TestLimitsForNeverExceedsTheValidatedBounds(t *testing.T) {
	got := LimitsFor(domain.ResourceDemand{CPUUnits: math.Inf(1), MemoryMB: math.MaxInt})
	if got == nil || got.CPUs != maxLimitCPUs || got.MemoryMB != maxLimitMemoryMB {
		t.Fatalf("limits %+v", got)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Limits{{CPUs: -1}, {MemoryMB: -1}, {CPUs: maxLimitCPUs + 1}, {MemoryMB: maxLimitMemoryMB + 1}, {}} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
}

// limitsFake is a systemd stand-in that records the launch arguments and
// answers observations with configurable unit properties.
type limitsFake struct {
	mu          sync.Mutex
	starts      int
	startArgs   []string
	showArgs    []string
	description string
	active      string
	result      string
	cpuQuota    string
	memoryMax   string
}

func (f *limitsFake) command(_ context.Context, command string, args ...string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if command == "systemd-run" {
		f.starts++
		f.startArgs = append([]string(nil), args...)
		for _, arg := range args {
			if strings.HasPrefix(arg, "--description=") {
				f.description = strings.TrimPrefix(arg, "--description=")
			}
		}
		return nil, nil
	}
	if args[1] == "stop" {
		return nil, nil
	}
	f.showArgs = append([]string(nil), args...)
	active, result := f.active, f.result
	if active == "" {
		active = "active/running"
	}
	if result == "" {
		result = "success"
	}
	state, sub, _ := strings.Cut(active, "/")
	return []byte(fmt.Sprintf("LoadState=loaded\nDescription=%s\nActiveState=%s\nSubState=%s\nInvocationID=limited\nResult=%s\nCPUQuotaPerSecUSec=%s\nMemoryMax=%s\n",
		f.description, state, sub, result, f.cpuQuota, f.memoryMax)), nil
}

func limitedFixture(t *testing.T, limits *Limits) (Supervisor, Launch, *limitsFake) {
	t.Helper()
	manager, launch, _ := supervisorFixture(t)
	fake := &limitsFake{cpuQuota: "infinity", memoryMax: "infinity"}
	manager.command = fake.command
	launch.Spec.Limits = limits
	return manager, launch, fake
}

func TestSupervisorStartPassesLimits(t *testing.T) {
	limits := LimitsFor(domain.ResourceDemand{CPUUnits: 4, MemoryMB: 6000})
	manager, launch, fake := limitedFixture(t, limits)
	fake.cpuQuota, fake.memoryMax = "4s", "6291456000"
	observation, err := manager.Start(context.Background(), launch)
	if err != nil {
		t.Fatal(err)
	}
	separator := slices.Index(fake.startArgs, "--")
	if separator < 0 {
		t.Fatalf("no command separator in %q", fake.startArgs)
	}
	for _, property := range []string{"CPUQuota=400%", "MemoryMax=6000M", "MemorySwapMax=0", "TasksMax=4096"} {
		if index := slices.Index(fake.startArgs, "--property="+property); index < 0 || index > separator {
			t.Fatalf("systemd-run arguments lack %s before the command: %q", property, fake.startArgs)
		}
	}
	if !reflect.DeepEqual(observation.Limits, limits) || observation.LimitsStatus != LimitsApplied {
		t.Fatalf("observation %+v", observation)
	}
	for _, property := range []string{"CPUQuotaPerSecUSec", "MemoryMax", "Result"} {
		if !strings.Contains(strings.Join(fake.showArgs, " "), property) {
			t.Fatalf("observation does not read back %s: %q", property, fake.showArgs)
		}
	}
	// The limits are in the digested launch spec.
	_, _, digest, _, err := manager.identity(launch)
	if err != nil {
		t.Fatal(err)
	}
	changed := launch
	changed.Spec.Limits = LimitsFor(domain.ResourceDemand{CPUUnits: 2, MemoryMB: 6000})
	_, _, other, _, err := manager.identity(changed)
	if err != nil {
		t.Fatal(err)
	}
	if digest == other || observation.Digest != digest {
		t.Fatalf("limits are not part of the launch digest: %s %s %s", observation.Digest, digest, other)
	}
	spec, err := os.ReadFile(filepath.Join(manager.Root, observation.Unit, "spec.json"))
	if err != nil {
		t.Fatal(err)
	}
	var recorded Spec
	if err := json.Unmarshal(spec, &recorded); err != nil || !reflect.DeepEqual(recorded.Limits, limits) {
		t.Fatalf("spec.json limits %+v %v", recorded.Limits, err)
	}
}

func TestSupervisorUnsizedLaunchIsUnchanged(t *testing.T) {
	manager, launch, fake := limitedFixture(t, nil)
	observation, err := manager.Start(context.Background(), launch)
	if err != nil {
		t.Fatal(err)
	}
	for _, arg := range fake.startArgs {
		for _, property := range []string{"CPUQuota", "MemoryMax", "MemorySwapMax", "TasksMax"} {
			if strings.Contains(arg, property) {
				t.Fatalf("unsized launch got %s: %q", arg, fake.startArgs)
			}
		}
	}
	// The recorded intent of an unsized launch is byte-for-byte what an
	// earlier release wrote, so in-flight intents keep matching after upgrade.
	intent, err := os.ReadFile(filepath.Join(manager.Root, observation.Unit, "intent.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(intent), "limits") {
		t.Fatalf("unsized intent gained a limits field: %s", intent)
	}
	if observation.Limits != nil || observation.LimitsStatus != "" {
		t.Fatalf("unsized observation reports limits: %+v", observation)
	}
}

func TestSupervisorRefusesRelaunchWithDifferentLimits(t *testing.T) {
	manager, launch, fake := limitedFixture(t, LimitsFor(domain.ResourceDemand{CPUUnits: 4, MemoryMB: 6000}))
	if _, err := manager.Start(context.Background(), launch); err != nil {
		t.Fatal(err)
	}
	recovered := Supervisor{Root: manager.Root, Executable: manager.Executable, command: fake.command}
	for name, limits := range map[string]*Limits{
		"smaller": LimitsFor(domain.ResourceDemand{CPUUnits: 2, MemoryMB: 6000}),
		"larger":  LimitsFor(domain.ResourceDemand{CPUUnits: 4, MemoryMB: 8000}),
		"removed": nil,
	} {
		changed := launch
		changed.Spec.Limits = limits
		if _, err := recovered.Start(context.Background(), changed); err == nil || !strings.Contains(err.Error(), "different launch") {
			t.Fatalf("%s: relaunch with different limits accepted: %v", name, err)
		}
		if _, err := recovered.Observe(context.Background(), changed); err == nil {
			t.Fatalf("%s: observation adopted different limits", name)
		}
	}
	if fake.starts != 1 {
		t.Fatalf("relaunch started %d units", fake.starts)
	}
	if _, err := recovered.Start(context.Background(), launch); err != nil {
		t.Fatalf("same limits refused: %v", err)
	}
}

func TestSupervisorRefusesMalformedLimitsBeforeAnyEffect(t *testing.T) {
	manager, launch, fake := limitedFixture(t, &Limits{CPUs: -1, MemoryMB: 100})
	if _, err := manager.Start(context.Background(), launch); err == nil {
		t.Fatal("malformed limits accepted")
	}
	entries, err := os.ReadDir(manager.Root)
	if err != nil {
		t.Fatal(err)
	}
	if fake.starts != 0 || len(entries) != 0 {
		t.Fatalf("malformed limits reserved or launched: starts=%d entries=%d", fake.starts, len(entries))
	}
}

func TestObserveReportsLimitsNotApplied(t *testing.T) {
	manager, launch, fake := limitedFixture(t, LimitsFor(domain.ResourceDemand{CPUUnits: 4, MemoryMB: 6000}))
	var mu sync.Mutex
	var warnings []string
	warn := func(message string, _ ...any) {
		mu.Lock()
		defer mu.Unlock()
		warnings = append(warnings, message)
	}
	manager.warn = warn
	fake.cpuQuota, fake.memoryMax = "infinity", "6291456000"
	observation, err := manager.Start(context.Background(), launch)
	if err != nil {
		t.Fatal(err)
	}
	if LimitsNotApplied != "limits not applied" || observation.LimitsStatus != LimitsNotApplied {
		t.Fatalf("observation %+v", observation)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := manager.Observe(context.Background(), launch); err != nil || got.LimitsStatus != LimitsNotApplied {
				t.Errorf("repeat observation %+v %v", got, err)
			}
		}()
	}
	wg.Wait()
	// A restarted worker observes the same unit without logging it again.
	recovered := Supervisor{Root: manager.Root, Executable: manager.Executable, command: fake.command, warn: warn}
	if got, err := recovered.Observe(context.Background(), launch); err != nil || got.LimitsStatus != LimitsNotApplied {
		t.Fatalf("recovered observation %+v %v", got, err)
	}
	if len(warnings) != 1 {
		t.Fatalf("limits-not-applied logged %d times", len(warnings))
	}
	// A memory limit the host ignored is reported the same way.
	fake.cpuQuota, fake.memoryMax = "4s", "infinity"
	if got, err := recovered.Observe(context.Background(), launch); err != nil || got.LimitsStatus != LimitsNotApplied {
		t.Fatalf("ignored memory limit %+v %v", got, err)
	}
	fake.cpuQuota, fake.memoryMax = "4s", "6291456000"
	if got, err := recovered.Observe(context.Background(), launch); err != nil || got.LimitsStatus != LimitsApplied {
		t.Fatalf("applied limits %+v %v", got, err)
	}
}

func TestContainedOOMFailureNamesTheReservation(t *testing.T) {
	manager, launch, fake := limitedFixture(t, LimitsFor(domain.ResourceDemand{CPUUnits: 4, MemoryMB: 6000}))
	fake.cpuQuota, fake.memoryMax = "4s", "6291456000"
	if _, err := manager.Start(context.Background(), launch); err != nil {
		t.Fatal(err)
	}
	fake.active, fake.result = "failed/failed", "oom-kill"
	observation, err := manager.Observe(context.Background(), launch)
	if err != nil {
		t.Fatal(err)
	}
	want := "contained run exceeded its 6000 MB memory reservation"
	if observation.State != "failed/failed" || observation.Failure != want {
		t.Fatalf("observation %+v", observation)
	}
	// The failure outlives the unit: stopping it keeps the named cause.
	stopped, err := manager.Stop(context.Background(), launch)
	if err != nil || !stopped.Stopped || stopped.Failure != want {
		t.Fatalf("stopped observation %+v %v", stopped, err)
	}
	// Another failure result is not reported as a memory overrun.
	other, otherLaunch, otherFake := limitedFixture(t, LimitsFor(domain.ResourceDemand{CPUUnits: 1, MemoryMB: 100}))
	if _, err := other.Start(context.Background(), otherLaunch); err != nil {
		t.Fatal(err)
	}
	otherFake.active, otherFake.result = "failed/failed", "exit-code"
	if got, err := other.Observe(context.Background(), otherLaunch); err != nil || got.Failure != "" {
		t.Fatalf("exit-code failure named the reservation: %+v %v", got, err)
	}
	// A failure record a crash left empty, or one that cannot be read, never
	// hides the cause or blocks observation and stop.
	torn, tornLaunch, tornFake := limitedFixture(t, LimitsFor(domain.ResourceDemand{CPUUnits: 4, MemoryMB: 6000}))
	started, err := torn.Start(context.Background(), tornLaunch)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(torn.Root, started.Unit, "failure"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	tornFake.active, tornFake.result = "failed/failed", "oom-kill"
	if got, err := torn.Observe(context.Background(), tornLaunch); err != nil || got.Failure != want {
		t.Fatalf("torn record observation %+v %v", got, err)
	}
	if got, err := torn.Stop(context.Background(), tornLaunch); err != nil || !got.Stopped || got.Failure != want {
		t.Fatalf("cause lost after stop with a torn record: %+v %v", got, err)
	}
	broken, brokenLaunch, _ := limitedFixture(t, LimitsFor(domain.ResourceDemand{MemoryMB: 100}))
	started, err = broken.Start(context.Background(), brokenLaunch)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(broken.Root, started.Unit, "failure"), []byte(strings.Repeat("x", 4096)), 0600); err != nil {
		t.Fatal(err)
	}
	broken.warn = func(string, ...any) {}
	if got, err := broken.Stop(context.Background(), brokenLaunch); err != nil || !got.Stopped {
		t.Fatalf("oversized failure record blocked stop: %+v %v", got, err)
	}
	// An unsized run killed by the kernel still says why.
	unsized, unsizedLaunch, unsizedFake := limitedFixture(t, nil)
	if _, err := unsized.Start(context.Background(), unsizedLaunch); err != nil {
		t.Fatal(err)
	}
	unsizedFake.active, unsizedFake.result = "failed/failed", "oom-kill"
	if got, err := unsized.Observe(context.Background(), unsizedLaunch); err != nil || got.Failure != "contained run was killed for exceeding available memory" {
		t.Fatalf("unsized oom %+v %v", got, err)
	}
}
