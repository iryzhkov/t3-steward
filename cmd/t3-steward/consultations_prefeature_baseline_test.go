package main

import (
	"bufio"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// TestConsultationsPrefeatureCoordinatorMeasurements records the existing
// coordinator boundary over ordinary ready work and supervision. Each sample
// owns a migrated, instrumented SQLite fixture; migration and fixture
// construction are outside the timed and counted region.
func TestConsultationsPrefeatureCoordinatorMeasurements(t *testing.T) {
	const samples = 30
	type sample struct {
		fixture *activationLeaseFixture
		cycle   coordinatorBoundaryCycle
		sql     *sqliteTraceMetrics
	}
	fixtures := make([]sample, 0, samples)
	for i := 0; i < samples; i++ {
		metrics := &sqliteTraceMetrics{}
		fixture := newActivationLeaseFixtureWithStore(t, func(path string) (*sqlite.Store, error) {
			return sqlite.OpenMigratedInstrumented(path, func(connector driver.Connector) driver.Connector {
				return tracingConnector{Connector: connector, metrics: metrics}
			})
		})
		records, err := fixture.store.LoadCoordinatorRecords(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for index := range records.Tasks {
			if records.Tasks[index].ID == "task-protected" {
				records.Tasks[index].Needs = []string{"producer"}
			}
		}
		if err := fixture.store.SaveCoordinatorRecords(context.Background(), records); err != nil {
			t.Fatal(err)
		}
		fixture.superviseRun(t)
		addActivationFairnessAttempt(t, fixture, "ordinary", "worker-ordinary", "ordinary-pool", fixture.now.Add(-time.Minute), false)
		quota := activationFairnessQuota(fixture.now,
			domain.QuotaPool{ID: "claude-main", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"}, Admission: domain.AdmissionOpen, MaxConcurrent: 1},
			domain.QuotaPool{ID: "ordinary-pool", Provider: "ordinary", ProviderInstanceIDs: []string{"instance-ordinary"}, Admission: domain.AdmissionOpen, MaxConcurrent: 1},
		)
		fixtures = append(fixtures, sample{fixture: fixture, cycle: activationFairnessCycle(t, fixture, quota), sql: metrics})
	}

	ctx := context.Background()
	latencies := make([]int64, 0, samples)
	sqlDurations := make([]int64, 0, samples)
	statementCounts := make([]int64, 0, samples)
	transactionCounts := make([]int64, 0, samples)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for _, sample := range fixtures {
		sample.sql.reset()
		start := time.Now()
		sample.cycle.Tick(ctx)
		latencies = append(latencies, time.Since(start).Nanoseconds())
		statements, transactions, sqlDuration := sample.sql.snapshot()
		statementCounts = append(statementCounts, statements)
		transactionCounts = append(transactionCounts, transactions)
		sqlDurations = append(sqlDurations, sqlDuration.Nanoseconds())
	}
	runtime.ReadMemStats(&after)

	for _, sample := range fixtures {
		records, err := sample.fixture.store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var ordinaryOffered bool
		for _, assignment := range records.Assignments {
			if assignment.AttemptID == "attempt-ordinary" && assignment.State == domain.AssignmentOffered {
				ordinaryOffered = true
			}
		}
		if !ordinaryOffered {
			t.Fatalf("ordinary ready attempt received no offered assignment: %+v", records.Assignments)
		}
		state, err := sample.fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
		if err != nil {
			t.Fatal(err)
		}
		if state.Activation.State != domain.ActivationPendingDispatch {
			t.Fatalf("supervision activation = %q, want %q", state.Activation.State, domain.ActivationPendingDispatch)
		}
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	sort.Slice(sqlDurations, func(i, j int) bool { return sqlDurations[i] < sqlDurations[j] })
	sort.Slice(statementCounts, func(i, j int) bool { return statementCounts[i] < statementCounts[j] })
	sort.Slice(transactionCounts, func(i, j int) bool { return transactionCounts[i] < transactionCounts[j] })
	var total int64
	for _, latency := range latencies {
		total += latency
	}
	load, _ := os.ReadFile("/proc/loadavg")
	receipt := map[string]any{
		"schema":                        "consultations-prefeature-coordinator-v2",
		"samples":                       samples,
		"fixture_creation_timed":        false,
		"fixture_creation_sql_counted":  false,
		"coordinator_passes_per_sample": 1,
		"ordinary_ready_per_sample":     1,
		"supervised_runs_per_sample":    1,
		"median_ns":                     latencies[len(latencies)/2],
		"p95_ns":                        latencies[(len(latencies)*95-1)/100],
		"throughput_passes_per_second":  float64(samples) / (float64(total) / float64(time.Second)),
		"mallocs_delta":                 after.Mallocs - before.Mallocs,
		"total_alloc_bytes_delta":       after.TotalAlloc - before.TotalAlloc,
		"sql_statement_count_min":       statementCounts[0],
		"sql_statement_count_median":    statementCounts[len(statementCounts)/2],
		"sql_statement_count_max":       statementCounts[len(statementCounts)-1],
		"transaction_count_min":         transactionCounts[0],
		"transaction_count_median":      transactionCounts[len(transactionCounts)/2],
		"transaction_count_max":         transactionCounts[len(transactionCounts)-1],
		"median_sql_execution_ns":       sqlDurations[len(sqlDurations)/2],
		"p95_sql_execution_ns":          sqlDurations[(len(sqlDurations)*95-1)/100],
		"sql_timing_scope":              "driver Exec plus complete Query rows lifetime",
		"transaction_counting_scope":    "successful or failed driver Begin/BeginTx calls",
		"go_version":                    runtime.Version(),
		"gomaxprocs":                    runtime.GOMAXPROCS(0),
		"host_loadavg":                  strings.TrimSpace(string(load)),
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(raw))
}

const idleCoordinatorChild = "T3_STEWARD_PREFEATURE_IDLE_COORDINATOR_CHILD"

// TestConsultationsPrefeatureIdleCoordinatorRSS records five independent
// dedicated test-process RSS observations. In each child, the same ordinary
// ready plus supervision coordinator cycle has completed one warm pass and is
// quiescent: no Tick or fixture construction overlaps the observation.
func TestConsultationsPrefeatureIdleCoordinatorRSS(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("idle coordinator RSS comparator requires Linux /proc")
	}
	if os.Getenv(idleCoordinatorChild) == "1" {
		runIdleCoordinatorChild(t)
		return
	}

	const samples = 5
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	rssSamples := make([]int64, 0, samples)
	for i := 0; i < samples; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		command := exec.CommandContext(ctx, executable, "-test.run=^TestConsultationsPrefeatureIdleCoordinatorRSS$", "-test.count=1")
		command.Env = append(os.Environ(), idleCoordinatorChild+"=1")
		stdin, err := command.StdinPipe()
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		stdout, err := command.StdoutPipe()
		if err != nil {
			_ = stdin.Close()
			cancel()
			t.Fatal(err)
		}
		command.Stderr = command.Stdout
		if err := command.Start(); err != nil {
			_ = stdin.Close()
			cancel()
			t.Fatal(err)
		}
		failed := func(format string, args ...any) {
			_ = stdin.Close()
			_ = command.Process.Kill()
			_ = command.Wait()
			cancel()
			t.Fatalf(format, args...)
		}
		scanner := bufio.NewScanner(stdout)
		ready := false
		for scanner.Scan() {
			if scanner.Text() == "IDLE_COORDINATOR_READY" {
				ready = true
				break
			}
		}
		if !ready {
			failed("idle coordinator child did not become ready: scan=%v context=%v", scanner.Err(), ctx.Err())
		}
		rss, ok := processRSSKB(command.Process.Pid)
		if !ok {
			failed("read RSS for idle coordinator pid %d", command.Process.Pid)
		}
		rssSamples = append(rssSamples, rss)
		if err := stdin.Close(); err != nil {
			failed("close idle coordinator child stdin: %v", err)
		}
		for scanner.Scan() {
		}
		if err := scanner.Err(); err != nil {
			failed("read idle coordinator child output: %v", err)
		}
		if err := command.Wait(); err != nil {
			cancel()
			t.Fatalf("wait for idle coordinator child: %v (context=%v)", err, ctx.Err())
		}
		cancel()
	}
	sort.Slice(rssSamples, func(i, j int) bool { return rssSamples[i] < rssSamples[j] })
	load, _ := os.ReadFile("/proc/loadavg")
	raw, err := json.Marshal(map[string]any{
		"schema":             "consultations-prefeature-idle-coordinator-rss-v1",
		"samples":            samples,
		"rss_kb_samples":     rssSamples,
		"median_rss_kb":      rssSamples[len(rssSamples)/2],
		"maximum_rss_kb":     rssSamples[len(rssSamples)-1],
		"fixture":            "one warmed ordinary-ready plus supervision coordinator cycle",
		"measurement_state":  "dedicated child process, coordinator quiescent, no in-flight Tick",
		"measurement_source": "/proc/<child-pid>/status VmRSS",
		"go_version":         runtime.Version(),
		"gomaxprocs":         runtime.GOMAXPROCS(0),
		"host_loadavg":       strings.TrimSpace(string(load)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(raw))
}

func runIdleCoordinatorChild(t *testing.T) {
	fixture := newActivationLeaseFixture(t)
	records, err := fixture.store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for index := range records.Tasks {
		if records.Tasks[index].ID == "task-protected" {
			records.Tasks[index].Needs = []string{"producer"}
		}
	}
	if err := fixture.store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	fixture.superviseRun(t)
	addActivationFairnessAttempt(t, fixture, "ordinary", "worker-ordinary", "ordinary-pool", fixture.now.Add(-time.Minute), false)
	cycle := activationFairnessCycle(t, fixture, activationFairnessQuota(fixture.now,
		domain.QuotaPool{ID: "claude-main", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"}, Admission: domain.AdmissionOpen, MaxConcurrent: 1},
		domain.QuotaPool{ID: "ordinary-pool", Provider: "ordinary", ProviderInstanceIDs: []string{"instance-ordinary"}, Admission: domain.AdmissionOpen, MaxConcurrent: 1},
	))
	cycle.Tick(context.Background())
	runtime.GC()
	debug.FreeOSMemory()
	time.Sleep(250 * time.Millisecond)
	fmt.Fprintln(os.Stdout, "IDLE_COORDINATOR_READY")
	_, _ = io.Copy(io.Discard, os.Stdin)
	runtime.KeepAlive(cycle)
}

func processRSSKB(pid int) (int64, bool) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "VmRSS:" && fields[2] == "kB" {
			value, err := strconv.ParseInt(fields[1], 10, 64)
			return value, err == nil
		}
	}
	return 0, false
}

type sqliteTraceMetrics struct {
	mu           sync.Mutex
	statements   int64
	transactions int64
	sqlDuration  time.Duration
}

func (m *sqliteTraceMetrics) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statements = 0
	m.transactions = 0
	m.sqlDuration = 0
}

func (m *sqliteTraceMetrics) beginStatement() time.Time {
	m.mu.Lock()
	m.statements++
	m.mu.Unlock()
	return time.Now()
}

func (m *sqliteTraceMetrics) finishStatement(start time.Time) {
	m.mu.Lock()
	m.sqlDuration += time.Since(start)
	m.mu.Unlock()
}

func (m *sqliteTraceMetrics) beginTransaction() {
	m.mu.Lock()
	m.transactions++
	m.mu.Unlock()
}

func (m *sqliteTraceMetrics) snapshot() (int64, int64, time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.statements, m.transactions, m.sqlDuration
}

type tracingConnector struct {
	driver.Connector
	metrics *sqliteTraceMetrics
}

func (c tracingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &tracingConn{Conn: conn, metrics: c.metrics}, nil
}

type tracingConn struct {
	driver.Conn
	metrics *sqliteTraceMetrics
}

func (c *tracingConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &tracingStmt{Stmt: stmt, metrics: c.metrics}, nil
}

func (c *tracingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if conn, ok := c.Conn.(driver.ConnPrepareContext); ok {
		stmt, err := conn.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		return &tracingStmt{Stmt: stmt, metrics: c.metrics}, nil
	}
	return c.Prepare(query)
}

func (c *tracingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	conn, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := c.metrics.beginStatement()
	defer c.metrics.finishStatement(start)
	return conn.ExecContext(ctx, query, args)
}

func (c *tracingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	conn, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := c.metrics.beginStatement()
	rows, err := conn.QueryContext(ctx, query, args)
	if err != nil {
		c.metrics.finishStatement(start)
		return nil, err
	}
	return &tracingRows{Rows: rows, metrics: c.metrics, start: start}, nil
}

func (c *tracingConn) Begin() (driver.Tx, error) {
	c.metrics.beginTransaction()
	return c.Conn.Begin()
}

func (c *tracingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	conn, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, driver.ErrSkip
	}
	c.metrics.beginTransaction()
	return conn.BeginTx(ctx, opts)
}

func (c *tracingConn) Ping(ctx context.Context) error {
	if conn, ok := c.Conn.(driver.Pinger); ok {
		return conn.Ping(ctx)
	}
	return nil
}

func (c *tracingConn) ResetSession(ctx context.Context) error {
	if conn, ok := c.Conn.(driver.SessionResetter); ok {
		return conn.ResetSession(ctx)
	}
	return nil
}

func (c *tracingConn) IsValid() bool {
	if conn, ok := c.Conn.(driver.Validator); ok {
		return conn.IsValid()
	}
	return true
}

func (c *tracingConn) CheckNamedValue(value *driver.NamedValue) error {
	if conn, ok := c.Conn.(driver.NamedValueChecker); ok {
		return conn.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

type tracingStmt struct {
	driver.Stmt
	metrics *sqliteTraceMetrics
}

func (s *tracingStmt) Exec(args []driver.Value) (driver.Result, error) {
	start := s.metrics.beginStatement()
	defer s.metrics.finishStatement(start)
	return s.Stmt.Exec(args)
}

func (s *tracingStmt) Query(args []driver.Value) (driver.Rows, error) {
	start := s.metrics.beginStatement()
	rows, err := s.Stmt.Query(args)
	if err != nil {
		s.metrics.finishStatement(start)
		return nil, err
	}
	return &tracingRows{Rows: rows, metrics: s.metrics, start: start}, nil
}

func (s *tracingStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	stmt, ok := s.Stmt.(driver.StmtExecContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := s.metrics.beginStatement()
	defer s.metrics.finishStatement(start)
	return stmt.ExecContext(ctx, args)
}

func (s *tracingStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	stmt, ok := s.Stmt.(driver.StmtQueryContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := s.metrics.beginStatement()
	rows, err := stmt.QueryContext(ctx, args)
	if err != nil {
		s.metrics.finishStatement(start)
		return nil, err
	}
	return &tracingRows{Rows: rows, metrics: s.metrics, start: start}, nil
}

func (s *tracingStmt) CheckNamedValue(value *driver.NamedValue) error {
	if stmt, ok := s.Stmt.(driver.NamedValueChecker); ok {
		return stmt.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

type tracingRows struct {
	driver.Rows
	metrics *sqliteTraceMetrics
	start   time.Time
	once    sync.Once
}

func (r *tracingRows) Close() error {
	err := r.Rows.Close()
	r.finish()
	return err
}

func (r *tracingRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if errors.Is(err, io.EOF) {
		r.finish()
	}
	return err
}

func (r *tracingRows) finish() {
	r.once.Do(func() {
		r.metrics.finishStatement(r.start)
	})
}
