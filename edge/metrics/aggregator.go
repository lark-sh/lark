// Package metrics provides in-memory metrics aggregation for lark-edge.
//
// # Overview
//
// This package receives per-database metrics over HTTP, accumulates them at
// per-(project, database) grain, detects anomalies, and periodically flushes
// to the database.
//
// # Flow
//
//  1. An upstream metrics shipper POSTs per-database metrics to /internal/metrics
//  2. MetricsAggregator.IngestMetrics() updates the in-memory accumulator for
//     that (project, database)
//  3. Anomaly detection runs and may insert database_events
//  4. Every flushInterval, Flush() writes one database_metrics row per
//     (project, database) that saw activity in the window
//
// # Thread Safety
//
// All methods are safe for concurrent use. Each accumulator uses atomic
// operations for its counters; the bucket map is guarded by a mutex.
package metrics

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"
)

// IncomingMetrics is the JSON payload posted to /internal/metrics (per-database metrics).
type IncomingMetrics struct {
	Type              string `json:"type"`     // "db_metrics"
	Timestamp         int64  `json:"ts"`       // Unix timestamp
	Server            string `json:"server"`   // Server ID
	Core              int    `json:"core"`     // Core ID
	Project           string `json:"project"`  // Project ID
	Database          string `json:"database"` // Database ID
	Writes            int64  `json:"writes"`
	Reads             int64  `json:"reads"`
	Transactions      int64  `json:"transactions"`
	WriteBytes        int64  `json:"write_bytes"`
	ReadBytes         int64  `json:"read_bytes"`
	EventsSent        int64  `json:"events_sent"`
	CCU               int32  `json:"ccu"`
	Subscriptions     int32  `json:"subscriptions"`
	DataSizeBytes     int64  `json:"data_size_bytes"`
	LatencyAvgUs      int32  `json:"latency_avg_us"`
	LatencyMaxUs      int32  `json:"latency_max_us"`
	PermissionDenials int32  `json:"permission_denials"`
	SizeRejections    int32  `json:"size_rejections"`
}

// bucketKey identifies a single database. Database IDs are not globally
// unique (e.g. "prod" can exist under two projects), so the project is part
// of the key.
type bucketKey struct {
	project  string
	database string
}

// databaseMetrics accumulates metrics for a single (project, database) over
// one flush window.
type databaseMetrics struct {
	// Counters (reset on flush — SUM over the window)
	BytesIn           atomic.Int64
	BytesOut          atomic.Int64
	Writes            atomic.Int64
	Reads             atomic.Int64
	EventsSent        atomic.Int64
	PermissionDenials atomic.Int32

	// Latency tracking (reset on flush — averaged for display)
	LatencySum   atomic.Int64
	LatencyCount atomic.Int32
	LatencyMax   atomic.Int32

	// Gauges (not reset on flush — reflect current state)
	CCU           atomic.Int32 // last reported CCU for this database
	PeakCCU       atomic.Int32 // MAX CCU over the window
	DataSizeBytes atomic.Int64 // last reported on-disk size

	// Bookkeeping
	LastSeen         atomic.Int64 // unix nanos of last metrics received
	LastLatencyAvgUs atomic.Int32 // for anomaly detection
}

// eventKey identifies a unique event for deduplication
type eventKey struct {
	DatabaseID string
	EventType  string
}

// staleTimeout is how long before a database with no metrics is considered gone
const staleTimeout = 3 * time.Minute

// MetricsAggregator accumulates per-database metrics and flushes them as
// database_metrics rows.
type MetricsAggregator struct {
	db            db.Store
	flushInterval time.Duration

	mu      sync.RWMutex
	buckets map[bucketKey]*databaseMetrics

	// Event deduplication
	lastEventMu sync.Mutex
	lastEvent   map[eventKey]time.Time

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewMetricsAggregator creates a new aggregator
func NewMetricsAggregator(database db.Store, flushInterval time.Duration) *MetricsAggregator {
	return &MetricsAggregator{
		db:            database,
		flushInterval: flushInterval,
		buckets:       make(map[bucketKey]*databaseMetrics),
		lastEvent:     make(map[eventKey]time.Time),
		stopCh:        make(chan struct{}),
	}
}

// Start begins the background flush and cleanup goroutines
func (m *MetricsAggregator) Start() {
	m.wg.Add(2)
	go m.flushLoop()
	go m.cleanupLoop()
}

// Stop stops the aggregator and flushes remaining data
func (m *MetricsAggregator) Stop() {
	close(m.stopCh)
	m.wg.Wait()

	// Final flush
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.Flush(ctx); err != nil {
		logger.Error("Final flush error", "error", err)
	}
}

// flushLoop periodically flushes metrics to the database
func (m *MetricsAggregator) flushLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := m.Flush(ctx); err != nil {
				logger.Error("Flush error", "error", err)
			}
			cancel()
		case <-m.stopCh:
			return
		}
	}
}

// cleanupLoop periodically removes stale databases that stopped reporting metrics
func (m *MetricsAggregator) cleanupLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupStaleDatabases()
		case <-m.stopCh:
			return
		}
	}
}

// cleanupStaleDatabases removes databases that haven't reported metrics in staleTimeout
func (m *MetricsAggregator) cleanupStaleDatabases() {
	threshold := time.Now().Add(-staleTimeout).UnixNano()

	m.mu.Lock()
	defer m.mu.Unlock()

	for key, b := range m.buckets {
		ls := b.LastSeen.Load()
		if ls != 0 && ls < threshold {
			delete(m.buckets, key)
			logger.Debug("Cleaned up stale database",
				"project", key.project,
				"database", key.database)
		}
	}
}

// IngestMetrics processes incoming per-database metrics
func (m *MetricsAggregator) IngestMetrics(metrics *IncomingMetrics) {
	b := m.getOrCreateBucket(metrics.Project, metrics.Database)

	// Counters — summed over the window.
	b.BytesIn.Add(metrics.WriteBytes)
	b.BytesOut.Add(metrics.ReadBytes)
	b.Writes.Add(metrics.Writes)
	b.Reads.Add(metrics.Reads)
	b.EventsSent.Add(metrics.EventsSent)
	b.PermissionDenials.Add(metrics.PermissionDenials)

	// CCU and data size are gauges — keep the latest reported value.
	b.CCU.Store(metrics.CCU)
	b.DataSizeBytes.Store(metrics.DataSizeBytes)

	// Latency tracking
	if metrics.LatencyAvgUs > 0 {
		b.LatencySum.Add(int64(metrics.LatencyAvgUs))
		b.LatencyCount.Add(1)

		// Track max latency
		for {
			oldMax := b.LatencyMax.Load()
			if metrics.LatencyMaxUs <= oldMax {
				break
			}
			if b.LatencyMax.CompareAndSwap(oldMax, metrics.LatencyMaxUs) {
				break
			}
		}
	}

	// Peak CCU over the window — just this database's own max.
	for {
		oldPeak := b.PeakCCU.Load()
		if metrics.CCU <= oldPeak {
			break
		}
		if b.PeakCCU.CompareAndSwap(oldPeak, metrics.CCU) {
			break
		}
	}

	b.LastLatencyAvgUs.Store(metrics.LatencyAvgUs)
	b.LastSeen.Store(time.Now().UnixNano())

	// Check for anomalies
	m.checkAnomalies(metrics)
}

// getOrCreateBucket returns the accumulator for a (project, database), creating it if needed
func (m *MetricsAggregator) getOrCreateBucket(project, database string) *databaseMetrics {
	key := bucketKey{project: project, database: database}

	m.mu.RLock()
	b, ok := m.buckets[key]
	m.mu.RUnlock()
	if ok {
		return b
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if b, ok := m.buckets[key]; ok {
		return b
	}

	b = &databaseMetrics{}
	m.buckets[key] = b
	return b
}

// checkAnomalies detects and logs notable events
func (m *MetricsAggregator) checkAnomalies(metrics *IncomingMetrics) {
	// High latency check (> 3x project average AND at least 10ms)
	// Only check if database sent 100+ events in this period (warmed up, not in startup)
	if metrics.LatencyAvgUs >= 10000 && metrics.EventsSent >= 100 {
		avgLatency := m.projectAvgLatency(metrics.Project)
		if avgLatency > 0 && metrics.LatencyAvgUs > avgLatency*3 {
			m.maybeLogEvent(metrics.Project, metrics.Database, "high_latency",
				"Latency %dms (project avg: %dms)",
				metrics.LatencyAvgUs/1000, avgLatency/1000)
		}
	}
}

// projectAvgLatency returns the current average latency across all databases
// in the project, for the current window.
func (m *MetricsAggregator) projectAvgLatency(project string) int32 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var sum int64
	var count int32
	for key, b := range m.buckets {
		if key.project != project {
			continue
		}
		sum += b.LatencySum.Load()
		count += b.LatencyCount.Load()
	}
	if count == 0 {
		return 0
	}
	return int32(sum / int64(count))
}

// maybeLogEvent logs an event if not recently logged (5 minute cooldown)
func (m *MetricsAggregator) maybeLogEvent(projectID, databaseID, eventType, format string, args ...interface{}) {
	key := eventKey{DatabaseID: databaseID, EventType: eventType}

	m.lastEventMu.Lock()
	last, ok := m.lastEvent[key]
	if ok && time.Since(last) < 5*time.Minute {
		m.lastEventMu.Unlock()
		return // Skip, already logged recently
	}
	m.lastEvent[key] = time.Now()
	m.lastEventMu.Unlock()

	// Format message
	message := format
	if len(args) > 0 {
		message = formatMessage(format, args...)
	}

	// Insert event
	event := &db.DatabaseEvent{
		Timestamp:  time.Now(),
		ProjectID:  projectID,
		DatabaseID: databaseID,
		EventType:  eventType,
		Message:    message,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.db.InsertDatabaseEvent(ctx, event); err != nil {
		logger.Error("Failed to insert event", "error", err)
	}
}

// formatMessage formats a message with arguments
func formatMessage(format string, args ...interface{}) string {
	// Simple implementation - use sprintf-style formatting
	result := format
	for _, arg := range args {
		// Find first %d or %s and replace
		for i := 0; i < len(result)-1; i++ {
			if result[i] == '%' && (result[i+1] == 'd' || result[i+1] == 's') {
				var replacement string
				switch v := arg.(type) {
				case int:
					replacement = intToString(v)
				case int32:
					replacement = intToString(int(v))
				case int64:
					replacement = intToString(int(v))
				case string:
					replacement = v
				default:
					replacement = "?"
				}
				result = result[:i] + replacement + result[i+2:]
				break
			}
		}
	}
	return result
}

// intToString converts int to string without importing strconv
func intToString(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}

// Flush writes one database_metrics row per active (project, database) and
// resets the window counters.
func (m *MetricsAggregator) Flush(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.buckets) == 0 {
		return nil
	}

	now := time.Now()
	var rows []*db.DatabaseMetricsRow

	for key, b := range m.buckets {
		// Swap and collect values
		ccu := b.CCU.Load()
		peakCCU := b.PeakCCU.Swap(ccu) // Reset peak to current
		bytesIn := b.BytesIn.Swap(0)
		bytesOut := b.BytesOut.Swap(0)
		writes := b.Writes.Swap(0)
		reads := b.Reads.Swap(0)
		eventsSent := b.EventsSent.Swap(0)
		permissionDenials := b.PermissionDenials.Swap(0)
		latencySum := b.LatencySum.Swap(0)
		latencyCount := b.LatencyCount.Swap(0)
		b.LatencyMax.Store(0)
		dataSizeBytes := b.DataSizeBytes.Load() // gauge — don't reset

		// Skip if no activity
		if writes == 0 && reads == 0 && eventsSent == 0 && bytesIn == 0 && bytesOut == 0 {
			continue
		}

		// Compute average latency
		var p50LatencyUs int
		if latencyCount > 0 {
			p50LatencyUs = int(latencySum / int64(latencyCount))
		}

		row := &db.DatabaseMetricsRow{
			Timestamp:            now,
			ProjectID:            key.project,
			DatabaseID:           key.database,
			CCU:                  int(ccu),
			PeakCCU:              int(peakCCU),
			BytesIn:              bytesIn,
			BytesOut:             bytesOut,
			Writes:               writes,
			Reads:                reads,
			EventsSent:           eventsSent,
			PermissionDenials:    int(permissionDenials),
			ConnectionRejections: 0, // TODO: Track this
			DataSizeBytes:        dataSizeBytes,
			P50LatencyUs:         p50LatencyUs,
			P99LatencyUs:         0, // TODO: Need histogram for p99
		}
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		return nil
	}

	logger.Debug("Flushing database metrics to Postgres", "count", len(rows))
	return m.db.InsertDatabaseMetricsBatch(ctx, rows)
}

// GetAllProjectIDs returns all project IDs currently being tracked
func (m *MetricsAggregator) GetAllProjectIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seen := make(map[string]struct{}, len(m.buckets))
	for key := range m.buckets {
		seen[key.project] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
