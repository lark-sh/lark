package api

import (
	"encoding/json"
	"net/http"

	"github.com/lark-sh/lark/edge/metrics"
)

// SetMetricsAggregator sets the metrics aggregator for the API server
func (s *Server) SetMetricsAggregator(aggregator *metrics.MetricsAggregator) {
	s.metricsAggregator = aggregator
}

// RegisterMetricsRoutes registers metrics-related routes
func (s *Server) RegisterMetricsRoutes() {
	// Internal endpoint (internal listener only) — authenticated with SERVER_SECRET
	// so a caller that reaches the port can't poison dashboard/billing metrics.
	s.mux.HandleFunc("POST /internal/metrics", s.requireServerSecret(s.handleIngestMetrics))
}

// handleIngestMetrics receives metrics from Vector
// POST /internal/metrics
// This endpoint is only accessible via the internal HTTP server (port 8080).
func (s *Server) handleIngestMetrics(w http.ResponseWriter, r *http.Request) {
	if s.metricsAggregator == nil {
		s.writeError(w, http.StatusServiceUnavailable, "metrics aggregator not configured")
		return
	}

	// Read body with limit
	r.Body = http.MaxBytesReader(w, r.Body, 10*1024*1024) // 10MB limit

	var metricsList []metrics.IncomingMetrics

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&metricsList); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON: expected array of metrics")
		return
	}

	// Process each metric
	for i := range metricsList {
		if metricsList[i].Type != "db_metrics" {
			continue
		}
		if metricsList[i].Project == "" {
			continue
		}
		s.metricsAggregator.IngestMetrics(&metricsList[i])
	}

	w.WriteHeader(http.StatusAccepted)
}
