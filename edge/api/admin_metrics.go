package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"
)

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------

// adminDashboardResponse is the payload the dashboard's home page consumes.
// Mirrors the shape used by the existing chart components. Fields that
// would require a live in-memory aggregator snapshot (e.g. "current ccu
// right now") are omitted in v1 — the timeseries' final point is good
// enough for most rendering purposes.
type adminDashboardResponse struct {
	Project    dashboardProject     `json:"project"`
	Summary    *dashboardSummary    `json:"summary,omitempty"`
	TimeRange  dashboardTimeRange   `json:"time_range"`
	Timeseries []dashboardPoint     `json:"timeseries"`
	RecentEvents []*db.DatabaseEvent `json:"recent_events"`
}

type dashboardProject struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type dashboardSummary struct {
	PeakCCU       int   `json:"peak_ccu"`
	TotalBytesIn  int64 `json:"total_bytes_in"`
	TotalBytesOut int64 `json:"total_bytes_out"`
	TotalWrites   int64 `json:"total_writes"`
	TotalReads    int64 `json:"total_reads"`
	TotalEvents   int64 `json:"total_events"`
	AvgLatencyUs  int   `json:"avg_latency_us"`
}

type dashboardTimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type dashboardPoint struct {
	Timestamp     time.Time `json:"ts"`
	CCU           int       `json:"ccu"`
	BytesIn       int64     `json:"bytes_in"`
	BytesOut      int64     `json:"bytes_out"`
	Writes        int64     `json:"writes"`
	Reads         int64     `json:"reads"`
	EventsSent    int64     `json:"events_sent"`
	P50LatencyUs  int       `json:"p50_latency_us"`
	P99LatencyUs  int       `json:"p99_latency_us"`
}

const (
	dashboardRecentEventsLimit = 25
	dashboardMaxRangeHours     = 30 * 24 // 30 days
)

func (s *Server) handleAdminProjectDashboard(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	project, err := s.db.GetProjectByID(r.Context(), projectID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "project not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	start, end, err := parseTimeRange(r, 24*time.Hour)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	metrics, err := s.db.GetProjectMetricsRange(r.Context(), projectID, start, end)
	if err != nil {
		logger.Error("admin/dashboard fetch", "error", err, "project_id", projectID)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	events, _, err := s.db.ListDatabaseEvents(r.Context(), projectID, dashboardRecentEventsLimit, 0)
	if err != nil {
		// Surface as zero events rather than fail the whole dashboard.
		logger.Warn("admin/dashboard events fetch", "error", err, "project_id", projectID)
		events = nil
	}

	resp := adminDashboardResponse{
		Project: dashboardProject{ID: project.ID, Name: project.Name},
		TimeRange: dashboardTimeRange{Start: start, End: end},
		Timeseries: buildTimeseries(metrics),
		RecentEvents: events,
	}
	if len(metrics) > 0 {
		summary := computeSummary(metrics)
		resp.Summary = &summary
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// parseTimeRange returns the [start, end] window for the dashboard query.
// Both are optional and default to "the last `defaultWindow`". A configured
// hard cap (30 days) keeps a misbehaving client from asking for a year of
// data in one shot.
func parseTimeRange(r *http.Request, defaultWindow time.Duration) (start, end time.Time, err error) {
	now := time.Now().UTC()
	end = now
	if v := r.URL.Query().Get("end"); v != "" {
		t, perr := time.Parse(time.RFC3339, v)
		if perr != nil {
			return time.Time{}, time.Time{}, errors.New("invalid end (expected RFC3339)")
		}
		end = t
	}
	start = end.Add(-defaultWindow)
	if v := r.URL.Query().Get("start"); v != "" {
		t, perr := time.Parse(time.RFC3339, v)
		if perr != nil {
			return time.Time{}, time.Time{}, errors.New("invalid start (expected RFC3339)")
		}
		start = t
	}
	if !end.After(start) {
		return time.Time{}, time.Time{}, errors.New("end must be after start")
	}
	if end.Sub(start) > dashboardMaxRangeHours*time.Hour {
		return time.Time{}, time.Time{}, errors.New("range too wide (max 30 days)")
	}
	return start, end, nil
}

func buildTimeseries(metrics []*db.ProjectMetricsRow) []dashboardPoint {
	if len(metrics) == 0 {
		return []dashboardPoint{}
	}
	out := make([]dashboardPoint, len(metrics))
	for i, m := range metrics {
		out[i] = dashboardPoint{
			Timestamp:    m.Timestamp,
			CCU:          m.CCU,
			BytesIn:      m.BytesIn,
			BytesOut:     m.BytesOut,
			Writes:       m.Writes,
			Reads:        m.Reads,
			EventsSent:   m.EventsSent,
			P50LatencyUs: m.P50LatencyUs,
			P99LatencyUs: m.P99LatencyUs,
		}
	}
	return out
}

func computeSummary(metrics []*db.ProjectMetricsRow) dashboardSummary {
	var s dashboardSummary
	var latencySum, latencyCount int64
	for _, m := range metrics {
		if m.PeakCCU > s.PeakCCU {
			s.PeakCCU = m.PeakCCU
		}
		s.TotalBytesIn += m.BytesIn
		s.TotalBytesOut += m.BytesOut
		s.TotalWrites += m.Writes
		s.TotalReads += m.Reads
		s.TotalEvents += m.EventsSent
		if m.P50LatencyUs > 0 {
			latencySum += int64(m.P50LatencyUs)
			latencyCount++
		}
	}
	if latencyCount > 0 {
		s.AvgLatencyUs = int(latencySum / latencyCount)
	}
	return s
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

type adminEventsResponse struct {
	Events []*db.DatabaseEvent `json:"events"`
	Limit  int                 `json:"limit"`
	Offset int                 `json:"offset"`
	Total  int                 `json:"total"`
}

const (
	eventsDefaultLimit = 50
	eventsMaxLimit     = 200
)

func (s *Server) handleAdminProjectEvents(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	if _, err := s.db.GetProjectByID(r.Context(), projectID); err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, http.StatusNotFound, "project not found")
			return
		}
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	limit := eventsDefaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > eventsMaxLimit {
		limit = eventsMaxLimit
	}
	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	events, total, err := s.db.ListDatabaseEvents(r.Context(), projectID, limit, offset)
	if err != nil {
		logger.Error("admin/events fetch", "error", err, "project_id", projectID)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if events == nil {
		events = []*db.DatabaseEvent{}
	}
	s.writeJSON(w, http.StatusOK, adminEventsResponse{
		Events: events,
		Limit:  limit,
		Offset: offset,
		Total:  total,
	})
}
