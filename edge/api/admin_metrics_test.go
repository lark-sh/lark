package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/lark-sh/lark/edge/db"
)

func seedProject(t *testing.T, store db.Store, id string) {
	t.Helper()
	if err := store.CreateProject(context.Background(), &db.Project{
		ID:             id,
		Name:           id,
		SecretKey:      "sk",
		AdminSecretKey: "ask",
		AutoCreate:     true,
	}); err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
}

func seedMetrics(t *testing.T, store db.Store, projectID string, rows []*db.DatabaseMetricsRow) {
	t.Helper()
	if err := store.InsertDatabaseMetricsBatch(context.Background(), rows); err != nil {
		t.Fatalf("InsertDatabaseMetricsBatch: %v", err)
	}
}

func seedEvent(t *testing.T, store db.Store, projectID, databaseID, evType, message string, ts time.Time) {
	t.Helper()
	if err := store.InsertDatabaseEvent(context.Background(), &db.DatabaseEvent{
		Timestamp:  ts,
		ProjectID:  projectID,
		DatabaseID: databaseID,
		EventType:  evType,
		Message:    message,
	}); err != nil {
		t.Fatalf("InsertDatabaseEvent: %v", err)
	}
}

func TestDashboard_EmptyProjectReturnsEmptySeries(t *testing.T) {
	s, store, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")
	seedProject(t, store, "p1")

	var resp adminDashboardResponse
	if code := getJSON(t, s, "/admin/api/projects/p1/dashboard", cookie, &resp); code != http.StatusOK {
		t.Fatalf("dashboard: got %d", code)
	}
	if resp.Project.ID != "p1" {
		t.Errorf("project.id: got %q", resp.Project.ID)
	}
	if len(resp.Timeseries) != 0 {
		t.Errorf("timeseries: got %d points, want 0", len(resp.Timeseries))
	}
	if resp.Summary != nil {
		t.Errorf("summary: got %+v, want nil for empty range", resp.Summary)
	}
}

func TestDashboard_AggregatesMetrics(t *testing.T) {
	s, store, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")
	seedProject(t, store, "p1")

	now := time.Now().UTC()
	rows := []*db.DatabaseMetricsRow{
		{Timestamp: now.Add(-30 * time.Minute), ProjectID: "p1", DatabaseID: "db1", CCU: 5, PeakCCU: 5, BytesIn: 100, BytesOut: 200, Writes: 10, Reads: 20, EventsSent: 5, P50LatencyUs: 1000, P99LatencyUs: 5000},
		{Timestamp: now.Add(-20 * time.Minute), ProjectID: "p1", DatabaseID: "db1", CCU: 8, PeakCCU: 10, BytesIn: 150, BytesOut: 250, Writes: 15, Reads: 25, EventsSent: 7, P50LatencyUs: 1200, P99LatencyUs: 6000},
		{Timestamp: now.Add(-10 * time.Minute), ProjectID: "p1", DatabaseID: "db1", CCU: 3, PeakCCU: 8, BytesIn: 50, BytesOut: 100, Writes: 5, Reads: 10, EventsSent: 2, P50LatencyUs: 800, P99LatencyUs: 4000},
	}
	seedMetrics(t, store, "p1", rows)

	var resp adminDashboardResponse
	if code := getJSON(t, s, "/admin/api/projects/p1/dashboard", cookie, &resp); code != http.StatusOK {
		t.Fatalf("dashboard: got %d", code)
	}
	if len(resp.Timeseries) != 3 {
		t.Fatalf("timeseries: got %d, want 3", len(resp.Timeseries))
	}
	if resp.Summary == nil {
		t.Fatal("summary is nil")
	}
	if resp.Summary.PeakCCU != 10 {
		t.Errorf("peak_ccu: got %d, want 10", resp.Summary.PeakCCU)
	}
	if resp.Summary.TotalBytesIn != 300 {
		t.Errorf("total_bytes_in: got %d, want 300", resp.Summary.TotalBytesIn)
	}
	if resp.Summary.TotalBytesOut != 550 {
		t.Errorf("total_bytes_out: got %d, want 550", resp.Summary.TotalBytesOut)
	}
	if resp.Summary.TotalWrites != 30 {
		t.Errorf("total_writes: got %d, want 30", resp.Summary.TotalWrites)
	}
	if resp.Summary.AvgLatencyUs != 1000 {
		// (1000 + 1200 + 800) / 3 = 1000
		t.Errorf("avg_latency_us: got %d, want 1000", resp.Summary.AvgLatencyUs)
	}
}

func TestDashboard_TimeRangeFiltersOutOfWindow(t *testing.T) {
	s, store, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")
	seedProject(t, store, "p1")

	now := time.Now().UTC()
	seedMetrics(t, store, "p1", []*db.DatabaseMetricsRow{
		// In window (last 1h)
		{Timestamp: now.Add(-30 * time.Minute), ProjectID: "p1", DatabaseID: "db1", PeakCCU: 7},
		// Out of window (3h ago)
		{Timestamp: now.Add(-3 * time.Hour), ProjectID: "p1", DatabaseID: "db1", PeakCCU: 99},
	})

	start := now.Add(-1 * time.Hour).Format(time.RFC3339)
	end := now.Format(time.RFC3339)
	url := "/admin/api/projects/p1/dashboard?start=" + start + "&end=" + end

	var resp adminDashboardResponse
	if code := getJSON(t, s, url, cookie, &resp); code != http.StatusOK {
		t.Fatalf("dashboard: got %d", code)
	}
	if len(resp.Timeseries) != 1 {
		t.Fatalf("timeseries: got %d, want 1 (only in-window row)", len(resp.Timeseries))
	}
	if resp.Summary.PeakCCU != 7 {
		t.Errorf("peak_ccu (in-window only): got %d, want 7", resp.Summary.PeakCCU)
	}
}

func TestDashboard_RejectsBadTimeRange(t *testing.T) {
	s, store, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")
	seedProject(t, store, "p1")

	if code := getJSON(t, s, "/admin/api/projects/p1/dashboard?start=not-a-date", cookie, nil); code != http.StatusBadRequest {
		t.Errorf("malformed start: got %d, want 400", code)
	}
}

func TestDashboard_UnknownProject_404(t *testing.T) {
	s, _, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")
	if code := getJSON(t, s, "/admin/api/projects/nope/dashboard", cookie, nil); code != http.StatusNotFound {
		t.Errorf("got %d, want 404", code)
	}
}

func TestEvents_ListsInDescOrder(t *testing.T) {
	s, store, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")
	seedProject(t, store, "p1")

	now := time.Now().UTC()
	seedEvent(t, store, "p1", "db1", "rate_limit_hit", "first", now.Add(-2*time.Minute))
	seedEvent(t, store, "p1", "db1", "rate_limit_hit", "second", now.Add(-1*time.Minute))
	seedEvent(t, store, "p1", "db1", "rate_limit_hit", "third", now)

	var resp adminEventsResponse
	if code := getJSON(t, s, "/admin/api/projects/p1/events", cookie, &resp); code != http.StatusOK {
		t.Fatalf("events: got %d", code)
	}
	if resp.Total != 3 {
		t.Errorf("total: got %d, want 3", resp.Total)
	}
	if len(resp.Events) != 3 {
		t.Fatalf("returned: got %d, want 3", len(resp.Events))
	}
	if resp.Events[0].Message != "third" || resp.Events[2].Message != "first" {
		t.Errorf("ordering wrong: [%q, %q, %q]",
			resp.Events[0].Message, resp.Events[1].Message, resp.Events[2].Message)
	}
}

func TestEvents_PaginationCaps(t *testing.T) {
	s, store, cleanup := newTestServer(t)
	defer cleanup()
	cookie := loggedIn(t, s, "admin@local", "admin-password-1")
	seedProject(t, store, "p1")

	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		seedEvent(t, store, "p1", "db1", "ev", "m", now.Add(-time.Duration(i)*time.Minute))
	}

	var resp adminEventsResponse
	if code := getJSON(t, s, "/admin/api/projects/p1/events?limit=2&offset=1", cookie, &resp); code != http.StatusOK {
		t.Fatalf("got %d", code)
	}
	if resp.Limit != 2 || resp.Offset != 1 || resp.Total != 5 {
		t.Errorf("page meta: %+v", resp)
	}
	if len(resp.Events) != 2 {
		t.Errorf("page size: got %d, want 2", len(resp.Events))
	}
}
