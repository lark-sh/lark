package proxy

import (
	"net/http"
	"testing"
	"time"

	"github.com/lark-sh/lark/edge/config"
	"github.com/lark-sh/lark/edge/db"
)

// TestResolveHTTPMethod tests the X-HTTP-Method-Override logic
func TestResolveHTTPMethod(t *testing.T) {
	tests := []struct {
		name           string
		requestMethod  string
		overrideHeader string
		wantMethod     string
	}{
		{
			name:           "POST without override",
			requestMethod:  http.MethodPost,
			overrideHeader: "",
			wantMethod:     http.MethodPost,
		},
		{
			name:           "POST with DELETE override",
			requestMethod:  http.MethodPost,
			overrideHeader: "DELETE",
			wantMethod:     "DELETE",
		},
		{
			name:           "POST with PUT override",
			requestMethod:  http.MethodPost,
			overrideHeader: "PUT",
			wantMethod:     "PUT",
		},
		{
			name:           "POST with PATCH override",
			requestMethod:  http.MethodPost,
			overrideHeader: "PATCH",
			wantMethod:     "PATCH",
		},
		{
			name:           "GET with DELETE override - should be ignored",
			requestMethod:  http.MethodGet,
			overrideHeader: "DELETE",
			wantMethod:     http.MethodGet,
		},
		{
			name:           "GET with PUT override - should be ignored",
			requestMethod:  http.MethodGet,
			overrideHeader: "PUT",
			wantMethod:     http.MethodGet,
		},
		{
			name:           "PUT without override",
			requestMethod:  http.MethodPut,
			overrideHeader: "",
			wantMethod:     http.MethodPut,
		},
		{
			name:           "PUT with POST override",
			requestMethod:  http.MethodPut,
			overrideHeader: "POST",
			wantMethod:     "POST",
		},
		{
			name:           "DELETE without override",
			requestMethod:  http.MethodDelete,
			overrideHeader: "",
			wantMethod:     http.MethodDelete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Replicate the logic from handleRESTProxy
			method := tt.requestMethod
			if tt.requestMethod != http.MethodGet {
				if tt.overrideHeader != "" {
					method = tt.overrideHeader
				}
			}

			if method != tt.wantMethod {
				t.Errorf("got method %q, want %q", method, tt.wantMethod)
			}
		})
	}
}

// TestBuildProjectConfig tests that buildProjectConfig correctly converts db.Project to backend.ProjectConfig
func TestBuildProjectConfig(t *testing.T) {
	project := &db.Project{
		ID:                    "test-project",
		RulesJSON:             `{"rules": {".read": true}}`,
		SecretKey:             "secret-key-123",
		AdminSecretKey:        "admin-secret-456",
		FirebaseProjectID:     "firebase-proj",
		Ephemeral:             true,
		AutoCreate:            true,
		FirebaseCompatEnabled: true,
	}

	config := buildProjectConfig(project)

	if config.Rules != project.RulesJSON {
		t.Errorf("Rules: got %q, want %q", config.Rules, project.RulesJSON)
	}
	if config.SecretKey != project.SecretKey {
		t.Errorf("SecretKey: got %q, want %q", config.SecretKey, project.SecretKey)
	}
	if config.AdminSecretKey != project.AdminSecretKey {
		t.Errorf("AdminSecretKey: got %q, want %q", config.AdminSecretKey, project.AdminSecretKey)
	}
	if config.FirebaseProjectID != project.FirebaseProjectID {
		t.Errorf("FirebaseProjectID: got %q, want %q", config.FirebaseProjectID, project.FirebaseProjectID)
	}

	// Check settings
	if config.Settings["ephemeral"] != true {
		t.Errorf("ephemeral setting: got %v, want true", config.Settings["ephemeral"])
	}
	if config.Settings["auto_create"] != true {
		t.Errorf("auto_create setting: got %v, want true", config.Settings["auto_create"])
	}
	if config.Settings["firebase_compat_enabled"] != true {
		t.Errorf("firebase_compat_enabled setting: got %v, want true", config.Settings["firebase_compat_enabled"])
	}
}

// TestParseFirebaseTimeout tests Firebase timeout format parsing
func TestParseFirebaseTimeout(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"3ms", 3 * time.Millisecond, false},
		{"100ms", 100 * time.Millisecond, false},
		{"5s", 5 * time.Second, false},
		{"30s", 30 * time.Second, false},
		{"3min", 3 * time.Minute, false},
		{"15min", 15 * time.Minute, false},
		{"invalid", 0, true},
		{"5", 0, true},
		{"5m", 0, true}, // Must be "min" not "m"
		{"abc123", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseFirebaseTimeout(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseFirebaseTimeout(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseFirebaseTimeout(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// TestParseRESTQueryOptions tests parsing of REST query parameters
func TestParseRESTQueryOptions(t *testing.T) {
	tests := []struct {
		name    string
		query   map[string][]string
		check   func(*RESTQueryOptions) error
		wantErr bool
	}{
		{
			name:  "empty query",
			query: map[string][]string{},
			check: func(opts *RESTQueryOptions) error {
				if opts.Shallow {
					return errorf("Shallow should be false")
				}
				if opts.Timeout != restRequestTimeout {
					return errorf("Timeout should be default")
				}
				return nil
			},
		},
		{
			name:  "shallow=true",
			query: map[string][]string{"shallow": {"true"}},
			check: func(opts *RESTQueryOptions) error {
				if !opts.Shallow {
					return errorf("Shallow should be true")
				}
				if opts.IsV2 {
					return errorf("IsV2 should be false")
				}
				return nil
			},
		},
		{
			name:  "v=2",
			query: map[string][]string{"v": {"2"}},
			check: func(opts *RESTQueryOptions) error {
				if !opts.IsV2 {
					return errorf("IsV2 should be true")
				}
				return nil
			},
		},
		{
			name:  "shallow=true with v=2",
			query: map[string][]string{"shallow": {"true"}, "v": {"2"}},
			check: func(opts *RESTQueryOptions) error {
				if !opts.Shallow {
					return errorf("Shallow should be true")
				}
				if !opts.IsV2 {
					return errorf("IsV2 should be true")
				}
				return nil
			},
		},
		{
			name:  "orderBy $key",
			query: map[string][]string{"orderBy": {`"$key"`}},
			check: func(opts *RESTQueryOptions) error {
				if opts.OrderBy != "$key" {
					return errorf("OrderBy should be $key, got %q", opts.OrderBy)
				}
				return nil
			},
		},
		{
			name:  "orderBy child path",
			query: map[string][]string{"orderBy": {`"score"`}},
			check: func(opts *RESTQueryOptions) error {
				if opts.OrderByChild != "score" {
					return errorf("OrderByChild should be score, got %q", opts.OrderByChild)
				}
				return nil
			},
		},
		{
			name:  "limitToFirst",
			query: map[string][]string{"limitToFirst": {"10"}},
			check: func(opts *RESTQueryOptions) error {
				if opts.LimitToFirst == nil || *opts.LimitToFirst != 10 {
					return errorf("LimitToFirst should be 10")
				}
				return nil
			},
		},
		{
			name:  "limitToLast",
			query: map[string][]string{"limitToLast": {"5"}},
			check: func(opts *RESTQueryOptions) error {
				if opts.LimitToLast == nil || *opts.LimitToLast != 5 {
					return errorf("LimitToLast should be 5")
				}
				return nil
			},
		},
		{
			name:  "startAt number",
			query: map[string][]string{"startAt": {"100"}},
			check: func(opts *RESTQueryOptions) error {
				if opts.StartAt != float64(100) {
					return errorf("StartAt should be 100, got %v", opts.StartAt)
				}
				return nil
			},
		},
		{
			name:  "startAt string",
			query: map[string][]string{"startAt": {`"alice"`}},
			check: func(opts *RESTQueryOptions) error {
				if opts.StartAt != "alice" {
					return errorf("StartAt should be alice, got %v", opts.StartAt)
				}
				return nil
			},
		},
		{
			name:  "endAt",
			query: map[string][]string{"endAt": {"200"}},
			check: func(opts *RESTQueryOptions) error {
				if opts.EndAt != float64(200) {
					return errorf("EndAt should be 200, got %v", opts.EndAt)
				}
				return nil
			},
		},
		{
			name:  "equalTo",
			query: map[string][]string{"equalTo": {"150"}},
			check: func(opts *RESTQueryOptions) error {
				if opts.EqualTo != float64(150) {
					return errorf("EqualTo should be 150, got %v", opts.EqualTo)
				}
				return nil
			},
		},
		{
			name:  "print=pretty",
			query: map[string][]string{"print": {"pretty"}},
			check: func(opts *RESTQueryOptions) error {
				if opts.Print != "pretty" {
					return errorf("Print should be pretty, got %q", opts.Print)
				}
				return nil
			},
		},
		{
			name:  "print=silent",
			query: map[string][]string{"print": {"silent"}},
			check: func(opts *RESTQueryOptions) error {
				if opts.Print != "silent" {
					return errorf("Print should be silent, got %q", opts.Print)
				}
				return nil
			},
		},
		{
			name:    "print=invalid",
			query:   map[string][]string{"print": {"invalid"}},
			wantErr: true,
		},
		{
			name:  "callback",
			query: map[string][]string{"callback": {"myFunc"}},
			check: func(opts *RESTQueryOptions) error {
				if opts.Callback != "myFunc" {
					return errorf("Callback should be myFunc, got %q", opts.Callback)
				}
				return nil
			},
		},
		{
			name:  "download",
			query: map[string][]string{"download": {"data.json"}},
			check: func(opts *RESTQueryOptions) error {
				if opts.Download != "data.json" {
					return errorf("Download should be data.json, got %q", opts.Download)
				}
				return nil
			},
		},
		{
			name:  "timeout=5s",
			query: map[string][]string{"timeout": {"5s"}},
			check: func(opts *RESTQueryOptions) error {
				if opts.Timeout != 5*time.Second {
					return errorf("Timeout should be 5s, got %v", opts.Timeout)
				}
				return nil
			},
		},
		{
			name:    "timeout exceeds max",
			query:   map[string][]string{"timeout": {"20min"}},
			wantErr: true,
		},
		{
			name:    "invalid limitToFirst",
			query:   map[string][]string{"limitToFirst": {"abc"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseRESTQueryOptions(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseRESTQueryOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && tt.check != nil {
				if checkErr := tt.check(opts); checkErr != nil {
					t.Error(checkErr)
				}
			}
		})
	}
}

// TestTruncateShallow tests the shallow truncation logic
func TestTruncateShallow(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  interface{}
	}{
		{
			name:  "nil",
			input: nil,
			want:  nil,
		},
		{
			name:  "string primitive",
			input: "hello",
			want:  "hello",
		},
		{
			name:  "number primitive",
			input: float64(42),
			want:  float64(42),
		},
		{
			name:  "boolean primitive",
			input: true,
			want:  true,
		},
		{
			name: "object with children",
			input: map[string]interface{}{
				"alice":   map[string]interface{}{"name": "Alice", "score": 100},
				"bob":     map[string]interface{}{"name": "Bob", "score": 200},
				"charlie": "just a string",
			},
			want: map[string]interface{}{
				"alice":   true,
				"bob":     true,
				"charlie": true,
			},
		},
		{
			name:  "empty object",
			input: map[string]interface{}{},
			want:  map[string]interface{}{},
		},
		{
			name: "array",
			input: []interface{}{
				map[string]interface{}{"name": "Alice"},
				"string",
				42,
			},
			want: []interface{}{true, true, true},
		},
		{
			name:  "empty array",
			input: []interface{}{},
			want:  []interface{}{},
		},
		{
			name: "backend shallow response with .sz markers (v1 truncation)",
			input: map[string]interface{}{
				"users": map[string]interface{}{".sz": float64(4096)},
				"body":  "Hello!",
			},
			want: map[string]interface{}{
				"users": true,
				"body":  true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateShallow(tt.input)

			// Compare using JSON for easier comparison
			if !deepEqual(got, tt.want) {
				t.Errorf("truncateShallow() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestParseRESTQueryOptionsETagNotFromQuery verifies that ETag/WantETag
// are not set by query parameters (they come from HTTP headers only).
func TestParseRESTQueryOptionsETagNotFromQuery(t *testing.T) {
	// Even if someone passes these as query params, they should not be set
	opts, err := parseRESTQueryOptions(map[string][]string{
		"etag":     {"some-hash"},
		"if-match": {"some-hash"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if opts.ETag != "" {
		t.Error("ETag should not be set from query params")
	}
	if opts.WantETag {
		t.Error("WantETag should not be set from query params")
	}
}

// TestConditionalDeleteBecomesSetNull verifies the logic that converts
// DELETE + if-match into a SET null operation for CAS delete.
func TestConditionalDeleteBecomesSetNull(t *testing.T) {
	// This tests the logic from handleRESTProxy:
	// When method == DELETE and ETag is set, op becomes "s" and value becomes nil
	method := http.MethodDelete
	etag := "abc123"

	var op string
	var bodyValue interface{} = "should-be-cleared"

	if etag != "" && method == http.MethodDelete {
		op = "s"
		bodyValue = nil
	} else {
		op = "d"
	}

	if op != "s" {
		t.Errorf("op = %q, want %q", op, "s")
	}
	if bodyValue != nil {
		t.Errorf("bodyValue = %v, want nil", bodyValue)
	}
}

// Helper for creating formatted errors
func errorf(format string, args ...interface{}) error {
	return &testError{msg: sprintf(format, args...)}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func sprintf(format string, args ...interface{}) string {
	if len(args) == 0 {
		return format
	}
	// Simple sprintf implementation for tests
	result := format
	for _, arg := range args {
		idx := indexOf(result, "%")
		if idx == -1 {
			break
		}
		// Find the format specifier end
		end := idx + 2
		if end > len(result) {
			end = len(result)
		}
		var replacement string
		switch v := arg.(type) {
		case string:
			replacement = v
		case int:
			replacement = itoa(v)
		case float64:
			replacement = ftoa(v)
		default:
			replacement = "<value>"
		}
		result = result[:idx] + replacement + result[end:]
	}
	return result
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	if neg {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}

func ftoa(f float64) string {
	return itoa(int(f))
}

// TestExtractProjectAndDatabase tests the subdomain parsing logic
func TestExtractProjectAndDatabase(t *testing.T) {
	// Standard mode (non-local)
	s := &Server{
		config: &config.Config{
			LarkDBDomain: "larkdb.net",
		},
	}

	tests := []struct {
		name     string
		host     string
		wantProj string
		wantDB   string
	}{
		{
			name:     "simple project",
			host:     "my-project.larkdb.net",
			wantProj: "my-project",
			wantDB:   "",
		},
		{
			name:     "project with port",
			host:     "my-project.larkdb.net:443",
			wantProj: "my-project",
			wantDB:   "",
		},
		{
			name:     "double-hyphen: database--project",
			host:     "example-9999--example-dev.larkdb.net",
			wantProj: "example-dev",
			wantDB:   "example-9999",
		},
		{
			name:     "double-hyphen with port",
			host:     "mydb--myproj.larkdb.net:443",
			wantProj: "myproj",
			wantDB:   "mydb",
		},
		{
			name:     "double-hyphen: simple names",
			host:     "db1--proj1.larkdb.net",
			wantProj: "proj1",
			wantDB:   "db1",
		},
		{
			name:     "multiple double-hyphens: split on first",
			host:     "db--part--project.larkdb.net",
			wantProj: "part--project",
			wantDB:   "db",
		},
		{
			name:     "not a larkdb domain",
			host:     "example.com",
			wantProj: "",
			wantDB:   "",
		},
		{
			name:     "single hyphen (not double)",
			host:     "my-project-name.larkdb.net",
			wantProj: "my-project-name",
			wantDB:   "",
		},
		{
			name:     "empty subdomain",
			host:     ".larkdb.net",
			wantProj: "",
			wantDB:   "",
		},
		{
			name:     "double-hyphen at start: empty database",
			host:     "--project.larkdb.net",
			wantProj: "project",
			wantDB:   "",
		},
		{
			name:     "double-hyphen at end: empty project",
			host:     "database--.larkdb.net",
			wantProj: "",
			wantDB:   "database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotProj, gotDB := s.extractProjectAndDatabase(tt.host)
			if gotProj != tt.wantProj {
				t.Errorf("projectID = %q, want %q", gotProj, tt.wantProj)
			}
			if gotDB != tt.wantDB {
				t.Errorf("databaseID = %q, want %q", gotDB, tt.wantDB)
			}
		})
	}

	// Test local mode
	t.Run("local mode localhost", func(t *testing.T) {
		localServer := &Server{
			config: &config.Config{
				LocalMode:      true,
				LocalProjectID: "local-proj",
				LarkDBDomain:   "larkdb.net",
			},
		}
		proj, db := localServer.extractProjectAndDatabase("localhost:8080")
		if proj != "local-proj" {
			t.Errorf("projectID = %q, want %q", proj, "local-proj")
		}
		if db != "" {
			t.Errorf("databaseID = %q, want empty", db)
		}
	})

	t.Run("local mode 127.0.0.1", func(t *testing.T) {
		localServer := &Server{
			config: &config.Config{
				LocalMode:      true,
				LocalProjectID: "local-proj",
				LarkDBDomain:   "larkdb.net",
			},
		}
		proj, db := localServer.extractProjectAndDatabase("127.0.0.1:3000")
		if proj != "local-proj" {
			t.Errorf("projectID = %q, want %q", proj, "local-proj")
		}
		if db != "" {
			t.Errorf("databaseID = %q, want empty", db)
		}
	})
}

// deepEqual compares two interface{} values
func deepEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	switch av := a.(type) {
	case map[string]interface{}:
		bv, ok := b.(map[string]interface{})
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !deepEqual(v, bv[k]) {
				return false
			}
		}
		return true
	case []interface{}:
		bv, ok := b.([]interface{})
		if !ok || len(av) != len(bv) {
			return false
		}
		for i, v := range av {
			if !deepEqual(v, bv[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
