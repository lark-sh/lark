// LarkDB HTTP Handler Implementation
//
// This file implements the HTTP handler for client-facing endpoints on *.larkdb.net.
// It routes requests to appropriate transports based on path and headers.
//
// # URL Structure
//
// Firebase-compatible URLs follow this pattern:
//
//	https://{project}.larkdb.net/.ws              → WebSocket (Lark protocol)
//	https://{project}.larkdb.net/.wss             → WebSocket (Firebase protocol)
//	https://{project}.larkdb.net/.lp              → Long Polling
//	https://{project}.larkdb.net/{db}/{path}.json → REST API
//	https://{project}.larkdb.net/{db}/{path}.json → SSE (if Accept: text/event-stream)
//
// # Request Routing
//
// handleLarkDB() is the main router:
//  1. Extract project ID from subdomain
//  2. Route by path prefix:
//     - /.ws → handleWebSocket()
//     - /.wss → handleFirebaseWebSocket()
//     - /.lp → handleLongPoll()
//     - /{db}/*.json → handleRESTProxy() or handleSSE()
//
// # REST API
//
// The REST handler (handleRESTProxy) supports Firebase REST API compatibility:
//   - GET: Read data (with query filters)
//   - PUT: Write/replace data
//   - POST: Push with generated key
//   - PATCH: Update/merge data
//   - DELETE: Remove data
//
// Query parameters for filtering and pagination are parsed from the URL
// and translated to Lark query operations.
//
// # SSE Streaming
//
// SSE is triggered by Accept: text/event-stream header on GET requests.
// The handler creates an SSETransport and streams events until disconnect.
//
// # Authentication
//
// REST/SSE requests can include auth tokens via:
//   - Authorization: Bearer <token>
//   - ?auth=<token> query parameter
//
// The token is validated and claims forwarded to the backend.
package proxy

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bytedance/sonic"

	"github.com/lark-sh/lark/edge/auth"
	"github.com/lark-sh/lark/edge/backend"
	"github.com/lark-sh/lark/edge/db"
	"github.com/lark-sh/lark/edge/logger"
)

// REST request timeout (default, can be overridden by ?timeout query param)
const restRequestTimeout = 30 * time.Second

// RESTQueryOptions holds parsed query parameters for REST requests
// These map to Firebase REST API query parameters
type RESTQueryOptions struct {
	// Query/filter parameters (passed to backend)
	Shallow      bool        // shallow=true - return keys only
	OrderBy      string      // orderBy="$key", "$value", "$priority"
	OrderByChild string      // orderBy="childKey" - order by child value
	LimitToFirst *int        // limitToFirst=N
	LimitToLast  *int        // limitToLast=N
	StartAt      interface{} // startAt=value
	StartAtKey   string      // startAt has optional second param for key
	EndAt        interface{} // endAt=value
	EndAtKey     string      // endAt has optional second param for key
	EqualTo      interface{} // equalTo=value
	EqualToKey   string      // equalTo has optional second param for key

	// Response formatting (handled by proxy)
	Print    string // print=pretty|silent
	Callback string // callback=funcName (JSONP)
	Download string // download=filename
	Timeout  time.Duration
	IsV2     bool // v=2 - modern Lark REST client (affects shallow response format)

	// Conditional request fields (from HTTP headers, not query params)
	ETag     string // if-match header value — conditional write (CAS)
	WantETag bool   // X-Firebase-ETag: true — compute and return ETag header on response
}

// parseRESTQueryOptions extracts REST query options from URL query parameters
func parseRESTQueryOptions(query map[string][]string) (*RESTQueryOptions, error) {
	opts := &RESTQueryOptions{
		Timeout: restRequestTimeout,
	}

	// v=2 (modern Lark REST client)
	if v := getQueryParam(query, "v"); v == "2" {
		opts.IsV2 = true
	}

	// shallow
	if v := getQueryParam(query, "shallow"); v == "true" {
		opts.Shallow = true
	}

	// orderBy - can be "$key", "$value", "$priority", or a child path
	if v := getQueryParam(query, "orderBy"); v != "" {
		// Firebase sends orderBy as JSON string, e.g., orderBy="$key" or orderBy="score"
		// Need to unquote if it's a JSON string
		unquoted, err := strconv.Unquote(v)
		if err == nil {
			v = unquoted
		}
		if v == "$key" || v == "$value" || v == "$priority" {
			opts.OrderBy = v
		} else {
			opts.OrderByChild = v
		}
	}

	// limitToFirst
	if v := getQueryParam(query, "limitToFirst"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid limitToFirst: %s", v)
		}
		opts.LimitToFirst = &n
	}

	// limitToLast
	if v := getQueryParam(query, "limitToLast"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid limitToLast: %s", v)
		}
		opts.LimitToLast = &n
	}

	// startAt - JSON value
	if v := getQueryParam(query, "startAt"); v != "" {
		var val interface{}
		if err := sonic.Unmarshal([]byte(v), &val); err != nil {
			// Try as bare string
			val = v
		}
		opts.StartAt = val
	}

	// endAt - JSON value
	if v := getQueryParam(query, "endAt"); v != "" {
		var val interface{}
		if err := sonic.Unmarshal([]byte(v), &val); err != nil {
			val = v
		}
		opts.EndAt = val
	}

	// equalTo - JSON value
	if v := getQueryParam(query, "equalTo"); v != "" {
		var val interface{}
		if err := sonic.Unmarshal([]byte(v), &val); err != nil {
			val = v
		}
		opts.EqualTo = val
	}

	// print
	if v := getQueryParam(query, "print"); v != "" {
		if v != "pretty" && v != "silent" {
			return nil, fmt.Errorf("invalid print value: %s (must be 'pretty' or 'silent')", v)
		}
		opts.Print = v
	}

	// callback (JSONP)
	opts.Callback = getQueryParam(query, "callback")

	// download
	opts.Download = getQueryParam(query, "download")

	// timeout - format: 3ms, 3s, 3min
	if v := getQueryParam(query, "timeout"); v != "" {
		d, err := parseFirebaseTimeout(v)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout: %s", v)
		}
		// Cap at 15 minutes (Firebase maximum)
		if d > 15*time.Minute {
			return nil, fmt.Errorf("timeout exceeds maximum of 15min")
		}
		if d <= 0 {
			return nil, fmt.Errorf("timeout must be positive")
		}
		opts.Timeout = d
	}

	return opts, nil
}

// getQueryParam gets a single query parameter value
func getQueryParam(query map[string][]string, key string) string {
	if vals, ok := query[key]; ok && len(vals) > 0 {
		return vals[0]
	}
	return ""
}

// parseFirebaseTimeout parses Firebase timeout format (3ms, 3s, 3min)
func parseFirebaseTimeout(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "min") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "min"))
		if err != nil {
			return 0, err
		}
		return time.Duration(n) * time.Minute, nil
	}
	if strings.HasSuffix(s, "ms") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "ms"))
		if err != nil {
			return 0, err
		}
		return time.Duration(n) * time.Millisecond, nil
	}
	if strings.HasSuffix(s, "s") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "s"))
		if err != nil {
			return 0, err
		}
		return time.Duration(n) * time.Second, nil
	}
	return 0, fmt.Errorf("invalid timeout format (use ms, s, or min)")
}

// handleLarkDB routes requests to *.larkdb.net
// - /ws → Native Lark WebSocket
// - /.ws → Firebase WebSocket
// - /.lp → Firebase Long Polling
// - /*.json → REST API proxy
func (s *Server) handleLarkDB(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	switch {
	case path == "/ws":
		s.handleWebSocket(w, r)
	case path == "/.ws":
		s.handleFirebaseWebSocket(w, r)
	case path == "/.lp":
		s.handleLongPoll(w, r)
	case strings.HasSuffix(path, ".json"):
		s.handleRESTProxy(w, r)
	default:
		logger.Debug("LarkDB 404 for path", "path", path)
		http.NotFound(w, r)
	}
}

// handleRESTProxy handles REST API requests using virtual Lark clients.
// REST is treated as just another transport type, like WebSocket or WebTransport.
// The proxy translates HTTP methods to Lark operations:
//   - GET    → get (read once)
//   - PUT    → set (replace)
//   - POST   → push (create with generated key)
//   - PATCH  → update (merge)
//   - DELETE → remove
//
// URL patterns:
//   - project.larkdb.net/database/path.json (standard - first segment is database)
//   - project.larkdb.net/path.json (Firebase legacy without path-segment → "default" database)
//
// SSE Streaming:
//   - Accept: text/event-stream header triggers SSE mode for GET requests
//   - Subscribes to the path and streams put/patch events
func (s *Server) handleRESTProxy(w http.ResponseWriter, r *http.Request) {
	// Check for SSE streaming request (Accept: text/event-stream)
	if r.Method == http.MethodGet && strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		s.handleSSE(w, r)
		return
	}

	// Count REST request for metrics
	s.restRequests.Add(1)

	// Extract project ID (and optional database ID) from subdomain
	projectID, subdomainDB := s.extractProjectAndDatabase(r.Host)
	if projectID == "" {
		s.jsonError(w, http.StatusBadRequest, "missing_project", "Missing project ID in subdomain")
		return
	}

	// Look up project (needed for Firebase compat check)
	project, err := s.GetProjectCached(r.Context(), projectID)
	if err != nil {
		logger.Warn("REST project lookup error", "project_id", projectID, "error", err)
		s.jsonError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Project '%s' not found", projectID))
		return
	}

	// Parse path to extract database and data path
	pathname := r.URL.Path
	pathWithoutJSON := strings.TrimSuffix(pathname, ".json")
	segments := strings.Split(strings.TrimPrefix(pathWithoutJSON, "/"), "/")

	// Filter empty segments
	var filteredSegments []string
	for _, seg := range segments {
		if seg != "" {
			filteredSegments = append(filteredSegments, seg)
		}
	}

	var databaseID string
	var dataPath string

	if subdomainDB != "" {
		// Database encoded in subdomain (e.g., example-9999--example-dev.larkdb.net)
		// Entire path is data path — no first-segment extraction
		databaseID = subdomainDB
		dataPath = "/" + strings.Join(filteredSegments, "/")
	} else {
		// Firebase legacy projects with use_first_path_segment_as_database=false use "default" database
		// All other projects use first segment as database
		// ?v=2 forces modern Lark style (first segment = database) regardless of project settings
		useDefaultDatabase := project.FirebaseCompatEnabled && !project.UseFirstPathSegmentAsDatabase
		if r.URL.Query().Get("v") == "2" {
			useDefaultDatabase = false
		}

		if useDefaultDatabase {
			// Firebase legacy mode: entire path is data path, use "default" database
			databaseID = "default"
			dataPath = "/" + strings.Join(filteredSegments, "/")
		} else {
			// Standard mode: first segment is database ID
			if len(filteredSegments) == 0 {
				s.jsonError(w, http.StatusBadRequest, "bad_request", "Database ID required in path")
				return
			}
			databaseID = filteredSegments[0]
			dataPath = "/" + strings.Join(filteredSegments[1:], "/")
		}
	}

	// Normalize paths
	if dataPath == "/" {
		dataPath = ""
	}

	// Check for X-HTTP-Method-Override header (for clients that don't support all HTTP methods)
	// Firebase supports this to allow POST requests to act as PUT, PATCH, or DELETE
	// Only allow override on non-GET requests to prevent GET from modifying data
	method := r.Method
	if r.Method != http.MethodGet {
		if override := r.Header.Get("X-HTTP-Method-Override"); override != "" {
			method = override
		}
	}

	// Extract auth token from query string
	queryValues := r.URL.Query()
	authToken := queryValues.Get("auth")
	if authToken == "" {
		authToken = queryValues.Get("access_token")
	}
	// Remove auth tokens from query (we handle them separately)
	delete(queryValues, "auth")
	delete(queryValues, "access_token")

	// Parse REST query options (shallow, orderBy, limitTo*, startAt, endAt, equalTo, print, callback, download, timeout)
	queryOpts, err := parseRESTQueryOptions(queryValues)
	if err != nil {
		s.jsonError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	// Validate: shallow cannot be mixed with other query parameters
	if queryOpts.Shallow && (queryOpts.OrderBy != "" || queryOpts.OrderByChild != "" ||
		queryOpts.LimitToFirst != nil || queryOpts.LimitToLast != nil ||
		queryOpts.StartAt != nil || queryOpts.EndAt != nil || queryOpts.EqualTo != nil) {
		s.jsonError(w, http.StatusBadRequest, "bad_request", "shallow cannot be mixed with other query parameters")
		return
	}

	// Read request body for POST/PUT/PATCH
	// Limit to 256MB to match Firebase REST API limits
	var body []byte
	if r.Body != nil && (method == "POST" || method == "PUT" || method == "PATCH") {
		r.Body = http.MaxBytesReader(w, r.Body, 256*1024*1024)
		body, err = io.ReadAll(r.Body)
		if err != nil {
			logger.Warn("REST failed to read request body", "error", err)
			s.jsonError(w, http.StatusBadRequest, "bad_request", "Request body too large (max 256MB)")
			return
		}
	}

	// Parse body as JSON value (for operations that need it)
	var bodyValue interface{}
	if len(body) > 0 {
		if err := sonic.Unmarshal(body, &bodyValue); err != nil {
			logger.Debug("REST failed to parse JSON body", "error", err)
			s.jsonError(w, http.StatusBadRequest, "bad_request", "Invalid JSON in request body")
			return
		}
	}

	// Validate auth token if provided
	var authInfo *auth.Info
	if authToken != "" {
		var authErr error
		authInfo, authErr = s.authValidator.ValidateForProject(
			authToken,
			project.SecretKey,
			project.AdminSecretKey,
			project.FirebaseProjectID,
		)
		if authErr != nil {
			logger.Debug("REST auth validation failed", "error", authErr)
			s.jsonError(w, http.StatusUnauthorized, "unauthenticated", auth.UserFriendlyError(authErr))
			return
		}
	}

	// Parse conditional request headers (ETag support)
	// X-Firebase-ETag: true → include ETag in response
	if r.Header.Get("X-Firebase-ETag") == "true" {
		queryOpts.WantETag = true
	}
	// if-match → conditional write (CAS)
	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" && method != http.MethodGet {
		queryOpts.ETag = ifMatch
	}

	uid := "anonymous"
	if authInfo != nil {
		uid = authInfo.UID
	}
	logger.Debug("REST request", "method", method, "project_id", projectID, "database_id", databaseID, "path", dataPath, "uid", uid)

	// Get or create virtual client for this database+auth combination
	// Different tokens get different pooled clients (keyed by token hash)
	vc, err := s.restPool.GetOrCreate(projectID, databaseID, authToken, authInfo)
	if err != nil {
		logger.Error("REST failed to create virtual client", "error", err)
		s.jsonError(w, http.StatusServiceUnavailable, "unavailable", "Failed to create connection")
		return
	}

	// Wait for client to be ready (joined + database loaded)
	if err := vc.WaitReady(restRequestTimeout); err != nil {
		logger.Warn("REST virtual client not ready", "error", err)
		s.jsonError(w, http.StatusServiceUnavailable, "unavailable", err.Error())
		return
	}

	// Map HTTP method to Lark operation
	// See LARK_WIRE_PROTOCOL.md for operation codes
	var op string
	var generatedPushID string // For POST requests, we generate the push ID
	switch method {
	case http.MethodGet:
		op = "o" // once (read)
	case http.MethodPut:
		op = "s" // set
	case http.MethodPost:
		// POST = push: generate a push ID and use SET at that child path
		// Firebase push() is just "generate ID + set at that path"
		generatedPushID = GeneratePushID()
		if dataPath == "" || dataPath == "/" {
			dataPath = "/" + generatedPushID
		} else {
			dataPath = dataPath + "/" + generatedPushID
		}
		op = "s" // set (not push - Lark doesn't have a push operation)
	case http.MethodPatch:
		op = "u" // update
	case http.MethodDelete:
		if queryOpts.ETag != "" {
			// Conditional delete: use SET null with hash (CAS)
			op = "s"
			bodyValue = nil
		} else {
			op = "d" // delete
		}
	default:
		s.jsonError(w, http.StatusMethodNotAllowed, "method_not_allowed", fmt.Sprintf("Method %s not supported", method))
		return
	}

	// Send the operation and wait for response
	resp, err := vc.SendOperation(op, dataPath, bodyValue, queryOpts, queryOpts.Timeout)
	if err != nil {
		logger.Warn("REST operation failed", "error", err)
		if err == ErrRESTTimeout {
			s.jsonError(w, http.StatusGatewayTimeout, "timeout", "Request timed out")
		} else {
			s.jsonError(w, http.StatusBadGateway, "upstream_error", "Failed to complete operation")
		}
		return
	}

	// Parse the Lark response
	var larkResp map[string]interface{}
	if err := sonic.Unmarshal(resp, &larkResp); err != nil {
		logger.Error("REST failed to parse response", "error", err)
		s.jsonError(w, http.StatusBadGateway, "upstream_error", "Invalid response from backend")
		return
	}

	// Handle condition_failed (412) with follow-up GET for current value + new ETag
	if queryOpts.ETag != "" {
		if errorCode, _ := larkResp["e"].(string); errorCode == "condition_failed" {
			s.handleConditionFailed(w, vc, dataPath, queryOpts)
			return
		}
	}

	// Translate Lark response to HTTP
	// Pass queryOpts for print/callback/download handling
	s.writeLarkResponseAsHTTP(w, method, dataPath, larkResp, bodyValue, queryOpts, generatedPushID)
}

// writeLarkResponseAsHTTP translates a Lark protocol response to an HTTP response
// Lark response formats:
//   - Once (GET): {"oc": "request-id", "ov": <value>}
//   - Ack (write success): {"a": "request-id"}
//   - Nack (error): {"n": "request-id", "e": "error_code", "m": "message"}
//
// requestBody is the original request body, used to echo back for PUT/PATCH (Firebase behavior)
// opts contains query options for print/callback/download handling
// generatedPushID is the push ID generated for POST requests (empty for other methods)
func (s *Server) writeLarkResponseAsHTTP(w http.ResponseWriter, method string, path string, resp map[string]interface{}, requestBody interface{}, opts *RESTQueryOptions, generatedPushID string) {
	// Check for NACK (error response) - errors are never silent
	if _, hasNack := resp["n"]; hasNack {
		errorCode, _ := resp["e"].(string)
		errorMsg, _ := resp["m"].(string)
		if errorCode == "" {
			errorCode = "error"
		}
		if errorMsg == "" {
			errorMsg = errorCode
		}

		httpStatus := mapLarkErrorToHTTP(errorCode)
		s.writeRESTResponse(w, httpStatus, map[string]string{"error": errorMsg}, opts)
		return
	}

	// Check for Once response (GET)
	if _, hasOC := resp["oc"]; hasOC {
		value := resp["ov"]
		// Apply shallow truncation if requested
		// v2 clients: backend returns primitives as-is and containers as {".sz": N} — pass through
		// v1 clients: truncate all children to true (Firebase behavior)
		if opts != nil && opts.Shallow && !opts.IsV2 {
			value = truncateShallow(value)
		}
		// Compute and set ETag header if requested
		if opts != nil && opts.WantETag {
			if etag, err := computeJCSHash(value); err == nil {
				w.Header().Set("ETag", etag)
			}
		}
		s.writeRESTResponse(w, http.StatusOK, value, opts)
		return
	}

	// Check for ACK (write success)
	// Firebase REST API behavior:
	//   - PUT/PATCH: echo back the written data
	//   - POST: return {"name": "<generated-key>"}
	//   - DELETE: return null
	if _, hasAck := resp["a"]; hasAck {
		// print=silent returns 204 No Content for writes
		if opts != nil && opts.Print == "silent" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		var responseBody interface{}
		switch method {
		case http.MethodPost:
			// POST returns the generated push ID (generated by proxy, not backend)
			responseBody = map[string]string{"name": generatedPushID}
		case http.MethodDelete:
			responseBody = nil // null
		case http.MethodPut, http.MethodPatch:
			// PUT/PATCH echo back the written data (Firebase behavior)
			responseBody = requestBody
		default:
			responseBody = map[string]bool{"ok": true}
		}

		s.writeRESTResponse(w, http.StatusOK, responseBody, opts)
		return
	}

	// Fallback: return the raw response (shouldn't normally happen)
	logger.Warn("REST unexpected response format")
	s.writeRESTResponse(w, http.StatusOK, resp, opts)
}

// writeRESTResponse writes a REST response with optional formatting (pretty, callback, download)
func (s *Server) writeRESTResponse(w http.ResponseWriter, status int, data interface{}, opts *RESTQueryOptions) {
	// Set download header if requested
	if opts != nil && opts.Download != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", opts.Download))
	}

	// Handle JSONP callback
	if opts != nil && opts.Callback != "" {
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(status)

		var jsonData []byte
		var err error
		if opts.Print == "pretty" {
			jsonData, err = sonic.MarshalIndent(data, "", "  ")
		} else {
			jsonData, err = sonic.Marshal(data)
		}
		if err != nil {
			w.Write([]byte(fmt.Sprintf("%s(null)", opts.Callback)))
			return
		}
		w.Write([]byte(fmt.Sprintf("%s(%s)", opts.Callback, string(jsonData))))
		return
	}

	// Standard JSON response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if data == nil {
		w.Write([]byte("null"))
		return
	}

	// Pretty print if requested
	// Firebase behavior:
	// - non-pretty: no trailing newline
	// - pretty: has trailing newline
	var jsonData []byte
	var err error
	if opts != nil && opts.Print == "pretty" {
		jsonData, err = sonic.MarshalIndent(data, "", "  ")
		if err == nil {
			jsonData = append(jsonData, '\n')
		}
	} else {
		jsonData, err = sonic.Marshal(data)
	}
	if err != nil {
		w.Write([]byte("null"))
		return
	}
	w.Write(jsonData)
}

// mapLarkErrorToHTTP maps Lark error codes to HTTP status codes
func mapLarkErrorToHTTP(status string) int {
	switch status {
	case "permission_denied":
		return http.StatusForbidden
	case "unauthenticated":
		return http.StatusUnauthorized
	case "invalid_data", "invalid_token":
		return http.StatusBadRequest
	case "not_found":
		return http.StatusNotFound
	case "unavailable":
		return http.StatusServiceUnavailable
	case "condition_failed":
		return http.StatusPreconditionFailed // 412
	default:
		return http.StatusInternalServerError
	}
}

// handleConditionFailed handles a 412 Precondition Failed response for conditional writes.
// Firebase behavior: return current value + new ETag so the client can retry.
func (s *Server) handleConditionFailed(w http.ResponseWriter, vc *RESTVirtualClient, path string, opts *RESTQueryOptions) {
	// Follow-up GET to fetch current value
	resp, err := vc.SendOperation("o", path, nil, nil, opts.Timeout)
	if err != nil {
		logger.Warn("REST condition_failed follow-up GET failed", "error", err)
		s.writeRESTResponse(w, http.StatusPreconditionFailed, map[string]string{"error": "condition_failed"}, opts)
		return
	}

	// Parse the once response
	var onceResp map[string]interface{}
	if err := sonic.Unmarshal(resp, &onceResp); err != nil {
		logger.Warn("REST condition_failed follow-up parse failed", "error", err)
		s.writeRESTResponse(w, http.StatusPreconditionFailed, map[string]string{"error": "condition_failed"}, opts)
		return
	}

	// Extract the current value
	currentValue := onceResp["ov"]

	// Compute new ETag for the current value
	if etag, err := computeJCSHash(currentValue); err == nil {
		w.Header().Set("ETag", etag)
	}

	s.writeRESTResponse(w, http.StatusPreconditionFailed, currentValue, opts)
}

// truncateShallow implements Firebase's shallow=true behavior:
// - If the value is a primitive (string, number, boolean, null), return as-is
// - If the value is an object, replace each child value with true
// - If the value is an array, replace each element with true
func truncateShallow(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case map[string]interface{}:
		// Object: replace each child with true
		result := make(map[string]interface{}, len(v))
		for key := range v {
			result[key] = true
		}
		return result
	case []interface{}:
		// Array: replace each element with true
		result := make([]interface{}, len(v))
		for i := range v {
			result[i] = true
		}
		return result
	default:
		// Primitive: return as-is
		return value
	}
}

// buildProjectConfig creates a backend.ProjectConfig from a db.Project
func buildProjectConfig(project *db.Project) *backend.ProjectConfig {
	return &backend.ProjectConfig{
		Rules:             project.RulesJSON,
		SecretKey:         project.SecretKey,
		AdminSecretKey:    project.AdminSecretKey,
		FirebaseProjectID: project.FirebaseProjectID,
		Ephemeral:         project.Ephemeral,
		ConfigVersion:     project.ConfigVersion,
		Settings: map[string]any{
			"ephemeral":                          project.Ephemeral,
			"auto_create":                        project.AutoCreate,
			"firebase_compat_enabled":            project.FirebaseCompatEnabled,
			"use_first_path_segment_as_database": project.UseFirstPathSegmentAsDatabase,
		},
	}
}

// extractProjectAndDatabase extracts the project ID and optional database ID from the subdomain.
// Supports two formats:
//   - "my-project.larkdb.net" → projectID="my-project", databaseID=""
//   - "my-db--my-project.larkdb.net" → projectID="my-project", databaseID="my-db"
//
// The "--" separator is split on first occurrence, so database names cannot contain "--".
func (s *Server) extractProjectAndDatabase(host string) (projectID, databaseID string) {
	// Remove port if present
	if idx := strings.LastIndex(host, ":"); idx != -1 {
		host = host[:idx]
	}

	// In local mode, localhost uses the configured local project ID
	if s.config.LocalMode && (host == "localhost" || host == "127.0.0.1") {
		return s.config.LocalProjectID, ""
	}

	// Check if it's a larkdb domain
	larkdbSuffix := "." + s.config.LarkDBDomain
	if !strings.HasSuffix(host, larkdbSuffix) {
		return "", ""
	}

	// Extract subdomain
	subdomain := strings.TrimSuffix(host, larkdbSuffix)

	// Check for double-hyphen separator: database--project
	if idx := strings.Index(subdomain, "--"); idx != -1 {
		return subdomain[idx+2:], subdomain[:idx]
	}

	return subdomain, ""
}

// isLarkDBDomain checks if the host is a *.larkdb.net domain
func (s *Server) isLarkDBDomain(host string) bool {
	// Remove port if present
	if idx := strings.LastIndex(host, ":"); idx != -1 {
		host = host[:idx]
	}

	// In local mode, localhost is treated as a LarkDB domain
	if s.config.LocalMode && (host == "localhost" || host == "127.0.0.1") {
		return true
	}

	larkdbSuffix := "." + s.config.LarkDBDomain
	return strings.HasSuffix(host, larkdbSuffix)
}

// jsonError writes a JSON error response
func (s *Server) jsonError(w http.ResponseWriter, status int, code string, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":"%s","message":"%s"}`, code, message)
}

// handleSSE handles Server-Sent Events streaming requests.
// This is used by Firebase clients for real-time data streaming via REST API.
// URL format: GET /database/path.json with Accept: text/event-stream
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	// Check for flusher support
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.jsonError(w, http.StatusInternalServerError, "streaming_not_supported", "Streaming not supported")
		return
	}

	// Extract project ID (and optional database ID) from subdomain
	projectID, subdomainDB := s.extractProjectAndDatabase(r.Host)
	if projectID == "" {
		s.jsonError(w, http.StatusBadRequest, "missing_project", "Missing project ID in subdomain")
		return
	}

	// Look up project (needed for Firebase compat check)
	project, err := s.GetProjectCached(r.Context(), projectID)
	if err != nil {
		logger.Warn("SSE project lookup error", "project_id", projectID, "error", err)
		s.jsonError(w, http.StatusNotFound, "not_found", fmt.Sprintf("Project '%s' not found", projectID))
		return
	}

	// Parse path to extract database and data path
	pathname := r.URL.Path
	pathWithoutJSON := strings.TrimSuffix(pathname, ".json")
	segments := strings.Split(strings.TrimPrefix(pathWithoutJSON, "/"), "/")

	// Filter empty segments
	var filteredSegments []string
	for _, seg := range segments {
		if seg != "" {
			filteredSegments = append(filteredSegments, seg)
		}
	}

	var databaseID string
	var dataPath string

	if subdomainDB != "" {
		// Database encoded in subdomain — entire path is data path
		databaseID = subdomainDB
		dataPath = "/" + strings.Join(filteredSegments, "/")
	} else {
		// Firebase legacy projects with use_first_path_segment_as_database=false use "default" database
		// ?v=2 forces modern Lark style (first segment = database) regardless of project settings
		useDefaultDatabase := project.FirebaseCompatEnabled && !project.UseFirstPathSegmentAsDatabase
		if r.URL.Query().Get("v") == "2" {
			useDefaultDatabase = false
		}

		if useDefaultDatabase {
			databaseID = "default"
			dataPath = "/" + strings.Join(filteredSegments, "/")
		} else {
			if len(filteredSegments) == 0 {
				s.jsonError(w, http.StatusBadRequest, "bad_request", "Database ID required in path")
				return
			}
			databaseID = filteredSegments[0]
			dataPath = "/" + strings.Join(filteredSegments[1:], "/")
		}
	}

	// Note: For SSE, we keep "/" as the root path (unlike REST which uses "")
	// This is because Lark subscriptions expect "/" for root subscriptions

	// Extract auth token from query string
	queryValues := r.URL.Query()
	authToken := queryValues.Get("auth")
	if authToken == "" {
		authToken = queryValues.Get("access_token")
	}

	// Validate auth token if provided
	var authInfo *auth.Info
	if authToken != "" {
		var authErr error
		authInfo, authErr = s.authValidator.ValidateForProject(
			authToken,
			project.SecretKey,
			project.AdminSecretKey,
			project.FirebaseProjectID,
		)
		if authErr != nil {
			logger.Debug("SSE auth validation failed", "error", authErr)
			s.jsonError(w, http.StatusUnauthorized, "unauthenticated", auth.UserFriendlyError(authErr))
			return
		}
	}

	sseUid := "anonymous"
	if authInfo != nil {
		sseUid = authInfo.UID
	}
	logger.Debug("SSE starting stream", "project_id", projectID, "database_id", databaseID, "path", dataPath, "uid", sseUid)

	// Create SSE virtual client
	vc, err := s.NewSSEVirtualClient(projectID, databaseID, authInfo)
	if err != nil {
		logger.Error("SSE failed to create virtual client", "error", err)
		s.jsonError(w, http.StatusServiceUnavailable, "unavailable", "Failed to create connection")
		return
	}
	defer vc.Close()

	// Wait for client to be ready
	if err := vc.WaitReady(restRequestTimeout); err != nil {
		logger.Warn("SSE virtual client not ready", "error", err)
		s.jsonError(w, http.StatusServiceUnavailable, "unavailable", err.Error())
		return
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Subscribe to the path
	if err := vc.Subscribe(dataPath); err != nil {
		logger.Warn("SSE failed to subscribe", "error", err)
		fmt.Fprintf(w, "event: cancel\ndata: {\"error\": %q}\n\n", err.Error())
		flusher.Flush()
		return
	}

	// Note: Firebase doesn't send an initial comment, so we don't either
	// Just flush to ensure headers are sent
	flusher.Flush()

	// Stream events until disconnect
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			// Client disconnected
			logger.Debug("SSE client disconnected", "project_id", projectID, "database_id", databaseID, "path", dataPath)
			return

		case <-vc.Done():
			// Backend connection closed
			logger.Debug("SSE backend connection closed", "project_id", projectID, "database_id", databaseID, "path", dataPath)
			return

		case eventData, ok := <-vc.Events():
			if !ok {
				return
			}

			// Parse the Lark event
			event := ParseSSEEvent(eventData)
			if event == nil {
				// Not a streamable event (ack, nack, join confirm, etc.)
				continue
			}

			// Format as SSE
			// Firebase SSE format: event: <type>\ndata: {"path": "<path>", "data": <value>}\n\n
			// Use struct to guarantee field order (path first, then data)
			type ssePayload struct {
				Path string      `json:"path"`
				Data interface{} `json:"data"`
			}
			sseData := ssePayload{
				Path: event.Path,
				Data: event.Data,
			}

			jsonData, err := sonic.Marshal(sseData)
			if err != nil {
				logger.Warn("SSE failed to marshal event", "error", err)
				continue
			}

			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, string(jsonData))
			flusher.Flush()
		}
	}
}
