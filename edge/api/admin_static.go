package api

import (
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"

	"github.com/lark-sh/lark/edge/dashboard"
)

// mountAdminSPA wires the dashboard SPA onto /admin/*. Hashed bundles (the
// Vite output under assets/) are served directly; any path that doesn't
// resolve to a file is rewritten to index.html so the SPA's client-side
// router can handle it.
//
// We serve files manually (rather than via http.FileServer) because
// FileServer's built-in canonicalization redirects "/index.html" back to
// "/" and "/dir" to "/dir/", neither of which plays well with the SPA
// fallback strategy below.
//
// The JSON API lives at /admin/api/* and is registered separately by
// [Server.registerAdminRoutes]. Go's mux gives more-specific patterns
// precedence, so "POST /admin/api/login" wins over the "/admin/" catch-all.
func (s *Server) mountAdminSPA() {
	dist := dashboard.FS()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// r.URL.Path is post-StripPrefix here (relative to /admin/).
		clean := strings.TrimPrefix(r.URL.Path, "/")
		if clean == "" {
			clean = "index.html"
		}
		if !fileExists(dist, clean) {
			// SPA route (e.g. /projects/abc) — let React Router resolve
			// it on the client.
			clean = "index.html"
		}
		serveEmbeddedFile(w, dist, clean)
	})

	s.mux.Handle("/admin/", http.StripPrefix("/admin", handler))
	// Bare /admin → /admin/ so relative asset URLs in index.html resolve.
	s.mux.Handle("GET /admin", http.RedirectHandler("/admin/", http.StatusMovedPermanently))
}

func fileExists(fsys fs.FS, name string) bool {
	info, err := fs.Stat(fsys, name)
	return err == nil && !info.IsDir()
}

func serveEmbeddedFile(w http.ResponseWriter, fsys fs.FS, name string) {
	data, err := fs.ReadFile(fsys, name)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if ct := mime.TypeByExtension(path.Ext(name)); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	// Hashed assets (assets/index-abc123.js) are immutable, so cache them
	// hard. The unhashed index.html must not be cached so SPA updates
	// take effect on the next refresh.
	if name == "index.html" {
		w.Header().Set("Cache-Control", "no-cache")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
	_, _ = w.Write(data)
}
