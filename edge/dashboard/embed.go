// Package dashboard exposes the lark-edge admin SPA as an [io/fs.FS] so the
// api package can mount it under /admin/* without taking a build-time
// dependency on the JS toolchain.
//
// The SPA source lives in dashboard/src/. After `npm run build`, Vite
// writes the production bundle to dashboard/dist/. The embed below pulls
// that directory into the Go binary at compile time.
//
// When the project has been cloned but the dashboard hasn't been built,
// dist/ contains only the placeholder index.html. The placeholder explains
// how to produce the real build.
package dashboard

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

// FS returns the dashboard's built static assets rooted at the dist/
// directory. Mount with http.FileServer(http.FS(FS())).
func FS() fs.FS {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		// Sub only errors on an invalid path; "dist" is a compile-time
		// constant we know exists, so this is unreachable.
		panic(err)
	}
	return sub
}
