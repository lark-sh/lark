package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/lark-sh/lark/edge/db"
)

// requireSession wraps an HTTP handler with session validation. If the
// cookie is missing, malformed, expired, or points at a deleted account,
// the wrapped handler is short-circuited with 401. Otherwise the account
// is attached to the request context (see [AccountFromContext]).
func (s *Server) requireSession(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(sessionCookieName)
		if err != nil || cookie.Value == "" {
			s.writeError(w, http.StatusUnauthorized, "not authenticated")
			return
		}

		session, err := s.db.GetSessionByID(r.Context(), cookie.Value)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				s.clearSessionCookie(w)
				s.writeError(w, http.StatusUnauthorized, "session not found")
				return
			}
			s.writeError(w, http.StatusInternalServerError, "session lookup failed")
			return
		}

		if session.ExpiresAt > 0 && session.ExpiresAt < db.NowMS() {
			// Best-effort cleanup; ignore the delete error.
			_ = s.db.DeleteSession(r.Context(), session.ID)
			s.clearSessionCookie(w)
			s.writeError(w, http.StatusUnauthorized, "session expired")
			return
		}

		account, err := s.db.GetAccountByID(r.Context(), session.AccountID)
		if err != nil {
			// Account was deleted out from under the session — wipe the
			// session so the cookie stops authenticating.
			_ = s.db.DeleteSession(r.Context(), session.ID)
			s.clearSessionCookie(w)
			s.writeError(w, http.StatusUnauthorized, "account no longer exists")
			return
		}

		ctx := contextWithAccount(r.Context(), account)
		next(w, r.WithContext(ctx))
	}
}

// ---------------------------------------------------------------------------
// Context plumbing.
// ---------------------------------------------------------------------------

type ctxKey int

const accountCtxKey ctxKey = iota

func contextWithAccount(ctx context.Context, a *db.Account) context.Context {
	return context.WithValue(ctx, accountCtxKey, a)
}

// AccountFromContext returns the authenticated account from a request that
// has passed through [Server.requireSession]. Returns nil otherwise; admin
// handlers can assume non-nil because requireSession would have refused
// the request.
func AccountFromContext(ctx context.Context) *db.Account {
	if a, ok := ctx.Value(accountCtxKey).(*db.Account); ok {
		return a
	}
	return nil
}

// ---------------------------------------------------------------------------
// Cookie helpers.
// ---------------------------------------------------------------------------

func (s *Server) setSessionCookie(w http.ResponseWriter, value string, ttl time.Duration) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.sessionCookieSecure(),
		SameSite: http.SameSiteStrictMode,
		Expires:  time.Now().Add(ttl),
		MaxAge:   int(ttl.Seconds()),
	})
}

func (s *Server) clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   s.sessionCookieSecure(),
		SameSite: http.SameSiteStrictMode,
		Expires:  time.Unix(0, 0),
		MaxAge:   -1,
	})
}

// sessionCookieSecure reports whether the Secure flag should be set on
// session cookies. Secure is set whenever TLS is configured at the edge;
// when DISABLE_TLS=true (e.g. the docker dev story) the cookie must drop
// Secure, otherwise the browser refuses to send it back over plain HTTP.
func (s *Server) sessionCookieSecure() bool {
	return !s.config.DisableTLS
}
