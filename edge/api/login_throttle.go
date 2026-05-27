package api

import (
	"sync"
	"time"
)

const (
	// loginFreeAttempts is how many failed logins an account gets before backoff
	// kicks in — enough headroom for ordinary fat-fingering.
	loginFreeAttempts = 5
	// loginBackoffBase is the delay applied to the first failure past the free
	// allowance; it doubles per additional failure.
	loginBackoffBase = 200 * time.Millisecond
	// loginBackoffCap bounds the per-attempt delay. Because we only ever *delay*
	// (never block), an attacker hammering an admin's email slows that admin's
	// failed attempts but can never lock them out — a correct password still
	// succeeds immediately.
	loginBackoffCap = 3 * time.Second
	// loginFailureWindow is how long a run of failures is remembered. After this
	// much idle time the run decays to zero, and the janitor evicts the entry.
	loginFailureWindow = 15 * time.Minute
)

// loginThrottle slows repeated failed logins per account, keyed by email, with
// an exponential capped backoff applied on failure.
//
// Keyed on email (not client IP) deliberately: it can't be evaded by spoofing
// X-Forwarded-For (audit L-2/L-3), it targets the real threat (guessing one
// account's password), and — because it only delays failures — it can't be used
// to lock a legitimate admin out. In-memory and per-process: failed-login state
// needn't survive a restart, and this keeps a DB write off the login path. A
// background janitor bounds memory under a flood of distinct (e.g. random) emails.
type loginThrottle struct {
	mu      sync.Mutex
	entries map[string]loginFailureState
}

type loginFailureState struct {
	failures int
	last     time.Time
}

// newLoginThrottle returns a throttle with its eviction janitor running.
func newLoginThrottle() *loginThrottle {
	t := &loginThrottle{entries: make(map[string]loginFailureState)}
	go t.janitor()
	return t
}

// fail records a failed login for email and returns how long the caller should
// delay the response. The delay grows exponentially once past the free
// allowance and is capped at loginBackoffCap.
func (t *loginThrottle) fail(email string) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()

	st := t.entries[email]
	// Decay a stale run so an old fumble doesn't penalize a fresh attempt.
	if time.Since(st.last) > loginFailureWindow {
		st.failures = 0
	}
	st.failures++
	st.last = time.Now()
	t.entries[email] = st
	return loginBackoff(st.failures)
}

// reset clears an account's failure run after a successful login.
func (t *loginThrottle) reset(email string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.entries, email)
}

// loginBackoff maps a failure count to a delay: zero within the free allowance,
// then loginBackoffBase doubling per extra failure, capped at loginBackoffCap.
func loginBackoff(failures int) time.Duration {
	over := failures - loginFreeAttempts
	if over <= 0 {
		return 0
	}
	// Bound the shift so it can't overflow the int64 nanosecond Duration; the
	// cap is reached long before this anyway.
	if over > 20 {
		over = 20
	}
	d := loginBackoffBase << uint(over-1)
	if d <= 0 || d > loginBackoffCap {
		d = loginBackoffCap
	}
	return d
}

// janitor periodically evicts entries whose failure run has decayed, bounding
// memory under a flood of distinct emails. It runs for the process lifetime.
func (t *loginThrottle) janitor() {
	ticker := time.NewTicker(loginFailureWindow)
	defer ticker.Stop()
	for range ticker.C {
		t.mu.Lock()
		for email, st := range t.entries {
			if time.Since(st.last) > loginFailureWindow {
				delete(t.entries, email)
			}
		}
		t.mu.Unlock()
	}
}
