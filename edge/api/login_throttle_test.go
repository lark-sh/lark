package api

import (
	"testing"
	"time"
)

func TestLoginBackoff(t *testing.T) {
	if d := loginBackoff(loginFreeAttempts); d != 0 {
		t.Errorf("at free allowance: got %v, want 0", d)
	}
	if d := loginBackoff(loginFreeAttempts + 1); d != loginBackoffBase {
		t.Errorf("first over allowance: got %v, want %v", d, loginBackoffBase)
	}
	if d := loginBackoff(loginFreeAttempts + 2); d != 2*loginBackoffBase {
		t.Errorf("second over allowance: got %v, want %v", d, 2*loginBackoffBase)
	}
	// Grows to the cap and never exceeds it, even for absurd counts (overflow guard).
	if d := loginBackoff(loginFreeAttempts + 1000); d != loginBackoffCap {
		t.Errorf("huge count: got %v, want cap %v", d, loginBackoffCap)
	}
	if d := loginBackoff(1_000_000); d > loginBackoffCap {
		t.Errorf("overflow guard: got %v, exceeds cap %v", d, loginBackoffCap)
	}
}

func TestLoginThrottleFailResetDecay(t *testing.T) {
	// Construct directly (no janitor goroutine) — we're testing the logic.
	tr := &loginThrottle{entries: make(map[string]loginFailureState)}
	email := "admin@example.com"

	// Failures within the free allowance incur no delay.
	for i := 0; i < loginFreeAttempts; i++ {
		if d := tr.fail(email); d != 0 {
			t.Fatalf("failure %d within free allowance: got %v, want 0", i+1, d)
		}
	}
	// The next failure crosses into backoff.
	if d := tr.fail(email); d != loginBackoffBase {
		t.Fatalf("first throttled failure: got %v, want %v", d, loginBackoffBase)
	}

	// A successful login resets the run.
	tr.reset(email)
	if d := tr.fail(email); d != 0 {
		t.Fatalf("after reset: got %v, want 0", d)
	}

	// A run idle longer than the window decays to zero on the next attempt.
	tr.entries[email] = loginFailureState{
		failures: 100,
		last:     time.Now().Add(-2 * loginFailureWindow),
	}
	if d := tr.fail(email); d != 0 {
		t.Fatalf("decayed run: got %v, want 0 (counter should reset)", d)
	}
}
