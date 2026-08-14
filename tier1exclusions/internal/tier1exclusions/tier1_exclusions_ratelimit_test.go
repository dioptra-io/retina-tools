package tier1exclusions

import (
	"testing"
	"time"
)

func TestSmoothedLimiter_BurstThenGradualPacing(t *testing.T) {
	// Real usage order is WaitIfNeeded() BEFORE sending, then Record() AFTER the
	// response comes back — mirrors request_json's actual call sequence. The first
	// call should return immediately (nothing recorded yet), and pacing delays should
	// only appear once accumulated usage exceeds the burst allowance.
	l := NewSmoothedLimiter("test", 100, time.Second, 0.5, 5*time.Second)

	start := time.Now()
	if err := l.WaitIfNeeded(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Errorf("expected first call (nothing recorded yet) to return immediately, took %s", elapsed)
	}
	l.Record(40) // 40% of cap, within the 50% burst allowance

	start = time.Now()
	if err := l.WaitIfNeeded(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("expected second call to still be within burst allowance (fast), took %s", elapsed)
	}
	l.Record(40) // cumulative 80%, now past the 50% burst allowance

	start = time.Now()
	if err := l.WaitIfNeeded(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("expected third call to incur real pacing delay past the burst allowance, took %s", elapsed)
	}
}

func TestSmoothedLimiter_PacesAfterBurstExceeded(t *testing.T) {
	l := NewSmoothedLimiter("test", 100, time.Second, 0.1, 5*time.Second)
	l.Record(90) // well past the 10% burst allowance
	start := time.Now()
	if err := l.WaitIfNeeded(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Errorf("expected some pacing delay after exceeding burst allowance, took %s", elapsed)
	}
}

func TestSmoothedLimiter_GivesUpPastMaxTotalWait(t *testing.T) {
	l := NewSmoothedLimiter("test", 10, time.Hour, 0.0, 100*time.Millisecond)
	l.Record(10) // immediately at the hard cap for a 1-hour window
	err := l.WaitIfNeeded()
	if err == nil {
		t.Fatal("expected WaitIfNeeded to give up and return an error, got nil")
	}
}

func TestNewSmoothedLimiter_PanicsOnInvalidBurstFraction(t *testing.T) {
	cases := []float64{-0.1, 1.1, -1, 2}
	for _, bf := range cases {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("burstFraction=%v: expected a panic, got none", bf)
				}
			}()
			NewSmoothedLimiter("test", 100, time.Hour, bf, time.Minute)
		}()
	}
}

func TestNewSmoothedLimiter_AcceptsValidBurstFraction(t *testing.T) {
	for _, bf := range []float64{0, 0.5, 1} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("burstFraction=%v: unexpected panic: %v", bf, r)
				}
			}()
			NewSmoothedLimiter("test", 100, time.Hour, bf, time.Minute)
		}()
	}
}

func TestSmoothedLimiter_CurrentUsagePrunesOldEntries(t *testing.T) {
	l := NewSmoothedLimiter("test", 100, 50*time.Millisecond, 1.0, time.Second)
	l.Record(30)
	if got := l.CurrentUsage(); got != 30 {
		t.Fatalf("expected usage 30 immediately after recording, got %v", got)
	}
	time.Sleep(80 * time.Millisecond) // longer than the window
	if got := l.CurrentUsage(); got != 0 {
		t.Errorf("expected usage to be pruned to 0 after window elapsed, got %v", got)
	}
}

func TestKeyPool_UnhealthyKeyDoesNotStarveHealthyKeys(t *testing.T) {
	// Regression test for the real bug found on 2026-08-06: a key that keeps failing
	// (429) never records volume, so under a volume-only selection rule it always
	// looks like it has the most headroom and gets picked repeatedly, starving other
	// keys that ARE succeeding (and therefore accumulating nonzero volume). Failure
	// count must be checked before volume, so a struggling key gets deprioritized.
	kp := NewKeyPool([]string{"bad", "good"}, 700, 95_000_000, 280)

	// "good" succeeds and accumulates real volume.
	kp.Limiters("good").Volume.Record(50_000_000)
	kp.ReportSuccess("good")

	// "bad" has failed repeatedly and recorded NO volume (stays at 0, same as if it
	// had never been used at all).
	kp.ReportFailure("bad")
	kp.ReportFailure("bad")
	kp.ReportFailure("bad")

	// Without the fix, "bad" (0 volume) would look better than "good" (50MB volume)
	// on every single call, regardless of rotation offset, because 0 < 50_000_000
	// unconditionally — not just as a tiebreak.
	pickedGood := false
	for i := 0; i < 10; i++ {
		if kp.NextKey() == "good" {
			pickedGood = true
			break
		}
	}
	if !pickedGood {
		t.Error("expected the pool to eventually prefer the healthy key over the " +
			"failing key, but it never did across 10 selections — starvation bug reproduced")
	}
}

func TestKeyPool_HealthyKeysTiebreakOnVolume(t *testing.T) {
	// Among equally-healthy keys (no failures), the one with lower volume usage
	// should still be preferred — this is the original headroom heuristic, and it
	// must keep working once failures are checked first.
	kp := NewKeyPool([]string{"a", "b"}, 700, 95_000_000, 280)
	kp.Limiters("a").Volume.Record(80_000_000)
	kp.Limiters("b").Volume.Record(10_000_000)

	picked := kp.NextKey()
	if picked != "b" {
		t.Errorf("expected lower-volume key 'b' to be picked, got %q", picked)
	}
}

func TestKeyPool_SuccessResetsFailureCount(t *testing.T) {
	kp := NewKeyPool([]string{"k1"}, 700, 95_000_000, 280)
	kp.ReportFailure("k1")
	kp.ReportFailure("k1")
	if got := kp.ConsecutiveFailures("k1"); got != 2 {
		t.Fatalf("expected 2 failures recorded, got %d", got)
	}
	kp.ReportSuccess("k1")
	if got := kp.ConsecutiveFailures("k1"); got != 0 {
		t.Errorf("expected failure count reset to 0 after success, got %d", got)
	}
}

func TestKeyPool_RotationBreaksTiesAmongEquallyHealthyKeys(t *testing.T) {
	// With no usage/failures recorded at all (fresh pool), repeated calls should not
	// pin to a single key forever — rotation should distribute selections.
	kp := NewKeyPool([]string{"x", "y", "z"}, 700, 95_000_000, 280)
	seen := map[string]bool{}
	for i := 0; i < 6; i++ {
		seen[kp.NextKey()] = true
	}
	if len(seen) < 2 {
		t.Errorf("expected rotation to visit more than one key among ties, only saw: %v", seen)
	}
}
