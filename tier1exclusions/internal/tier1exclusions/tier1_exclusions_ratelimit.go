// This file provides proactive, smoothed pacing against rolling-window API limits
// (requests/hour, bytes/hour, exec-seconds/hour), plus a multi-key pool that picks the
// healthiest available key rather than a blind round-robin. All three resource
// dimensions use the same leaky-bucket smoothing — mixing a reactive backoff for one
// dimension with proactive smoothing for others was a real bug in the original Python
// prototype (still triggered 429s after volume/exec-time smoothing was added).
package tier1exclusions

import (
	"fmt"
	"sync"
	"time"
)

// SmoothedLimiter paces consumption of a resource (requests, bytes, seconds) against a
// cap per rolling time window. Rather than allowing full-speed use until the cap hits
// and then blocking until the whole window clears, it spreads consumption evenly across
// the window — a leaky-bucket-style virtual cursor tracks the ideal pace and delays
// callers just enough to stay on it. A small initial allowance still permits a short
// burst before this smoothing takes effect.
type SmoothedLimiter struct {
	mu sync.Mutex

	cap            float64
	window         time.Duration
	rate           float64 // units per second, sustained
	burstAllowance time.Duration
	maxTotalWait   time.Duration
	label          string

	entries  []logEntry
	nextSlot time.Time // zero value = not yet initialized
}

type logEntry struct {
	at     time.Time
	amount float64
}

// NewSmoothedLimiter constructs a limiter for cap units per window. burstFraction
// (0-1) is the fraction of cap available as an immediate burst before pacing kicks in.
// maxTotalWait is a hard ceiling on how long a single WaitIfNeeded call will block
// before giving up, rather than hanging for close to an hour.
func NewSmoothedLimiter(label string, cap float64, window time.Duration, burstFraction float64, maxTotalWait time.Duration) *SmoothedLimiter {
	if burstFraction < 0 || burstFraction > 1 {
		panic(fmt.Sprintf("NewSmoothedLimiter(%s): burstFraction must be in [0,1], got %f", label, burstFraction))
	}
	rate := cap / window.Seconds()
	return &SmoothedLimiter{
		label:          label,
		cap:            cap,
		window:         window,
		rate:           rate,
		burstAllowance: time.Duration((cap * burstFraction / rate) * float64(time.Second)),
		maxTotalWait:   maxTotalWait,
	}
}

// prune discards entries older than the rolling window so usage sums (in CurrentUsage
// and WaitIfNeeded) reflect only the last `window` duration, not all-time totals — and
// so l.entries doesn't grow unbounded over a long-running process. Assumes entries are
// in chronological order, which holds because Record only ever appends.
func (l *SmoothedLimiter) prune(now time.Time) {
	cutoff := now.Add(-l.window)
	i := 0
	for i < len(l.entries) && l.entries[i].at.Before(cutoff) {
		i++
	}
	l.entries = l.entries[i:]
}

// CurrentUsage returns total recorded usage within the current rolling window.
func (l *SmoothedLimiter) CurrentUsage() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.prune(now)
	var sum float64
	for _, e := range l.entries {
		sum += e.amount
	}
	return sum
}

// Record logs a completed unit of consumption and advances the smoothing cursor.
// Deliberately doesn't check against cap — that's WaitIfNeeded's job, checked before
// the *next* request. Record just reports what already happened.
func (l *SmoothedLimiter) Record(amount float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.entries = append(l.entries, logEntry{at: now, amount: amount})

	cost := time.Duration((amount / l.rate) * float64(time.Second))
	if l.nextSlot.IsZero() {
		// Cold start: init as far behind "now" as the burst allowance permits, so the
		// first burst is genuinely free rather than immediately queuing a delay —
		// without this, burstFraction only ever protects against later drift and
		// never actually grants the advertised initial capacity.
		l.nextSlot = now.Add(-l.burstAllowance)
	}
	floor := now.Add(-l.burstAllowance)
	if l.nextSlot.Before(floor) {
		l.nextSlot = floor
	}
	l.nextSlot = l.nextSlot.Add(cost)
}

// WaitIfNeeded blocks (if necessary) to stay within the smoothed pace, plus a hard
// safety net that never lets true rolling-window usage exceed cap even if the
// smoothing math drifts. Returns an error rather than blocking past maxTotalWait.
func (l *SmoothedLimiter) WaitIfNeeded() error {
	var totalWaited time.Duration

	for {
		l.mu.Lock()
		now := time.Now()
		l.prune(now)
		var used float64
		for _, e := range l.entries {
			used += e.amount
		}
		if used >= l.cap && len(l.entries) > 0 {
			sleepFor := l.window - now.Sub(l.entries[0].at) + time.Second
			if sleepFor < time.Second {
				sleepFor = time.Second
			}
			if totalWaited+sleepFor > l.maxTotalWait {
				l.mu.Unlock()
				return fmt.Errorf("%s: hard cap reached (%.0f/%.0f), would need %s total (> max %s) — giving up",
					l.label, used, l.cap, (totalWaited + sleepFor).Round(time.Second), l.maxTotalWait)
			}
			l.mu.Unlock()
			time.Sleep(sleepFor)
			totalWaited += sleepFor
			continue
		}
		l.mu.Unlock()
		break
	}

	l.mu.Lock()
	var sleepFor time.Duration
	if !l.nextSlot.IsZero() {
		sleepFor = time.Until(l.nextSlot)
	}
	l.mu.Unlock()
	if sleepFor > 0 {
		time.Sleep(sleepFor)
	}
	return nil
}

// KeyLimiters bundles the three independent SmoothedLimiters for one API key.
type KeyLimiters struct {
	Rate     *SmoothedLimiter
	Volume   *SmoothedLimiter
	ExecTime *SmoothedLimiter
}

// KeyPool holds one KeyLimiters set per API key and picks which to use next.
// Selection prioritizes fewest consecutive failures first, then lowest volume usage
// as a tiebreaker. Checking failures first is required, not cosmetic: Volume.Record
// only fires on success, so a key being rate-limited never accumulates volume and
// would otherwise always look like it has the most headroom — starving healthy keys
// (a real bug found in practice).
type KeyPool struct {
	mu                  sync.Mutex
	keys                []string
	i                   int
	limiters            map[string]KeyLimiters
	consecutiveFailures map[string]int
}

// NewKeyPool builds a pool with a fresh limiter set per key. maxRequests etc. should
// include real margin below the documented per-hour cap — the API gives no
// quota-remaining headers, so a freshly-started process can't know how much of the
// true budget earlier activity (a crashed prior run, manual testing) already consumed.
func NewKeyPool(keys []string, maxRequests, maxVolumeBytes, maxExecSeconds float64) *KeyPool {
	kp := &KeyPool{
		keys:                keys,
		limiters:            make(map[string]KeyLimiters, len(keys)),
		consecutiveFailures: make(map[string]int, len(keys)),
	}
	for _, k := range keys {
		kp.limiters[k] = KeyLimiters{
			Rate:     NewSmoothedLimiter("request-rate", maxRequests, time.Hour, 0.1, 65*time.Minute),
			Volume:   NewSmoothedLimiter("volume", maxVolumeBytes, time.Hour, 0.2, 65*time.Minute),
			ExecTime: NewSmoothedLimiter("exec-time", maxExecSeconds, time.Hour, 0.2, 65*time.Minute),
		}
		kp.consecutiveFailures[k] = 0
	}
	return kp
}

// NextKey returns the best available key: fewest consecutive failures first, lowest
// volume usage as a tiebreaker. The scan starts at a rotating offset (not always index
// 0) so that when several keys are tied, which one wins rotates across calls instead of
// always favoring the same key — the first key encountered in the offset-started sweep
// wins any tie, matching sort.SliceStable's stability semantics without needing to
// build and sort a whole copy of the key list on every call.
func (kp *KeyPool) NextKey() string {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	kp.i++
	n := len(kp.keys)
	offset := kp.i % n

	best := kp.keys[offset]
	bestFails := kp.consecutiveFailures[best]
	bestVolume := kp.limiters[best].Volume.CurrentUsage()

	for j := 1; j < n; j++ {
		k := kp.keys[(offset+j)%n]
		fails := kp.consecutiveFailures[k]
		volume := kp.limiters[k].Volume.CurrentUsage()
		if fails < bestFails || (fails == bestFails && volume < bestVolume) {
			best, bestFails, bestVolume = k, fails, volume
		}
	}
	return best
}

// Limiters returns the limiter set for a given key.
func (kp *KeyPool) Limiters(key string) KeyLimiters {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	return kp.limiters[key]
}

// ReportSuccess resets a key's consecutive-failure count.
func (kp *KeyPool) ReportSuccess(key string) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	kp.consecutiveFailures[key] = 0
}

// ReportFailure increments a key's consecutive-failure count.
func (kp *KeyPool) ReportFailure(key string) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	kp.consecutiveFailures[key]++
}

// ConsecutiveFailures exposes the current failure count for a key (for logging).
func (kp *KeyPool) ConsecutiveFailures(key string) int {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	return kp.consecutiveFailures[key]
}
