package tier1exclusions

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const userAgent = "retina-tools/tier1exclusions"

// baseBackoff starts at 1 minute: bgproutes.io's rate limiting can be far more
// aggressive than its documented limits suggest (a request shape reliable for weeks
// started returning 429 consistently on 2026-08-07), and a short backoff just
// re-triggers the same rejection. Both vars (not consts) so tests can shrink them.
var (
	maxRetries  = 5
	baseBackoff = 1 * time.Minute
)

// Vars, not consts, so tests can point these at an httptest fake server.
var (
	bgproutesBaseURL = "https://api.bgproutes.io/v1"
	ripestatBaseURL  = "https://stat.ripe.net/data"
)

// Timeout must exceed maxRetries*max(backoffFor) or a slow-but-alive server could get
// cut off mid-retry-sequence; 20 minutes covers the worst case (1+2+4+8+16=31min of
// sleep is still possible across retries, so this bounds a single REQUEST attempt, not
// the whole retry loop — context cancellation, not this, is what should stop the loop
// early on shutdown).
var httpClient = &http.Client{Timeout: 20 * time.Minute}

// APIClient wraps a KeyPool for the retry/rate-limit-aware bgproutes.io calls.
// RIPEstat calls need no key/limiter — see FetchAnnouncedPrefixes.
type APIClient struct {
	Keys   *KeyPool
	Logger *slog.Logger
}

// callBgproutes performs a rate-limited, retried GET against bgproutes.io, rotating
// keys via KeyPool. Network failures don't consume request-count quota (never reached
// the server); any actual response, success or error, does. ctx cancellation (e.g.
// SIGINT) stops the retry loop promptly rather than waiting out the current backoff.
func (c *APIClient) callBgproutes(ctx context.Context, path string, out any) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		key := c.Keys.NextKey()
		lim := c.Keys.Limiters(key)

		if fails := c.Keys.ConsecutiveFailures(key); fails > 0 {
			c.Logger.Debug("Selected key has recent failures", "key_prefix", key[:min(8, len(key))], "consecutive_failures", fails)
		}

		// A limiter error here means THIS key is stuck badly enough that waiting
		// out its rolling-window cap would exceed maxTotalWait (~65min) — it does
		// NOT mean every key is stuck. Each key has its own independent
		// Rate/Volume/ExecTime budget (see KeyPool), so report the failure and try
		// the next key on the next attempt rather than aborting the whole call —
		// with multiple keys on staggered usage, another key is often available
		// well before this one's window clears.
		if err := lim.Rate.WaitIfNeeded(); err != nil {
			c.Keys.ReportFailure(key)
			lastErr = fmt.Errorf("rate limiter: %w", err)
			c.Logger.Warn("Rate limiter hard cap reached, trying next key", "key_prefix", key[:min(8, len(key))], "error", err)
			continue
		}
		if err := lim.Volume.WaitIfNeeded(); err != nil {
			c.Keys.ReportFailure(key)
			lastErr = fmt.Errorf("volume limiter: %w", err)
			c.Logger.Warn("Volume limiter hard cap reached, trying next key", "key_prefix", key[:min(8, len(key))], "error", err)
			continue
		}
		if err := lim.ExecTime.WaitIfNeeded(); err != nil {
			c.Keys.ReportFailure(key)
			lastErr = fmt.Errorf("exec-time limiter: %w", err)
			c.Logger.Warn("Exec-time limiter hard cap reached, trying next key", "key_prefix", key[:min(8, len(key))], "error", err)
			continue
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, bgproutesBaseURL+path, nil)
		if err != nil {
			return fmt.Errorf("building request: %w", err)
		}
		req.Header.Set("accept", "application/json")
		req.Header.Set("x-api-key", key)
		req.Header.Set("User-Agent", userAgent)

		resp, err := httpClient.Do(req)
		if err != nil {
			c.Keys.ReportFailure(key)
			lastErr = fmt.Errorf("network error: %w", err)
			c.Logger.Warn("Network error", "attempt", attempt+1, "max_attempts", maxRetries, "error", err)
			if sleepOrDone(ctx, backoffFor(attempt)) {
				return ctx.Err()
			}
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			c.Logger.Warn("Failed to read response body", "error", readErr)
		}
		lim.Rate.Record(1)

		switch {
		case resp.StatusCode == http.StatusOK:
			lim.Volume.Record(float64(len(body)))
			c.Keys.ReportSuccess(key)
			var envelope struct {
				Seconds float64 `json:"seconds"`
			}
			if err := json.Unmarshal(body, &envelope); err != nil {
				c.Logger.Debug("Could not parse 'seconds' field for exec-time accounting", "error", err)
			}
			lim.ExecTime.Record(envelope.Seconds)

			if err := json.Unmarshal(body, out); err != nil {
				return fmt.Errorf("decoding response: %w", err)
			}
			return nil

		case resp.StatusCode == http.StatusTooManyRequests:
			c.Keys.ReportFailure(key)
			wait := backoffFor(attempt)
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, err := strconv.Atoi(ra); err == nil {
					wait = time.Duration(secs) * time.Second
				}
			}
			lastErr = fmt.Errorf("rate limited (429)")
			c.Logger.Warn("Rate limited (429)", "key_prefix", key[:min(8, len(key))],
				"pausing", wait, "consecutive_failures", c.Keys.ConsecutiveFailures(key))
			if sleepOrDone(ctx, wait) {
				return ctx.Err()
			}
			continue

		case resp.StatusCode >= 500 && resp.StatusCode < 600:
			c.Keys.ReportFailure(key)
			lastErr = fmt.Errorf("server error: HTTP %d", resp.StatusCode)
			c.Logger.Warn("Server error, retrying", "status_code", resp.StatusCode)
			if sleepOrDone(ctx, backoffFor(attempt)) {
				return ctx.Err()
			}
			continue

		default:
			c.Keys.ReportFailure(key)
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

// sleepOrDone waits for d or ctx cancellation, whichever comes first. Returns true if
// ctx was cancelled (caller should abort rather than continue retrying).
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return false
	case <-ctx.Done():
		return true
	}
}

func backoffFor(attempt int) time.Duration {
	d := baseBackoff
	for i := 0; i < attempt; i++ {
		d *= 2
	}
	return d
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// VantagePoint is the subset of bgproutes.io's vantage_points() response this tool uses.
type VantagePoint struct {
	ID        json.Number `json:"id"`
	ASN       json.Number `json:"asn"`
	Source    string      `json:"source"`
	OrgName   string      `json:"org_name"`
	RibSizeV4 int64       `json:"rib_size_v4"`
	RibSizeV6 int64       `json:"rib_size_v6"`
	Status    []string    `json:"status"`
}

type vantagePointsResponse struct {
	Data struct {
		BGP []VantagePoint `json:"bgp"`
	} `json:"data"`
}

// CheckVPStatus returns (statuses, checkSucceeded). checkSucceeded=false means the
// request itself failed — must not be treated as "all VPs are down" (a connect-timeout
// once caused exactly that misread, flagging all 10 pinned VPs "not ready" when they
// were fine).
func (c *APIClient) CheckVPStatus(ctx context.Context, vpIDs []string, ribDate time.Time) (map[string][]string, bool) {
	if len(vpIDs) == 0 {
		return map[string][]string{}, true
	}
	q := url.Values{}
	q.Set("vp_bgp_ids", strings.Join(vpIDs, ","))
	q.Set("peering_protocol", "BGP")
	q.Set("date", ribDate.UTC().Format("2006-01-02T15:04:05"))
	q.Set("status", "ready,up,down")

	var resp vantagePointsResponse
	if err := c.callBgproutes(ctx, "/vantage_points?"+q.Encode(), &resp); err != nil {
		c.Logger.Warn("Vantage_points status check failed", "error", err)
		return nil, false
	}
	out := make(map[string][]string, len(resp.Data.BGP))
	for _, vp := range resp.Data.BGP {
		out[vp.ID.String()] = vp.Status
	}
	return out, true
}

// FindFullFeedVPs queries live for VPs above a rib_size threshold. Used only when no
// pinned list is configured.
// FindFullFeedVPs queries live for VPs above a rib_size threshold. Used only when no
// pinned list is configured (currently unreachable in production, since both AFI
// configs pin their VP lists — but kept correct for when a new AFI is added without
// one, or an existing pinned list is accidentally cleared).
func (c *APIClient) FindFullFeedVPs(ctx context.Context, sources string, afiIs4 bool, threshold int64, limit int) ([]string, error) {
	field := "rib_size_v6"
	if afiIs4 {
		field = "rib_size_v4"
	}
	q := url.Values{}
	q.Set("sources", sources)
	q.Set("peering_protocol", "BGP")
	q.Set(field, fmt.Sprintf(">,%d", threshold)) // bgproutes.io operator syntax: ">,N" means greater than N

	var resp vantagePointsResponse
	if err := c.callBgproutes(ctx, "/vantage_points?"+q.Encode(), &resp); err != nil {
		return nil, err
	}

	// Sort by VP ID before truncating to limit — bgproutes.io doesn't guarantee a
	// stable response order, so without this, which VPs get selected (and thus
	// which ones a snapshot actually queried) could vary run to run with no
	// underlying infrastructure change.
	vps := make([]VantagePoint, len(resp.Data.BGP))
	copy(vps, resp.Data.BGP)
	sort.Slice(vps, func(i, j int) bool {
		vi, erri := vps[i].ID.Int64()
		vj, errj := vps[j].ID.Int64()
		if erri != nil || errj != nil {
			// Non-numeric VP id (unexpected, but don't crash sorting over it) —
			// fall back to a stable string comparison so the sort is still total.
			return vps[i].ID.String() < vps[j].ID.String()
		}
		return vi < vj
	})

	ids := make([]string, 0, len(vps))
	for _, vp := range vps {
		ids = append(ids, vp.ID.String())
	}
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

// ribResponse mirrors bgproutes.io's /rib shape: data.bgp[vpID][prefix] =
// [as_path, communities, aspa_status, rov_status, bmp_feed_type].
type ribResponse struct {
	Data struct {
		BGP map[string]map[string][]json.RawMessage `json:"bgp"`
		BMP map[string]map[string][]json.RawMessage `json:"bmp"`
	} `json:"data"`
}

// QueryRIBBatch returns discovered more-specific prefixes with every distinct origin
// ASN observed across all VPs/protocols in the response.
func (c *APIClient) QueryRIBBatch(ctx context.Context, vpIDs []string, supernets []string, afiIs4 bool, ribDate time.Time) ([]MoreSpecific, error) {
	afi := "6"
	if afiIs4 {
		afi = "4"
	}
	var filterParts []string
	for _, supernet := range supernets {
		filterParts = append(filterParts, "<<:"+supernet)
	}

	q := url.Values{}
	q.Set("vp_bgp_ids", strings.Join(vpIDs, ","))
	q.Set("prefix_filter", strings.Join(filterParts, ","))
	q.Set("date", ribDate.UTC().Format("2006-01-02T15:04:05"))
	q.Set("data_afi", afi)
	q.Set("return_aspath", "True")
	q.Set("return_community", "False")

	var resp ribResponse
	if err := c.callBgproutes(ctx, "/rib?"+q.Encode(), &resp); err != nil {
		return nil, err
	}
	return collectMoreSpecifics(resp, c.Logger), nil
}

func collectMoreSpecifics(resp ribResponse, logger *slog.Logger) []MoreSpecific {
	originsByPrefix := map[string]map[string]struct{}{}
	merge := func(prefixesByVP map[string]map[string][]json.RawMessage) {
		for _, prefixes := range prefixesByVP {
			for prefix, attrs := range prefixes {
				if len(attrs) == 0 {
					continue
				}
				var asPath string
				if err := json.Unmarshal(attrs[0], &asPath); err != nil {
					logger.Debug("Could not parse AS path", "raw", string(attrs[0]), "error", err)
					continue
				}
				origin := extractOriginASN(asPath, logger)
				if origin == "" {
					continue
				}
				if originsByPrefix[prefix] == nil {
					originsByPrefix[prefix] = map[string]struct{}{}
				}
				originsByPrefix[prefix][origin] = struct{}{}
			}
		}
	}
	merge(resp.Data.BGP)
	merge(resp.Data.BMP) // both are equally valid evidence of a prefix's origin — combine, don't prefer one

	moreSpecifics := make([]MoreSpecific, 0, len(originsByPrefix))
	for prefixStr, origins := range originsByPrefix {
		p, err := netip.ParsePrefix(prefixStr)
		if err != nil {
			continue
		}
		moreSpecifics = append(moreSpecifics, MoreSpecific{Prefix: p, Origins: origins})
	}
	return moreSpecifics
}

// extractOriginASN returns the last hop of an AS path — the first hop is always the
// querying VP's own ASN, not the origin. For an AS-SET ("{65001,65002}"), only the
// first member is returned; members of a real AS-SET should share an origin, but this
// isn't validated here — logged at debug so an unexpected case is visible rather than
// silently narrowed.
func extractOriginASN(asPath string, logger *slog.Logger) string {
	asPath = strings.TrimSpace(asPath)
	if asPath == "" {
		return ""
	}
	fields := strings.Fields(asPath)
	last := fields[len(fields)-1]
	if strings.HasPrefix(last, "{") {
		logger.Debug("AS-SET encountered in path, using first member as origin", "as_set", last)
	}
	last = strings.Trim(last, "{}")
	if idx := strings.Index(last, ","); idx >= 0 {
		last = last[:idx]
	}
	return last
}

// risPrefixesResponse mirrors RIPEstat's ris-prefixes shape, which splits results by
// address family natively.
type risPrefixesResponse struct {
	Data struct {
		Prefixes struct {
			V4 struct {
				Originating []string `json:"originating"`
			} `json:"v4"`
			V6 struct {
				Originating []string `json:"originating"`
			} `json:"v6"`
		} `json:"prefixes"`
	} `json:"data"`
}

// FetchAnnouncedPrefixes returns (prefixes, ok) for the given ASN/AFI as of ribDate,
// via RIPEstat's ris-prefixes endpoint — a genuine historical lookup, not a live one.
// Pinning this to ribDate matters at the monthly run cadence this tool operates at: a
// run that pauses/resumes across days (rate limits, circuit breaker) must not silently
// mix a live parent-list fetch from resume time with a rib() more-specifics lookup
// still pinned to the original snapshot date — and a fixed snapshot date is also what
// makes a given month's run reproducible/auditable after the fact.
// ok=false means the request itself failed — distinct from a successful request
// returning zero prefixes; callers must not conflate the two.
func FetchAnnouncedPrefixes(ctx context.Context, asn string, afiIs4 bool, ribDate time.Time, logger *slog.Logger) ([]string, bool) {
	queryTime := ribDate.UTC().Format("2006-01-02T15:04:05")
	u := fmt.Sprintf("%s/ris-prefixes/data.json?resource=AS%s&list_prefixes=true&query_time=%s",
		ripestatBaseURL, asn, queryTime)

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, false
		}
		req.Header.Set("User-Agent", userAgent)

		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
			logger.Warn("RIPEstat network error", "asn", asn, "attempt", attempt+1, "max_attempts", maxRetries, "error", err)
			if sleepOrDone(ctx, backoffFor(attempt)) {
				return nil, false
			}
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			logger.Warn("Failed to read RIPEstat response body", "asn", asn, "error", readErr)
		}

		switch {
		case resp.StatusCode == http.StatusOK:
			var parsed risPrefixesResponse
			if err := json.Unmarshal(body, &parsed); err != nil {
				logger.Error("RIPEstat: bad JSON", "asn", asn, "error", err)
				return nil, false
			}
			originating := parsed.Data.Prefixes.V6.Originating
			if afiIs4 {
				originating = parsed.Data.Prefixes.V4.Originating
			}
			return originating, true

		case resp.StatusCode >= 500:
			lastErr = fmt.Errorf("server error: HTTP %d", resp.StatusCode)
			logger.Warn("RIPEstat server error, retrying", "asn", asn, "status_code", resp.StatusCode)
			if sleepOrDone(ctx, backoffFor(attempt)) {
				return nil, false
			}
			continue

		default:
			logger.Error("RIPEstat HTTP error", "asn", asn, "status_code", resp.StatusCode, "body", truncate(string(body), 200))
			return nil, false
		}
	}
	logger.Error("RIPEstat: all attempts failed", "asn", asn, "max_attempts", maxRetries, "error", lastErr)
	return nil, false
}
