package tier1exclusions

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- pure-logic tests (no network) ---

func TestTruncate(t *testing.T) {
	if got := truncate("short", 10); got != "short" {
		t.Errorf("expected unchanged string under limit, got %q", got)
	}
	if got := truncate("this is a long string", 4); got != "this" {
		t.Errorf("expected truncation to 4 chars, got %q", got)
	}
}

func TestExtractOriginASN(t *testing.T) {
	logger := testLogger()
	cases := []struct {
		asPath string
		want   string
	}{
		{"199524 174 3356", "3356"},                     // normal path, origin = last hop
		{"199524 174 3356 46378", "46378"},              // longer path, still last hop
		{"3356", "3356"},                                // single-hop path (the VP's own network originates)
		{"", ""},                                        // empty path
		{"  199524   174  ", "174"},                     // extra whitespace
		{"199524 174 {65001,65002}", "65001"},           // AS-SET origin -> first member
		{"199524 174 18899 18899 18899 18899", "18899"}, // prepending, still correct last hop
	}
	for _, c := range cases {
		if got := extractOriginASN(c.asPath, logger); got != c.want {
			t.Errorf("extractOriginASN(%q) = %q, want %q", c.asPath, got, c.want)
		}
	}
}

func TestCollectMoreSpecifics_UnionsAcrossVPsAndProtocols(t *testing.T) {
	// Two VPs see the same prefix with the SAME origin -> one MoreSpecific entry with
	// one origin. A second prefix is seen by only one VP.
	raw := `{
        "data": {
            "bgp": {
                "215": {
                    "8.0.0.0/12": ["199524 174 3356", null, "", "", -1],
                    "8.10.120.0/24": ["199524 174 33182", null, "", "", -1]
                },
                "1749": {
                    "8.0.0.0/12": ["25091 174 3356", null, "", "", -1]
                }
            },
            "bmp": {}
        }
    }`
	var resp ribResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		t.Fatalf("failed to parse fixture: %v", err)
	}

	got := collectMoreSpecifics(resp, testLogger())
	byPrefix := map[string]map[string]struct{}{}
	for _, ms := range got {
		byPrefix[ms.Prefix.String()] = ms.Origins
	}

	if len(byPrefix) != 2 {
		t.Fatalf("expected 2 distinct prefixes, got %d: %+v", len(byPrefix), byPrefix)
	}
	wantAggregate := map[string]struct{}{"3356": {}}
	if !reflect.DeepEqual(byPrefix["8.0.0.0/12"], wantAggregate) {
		t.Errorf("8.0.0.0/12 origins = %v, want %v (seen by 2 VPs, same origin -> unioned to one)",
			byPrefix["8.0.0.0/12"], wantAggregate)
	}
	wantCustomer := map[string]struct{}{"33182": {}}
	if !reflect.DeepEqual(byPrefix["8.10.120.0/24"], wantCustomer) {
		t.Errorf("8.10.120.0/24 origins = %v, want %v", byPrefix["8.10.120.0/24"], wantCustomer)
	}
}

func TestCollectMoreSpecifics_MOASDisagreementPreserved(t *testing.T) {
	// If two VPs disagree on origin for the same prefix (a real MOAS case, or a
	// visibility artifact), BOTH origins must survive in the resulting set — this is
	// what lets AssignExclusions apply its conservative "any differing origin ->
	// exclude" rule downstream.
	raw := `{
        "data": {
            "bgp": {
                "215": {"8.0.1.0/24": ["199524 174 111", null, "", "", -1]},
                "1749": {"8.0.1.0/24": ["25091 174 222", null, "", "", -1]}
            },
            "bmp": {}
        }
    }`
	var resp ribResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		t.Fatalf("failed to parse fixture: %v", err)
	}
	got := collectMoreSpecifics(resp, testLogger())
	if len(got) != 1 {
		t.Fatalf("expected 1 prefix, got %d", len(got))
	}
	want := map[string]struct{}{"111": {}, "222": {}}
	if !reflect.DeepEqual(got[0].Origins, want) {
		t.Errorf("expected both disagreeing origins preserved, got %v, want %v", got[0].Origins, want)
	}
}

// --- FindFullFeedVPs ---

func TestFindFullFeedVPs_ReturnsVPsAboveThreshold(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/vantage_points", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"data": map[string]any{
				"bgp": []map[string]any{
					{"id": "215", "rib_size_v4": 1_100_000},
					{"id": "432", "rib_size_v4": 1_050_000},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{Keys: NewKeyPool([]string{"fake-key"}, 700, 95_000_000, 280), Logger: testLogger()}
	ids, err := client.FindFullFeedVPs(context.Background(), "ris", true, 900_000, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 VP ids, got %v", ids)
	}
}

func TestFindFullFeedVPs_LimitsResultCount(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/vantage_points", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"data": map[string]any{
				"bgp": []map[string]any{
					{"id": "1", "rib_size_v4": 1_000_000},
					{"id": "2", "rib_size_v4": 1_000_000},
					{"id": "3", "rib_size_v4": 1_000_000},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{Keys: NewKeyPool([]string{"fake-key"}, 700, 95_000_000, 280), Logger: testLogger()}
	ids, err := client.FindFullFeedVPs(context.Background(), "ris", true, 900_000, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 2 {
		t.Errorf("expected limit of 2 to be respected, got %d: %v", len(ids), ids)
	}
	// With the response-order-independence fix, ids are sorted by numeric VP id
	// before truncation, so this is deterministic: "1" and "2", not whichever two
	// the fake server happened to list first.
	if ids[0] != "1" || ids[1] != "2" {
		t.Errorf("expected deterministic lowest-id-first selection [1 2], got %v", ids)
	}
}

func TestFindFullFeedVPs_PropagatesRequestError(t *testing.T) {
	origMaxRetries, origBackoff := maxRetries, baseBackoff
	maxRetries = 1
	baseBackoff = time.Millisecond
	t.Cleanup(func() { maxRetries, baseBackoff = origMaxRetries, origBackoff })

	mux := http.NewServeMux()
	mux.HandleFunc("/vantage_points", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{Keys: NewKeyPool([]string{"fake-key"}, 700, 95_000_000, 280), Logger: testLogger()}
	_, err := client.FindFullFeedVPs(context.Background(), "ris", true, 900_000, 10)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

// --- callBgproutes ---

func TestCallBgproutes_BadJSONBodyOn200(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/broken", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not valid json{{{"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{Keys: NewKeyPool([]string{"fake-key"}, 700, 95_000_000, 280), Logger: testLogger()}
	var out map[string]any
	err := client.callBgproutes(context.Background(), "/broken", &out)
	if err == nil {
		t.Fatal("expected a JSON decode error, got nil")
	}
}

func TestCallBgproutes_UnexpectedStatusCodeIsTerminal(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/notfound", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"detail":"not found"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{Keys: NewKeyPool([]string{"fake-key"}, 700, 95_000_000, 280), Logger: testLogger()}
	var out map[string]any
	err := client.callBgproutes(context.Background(), "/notfound", &out)
	if err == nil {
		t.Fatal("expected an error for 404, got nil")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("expected error to mention 404, got: %v", err)
	}
}

func TestCallBgproutes_RateLimiterHardCapEventuallyFails(t *testing.T) {
	// A KeyPool whose rate cap is already exhausted (0 capacity, nothing pruned yet)
	// makes WaitIfNeeded fail fast (no real sleep, maxTotalWait forces immediate
	// give-up) rather than callBgproutes ever reaching the network. With only one
	// key configured, every attempt hits the same exhausted limiter, so this now
	// takes maxRetries attempts (still fast, no real sleep) rather than failing on
	// the very first one — see the report-failure-and-try-next-key fix, which no
	// longer aborts the whole call on a single key's limiter error.
	keys := NewKeyPool([]string{"fake-key"}, 700, 95_000_000, 280)
	lim := keys.Limiters("fake-key")
	lim.Rate.cap = 1
	lim.Rate.maxTotalWait = time.Millisecond // force an immediate give-up
	lim.Rate.Record(1)                       // already at cap before any request is made

	client := &APIClient{Keys: keys, Logger: testLogger()}
	var out map[string]any
	err := client.callBgproutes(context.Background(), "/whatever", &out)
	if err == nil {
		t.Fatal("expected the rate limiter's hard cap to produce an error, got nil")
	}
	if !strings.Contains(err.Error(), "rate limiter") {
		t.Errorf("expected error to mention the rate limiter, got: %v", err)
	}
}

func TestCallBgproutes_ContextCancellationStopsRetryPromptly(t *testing.T) {
	origMaxRetries, origBackoff := maxRetries, baseBackoff
	maxRetries = 5
	baseBackoff = 10 * time.Second // deliberately long — cancellation should preempt this
	t.Cleanup(func() { maxRetries, baseBackoff = origMaxRetries, origBackoff })

	mux := http.NewServeMux()
	mux.HandleFunc("/rib", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{
		Keys:   NewKeyPool([]string{"fake-key"}, 700, 95_000_000, 280),
		Logger: testLogger(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	var out map[string]any
	err := client.callBgproutes(ctx, "/rib?vp_bgp_ids=1", &out)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error from cancelled context, got nil")
	}
	// Should return promptly after the 100ms context deadline, not wait out the 10s backoff.
	if elapsed > 2*time.Second {
		t.Errorf("expected cancellation to stop retries promptly, took %s", elapsed)
	}
}

func TestCallBgproutes_ExhaustedRetries_ErrorMessageNotGarbled(t *testing.T) {
	// Regression test: callBgproutes's lastErr was only ever set on network-error
	// failures. If every attempt instead failed via 429/5xx (no network error at
	// all — the far more common real-world case), lastErr stayed nil, and
	// fmt.Errorf("...: %w", nil) produced a garbled "%!w(<nil>)" in the final error
	// message instead of a real reason.
	origMaxRetries, origBackoff := maxRetries, baseBackoff
	maxRetries = 2
	baseBackoff = time.Millisecond
	t.Cleanup(func() { maxRetries, baseBackoff = origMaxRetries, origBackoff })

	mux := http.NewServeMux()
	mux.HandleFunc("/rib", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests) // every attempt fails via 429, never a network error
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{
		Keys:   NewKeyPool([]string{"fake-key"}, 700, 95_000_000, 280),
		Logger: testLogger(),
	}

	var out map[string]any
	err := client.callBgproutes(context.Background(), "/rib?vp_bgp_ids=1", &out)

	if err == nil {
		t.Fatal("expected an error after exhausting retries, got nil")
	}
	if strings.Contains(err.Error(), "%!w") {
		t.Errorf("error message is garbled (nil wrapped with %%w): %q", err.Error())
	}
	if !strings.Contains(err.Error(), "429") && !strings.Contains(err.Error(), "rate limited") {
		t.Errorf("expected error message to mention the actual failure reason (429/rate limited), got: %q", err.Error())
	}
}

// --- FetchAnnouncedPrefixes (ris-prefixes: AFI-split, historical, ribDate-pinned) ---

func TestFetchAnnouncedPrefixes_Success(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ris-prefixes/data.json", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"data": map[string]any{
				"prefixes": map[string]any{
					"v4": map[string]any{"originating": []string{"8.0.0.0/9"}},
					"v6": map[string]any{"originating": []string{}},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origRIPE := ripestatBaseURL
	ripestatBaseURL = srv.URL
	t.Cleanup(func() { ripestatBaseURL = origRIPE })

	prefixes, ok := FetchAnnouncedPrefixes(context.Background(), "3356", true, time.Now(), testLogger())
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(prefixes) != 1 || prefixes[0] != "8.0.0.0/9" {
		t.Errorf("unexpected prefixes: %v", prefixes)
	}
}

func TestFetchAnnouncedPrefixes_BadJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ris-prefixes/data.json", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not json"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origRIPE := ripestatBaseURL
	ripestatBaseURL = srv.URL
	t.Cleanup(func() { ripestatBaseURL = origRIPE })

	_, ok := FetchAnnouncedPrefixes(context.Background(), "3356", true, time.Now(), testLogger())
	if ok {
		t.Fatal("expected ok=false for bad JSON")
	}
}

func TestFetchAnnouncedPrefixes_NonRetryableStatusFailsImmediately(t *testing.T) {
	origMaxRetries, origBackoff := maxRetries, baseBackoff
	maxRetries = 3
	baseBackoff = time.Millisecond
	t.Cleanup(func() { maxRetries, baseBackoff = origMaxRetries, origBackoff })

	calls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/ris-prefixes/data.json", func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusBadRequest)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origRIPE := ripestatBaseURL
	ripestatBaseURL = srv.URL
	t.Cleanup(func() { ripestatBaseURL = origRIPE })

	_, ok := FetchAnnouncedPrefixes(context.Background(), "3356", true, time.Now(), testLogger())
	if ok {
		t.Fatal("expected ok=false")
	}
	if calls != 1 {
		t.Errorf("expected exactly 1 call (400 is not retried), got %d", calls)
	}
}

func TestFetchAnnouncedPrefixes_RetriesOn5xxThenSucceeds(t *testing.T) {
	origMaxRetries, origBackoff := maxRetries, baseBackoff
	maxRetries = 3
	baseBackoff = time.Millisecond
	t.Cleanup(func() { maxRetries, baseBackoff = origMaxRetries, origBackoff })

	calls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/ris-prefixes/data.json", func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		resp := map[string]any{
			"data": map[string]any{
				"prefixes": map[string]any{
					"v4": map[string]any{"originating": []string{"9.0.0.0/8"}},
					"v6": map[string]any{"originating": []string{}},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origRIPE := ripestatBaseURL
	ripestatBaseURL = srv.URL
	t.Cleanup(func() { ripestatBaseURL = origRIPE })

	prefixes, ok := FetchAnnouncedPrefixes(context.Background(), "3356", true, time.Now(), testLogger())
	if !ok {
		t.Fatal("expected eventual success after retry")
	}
	if len(prefixes) != 1 || prefixes[0] != "9.0.0.0/8" {
		t.Errorf("unexpected prefixes: %v", prefixes)
	}
	if calls != 2 {
		t.Errorf("expected exactly 2 calls (1 fail + 1 success), got %d", calls)
	}
}

func TestFetchAnnouncedPrefixes_NetworkErrorRetriesThenGivesUp(t *testing.T) {
	origMaxRetries, origBackoff := maxRetries, baseBackoff
	maxRetries = 2
	baseBackoff = time.Millisecond
	t.Cleanup(func() { maxRetries, baseBackoff = origMaxRetries, origBackoff })

	origRIPE := ripestatBaseURL
	ripestatBaseURL = "http://127.0.0.1:1" // nothing listening -> connection refused
	t.Cleanup(func() { ripestatBaseURL = origRIPE })

	_, ok := FetchAnnouncedPrefixes(context.Background(), "3356", true, time.Now(), testLogger())
	if ok {
		t.Fatal("expected ok=false after exhausting retries on network error")
	}
}

func TestFetchAnnouncedPrefixes_V6SelectsV6Field(t *testing.T) {
	// Regression coverage for the AFI split itself: a v6 request must read
	// Data.Prefixes.V6.Originating, not silently fall through to V4's.
	mux := http.NewServeMux()
	mux.HandleFunc("/ris-prefixes/data.json", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"data": map[string]any{
				"prefixes": map[string]any{
					"v4": map[string]any{"originating": []string{"8.0.0.0/9"}},
					"v6": map[string]any{"originating": []string{"2001:db8::/32"}},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origRIPE := ripestatBaseURL
	ripestatBaseURL = srv.URL
	t.Cleanup(func() { ripestatBaseURL = origRIPE })

	prefixes, ok := FetchAnnouncedPrefixes(context.Background(), "3356", false, time.Now(), testLogger())
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(prefixes) != 1 || prefixes[0] != "2001:db8::/32" {
		t.Errorf("expected v6 originating prefixes only, got %v", prefixes)
	}
}

func TestCallBgproutes_RotatesToNextKeyWhenOneIsCapped(t *testing.T) {
	// Two keys: "bad" is already at its hard rate cap (WaitIfNeeded fails fast), the
	// other, "good", is healthy. callBgproutes should report the failure on "bad",
	// rotate to "good" on the next attempt, and succeed — not abort the whole call
	// just because the first-tried key was capped.
	const body = `{"seconds":0.01,"data":{"bgp":{},"bmp":{}}}`
	var gotRequestWithKey string
	mux := http.NewServeMux()
	mux.HandleFunc("/rib", func(w http.ResponseWriter, r *http.Request) {
		gotRequestWithKey = r.Header.Get("x-api-key")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	keys := NewKeyPool([]string{"bad", "good"}, 700, 95_000_000, 280)
	badLim := keys.Limiters("bad")
	badLim.Rate.cap = 1
	badLim.Rate.maxTotalWait = time.Millisecond // force an immediate give-up on "bad"
	badLim.Rate.Record(1)                       // already at cap before any request is made

	// Force the rotation offset so "bad" is tried first (NextKey's tie-breaking
	// otherwise depends on internal call-count state, not key list order) —
	// same-package access to the unexported field is fine here, this is a
	// white-box test of the retry-and-rotate behavior specifically.
	keys.i = 1

	client := &APIClient{Keys: keys, Logger: testLogger()}
	var out map[string]any
	err := client.callBgproutes(context.Background(), "/rib?vp_bgp_ids=1", &out)

	if err != nil {
		t.Fatalf("expected success via the healthy key, got error: %v", err)
	}
	if gotRequestWithKey != "good" {
		t.Errorf("expected the request to eventually use the healthy key, got x-api-key=%q", gotRequestWithKey)
	}
	if fails := keys.ConsecutiveFailures("bad"); fails == 0 {
		t.Error("expected the capped key's failure to be reported, not silently skipped")
	}
}
