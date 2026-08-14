package tier1exclusions

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeBgproutesServer builds an httptest.Server simulating just enough of
// bgproutes.io's /vantage_points and /rib endpoints for Run() to complete a full
// pipeline pass. failRibForCovers, if non-empty, makes /rib return 500 whenever the
// requested prefix_filter contains one of those cover strings — used to exercise the
// "run completes with errors, progress file retained" path.
func fakeBgproutesServer(t *testing.T, vpID string, failRibForCovers map[string]bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/vantage_points", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"data": map[string]any{
				"bgp": []map[string]any{
					{
						"id":          vpID,
						"asn":         "64500",
						"source":      "ris",
						"org_name":    "TESTORG",
						"rib_size_v4": 1_100_000,
						"rib_size_v6": 260_000,
						"status":      []string{"ready"},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/rib", func(w http.ResponseWriter, r *http.Request) {
		filter := r.URL.Query().Get("prefix_filter")
		for badCover := range failRibForCovers {
			if strings.Contains(filter, badCover) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"simulated failure"}`))
				return
			}
		}
		// One synthetic customer more-specific under whatever the first requested
		// cover is, so the pipeline produces a real, non-empty exclusion.
		resp := map[string]any{
			"seconds": 0.05,
			"data": map[string]any{
				"bgp": map[string]any{
					vpID: map[string]any{
						"8.0.1.0/24": []any{"64500 174 99999", nil, "", "", -1},
					},
				},
				"bmp": map[string]any{},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	return httptest.NewServer(mux)
}

// fakeRipestatServer simulates RIPEstat's ris-prefixes endpoint, always returning the
// same small, fixed prefix list regardless of which ASN is asked for — enough for a
// single-ASN end-to-end test. Only v4 is populated since these tests use AFI 4.
func fakeRipestatServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/ris-prefixes/data.json", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("query_time"); got == "" {
			t.Errorf("expected query_time param to be set, got empty")
		}
		resp := map[string]any{
			"data": map[string]any{
				"prefixes": map[string]any{
					"v4": map[string]any{"originating": []string{"8.0.0.0/16"}},
					"v6": map[string]any{"originating": []string{}},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	return httptest.NewServer(mux)
}

// fakeRipestatServerManyPrefixes returns enough distinct /24 prefixes, spread across
// enough /12 covers, that a fully-failing rib() endpoint will exceed
// MaxConsecutiveBatchFailures — needed to actually exercise the circuit breaker at the
// Run() level (a single-prefix ASN never generates enough batches to trip it).
func fakeRipestatServerManyPrefixes(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/ris-prefixes/data.json", func(w http.ResponseWriter, r *http.Request) {
		numCovers := (MaxConsecutiveBatchFailures + 2) * 10
		prefixes := make([]string, 0, numCovers)
		for i := 0; i < numCovers; i++ {
			prefixes = append(prefixes, fmt.Sprintf("%d.0.0.0/16", (i%200)+1)) // spread across first octet
		}
		resp := map[string]any{
			"data": map[string]any{
				"prefixes": map[string]any{
					"v4": map[string]any{"originating": prefixes},
					"v6": map[string]any{"originating": []string{}},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	return httptest.NewServer(mux)
}

// withFakeAPIs points the package's HTTP clients at fake servers for the duration of
// one test, restoring the real URLs on cleanup.
func withFakeAPIs(t *testing.T, bgproutes, ripestat *httptest.Server) {
	t.Helper()
	origBG, origRIPE := bgproutesBaseURL, ripestatBaseURL
	bgproutesBaseURL = bgproutes.URL
	ripestatBaseURL = ripestat.URL
	t.Cleanup(func() {
		bgproutesBaseURL = origBG
		ripestatBaseURL = origRIPE
		bgproutes.Close()
		ripestat.Close()
	})
}

func TestRun_EndToEnd_SuccessRemovesProgressFile(t *testing.T) {
	bg := fakeBgproutesServer(t, "215", nil)
	ripe := fakeRipestatServer(t)
	withFakeAPIs(t, bg, ripe)

	outDir := t.TempDir()
	ribDate := time.Date(2026, 8, 6, 8, 0, 0, 0, time.UTC)

	cfg := Config{
		TargetASNs: []string{"3356"},
		RIBDate:    ribDate,
		OutputDir:  outDir,
		Keys:       NewKeyPool([]string{"fake-key"}, 700, 95_000_000, 280),
		AFIs: map[int]AFIConfig{
			4: {
				GroupPrefixLen: 12,
				PinnedVPIDs:    []string{"215"},
				IsV4:           true,
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	result, err := Run(context.Background(), cfg, 4, logger)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Result should contain the exclusion the fake server injected.
	entries, ok := result["3356"]
	if !ok || len(entries) == 0 {
		t.Fatalf("expected exclusions for AS3356, got %+v", result)
	}
	found := false
	for _, e := range entries {
		for _, excl := range e.Exclusions {
			if excl == "8.0.1.0/24" {
				found = true
			}
		}
	}
	if !found {
		t.Errorf("expected exclusion 8.0.1.0/24 somewhere in result, got %+v", entries)
	}

	// Result file should exist on disk.
	outPath := filepath.Join(outDir, "tier1_exclusions_v4_2026-08-06.json")
	if _, err := os.Stat(outPath); err != nil {
		t.Errorf("expected result file to exist: %v", err)
	}

	// Progress file should have been cleaned up after full success.
	progressPath := filepath.Join(outDir, "tier1_progress_v4_2026-08-06.json")
	if _, err := os.Stat(progressPath); !os.IsNotExist(err) {
		t.Errorf("expected progress file to be removed after successful run, stat err=%v", err)
	}

	// Metadata file should exist and record the VP actually used — this is the
	// provenance record that lets two snapshots later be checked for a VP-set
	// mismatch, rather than that being indistinguishable from real churn.
	metaPath := filepath.Join(outDir, "tier1_meta_v4_2026-08-06.json")
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("expected metadata file to exist: %v", err)
	}
	var meta RunMetadata
	if err := json.Unmarshal(metaData, &meta); err != nil {
		t.Fatalf("failed to parse metadata file: %v", err)
	}
	if len(meta.VPIDs) != 1 || meta.VPIDs[0] != "215" {
		t.Errorf("expected metadata to record VP 215, got %v", meta.VPIDs)
	}
	if !meta.Succeeded {
		t.Error("expected metadata to record Succeeded=true")
	}
}

func TestRun_EndToEnd_PartialFailureRetainsProgressFile(t *testing.T) {
	// Force EVERY /rib call to fail so enough consecutive batch failures accumulate
	// to trip the circuit breaker, and confirm the progress file is retained (not
	// cleaned up) since the run did not fully succeed. Needs enough covers to exceed
	// MaxConsecutiveBatchFailures batches — a single-prefix ASN never generates
	// enough batches to trip it (see fakeRipestatServerManyPrefixes).
	origMaxRetries, origBackoff := maxRetries, baseBackoff
	maxRetries = 1
	baseBackoff = time.Millisecond
	t.Cleanup(func() { maxRetries, baseBackoff = origMaxRetries, origBackoff })

	bg := fakeBgproutesServer(t, "215", map[string]bool{"": true}) // fail every /rib call unconditionally
	ripe := fakeRipestatServerManyPrefixes(t)
	withFakeAPIs(t, bg, ripe)

	outDir := t.TempDir()
	ribDate := time.Date(2026, 8, 6, 8, 0, 0, 0, time.UTC)

	cfg := Config{
		TargetASNs: []string{"3356"},
		RIBDate:    ribDate,
		OutputDir:  outDir,
		Keys:       NewKeyPool([]string{"fake-key"}, 700, 95_000_000, 280),
		AFIs: map[int]AFIConfig{
			4: {
				GroupPrefixLen: 12,
				PinnedVPIDs:    []string{"215"},
				IsV4:           true,
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := Run(context.Background(), cfg, 4, logger); err != nil {
		t.Fatalf("Run itself should not return an error (per-ASN failures are logged and skipped): %v", err)
	}

	progressPath := filepath.Join(outDir, "tier1_progress_v4_2026-08-06.json")
	if _, err := os.Stat(progressPath); err != nil {
		t.Errorf("expected progress file to be RETAINED after a partial failure, but stat failed: %v", err)
	}
}

func TestRun_ReturnsErrorForUnknownAFI(t *testing.T) {
	cfg := Config{
		AFIs: map[int]AFIConfig{4: {GroupPrefixLen: 12, PinnedVPIDs: []string{"1"}}},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := Run(context.Background(), cfg, 6, logger); err == nil {
		t.Error("expected an error for an AFI with no config, got nil")
	}
}
