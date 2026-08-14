package tier1exclusions

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// fakeRibQuerier lets tests control exactly what QueryRIBBatch returns per call,
// without any real network access.
type fakeRibQuerier struct {
	calls     [][]string // records the supernets requested per call, in order
	responses []fakeResponse
	callIdx   int
}

type fakeResponse struct {
	moreSpecifics []MoreSpecific
	err           error
}

func (f *fakeRibQuerier) QueryRIBBatch(ctx context.Context, vpIDs []string, supernets []string, afiIs4 bool, ribDate time.Time) ([]MoreSpecific, error) {
	f.calls = append(f.calls, append([]string(nil), supernets...))
	if f.callIdx >= len(f.responses) {
		return nil, errors.New("fakeRibQuerier: no more scripted responses")
	}
	r := f.responses[f.callIdx]
	f.callIdx++
	return r.moreSpecifics, r.err
}

const testBatchSize = 10 // most tests want batching behavior exercised, matching the old default

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func mkGroup(t *testing.T, supernet string, prefixes ...string) QueryGroup {
	t.Helper()
	s, err := netip.ParsePrefix(supernet)
	if err != nil {
		t.Fatalf("bad supernet %q: %v", supernet, err)
	}
	var ps []netip.Prefix
	for _, pr := range prefixes {
		pp, err := netip.ParsePrefix(pr)
		if err != nil {
			t.Fatalf("bad prefix %q: %v", pr, err)
		}
		ps = append(ps, pp)
	}
	return QueryGroup{Supernet: s, Prefixes: ps}
}

func TestProcessQueryGroups_ZeroBatchSizeDefaultsToOne(t *testing.T) {
	groups := []QueryGroup{
		mkGroup(t, "8.0.0.0/16"),
		mkGroup(t, "9.0.0.0/16"),
	}
	fq := &fakeRibQuerier{
		responses: []fakeResponse{{}, {}}, // two empty-but-successful responses
	}

	_, err := processQueryGroups(context.Background(), fq, "3356", groups, true, []string{"215"}, time.Now(), 0, noopLogger(),
		nil, AsnProgress{}, func(map[string][]string, AsnProgress) {})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// batchSize=0 should default to DefaultBatchSize=1, meaning 2 groups -> 2 separate calls.
	if len(fq.calls) != 2 {
		t.Errorf("expected batchSize=0 to default to 1 (2 groups -> 2 calls), got %d calls: %v",
			len(fq.calls), fq.calls)
	}
}

func TestProcessQueryGroups_BatchSizeControlsGrouping(t *testing.T) {
	groups := []QueryGroup{
		mkGroup(t, "8.0.0.0/16"),
		mkGroup(t, "9.0.0.0/16"),
		mkGroup(t, "10.0.0.0/16"),
	}
	fq := &fakeRibQuerier{
		responses: []fakeResponse{{}, {}}, // 3 groups at batchSize=2 -> 2 calls (2+1)
	}

	_, err := processQueryGroups(context.Background(), fq, "3356", groups, true, []string{"215"}, time.Now(), 2, noopLogger(),
		nil, AsnProgress{}, func(map[string][]string, AsnProgress) {})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fq.calls) != 2 {
		t.Fatalf("expected 2 calls (batches of 2, then 1), got %d: %v", len(fq.calls), fq.calls)
	}
	if len(fq.calls[0]) != 2 {
		t.Errorf("expected first call to batch 2 groups, got %d: %v", len(fq.calls[0]), fq.calls[0])
	}
	if len(fq.calls[1]) != 1 {
		t.Errorf("expected second call to have the remaining 1 group, got %d: %v", len(fq.calls[1]), fq.calls[1])
	}
}

func TestBuildRunMetadata(t *testing.T) {
	result := ExclusionResult{
		"3356": []ParentExclusions{
			{ParentBlock: "8.0.0.0/12", Exclusions: []string{"8.0.1.0/24", "8.0.2.0/24"}},
			{ParentBlock: "9.0.0.0/12", Exclusions: []string{}},
		},
	}
	ribDate := time.Date(2026, 8, 10, 8, 0, 0, 0, time.UTC)

	meta := buildRunMetadata(4, ribDate, 12, []string{"215", "1749"}, result, true)

	if meta.AFI != 4 {
		t.Errorf("expected AFI 4, got %d", meta.AFI)
	}
	if meta.RIBDate != "2026-08-10T08:00:00" {
		t.Errorf("unexpected RIBDate: %s", meta.RIBDate)
	}
	if meta.VPCount != 2 || len(meta.VPIDs) != 2 {
		t.Errorf("expected 2 VPs recorded, got %+v", meta.VPIDs)
	}
	if !meta.Succeeded {
		t.Error("expected Succeeded=true")
	}
	counts, ok := meta.Counts["3356"]
	if !ok {
		t.Fatal("expected counts for AS3356")
	}
	if counts.ParentBlocks != 2 {
		t.Errorf("expected 2 parent blocks, got %d", counts.ParentBlocks)
	}
	if counts.Exclusions != 2 {
		t.Errorf("expected 2 total exclusions, got %d", counts.Exclusions)
	}
}

func TestToParentExclusions_EmptyExclusionsMarshalAsEmptyArrayNotNull(t *testing.T) {
	// Regression test: a parent block with zero exclusions used to leave its
	// Exclusions field as a nil []string, which encoding/json marshals as `null`,
	// not `[]` — silently breaking every downstream tool (compare scripts, etc.)
	// that expects a real, iterable array.
	excl := map[string][]string{
		"8.0.0.0/12": nil, // zero exclusions for this parent
		"9.0.0.0/12": {"9.0.1.0/24"},
	}
	result := toParentExclusions(excl)

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if strings.Contains(string(data), "null") {
		t.Errorf("expected no 'null' anywhere in the JSON output, got: %s", data)
	}

	for _, pe := range result {
		if pe.ParentBlock == "8.0.0.0/12" {
			if pe.Exclusions == nil {
				t.Error("expected a non-nil (empty) slice for a parent with zero exclusions, got nil")
			}
			if len(pe.Exclusions) != 0 {
				t.Errorf("expected zero exclusions, got %v", pe.Exclusions)
			}
		}
	}
}

func TestProcessQueryGroups_HappyPath(t *testing.T) {
	groups := []QueryGroup{
		mkGroup(t, "8.0.0.0/16", "8.0.0.0/16"),
	}
	fq := &fakeRibQuerier{
		responses: []fakeResponse{
			{moreSpecifics: []MoreSpecific{
				{Prefix: mustP(t, "8.0.1.0/24"), Origins: set("64500")},
			}},
		},
	}

	var checkpoints int
	result, err := processQueryGroups(context.Background(), fq, "3356", groups, true, []string{"215"}, time.Now(), testBatchSize, noopLogger(),
		nil, AsnProgress{}, func(map[string][]string, AsnProgress) { checkpoints++ })

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].ParentBlock != "8.0.0.0/16" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if !reflect.DeepEqual(result[0].Exclusions, []string{"8.0.1.0/24"}) {
		t.Errorf("expected exclusion 8.0.1.0/24, got %v", result[0].Exclusions)
	}
	if checkpoints == 0 {
		t.Error("expected at least one checkpoint call")
	}
}

func TestProcessQueryGroups_ResumesFromPriorProgress(t *testing.T) {
	// Two query groups; the first is already marked done in priorProgress with its
	// exclusion pre-seeded via existingExcl — only the second should generate a
	// fresh QueryRIBBatch call.
	groups := []QueryGroup{
		mkGroup(t, "8.0.0.0/16", "8.0.0.0/16"),
		mkGroup(t, "9.0.0.0/16", "9.0.0.0/16"),
	}
	fq := &fakeRibQuerier{
		responses: []fakeResponse{
			{moreSpecifics: []MoreSpecific{
				{Prefix: mustP(t, "9.0.1.0/24"), Origins: set("64500")},
			}},
		},
	}

	existingExcl := map[string][]string{"8.0.0.0/16": {"8.0.9.0/24"}}
	priorProgress := AsnProgress{DoneGroups: []string{"8.0.0.0/16"}}

	result, err := processQueryGroups(context.Background(), fq, "3356", groups, true, []string{"215"}, time.Now(), testBatchSize, noopLogger(),
		existingExcl, priorProgress, func(map[string][]string, AsnProgress) {})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fq.calls) != 1 {
		t.Fatalf("expected exactly 1 API call (only the un-done group), got %d: %v", len(fq.calls), fq.calls)
	}
	if fq.calls[0][0] != "9.0.0.0/16" {
		t.Errorf("expected the un-done group to be queried, got %v", fq.calls[0])
	}

	byParent := map[string][]string{}
	for _, r := range result {
		byParent[r.ParentBlock] = r.Exclusions
	}
	if !reflect.DeepEqual(byParent["8.0.0.0/16"], []string{"8.0.9.0/24"}) {
		t.Errorf("expected pre-seeded exclusion preserved, got %v", byParent["8.0.0.0/16"])
	}
	if !reflect.DeepEqual(byParent["9.0.0.0/16"], []string{"9.0.1.0/24"}) {
		t.Errorf("expected fresh exclusion for the un-done group, got %v", byParent["9.0.0.0/16"])
	}
}

func TestProcessQueryGroups_StaleProgressForRemovedGroupIsIgnored(t *testing.T) {
	// priorProgress references a group that no longer exists in this snapshot's
	// group list (e.g. the ASN's prefixes changed) — it must be pruned, not treated
	// as still-valid "done" state.
	groups := []QueryGroup{
		mkGroup(t, "8.0.0.0/16", "8.0.0.0/16"),
	}
	fq := &fakeRibQuerier{
		responses: []fakeResponse{{moreSpecifics: nil}},
	}
	priorProgress := AsnProgress{DoneGroups: []string{"99.0.0.0/16"}} // stale, not in `groups`

	_, err := processQueryGroups(context.Background(), fq, "3356", groups, true, []string{"215"}, time.Now(), testBatchSize, noopLogger(),
		nil, priorProgress, func(map[string][]string, AsnProgress) {})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fq.calls) != 1 {
		t.Fatalf("expected the current group to still be queried despite stale progress, got %d calls", len(fq.calls))
	}
}

func TestProcessQueryGroups_CircuitBreakerTripsAfterMaxConsecutiveFailures(t *testing.T) {
	// The circuit breaker counts consecutive BATCH failures (each batch = up to
	// testBatchSize groups = 1 API call), not individual group failures — so enough
	// groups are needed to generate MaxConsecutiveBatchFailures+ batches.
	numGroups := (MaxConsecutiveBatchFailures + 2) * testBatchSize
	var groups []QueryGroup
	for i := 0; i < numGroups; i++ {
		// Distinct prefixes so BuildQueryGroups-style dedup concerns don't apply
		// here — processQueryGroups batches whatever QueryGroup slice it's given directly.
		groups = append(groups, mkGroup(t, netip.PrefixFrom(
			netip.AddrFrom4([4]byte{8, byte(i / 256), byte(i % 256), 0}), 24).String()))
	}
	fq := &fakeRibQuerier{}
	for i := 0; i < numGroups/testBatchSize+1; i++ {
		fq.responses = append(fq.responses, fakeResponse{err: errors.New("simulated failure")})
	}

	_, err := processQueryGroups(context.Background(), fq, "3356", groups, true, []string{"215"}, time.Now(), testBatchSize, noopLogger(),
		nil, AsnProgress{}, func(map[string][]string, AsnProgress) {})

	if err == nil {
		t.Fatal("expected circuit breaker to trip and return an error, got nil")
	}
}

func mustP(t *testing.T, s string) netip.Prefix {
	t.Helper()
	p, err := netip.ParsePrefix(s)
	if err != nil {
		t.Fatalf("bad prefix %q: %v", s, err)
	}
	return p
}

func TestProgressFile_SaveLoadRemoveRoundTrip(t *testing.T) {
	// Sanity check for the save/load/remove mechanics Run() relies on to implement
	// "delete the progress file after a fully successful run" — the progress file
	// is pure resume scaffolding with no value once nothing's left to resume.
	dir := t.TempDir()
	path := dir + "/progress.json"

	// No file yet -> empty state, no error.
	p, err := loadProgress(path)
	if err != nil {
		t.Fatalf("unexpected error loading missing progress file: %v", err)
	}
	if len(p) != 0 {
		t.Errorf("expected empty progress for missing file, got %v", p)
	}

	// Save, then load back.
	p["3356"] = AsnProgress{DoneGroups: []string{"8.0.0.0/12"}}
	if err := saveProgress(path, p); err != nil {
		t.Fatalf("saveProgress failed: %v", err)
	}
	loaded, err := loadProgress(path)
	if err != nil {
		t.Fatalf("loadProgress failed: %v", err)
	}
	if !reflect.DeepEqual(loaded["3356"].DoneGroups, []string{"8.0.0.0/12"}) {
		t.Errorf("round-trip mismatch: got %v", loaded["3356"])
	}

	// Simulate the post-success cleanup Run() performs.
	if err := os.Remove(path); err != nil {
		t.Fatalf("failed to remove progress file: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("expected progress file to be gone after removal")
	}

	// Loading after removal should behave the same as "never existed".
	afterRemoval, err := loadProgress(path)
	if err != nil {
		t.Fatalf("unexpected error loading removed progress file: %v", err)
	}
	if len(afterRemoval) != 0 {
		t.Errorf("expected empty progress after removal, got %v", afterRemoval)
	}
}

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "conf.json")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}
	return path
}

func TestLoadFileConfig_Valid(t *testing.T) {
	path := writeTempConfig(t, `{
		"target_asns": ["3356", "1299"],
		"output_dir": ".",
		"afis": {
			"4": {"group_prefix_len": 12, "pinned_vp_ids": ["1", "2"]}
		}
	}`)

	fc, err := LoadFileConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fc.TargetASNs) != 2 {
		t.Errorf("expected 2 ASNs, got %d", len(fc.TargetASNs))
	}
	afi4, ok := fc.AFIs["4"]
	if !ok {
		t.Fatal("expected afi 4 config present")
	}
	if afi4.GroupPrefixLen != 12 {
		t.Errorf("expected group_prefix_len=12, got %d", afi4.GroupPrefixLen)
	}
}

func TestLoadFileConfig_RejectsEmptyASNs(t *testing.T) {
	path := writeTempConfig(t, `{"target_asns": [], "afis": {}}`)
	_, err := LoadFileConfig(path)
	if err == nil {
		t.Fatal("expected error for empty target_asns, got nil")
	}
}

func TestLoadFileConfig_RejectsAFIWithNoVPSource(t *testing.T) {
	// An AFI with neither pinned VPs nor a usable threshold for live lookup is a
	// config that would silently fail at runtime — better to catch it at load time.
	path := writeTempConfig(t, `{
		"target_asns": ["3356"],
		"afis": {
			"4": {"group_prefix_len": 12}
		}
	}`)
	_, err := LoadFileConfig(path)
	if err == nil {
		t.Fatal("expected error for AFI with no pinned VPs and no threshold, got nil")
	}
}

func TestLoadFileConfig_MissingFile(t *testing.T) {
	_, err := LoadFileConfig("/nonexistent/path/conf.json")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestAFIFile_ToAFIConfig_SetsIsV4Correctly(t *testing.T) {
	a := AFIFile{GroupPrefixLen: 12, PinnedVPIDs: []string{"1"}}
	if got := a.ToAFIConfig(4); !got.IsV4 {
		t.Error("expected IsV4=true for afi=4")
	}
	if got := a.ToAFIConfig(6); got.IsV4 {
		t.Error("expected IsV4=false for afi=6")
	}
}

// --- resolveVPs ---

func vpStatusServer(t *testing.T, statuses map[string][]string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/vantage_points", func(w http.ResponseWriter, r *http.Request) {
		var bgp []map[string]any
		for id, st := range statuses {
			bgp = append(bgp, map[string]any{"id": id, "status": st})
		}
		resp := map[string]any{"data": map[string]any{"bgp": bgp}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	return httptest.NewServer(mux)
}

func TestResolveVPs_LiveLookupWhenNoPinnedVPs(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/vantage_points", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{"data": map[string]any{"bgp": []map[string]any{
			{"id": "215", "rib_size_v4": 1_100_000},
		}}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{Keys: NewKeyPool([]string{"k"}, 700, 95_000_000, 280), Logger: testLogger()}
	cfg := AFIConfig{Sources: "ris", IsV4: true, VPSizeThreshold: 900_000} // no PinnedVPIDs

	ids, err := resolveVPs(context.Background(), client, cfg, time.Now(), testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 1 || ids[0] != "215" {
		t.Errorf("expected live-lookup result [215], got %v", ids)
	}
}

func TestResolveVPs_LiveLookupErrorPropagates(t *testing.T) {
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

	client := &APIClient{Keys: NewKeyPool([]string{"k"}, 700, 95_000_000, 280), Logger: testLogger()}
	cfg := AFIConfig{Sources: "ris", IsV4: true, VPSizeThreshold: 900_000}

	_, err := resolveVPs(context.Background(), client, cfg, time.Now(), testLogger())
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestResolveVPs_AllPinnedHealthy(t *testing.T) {
	srv := vpStatusServer(t, map[string][]string{"1": {"ready"}, "2": {"up"}})
	defer srv.Close()
	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{Keys: NewKeyPool([]string{"k"}, 700, 95_000_000, 280), Logger: testLogger()}
	cfg := AFIConfig{PinnedVPIDs: []string{"1", "2"}}

	ids, err := resolveVPs(context.Background(), client, cfg, time.Now(), testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 2 {
		t.Errorf("expected both pinned VPs kept, got %v", ids)
	}
}

func TestResolveVPs_StatusCheckFailsTrustsPinnedList(t *testing.T) {
	origMaxRetries, origBackoff := maxRetries, baseBackoff
	maxRetries = 1
	baseBackoff = time.Millisecond
	t.Cleanup(func() { maxRetries, baseBackoff = origMaxRetries, origBackoff })

	origBG := bgproutesBaseURL
	bgproutesBaseURL = "http://127.0.0.1:1" // connection refused -> check fails
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{Keys: NewKeyPool([]string{"k"}, 700, 95_000_000, 280), Logger: testLogger()}
	cfg := AFIConfig{PinnedVPIDs: []string{"1", "2"}}

	ids, err := resolveVPs(context.Background(), client, cfg, time.Now(), testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 2 || ids[0] != "1" || ids[1] != "2" {
		t.Errorf("expected pinned list trusted as-is, got %v", ids)
	}
}

func TestResolveVPs_DeadVPSubstitutedFromBackup(t *testing.T) {
	srv := vpStatusServer(t, map[string][]string{"1": {"ready"}, "2": {"down"}})
	defer srv.Close()
	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{Keys: NewKeyPool([]string{"k"}, 700, 95_000_000, 280), Logger: testLogger()}
	cfg := AFIConfig{PinnedVPIDs: []string{"1", "2"}, BackupVPIDs: []string{"3"}}

	ids, err := resolveVPs(context.Background(), client, cfg, time.Now(), testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found3 := false
	for _, id := range ids {
		if id == "3" {
			found3 = true
		}
		if id == "2" {
			t.Error("dead VP 2 should have been dropped")
		}
	}
	if !found3 {
		t.Errorf("expected backup VP 3 substituted in, got %v", ids)
	}
}

func TestResolveVPs_InsufficientBackupsStillReturnsWhatItHas(t *testing.T) {
	srv := vpStatusServer(t, map[string][]string{"1": {"ready"}, "2": {"down"}, "3": {"down"}})
	defer srv.Close()
	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{Keys: NewKeyPool([]string{"k"}, 700, 95_000_000, 280), Logger: testLogger()}
	// 2 dead VPs, only 1 backup available -> insufficient, but should still return what it has.
	cfg := AFIConfig{PinnedVPIDs: []string{"1", "2", "3"}, BackupVPIDs: []string{"4"}}

	ids, err := resolveVPs(context.Background(), client, cfg, time.Now(), testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// good=[1], backups=[4], n=min(2 dead, 1 backup)=1 -> result = [1, 4]
	if len(ids) != 2 {
		t.Errorf("expected 2 usable VPs (1 healthy + 1 backup), got %v", ids)
	}
}

func TestResolveVPs_AllDeadNoBackupsReturnsError(t *testing.T) {
	srv := vpStatusServer(t, map[string][]string{"1": {"down"}, "2": {"down"}})
	defer srv.Close()
	origBG := bgproutesBaseURL
	bgproutesBaseURL = srv.URL
	t.Cleanup(func() { bgproutesBaseURL = origBG })

	client := &APIClient{Keys: NewKeyPool([]string{"k"}, 700, 95_000_000, 280), Logger: testLogger()}
	cfg := AFIConfig{PinnedVPIDs: []string{"1", "2"}} // no backups at all

	_, err := resolveVPs(context.Background(), client, cfg, time.Now(), testLogger())
	if err == nil {
		t.Fatal("expected an error when all pinned VPs are down and no backups exist")
	}
}

func TestContainsStr(t *testing.T) {
	ss := []string{"a", "b", "c"}
	if !containsStr(ss, "b") {
		t.Error("expected true for present element")
	}
	if containsStr(ss, "z") {
		t.Error("expected false for absent element")
	}
	if containsStr(nil, "a") {
		t.Error("expected false for nil slice")
	}
}

// --- small helpers: sample, afiLabel, appendUnique, removeAll ---

func TestSample(t *testing.T) {
	if got := sample([]string{"a", "b", "c"}, 5); len(got) != 3 {
		t.Errorf("expected all 3 elements when n > len, got %v", got)
	}
	if got := sample([]string{"a", "b", "c"}, 2); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("expected first 2 elements, got %v", got)
	}
	if got := sample(nil, 5); len(got) != 0 {
		t.Errorf("expected empty result for nil input, got %v", got)
	}
	if got := sample([]string{"a", "b"}, 0); len(got) != 0 {
		t.Errorf("expected empty result for n=0, got %v", got)
	}
}

func TestAfiLabel(t *testing.T) {
	if got := afiLabel(true); got != "4" {
		t.Errorf("afiLabel(true) = %q, want \"4\"", got)
	}
	if got := afiLabel(false); got != "6" {
		t.Errorf("afiLabel(false) = %q, want \"6\"", got)
	}
}

func TestAppendUnique(t *testing.T) {
	if got := appendUnique([]string{"a", "b"}, "c"); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("expected new element appended, got %v", got)
	}
	if got := appendUnique([]string{"a", "b"}, "b"); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("expected no duplicate on existing element, got %v", got)
	}
	if got := appendUnique(nil, "a"); !reflect.DeepEqual(got, []string{"a"}) {
		t.Errorf("expected single-element result from nil slice, got %v", got)
	}
}

func TestRemoveAll(t *testing.T) {
	if got := removeAll([]string{"a", "b", "c"}, "b"); !reflect.DeepEqual(got, []string{"a", "c"}) {
		t.Errorf("expected 'b' removed, got %v", got)
	}
	if got := removeAll([]string{"a", "b"}, "z"); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("expected no change when target absent, got %v", got)
	}
	if got := removeAll([]string{"a", "a", "b"}, "a"); !reflect.DeepEqual(got, []string{"b"}) {
		t.Errorf("expected all occurrences removed, got %v", got)
	}
	if got := removeAll(nil, "a"); len(got) != 0 {
		t.Errorf("expected empty result for nil input, got %v", got)
	}
}

// --- processASN ---

func TestProcessASN_HappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ris-prefixes/data.json", func(w http.ResponseWriter, r *http.Request) {
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
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origRIPE := ripestatBaseURL
	ripestatBaseURL = srv.URL
	t.Cleanup(func() { ripestatBaseURL = origRIPE })

	fq := &fakeRibQuerier{
		responses: []fakeResponse{
			{moreSpecifics: []MoreSpecific{
				{Prefix: mustP(t, "8.0.1.0/24"), Origins: set("64500")},
			}},
		},
	}

	result, err := processASN(context.Background(), fq, "3356", true, 12, []string{"215"},
		time.Now(), testBatchSize, noopLogger(), nil, AsnProgress{}, func(map[string][]string, AsnProgress) {})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].ParentBlock != "8.0.0.0/16" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if !reflect.DeepEqual(result[0].Exclusions, []string{"8.0.1.0/24"}) {
		t.Errorf("expected exclusion 8.0.1.0/24, got %v", result[0].Exclusions)
	}
}

func TestProcessASN_RIPEstatFetchFailureReturnsError(t *testing.T) {
	origMaxRetries, origBackoff := maxRetries, baseBackoff
	maxRetries = 1
	baseBackoff = time.Millisecond
	t.Cleanup(func() { maxRetries, baseBackoff = origMaxRetries, origBackoff })

	mux := http.NewServeMux()
	mux.HandleFunc("/ris-prefixes/data.json", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	origRIPE := ripestatBaseURL
	ripestatBaseURL = srv.URL
	t.Cleanup(func() { ripestatBaseURL = origRIPE })

	fq := &fakeRibQuerier{}

	_, err := processASN(context.Background(), fq, "3356", true, 12, []string{"215"},
		time.Now(), testBatchSize, noopLogger(), nil, AsnProgress{}, func(map[string][]string, AsnProgress) {})

	if err == nil {
		t.Fatal("expected an error when RIPEstat fetch fails, got nil")
	}
	if len(fq.calls) != 0 {
		t.Errorf("expected no rib() calls when the prefix fetch itself failed, got %d", len(fq.calls))
	}
}

func TestProcessASN_NoPrefixesAfterFilteringReturnsNilNil(t *testing.T) {
	// A genuinely empty (but successful) RIPEstat response should short-circuit to
	// (nil, nil) — distinct from a fetch failure, and distinct from an ASN that has
	// real prefixes to process.
	mux := http.NewServeMux()
	mux.HandleFunc("/ris-prefixes/data.json", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"data": map[string]any{
				"prefixes": map[string]any{
					"v4": map[string]any{"originating": []string{}},
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

	fq := &fakeRibQuerier{}

	result, err := processASN(context.Background(), fq, "3356", true, 12, []string{"215"},
		time.Now(), testBatchSize, noopLogger(), nil, AsnProgress{}, func(map[string][]string, AsnProgress) {})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result for an ASN with no prefixes, got %+v", result)
	}
	if len(fq.calls) != 0 {
		t.Errorf("expected no rib() calls when there are no prefixes to query, got %d", len(fq.calls))
	}
}
