package tier1exclusions

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"sort"
	"time"
)

// AFIConfig holds per-address-family settings.
type AFIConfig struct {
	GroupPrefixLen  int
	PinnedVPIDs     []string
	BackupVPIDs     []string
	Sources         string
	VPSizeThreshold int64
	IsV4            bool
}

// Config is the full pipeline configuration for one run.
type Config struct {
	TargetASNs []string
	RIBDate    time.Time
	AFIs       map[int]AFIConfig // keyed by 4 or 6
	// BatchSize: << conditions batched per rib() call. 0 = DefaultBatchSize (1) — see
	// README.md for why bgproutes.io's real safe batch size isn't a fixed constant.
	BatchSize int
	OutputDir string
	Keys      *KeyPool // shared across AFIs — the API key's limits are account-wide
}

// DefaultBatchSize is used when Config.BatchSize is left at zero.
const DefaultBatchSize = 1

// FileConfig is the on-disk shape of the config file: everything except secrets
// (API keys stay in BGP_API_KEYS) and run-specific values (RIBDate is a flag).
// Rate-limit caps aren't here — see DefaultMaxRequestsPerHour etc. below.
type FileConfig struct {
	TargetASNs []string           `json:"target_asns"`
	OutputDir  string             `json:"output_dir"`
	BatchSize  int                `json:"batch_size"` // 0 = DefaultBatchSize
	AFIs       map[string]AFIFile `json:"afis"`       // keyed by "4" or "6"
}

// AFIFile is the JSON shape for one address family's settings.
type AFIFile struct {
	GroupPrefixLen  int      `json:"group_prefix_len"`
	PinnedVPIDs     []string `json:"pinned_vp_ids"`
	BackupVPIDs     []string `json:"backup_vp_ids"`
	Sources         string   `json:"sources"`
	VPSizeThreshold int64    `json:"vp_size_threshold"`
}

// LoadFileConfig reads, parses, and validates a JSON config file.
func LoadFileConfig(path string) (FileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return FileConfig{}, fmt.Errorf("reading config file %s: %w", path, err)
	}
	var fc FileConfig
	if err := json.Unmarshal(data, &fc); err != nil {
		return FileConfig{}, fmt.Errorf("parsing config file %s: %w", path, err)
	}
	if err := fc.validate(); err != nil {
		return FileConfig{}, fmt.Errorf("config file %s: %w", path, err)
	}
	return fc, nil
}

func (fc FileConfig) validate() error {
	if len(fc.TargetASNs) == 0 {
		return fmt.Errorf("target_asns must not be empty")
	}
	for _, key := range []string{"4", "6"} {
		afiCfg, ok := fc.AFIs[key]
		if !ok {
			continue // an AFI can legitimately be unconfigured if unused
		}
		if afiCfg.GroupPrefixLen <= 0 {
			return fmt.Errorf("afis.%s.group_prefix_len must be positive", key)
		}
		if len(afiCfg.PinnedVPIDs) == 0 && afiCfg.VPSizeThreshold <= 0 {
			return fmt.Errorf("afis.%s: need either pinned_vp_ids or a positive vp_size_threshold for live lookup", key)
		}
	}
	return nil
}

// ToAFIConfig converts one AFIFile entry into the runtime AFIConfig.
func (a AFIFile) ToAFIConfig(afi int) AFIConfig {
	return AFIConfig{
		GroupPrefixLen:  a.GroupPrefixLen,
		PinnedVPIDs:     a.PinnedVPIDs,
		BackupVPIDs:     a.BackupVPIDs,
		Sources:         a.Sources,
		VPSizeThreshold: a.VPSizeThreshold,
		IsV4:            afi == 4,
	}
}

// Fixed margins below bgproutes.io's documented limits (1000 req/h, 100MB/h, 5min/h)
// — see NewKeyPool. Not configurable via file; edit these directly if that changes.
const (
	DefaultMaxRequestsPerHour = 700
	DefaultMaxBytesPerHour    = 95_000_000
	DefaultMaxExecSecPerHour  = 280
)

// MaxConsecutiveBatchFailures aborts an ASN's processing (not the whole run) after
// this many consecutive batch failures in a row — a sign the endpoint is genuinely
// saturated or something structural is wrong, not just one-off transient errors that
// retry-with-backoff already absorbs at the request level.
const MaxConsecutiveBatchFailures = 15

// AsnProgress tracks which covers have been completed/failed for one ASN, enabling
// resume at the per-batch level rather than restarting a whole ASN after an interruption.
type AsnProgress struct {
	DoneGroups   []string `json:"done_groups"`
	FailedGroups []string `json:"failed_groups"`
}

// ProgressState is the full per-AFI-run progress file: ASN -> its AsnProgress.
type ProgressState map[string]AsnProgress

// ParentExclusions is one parent block's exclusion list — the final per-ASN output shape.
type ParentExclusions struct {
	ParentBlock string   `json:"parent_block"`
	Exclusions  []string `json:"exclusions"`
}

// ExclusionResult is the full output for one AFI run: ASN -> its parent blocks + exclusions.
type ExclusionResult map[string][]ParentExclusions

// resolveVPs picks the VP set to use for one AFI: validates the pinned list's live
// status and substitutes from the backup pool for any that are down — UNLESS the
// status check itself failed (network error), in which case it trusts the pinned list
// as-is rather than treating a check failure as a mass VP outage (see CheckVPStatus).
func resolveVPs(ctx context.Context, client *APIClient, cfg AFIConfig, ribDate time.Time, logger *slog.Logger) ([]string, error) {
	if len(cfg.PinnedVPIDs) == 0 {
		ids, err := client.FindFullFeedVPs(ctx, cfg.Sources, cfg.IsV4, cfg.VPSizeThreshold, 10)
		if err != nil {
			return nil, fmt.Errorf("no pinned VPs configured and live lookup failed: %w", err)
		}
		return ids, nil
	}

	statuses, checkSucceeded := client.CheckVPStatus(ctx, cfg.PinnedVPIDs, ribDate)
	if !checkSucceeded {
		logger.Warn("VP status check itself failed (network/timeout) — trusting pinned list as-is",
			"reason", "not treating this as a mass VP outage")
		return append([]string(nil), cfg.PinnedVPIDs...), nil
	}

	var good, dead []string
	for _, id := range cfg.PinnedVPIDs {
		st := statuses[id]
		if containsStr(st, "ready") || containsStr(st, "up") {
			good = append(good, id)
		} else {
			dead = append(dead, id)
		}
	}

	if len(dead) > 0 {
		logger.Warn("Pinned VP(s) not ready, substituting from backup pool", "dead_vps", dead)
		var backups []string
		for _, b := range cfg.BackupVPIDs {
			if !containsStr(good, b) && !containsStr(dead, b) {
				backups = append(backups, b)
			}
		}
		if len(backups) < len(dead) {
			logger.Error("Insufficient backup VPs available", "needed", len(dead), "available", len(backups))
		}
		n := len(dead)
		if n > len(backups) {
			n = len(backups)
		}
		good = append(good, backups[:n]...)
	}

	if len(good) == 0 {
		return nil, fmt.Errorf("no usable VPs (all pinned down, no backups)")
	}
	return good, nil
}

func containsStr(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// RibQuerier is the subset of APIClient's behavior processQueryGroups needs —
// extracted as an interface so the resume/circuit-breaker logic can be tested with a
// fake, without requiring real network access.
type RibQuerier interface {
	QueryRIBBatch(ctx context.Context, vpIDs []string, supernets []string, afiIs4 bool, ribDate time.Time) ([]MoreSpecific, error)
}

// checkpointFunc is called after every batch (success or failure) so the caller can
// persist progress incrementally — a crash mid-ASN then resumes from the last
// completed batch, not from the start of the whole ASN.
type checkpointFunc func(excl map[string][]string, progress AsnProgress)

// processASN runs the full pipeline for one ASN/AFI: fetch, optimize, group,
// batch-query, assign exclusions. Resumable via existingExcl/priorProgress. A
// genuinely empty RIPEstat result returns (nil, nil) — distinct from a fetch failure,
// which returns a non-nil error.
func processASN(ctx context.Context, client RibQuerier, asn string, afiIs4 bool, groupPrefixLen int, vpIDs []string,
	ribDate time.Time, batchSize int, logger *slog.Logger, existingExcl map[string][]string,
	priorProgress AsnProgress, checkpoint checkpointFunc) ([]ParentExclusions, error) {

	log := logger.With("asn", asn, "afi", afiLabel(afiIs4))

	// afiIs4 and ribDate flow through to FetchAnnouncedPrefixes as well — it queries
	// RIPEstat's ris-prefixes, split by address family and pinned to ribDate. See
	// FetchAnnouncedPrefixes for why pinning to ribDate matters at this pipeline's
	// monthly run cadence.
	raw, ok := FetchAnnouncedPrefixes(ctx, asn, afiIs4, ribDate, logger)
	if !ok {
		return nil, fmt.Errorf("AS%s: RIPEstat fetch failed", asn)
	}

	opt := Optimize(raw, afiIs4)
	if len(opt.Invalid) > 0 {
		log.Warn("Dropped unparseable prefixes", "count", len(opt.Invalid), "sample", sample(opt.Invalid, 5))
	}
	if len(opt.TooBroad) > 0 {
		log.Warn("Dropped overly-broad prefixes", "count", len(opt.TooBroad), "sample", sample(opt.TooBroad, 5))
	}
	if len(opt.Collapsed) == 0 {
		log.Warn("No prefixes after filtering, skipping")
		return nil, nil
	}

	queryGroups := BuildQueryGroups(opt.Collapsed, groupPrefixLen)
	log.Info("Prefix summary", "raw", len(raw), "collapsed", len(opt.Collapsed), "query_groups", len(queryGroups))

	return processQueryGroups(ctx, client, asn, queryGroups, afiIs4, vpIDs, ribDate, batchSize, logger, existingExcl, priorProgress, checkpoint)
}

// processQueryGroups is the resumable, circuit-breaker-protected batch loop, isolated
// so it's testable with a fake RibQuerier and no real network access.
func processQueryGroups(ctx context.Context, client RibQuerier, asn string, queryGroups []QueryGroup, afiIs4 bool, vpIDs []string,
	ribDate time.Time, batchSize int, logger *slog.Logger, existingExcl map[string][]string,
	priorProgress AsnProgress, checkpoint checkpointFunc) ([]ParentExclusions, error) {

	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	log := logger.With("asn", asn)

	// Prune progress/exclusions for query groups that no longer exist in this
	// snapshot (e.g. the ASN's prefix list changed since a prior partial run) —
	// stale leftover state shouldn't silently persist into a different snapshot.
	validGroups := map[string]bool{}
	for _, qg := range queryGroups {
		validGroups[qg.Supernet.String()] = true
	}
	done := filterValid(priorProgress.DoneGroups, validGroups)
	doneSet := toSet(done)

	todo := make([]QueryGroup, 0, len(queryGroups))
	for _, qg := range queryGroups {
		if !doneSet[qg.Supernet.String()] {
			todo = append(todo, qg)
		}
	}

	if len(done) > 0 {
		log.Info("Resuming from prior progress", "done_groups", len(done), "remaining_groups", len(todo))
	}

	excl := map[string][]string{}
	for k, v := range existingExcl {
		excl[k] = append([]string(nil), v...)
	}

	consecutiveFailures := 0
	totalBatches := (len(todo) + batchSize - 1) / batchSize
	for i := 0; i < len(todo); i += batchSize {
		end := i + batchSize
		if end > len(todo) {
			end = len(todo)
		}
		batch := todo[i:end]
		batchNum := (i / batchSize) + 1

		supernetStrs := make([]string, len(batch))
		var batchPrefixes []netip.Prefix
		for j, qg := range batch {
			supernetStrs[j] = qg.Supernet.String()
			batchPrefixes = append(batchPrefixes, qg.Prefixes...)
		}

		moreSpecifics, err := client.QueryRIBBatch(ctx, vpIDs, supernetStrs, afiIs4, ribDate)
		if err != nil {
			consecutiveFailures++
			log.Warn("Batch failed", "batch", batchNum, "total_batches", totalBatches,
				"consecutive_failures", consecutiveFailures, "error", err)
			for _, qg := range batch {
				priorProgress.FailedGroups = appendUnique(priorProgress.FailedGroups, qg.Supernet.String())
			}
			checkpoint(excl, priorProgress)
			if consecutiveFailures >= MaxConsecutiveBatchFailures {
				return nil, fmt.Errorf("AS%s: circuit breaker tripped after %d consecutive batch "+
					"failures — progress saved, re-run to resume", asn, consecutiveFailures)
			}
			continue
		}
		consecutiveFailures = 0

		batchResult := AssignExclusions(moreSpecifics, batchPrefixes, asn)
		for parent, exclusions := range batchResult {
			excl[parent] = append(excl[parent], exclusions...)
		}
		for _, qg := range batch {
			key := qg.Supernet.String()
			priorProgress.DoneGroups = appendUnique(priorProgress.DoneGroups, key)
			priorProgress.FailedGroups = removeAll(priorProgress.FailedGroups, key)
		}
		log.Info("Batch done", "batch", batchNum, "total_batches", totalBatches,
			"groups_done_this_run", end, "groups_to_do_this_run", len(todo))
		checkpoint(excl, priorProgress)
	}

	return toParentExclusions(excl), nil
}

func filterValid(covers []string, valid map[string]bool) []string {
	var out []string
	for _, c := range covers {
		if valid[c] {
			out = append(out, c)
		}
	}
	return out
}

func toSet(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}

func appendUnique(ss []string, s string) []string {
	for _, x := range ss {
		if x == s {
			return ss
		}
	}
	return append(ss, s)
}

func removeAll(ss []string, s string) []string {
	out := ss[:0]
	for _, x := range ss {
		if x != s {
			out = append(out, x)
		}
	}
	return out
}

func sample(ss []string, n int) []string {
	if len(ss) <= n {
		return ss
	}
	return ss[:n]
}

func afiLabel(is4 bool) string {
	if is4 {
		return "4"
	}
	return "6"
}

// Run executes the full pipeline for one address family. On full success, the
// progress file is deleted (no value once nothing's left to resume); on partial
// failure it's retained so the next run can pick up where this one left off.
func Run(ctx context.Context, cfg Config, afi int, logger *slog.Logger) (ExclusionResult, error) {
	log := logger.With("afi", afi)

	afiCfg, ok := cfg.AFIs[afi]
	if !ok {
		return nil, fmt.Errorf("no config for AFI %d", afi)
	}

	client := &APIClient{
		Keys:   cfg.Keys,
		Logger: logger,
	}

	vpIDs, err := resolveVPs(ctx, client, afiCfg, cfg.RIBDate, logger)
	if err != nil {
		return nil, err
	}
	log.Info("Resolved vantage points", "vp_ids", vpIDs)

	dateTag := cfg.RIBDate.UTC().Format("2006-01-02")
	outPath := fmt.Sprintf("%s/tier1_exclusions_v%d_%s.json", cfg.OutputDir, afi, dateTag)
	progressPath := fmt.Sprintf("%s/tier1_progress_v%d_%s.json", cfg.OutputDir, afi, dateTag)

	result, err := loadExistingResult(outPath)
	if err != nil {
		log.Warn("Could not load existing checkpoint, starting fresh", "error", err)
		result = ExclusionResult{}
	}
	progress, err := loadProgress(progressPath)
	if err != nil {
		log.Warn("Could not load existing progress, starting fresh", "error", err)
		progress = ProgressState{}
	}

	allSucceeded := true
	for _, asn := range cfg.TargetASNs {
		if ctx.Err() != nil {
			log.Warn("Context cancelled, stopping before remaining ASNs", "error", ctx.Err())
			allSucceeded = false
			break
		}

		log.Info("Processing ASN", "asn", asn)

		existingExcl := map[string][]string{}
		for _, pe := range result[asn] {
			existingExcl[pe.ParentBlock] = pe.Exclusions
		}

		checkpoint := func(excl map[string][]string, asnProgress AsnProgress) {
			result[asn] = toParentExclusions(excl)
			progress[asn] = asnProgress
			if err := saveResult(outPath, result); err != nil {
				log.Error("Checkpoint save (result) failed", "error", err)
			}
			if err := saveProgress(progressPath, progress); err != nil {
				log.Error("Checkpoint save (progress) failed", "error", err)
			}
		}

		parents, err := processASN(ctx, client, asn, afiCfg.IsV4, afiCfg.GroupPrefixLen, vpIDs,
			cfg.RIBDate, cfg.BatchSize, logger, existingExcl, progress[asn], checkpoint)
		if err != nil {
			log.Error("ASN processing failed, moving to next", "asn", asn, "error", err)
			allSucceeded = false
			continue
		}
		if parents != nil {
			result[asn] = parents
			if err := saveResult(outPath, result); err != nil {
				log.Error("Checkpoint save failed", "error", err)
			}
		}
	}

	if allSucceeded {
		if err := os.Remove(progressPath); err != nil && !os.IsNotExist(err) {
			log.Warn("Could not remove progress file after successful run", "error", err)
		} else {
			log.Info("Run completed successfully, progress file removed")
		}
	} else {
		log.Warn("Run completed with errors, progress file retained for resume", "path", progressPath)
	}

	metaPath := fmt.Sprintf("%s/tier1_meta_v%d_%s.json", cfg.OutputDir, afi, dateTag)
	meta := buildRunMetadata(afi, cfg.RIBDate, afiCfg.GroupPrefixLen, vpIDs, result, allSucceeded)
	if err := saveJSON(metaPath, meta); err != nil {
		log.Warn("Could not write metadata file", "error", err)
	}

	return result, nil
}

// RunMetadata records provenance for one run — which VPs were actually used (a pinned
// list can silently differ from what was requested if backup substitution happened),
// the RIB date/config in effect, and basic result shape. Written always, even on
// partial failure, so it's possible to answer "what VPs did this snapshot actually use"
// after the fact — without it, two snapshots that appear to disagree can't be told
// apart from "real churn" vs. "different VP visibility" after the run has finished.
type RunMetadata struct {
	AFI            int                  `json:"afi"`
	RIBDate        string               `json:"rib_date"`
	GroupPrefixLen int                  `json:"group_prefix_len"`
	VPIDs          []string             `json:"vp_ids"`
	VPCount        int                  `json:"vp_count"`
	Succeeded      bool                 `json:"succeeded"`
	GeneratedAt    string               `json:"generated_at"`
	Counts         map[string]ASNCounts `json:"counts"`
}

// ASNCounts is the per-ASN summary embedded in RunMetadata.
type ASNCounts struct {
	ParentBlocks int `json:"parent_blocks"`
	Exclusions   int `json:"exclusions"`
}

func buildRunMetadata(afi int, ribDate time.Time, groupPrefixLen int, vpIDs []string, result ExclusionResult, succeeded bool) RunMetadata {
	counts := make(map[string]ASNCounts, len(result))
	for asn, entries := range result {
		total := 0
		for _, e := range entries {
			total += len(e.Exclusions)
		}
		counts[asn] = ASNCounts{ParentBlocks: len(entries), Exclusions: total}
	}
	return RunMetadata{
		AFI:            afi,
		RIBDate:        ribDate.UTC().Format("2006-01-02T15:04:05"),
		GroupPrefixLen: groupPrefixLen,
		VPIDs:          vpIDs,
		VPCount:        len(vpIDs),
		Succeeded:      succeeded,
		GeneratedAt:    time.Now().UTC().Format("2006-01-02T15:04:05Z"),
		Counts:         counts,
	}
}

// toParentExclusions converts the internal exclusion map into the sorted, JSON-ready
// slice. Exclusions is always a non-nil slice (possibly empty), never left as Go's
// zero-value nil — a nil []string marshals to JSON `null`, not `[]`, which silently
// broke every downstream comparison script expecting a real (if empty) array.
func toParentExclusions(excl map[string][]string) []ParentExclusions {
	out := make([]ParentExclusions, 0, len(excl))
	for parent, exclusions := range excl {
		sorted := append([]string{}, exclusions...)
		sort.Strings(sorted)
		out = append(out, ParentExclusions{ParentBlock: parent, Exclusions: sorted})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ParentBlock < out[j].ParentBlock })
	return out
}

// loadJSON reads and parses a JSON file into T, treating a missing file as a valid
// "empty" zero-value result rather than an error — the normal case on a fresh run
// with no prior checkpoint.
func loadJSON[T any](path string) (T, error) {
	var zero T
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return zero, nil
		}
		return zero, err
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return zero, err
	}
	return v, nil
}

// saveJSON writes v to path as indented JSON, via a temp-file-then-rename so a
// concurrent reader (or a crash mid-write) never sees a partially-written file.
func saveJSON[T any](path string, v T) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func loadProgress(path string) (ProgressState, error) {
	p, err := loadJSON[ProgressState](path)
	if err != nil {
		return nil, err
	}
	if p == nil {
		p = ProgressState{}
	}
	return p, nil
}

func saveProgress(path string, progress ProgressState) error {
	return saveJSON(path, progress)
}

func loadExistingResult(path string) (ExclusionResult, error) {
	r, err := loadJSON[ExclusionResult](path)
	if err != nil {
		return nil, err
	}
	if r == nil {
		r = ExclusionResult{}
	}
	return r, nil
}

func saveResult(path string, result ExclusionResult) error {
	return saveJSON(path, result)
}
