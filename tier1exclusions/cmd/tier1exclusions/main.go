// Command tier1exclusions extracts, for each tier-1 ASN, its announced address blocks
// minus the sub-blocks that belong to other networks — the target space for Retina.
// See tier1exclusions/README.md for the full pipeline.
//
// Config precedence: CLI flag > env var > config file > default.
//
// Exit codes: 0 success, 1 config/validation error (nothing ran), 2 one or more AFI
// runs failed.
//
// Example:
//
//	BGP_API_KEYS="key1,key2" RIB_DATE=2026-08-07 tier1exclusions --config prod.conf.json
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dioptra-io/retina-tools/tier1exclusions/internal/tier1exclusions"
)

const (
	exitOK          = 0
	exitConfigError = 1
	exitRunFailed   = 2
	defaultRibHour  = "08:00:00" // fixed time-of-day for every snapshot; see parseRibDate
)

// parseRibDate accepts a bare date, e.g. "2026-08-07". Not user-configurable down to
// the hour: the underlying RIS/RouteViews/PCH/CGTF archives only snapshot every 2-8
// hours anyway, so hour-level input precision wouldn't buy anything real, and a fixed
// hour keeps day-to-day comparisons from being skewed by diurnal BGP patterns.
func parseRibDate(s string) (time.Time, error) {
	if _, err := time.Parse("2006-01-02", s); err != nil {
		return time.Time{}, fmt.Errorf("expected YYYY-MM-DD, got %q", s)
	}
	return time.Parse("2006-01-02T15:04:05", s+"T"+defaultRibHour)
}

// newRunID returns a short random hex id for correlating this run's log lines (e.g.
// when several invocations' logs land in the same Loki stream).
func newRunID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b)
}

// cliFlags holds the raw parsed flag values, passed into buildRunConfig separately
// from env/file lookups so the whole precedence chain is testable without needing a
// real flag.FlagSet or process environment.
type cliFlags struct {
	configPath    string
	ribDateFlag   string
	batchFlag     int
	afisFlag      string
	outputDirFlag string
}

// buildRunConfig resolves the full CLI-flag > env-var > config-file > default
// precedence chain into a ready-to-run tier1exclusions.Config and AFI list. Pulled out
// of main() so this branching logic is testable without mocking flag parsing or
// network calls. getenv is injected (rather than calling os.Getenv directly) so tests
// can supply a fake environment without mutating real process state.
func buildRunConfig(flags cliFlags, getenv func(string) string, keys *tier1exclusions.KeyPool) (tier1exclusions.Config, []int, error) {
	fc, err := tier1exclusions.LoadFileConfig(flags.configPath)
	if err != nil {
		return tier1exclusions.Config{}, nil, fmt.Errorf("config load failed: %w", err)
	}

	ribDateStr := flags.ribDateFlag
	if ribDateStr == "" {
		ribDateStr = getenv("RIB_DATE")
	}
	var ribDate time.Time
	if ribDateStr != "" {
		parsed, err := parseRibDate(ribDateStr)
		if err != nil {
			return tier1exclusions.Config{}, nil, fmt.Errorf("bad rib date %q: %w", ribDateStr, err)
		}
		ribDate = parsed
	} else {
		today := time.Now().UTC().Format("2006-01-02")
		ribDate, _ = parseRibDate(today)
	}

	// 0 is the valid "not specified, use env/config/default" sentinel throughout this
	// chain (see tier1exclusions.DefaultBatchSize) — only reject an explicitly
	// negative value, which can only mean a genuine input mistake.
	batchSize := flags.batchFlag
	if batchSize < 0 {
		return tier1exclusions.Config{}, nil, fmt.Errorf("invalid --batch-size %d, must be >= 0", batchSize)
	}
	if batchSize == 0 {
		if envVal := getenv("BATCH_SIZE"); envVal != "" {
			n, err := strconv.Atoi(envVal)
			if err != nil {
				return tier1exclusions.Config{}, nil, fmt.Errorf("bad BATCH_SIZE env var %q: %w", envVal, err)
			}
			if n < 0 {
				return tier1exclusions.Config{}, nil, fmt.Errorf("invalid BATCH_SIZE env var %d, must be >= 0", n)
			}
			batchSize = n
		}
	}
	if batchSize == 0 {
		batchSize = fc.BatchSize
	}

	outputDir := flags.outputDirFlag
	if outputDir == "" {
		outputDir = getenv("OUTPUT_DIR")
	}
	if outputDir == "" {
		outputDir = fc.OutputDir
	}

	// Parse and validate every --afis entry up front, before running any of them —
	// fail fast on a typo rather than partially executing then reporting the error.
	var afis []int
	for _, afiStr := range strings.Split(flags.afisFlag, ",") {
		var afi int
		if _, err := fmt.Sscanf(afiStr, "%d", &afi); err != nil || (afi != 4 && afi != 6) {
			return tier1exclusions.Config{}, nil, fmt.Errorf("invalid --afis entry %q, expected 4 or 6", afiStr)
		}
		afis = append(afis, afi)
	}

	afiConfigs := map[int]tier1exclusions.AFIConfig{}
	for _, afi := range []int{4, 6} {
		if raw, ok := fc.AFIs[fmt.Sprint(afi)]; ok {
			afiConfigs[afi] = raw.ToAFIConfig(afi)
		}
	}

	cfg := tier1exclusions.Config{
		TargetASNs: fc.TargetASNs,
		RIBDate:    ribDate,
		OutputDir:  outputDir,
		BatchSize:  batchSize,
		Keys:       keys,
		AFIs:       afiConfigs,
	}
	return cfg, afis, nil
}

// parseAPIKeys splits and trims BGP_API_KEYS, dropping empty entries.
func parseAPIKeys(raw string) []string {
	var out []string
	for _, k := range strings.Split(raw, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return out
}

// afiRunner matches tier1exclusions.Run's signature — a func type so tests can inject
// a fake instead of making real network calls.
type afiRunner func(ctx context.Context, cfg tier1exclusions.Config, afi int, logger *slog.Logger) (tier1exclusions.ExclusionResult, error)

// runAllAFIs runs each requested AFI via runFn, logging progress, and returns whether
// every one succeeded. Extracted from main() so this orchestration logic — skip AFIs
// missing from config, track overall success — is testable without real network calls.
func runAllAFIs(ctx context.Context, cfg tier1exclusions.Config, afis []int, configPath string, logger *slog.Logger, runFn afiRunner) bool {
	allSucceeded := true
	for _, afi := range afis {
		if _, ok := cfg.AFIs[afi]; !ok {
			logger.Error("No config found for AFI, skipping", "afi", afi, "config_path", configPath)
			allSucceeded = false
			continue
		}
		logger.Info("Starting tier1exclusions run", "afi", afi, "batch_size", cfg.BatchSize, "rib_date", cfg.RIBDate.Format("2006-01-02T15:04:05"))
		if _, err := runFn(ctx, cfg, afi, logger); err != nil {
			logger.Error("AFI run failed", "afi", afi, "error", err)
			allSucceeded = false
			continue
		}
		logger.Info("Extraction complete", "afi", afi)
	}
	return allSucceeded
}

func main() {
	var flags cliFlags
	flag.StringVar(&flags.configPath, "config", "tier1exclusions.conf.json", "path to the JSON config file")
	flag.StringVar(&flags.ribDateFlag, "rib-date", "", "RIB snapshot date, YYYY-MM-DD. Overrides RIB_DATE env var.")
	flag.IntVar(&flags.batchFlag, "batch-size", 0, "covers per rib() call, 0 = use env/config/default. Overrides BATCH_SIZE env var.")
	flag.StringVar(&flags.afisFlag, "afis", "4,6", "comma-separated address families to run (4, 6, or both)")
	flag.StringVar(&flags.outputDirFlag, "output-dir", "", "directory for output files. Overrides OUTPUT_DIR env var and the config file's output_dir.")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// stderr, not stdout: this tool writes no data to stdout, but keeping logs on
	// stderr is the standard CLI convention (stdout reserved for a tool's actual
	// output) and costs nothing to follow.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger = logger.With("run_id", newRunID())

	cleanKeys := parseAPIKeys(os.Getenv("BGP_API_KEYS"))
	if len(cleanKeys) == 0 {
		logger.Error("BGP_API_KEYS is not set", "hint", `export BGP_API_KEYS="key1,key2,key3"`)
		os.Exit(exitConfigError)
	}
	keys := tier1exclusions.NewKeyPool(cleanKeys,
		tier1exclusions.DefaultMaxRequestsPerHour,
		tier1exclusions.DefaultMaxBytesPerHour,
		tier1exclusions.DefaultMaxExecSecPerHour)

	cfg, afis, err := buildRunConfig(flags, os.Getenv, keys)
	if err != nil {
		logger.Error("Config error", "error", err)
		os.Exit(exitConfigError)
	}

	allSucceeded := runAllAFIs(ctx, cfg, afis, flags.configPath, logger, tier1exclusions.Run)

	// Single terminal line for alerting to match on — e.g. a Loki/Alertmanager rule
	// watching for status="failed" here, rather than needing to reason about every
	// intermediate error line.
	if !allSucceeded {
		logger.Error("Process failed to complete", "status", "failed")
		os.Exit(exitRunFailed)
	}
	logger.Info("Process completed successfully", "status", "success")
	os.Exit(exitOK)
}
