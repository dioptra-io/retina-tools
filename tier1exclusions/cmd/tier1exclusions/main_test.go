package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dioptra-io/retina-tools/tier1exclusions/internal/tier1exclusions"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "conf.json")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}
	return path
}

const validConfigJSON = `{
	"target_asns": ["3356"],
	"output_dir": ".",
	"afis": {
		"4": {"group_prefix_len": 12, "pinned_vp_ids": ["1"]}
	}
}`

func fakeGetenv(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestParseRibDate(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
		want    string
	}{
		{"2026-08-07", false, "2026-08-07T08:00:00"},
		{"2026-08-07T14:30:00", true, ""}, // full timestamps no longer accepted
		{"garbage", true, ""},
		{"", true, ""},
	}
	for _, c := range cases {
		got, err := parseRibDate(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseRibDate(%q): expected error, got nil", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseRibDate(%q): unexpected error: %v", c.in, err)
			continue
		}
		if got.Format("2006-01-02T15:04:05") != c.want {
			t.Errorf("parseRibDate(%q) = %v, want %s", c.in, got, c.want)
		}
	}
}

func TestNewRunID(t *testing.T) {
	a := newRunID()
	b := newRunID()
	if a == "" || b == "" {
		t.Fatal("expected non-empty run IDs")
	}
	if a == b {
		t.Error("expected two calls to produce different run IDs")
	}
	if len(a) != 8 { // 4 bytes hex-encoded
		t.Errorf("expected 8-char hex id, got %q (len %d)", a, len(a))
	}
}

func TestParseAPIKeys(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"key1,key2,key3", []string{"key1", "key2", "key3"}},
		{" key1 , key2 ", []string{"key1", "key2"}},
		{"", nil},
		{",,", nil},
		{"key1,,key2", []string{"key1", "key2"}},
	}
	for _, c := range cases {
		got := parseAPIKeys(c.in)
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("parseAPIKeys(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestBuildRunConfig_Defaults(t *testing.T) {
	path := writeTempConfig(t, validConfigJSON)
	flags := cliFlags{configPath: path, afisFlag: "4,6"}
	keys := tier1exclusions.NewKeyPool([]string{"k"}, 700, 95_000_000, 280)

	cfg, afis, err := buildRunConfig(flags, fakeGetenv(nil), keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(afis) != 2 || afis[0] != 4 || afis[1] != 6 {
		t.Errorf("expected afis [4 6], got %v", afis)
	}
	if cfg.BatchSize != 0 {
		t.Errorf("expected BatchSize 0 (config didn't set one), got %d", cfg.BatchSize)
	}
	// No rib-date given -> defaults to today.
	today := time.Now().UTC().Format("2006-01-02")
	if cfg.RIBDate.Format("2006-01-02") != today {
		t.Errorf("expected today's date, got %v", cfg.RIBDate)
	}
}

func TestBuildRunConfig_RibDatePrecedence(t *testing.T) {
	path := writeTempConfig(t, validConfigJSON)
	keys := tier1exclusions.NewKeyPool([]string{"k"}, 700, 95_000_000, 280)

	// Flag wins over env.
	flags := cliFlags{configPath: path, ribDateFlag: "2026-08-01", afisFlag: "4"}
	cfg, _, err := buildRunConfig(flags, fakeGetenv(map[string]string{"RIB_DATE": "2026-08-02"}), keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RIBDate.Format("2006-01-02") != "2026-08-01" {
		t.Errorf("expected flag to win, got %v", cfg.RIBDate)
	}

	// Env used when flag is empty.
	flags2 := cliFlags{configPath: path, afisFlag: "4"}
	cfg2, _, err := buildRunConfig(flags2, fakeGetenv(map[string]string{"RIB_DATE": "2026-08-02"}), keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg2.RIBDate.Format("2006-01-02") != "2026-08-02" {
		t.Errorf("expected env var to be used, got %v", cfg2.RIBDate)
	}
}

func TestBuildRunConfig_BatchSizePrecedence(t *testing.T) {
	path := writeTempConfig(t, `{
		"target_asns": ["3356"], "batch_size": 3,
		"afis": {"4": {"group_prefix_len": 12, "pinned_vp_ids": ["1"]}}
	}`)
	keys := tier1exclusions.NewKeyPool([]string{"k"}, 700, 95_000_000, 280)

	// Flag wins over env and config.
	flags := cliFlags{configPath: path, batchFlag: 5, afisFlag: "4"}
	cfg, _, err := buildRunConfig(flags, fakeGetenv(map[string]string{"BATCH_SIZE": "7"}), keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.BatchSize != 5 {
		t.Errorf("expected flag value 5, got %d", cfg.BatchSize)
	}

	// Env wins over config when flag is 0.
	flags2 := cliFlags{configPath: path, afisFlag: "4"}
	cfg2, _, err := buildRunConfig(flags2, fakeGetenv(map[string]string{"BATCH_SIZE": "7"}), keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg2.BatchSize != 7 {
		t.Errorf("expected env value 7, got %d", cfg2.BatchSize)
	}

	// Config value used when neither flag nor env set.
	flags3 := cliFlags{configPath: path, afisFlag: "4"}
	cfg3, _, err := buildRunConfig(flags3, fakeGetenv(nil), keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg3.BatchSize != 3 {
		t.Errorf("expected config value 3, got %d", cfg3.BatchSize)
	}
}

func TestBuildRunConfig_OutputDirPrecedence(t *testing.T) {
	path := writeTempConfig(t, `{
		"target_asns": ["3356"], "output_dir": "/from/config",
		"afis": {"4": {"group_prefix_len": 12, "pinned_vp_ids": ["1"]}}
	}`)
	keys := tier1exclusions.NewKeyPool([]string{"k"}, 700, 95_000_000, 280)

	// Flag wins over env and config.
	flags := cliFlags{configPath: path, outputDirFlag: "/from/flag", afisFlag: "4"}
	cfg, _, err := buildRunConfig(flags, fakeGetenv(map[string]string{"OUTPUT_DIR": "/from/env"}), keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.OutputDir != "/from/flag" {
		t.Errorf("expected flag value, got %q", cfg.OutputDir)
	}

	// Env wins over config when flag is empty.
	flags2 := cliFlags{configPath: path, afisFlag: "4"}
	cfg2, _, err := buildRunConfig(flags2, fakeGetenv(map[string]string{"OUTPUT_DIR": "/from/env"}), keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg2.OutputDir != "/from/env" {
		t.Errorf("expected env value, got %q", cfg2.OutputDir)
	}

	// Config value used when neither flag nor env set — preserves old behavior.
	flags3 := cliFlags{configPath: path, afisFlag: "4"}
	cfg3, _, err := buildRunConfig(flags3, fakeGetenv(nil), keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg3.OutputDir != "/from/config" {
		t.Errorf("expected config value, got %q", cfg3.OutputDir)
	}
}

func TestBuildRunConfig_RejectsNegativeBatchFlag(t *testing.T) {
	path := writeTempConfig(t, validConfigJSON)
	keys := tier1exclusions.NewKeyPool([]string{"k"}, 700, 95_000_000, 280)
	flags := cliFlags{configPath: path, batchFlag: -1, afisFlag: "4"}

	_, _, err := buildRunConfig(flags, fakeGetenv(nil), keys)
	if err == nil {
		t.Fatal("expected error for negative --batch-size, got nil")
	}
}

func TestBuildRunConfig_RejectsNegativeBatchEnv(t *testing.T) {
	path := writeTempConfig(t, validConfigJSON)
	keys := tier1exclusions.NewKeyPool([]string{"k"}, 700, 95_000_000, 280)
	flags := cliFlags{configPath: path, afisFlag: "4"}

	_, _, err := buildRunConfig(flags, fakeGetenv(map[string]string{"BATCH_SIZE": "-3"}), keys)
	if err == nil {
		t.Fatal("expected error for negative BATCH_SIZE env var, got nil")
	}
}

func TestBuildRunConfig_RejectsBadBatchEnv(t *testing.T) {
	path := writeTempConfig(t, validConfigJSON)
	keys := tier1exclusions.NewKeyPool([]string{"k"}, 700, 95_000_000, 280)
	flags := cliFlags{configPath: path, afisFlag: "4"}

	_, _, err := buildRunConfig(flags, fakeGetenv(map[string]string{"BATCH_SIZE": "not-a-number"}), keys)
	if err == nil {
		t.Fatal("expected error for non-numeric BATCH_SIZE env var, got nil")
	}
}

func TestBuildRunConfig_RejectsBadRibDate(t *testing.T) {
	path := writeTempConfig(t, validConfigJSON)
	keys := tier1exclusions.NewKeyPool([]string{"k"}, 700, 95_000_000, 280)
	flags := cliFlags{configPath: path, ribDateFlag: "not-a-date", afisFlag: "4"}

	_, _, err := buildRunConfig(flags, fakeGetenv(nil), keys)
	if err == nil {
		t.Fatal("expected error for bad --rib-date, got nil")
	}
}

func TestBuildRunConfig_RejectsInvalidAFI(t *testing.T) {
	path := writeTempConfig(t, validConfigJSON)
	keys := tier1exclusions.NewKeyPool([]string{"k"}, 700, 95_000_000, 280)
	flags := cliFlags{configPath: path, afisFlag: "4,99"}

	_, _, err := buildRunConfig(flags, fakeGetenv(nil), keys)
	if err == nil {
		t.Fatal("expected error for invalid --afis entry, got nil")
	}
}

func TestBuildRunConfig_RejectsMissingConfigFile(t *testing.T) {
	keys := tier1exclusions.NewKeyPool([]string{"k"}, 700, 95_000_000, 280)
	flags := cliFlags{configPath: "/nonexistent/conf.json", afisFlag: "4"}

	_, _, err := buildRunConfig(flags, fakeGetenv(nil), keys)
	if err == nil {
		t.Fatal("expected error for missing config file, got nil")
	}
}

// buildTestBinary compiles the current package once and returns the binary path,
// reused across subprocess tests below.
func buildTestBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "tier1exclusions_test_bin")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build test binary: %v\n%s", err, out)
	}
	return bin
}

// These exercise main() itself via a real subprocess — the only way to meaningfully
// cover flag registration and the os.Exit paths without either faking network calls
// inside main() or never testing it at all. Only the config-error paths are tested
// here (exit 1), since those fail before any real network call would happen.
func TestMain_ExitsNonZeroWithoutAPIKeys(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin)
	cmd.Env = []string{} // deliberately no BGP_API_KEYS
	err := cmd.Run()

	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected an ExitError, got %v (err=%v)", cmd.ProcessState, err)
	}
	if exitErr.ExitCode() != exitConfigError {
		t.Errorf("expected exit code %d, got %d", exitConfigError, exitErr.ExitCode())
	}
}

func TestMain_ExitsNonZeroWithMissingConfigFile(t *testing.T) {
	bin := buildTestBinary(t)
	cmd := exec.Command(bin, "--config", "/nonexistent/conf.json")
	cmd.Env = []string{"BGP_API_KEYS=fake-key"}
	err := cmd.Run()

	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected an ExitError, got %v (err=%v)", cmd.ProcessState, err)
	}
	if exitErr.ExitCode() != exitConfigError {
		t.Errorf("expected exit code %d, got %d", exitConfigError, exitErr.ExitCode())
	}
}

func TestMain_ExitsNonZeroWithInvalidAFIs(t *testing.T) {
	bin := buildTestBinary(t)
	path := writeTempConfig(t, validConfigJSON)
	cmd := exec.Command(bin, "--config", path, "--afis", "4,99")
	cmd.Env = []string{"BGP_API_KEYS=fake-key"}
	err := cmd.Run()

	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected an ExitError, got %v (err=%v)", cmd.ProcessState, err)
	}
	if exitErr.ExitCode() != exitConfigError {
		t.Errorf("expected exit code %d, got %d", exitConfigError, exitErr.ExitCode())
	}
}

func TestRunAllAFIs_AllSucceed(t *testing.T) {
	cfg := tier1exclusions.Config{
		AFIs: map[int]tier1exclusions.AFIConfig{4: {}, 6: {}},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var calledAFIs []int
	fakeRun := func(ctx context.Context, cfg tier1exclusions.Config, afi int, logger *slog.Logger) (tier1exclusions.ExclusionResult, error) {
		calledAFIs = append(calledAFIs, afi)
		return tier1exclusions.ExclusionResult{}, nil
	}

	ok := runAllAFIs(context.Background(), cfg, []int{4, 6}, "conf.json", logger, fakeRun)
	if !ok {
		t.Error("expected all AFIs to succeed")
	}
	if len(calledAFIs) != 2 {
		t.Errorf("expected both AFIs to be attempted, got %v", calledAFIs)
	}
}

func TestRunAllAFIs_MissingAFIConfigCountsAsFailure(t *testing.T) {
	cfg := tier1exclusions.Config{
		AFIs: map[int]tier1exclusions.AFIConfig{4: {}}, // no config for 6
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var calledAFIs []int
	fakeRun := func(ctx context.Context, cfg tier1exclusions.Config, afi int, logger *slog.Logger) (tier1exclusions.ExclusionResult, error) {
		calledAFIs = append(calledAFIs, afi)
		return tier1exclusions.ExclusionResult{}, nil
	}

	ok := runAllAFIs(context.Background(), cfg, []int{4, 6}, "conf.json", logger, fakeRun)
	if ok {
		t.Error("expected overall failure since AFI 6 has no config")
	}
	if len(calledAFIs) != 1 || calledAFIs[0] != 4 {
		t.Errorf("expected only AFI 4 to be attempted (6 skipped), got %v", calledAFIs)
	}
}

func TestRunAllAFIs_RunErrorCountsAsFailureButOthersStillAttempted(t *testing.T) {
	cfg := tier1exclusions.Config{
		AFIs: map[int]tier1exclusions.AFIConfig{4: {}, 6: {}},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var calledAFIs []int
	fakeRun := func(ctx context.Context, cfg tier1exclusions.Config, afi int, logger *slog.Logger) (tier1exclusions.ExclusionResult, error) {
		calledAFIs = append(calledAFIs, afi)
		if afi == 4 {
			return nil, errors.New("simulated failure")
		}
		return tier1exclusions.ExclusionResult{}, nil
	}

	ok := runAllAFIs(context.Background(), cfg, []int{4, 6}, "conf.json", logger, fakeRun)
	if ok {
		t.Error("expected overall failure since AFI 4's run errored")
	}
	if len(calledAFIs) != 2 {
		t.Errorf("expected AFI 6 to still be attempted after AFI 4 failed, got %v", calledAFIs)
	}
}
