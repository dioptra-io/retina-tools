#!/usr/bin/env bash
#
# Generates IPv4 and (if available) IPv6 probing directive (PD) files from the Iris
# links tables that pipeline/fetch_iris_links.sh loaded into ClickHouse for a given
# date. A standalone tool — usable manually or from pipeline/pd_pipeline.sh, which
# calls it as step 3 of the PD-generation pipeline.
#
# Produces two kinds of output:
#   pds_v4.jsonl / pds_v6.jsonl        full snapshot of today's PDs, split by AFI —
#                                       for bootstrap (an orchestrator loading its
#                                       entire initial state) or manual inspection.
#   pds_diff.jsonl                     one combined file (not split by AFI — ApplyDiff
#                                       takes mixed v4+v6 lists in one call, IPVersion
#                                       is just a field on each record, not something
#                                       the consumer branches on), op-tagged JSONL:
#                                       {"op":"insert", ...full PD fields...} or
#                                       {"op":"remove","probing_directive_id":<id>}
#                                       (minimal on purpose — the only field a remove
#                                       operation actually needs downstream).
#
# Commit protocol: all artifacts (both full files, the combined diff, both baseline
# advances) are PREPARED before anything is published — nothing above this point in
# main() has touched a live output file or baseline. The combined diff is installed
# BEFORE either baseline is advanced, which is the guarantee that actually matters:
# a baseline must never move before its corresponding diff has been durably
# installed (durably = successfully renamed into place; this does not add fsync, so
# it is not power-loss-durable — acceptable for this job). Publication itself is
# NOT one atomic unit, though — it is a sequential series of installs and then
# sequential baseline exchanges (v4 file, v4 sidecar, v6 file, v6 sidecar, combined
# diff, v4 baseline, v4 state, v6 baseline, v6 state). A crash partway through that
# final sequence can leave a partial commit. That residual risk is accepted,
# matching the same category of accepted risk as the v4/v6 EXCHANGE TABLES pair not
# being atomic elsewhere in this pipeline, rather than building a full durable
# run-tracking/recovery system for a once-daily, human-supervised job. What the
# protocol actually eliminates is the earlier, more serious failure mode: a baseline
# silently advancing while its diff was never published at all — not every possible
# partial-failure ordering.
#
# The "baseline" is tracked internally in ClickHouse (pd_baseline_v4/v6 — just
# probing_directive_id) and pd_generation_state tracks the last committed --date per
# AFI, to refuse a run that would rewind an already-advanced baseline (a real risk
# given this tool's own standalone/manual-use design — a backfill run for an old
# date must not be allowed to silently corrupt the live daily pipeline's baseline).
#
# Diffing assumes every previous diff was actually applied downstream and nothing
# else changed the orchestrator's live set out of band. If that assumption is ever
# violated, the baseline silently drifts from reality — not fixed here.
#
# Expects the input tables to already exist, named
# iris_zeph__links__<date>_<index> for each index in --zeph-indices (not
# necessarily 0..n-1 — see fetch_iris_links.sh), and (if ipv6-fetched=1)
# iris_ipv6__links__<date> — exactly what fetch_iris_links.sh produces.
#
# On success, prints to stdout:
#   V4_PDS=<row count>
#   V6_PDS=<row count, 0 if IPv6 wasn't generated>
#   V4_DIFF_INSERT=<count>  V4_DIFF_REMOVE=<count>
#   V6_DIFF_INSERT=<count>  V6_DIFF_REMOVE=<count>
#

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1091"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
TOPLEVEL="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
readonly TOPLEVEL
source "${TOPLEVEL}/pipeline/common.sh"

#
# Global variables to support command line flags and arguments.
#
DATE=""			# --date
ZEPH_INDICES=""		# --zeph-indices (comma-separated, e.g. "0,2,3")
IPV6_FETCHED=0		# --ipv6-fetched
DRY_RUN=false		# --dry-run
OUTPUT_DIR="${HOME}/pds"	# --output-dir
VERBOSE=1		# --verbose

# Resources created during a run that must be cleaned up on any exit, success or
# failure — the staged-commit protocol keeps more state alive across more steps
# than before, so an interrupted run has more that could be left behind if this
# isn't tracked explicitly.
CLEANUP_TMP_FILES=()
CLEANUP_TABLES=()

cleanup() {
	local exit_code=$?

	if ((${#CLEANUP_TMP_FILES[@]} > 0)); then
		rm -f -- "${CLEANUP_TMP_FILES[@]}" 2>/dev/null || true
	fi
	# Best-effort: ClickHouse may itself be unavailable during cleanup (e.g. the
	# reason this run is failing at all), so these must never be allowed to mask
	# the real exit code or hang the exit path.
	local table
	for table in "${CLEANUP_TABLES[@]}"; do
		clickhouse client --query "DROP TABLE IF EXISTS ${table}" >/dev/null 2>&1 || true
	done

	exit "${exit_code}"
}
trap cleanup EXIT

#
# Print usage message and exit.
#
usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} -h
	${PROG_NAME} [-v <n>] [-n] -d <YYYYMMDD> --zeph-indices <indices> [--ipv6-fetched]

	-d, --date		date PDs are being generated for, YYYYMMDD (required)
	-h, --help		print help message and exit
	--zeph-indices <list>	comma-separated indices of the zeph tables to use as
				the stable-core input, e.g. "0,2,3" (required) — see
				fetch_iris_links.sh's ZEPH_INDICES output
	--ipv6-fetched		iris_ipv6__links__<date> exists and should be used
	-n, --dry-run		print what would be generated without querying
				ClickHouse or writing output files
	-o, --output-dir	directory for output PD files (default: ${OUTPUT_DIR})
	-v, --verbose		set the verbosity level, 0-3 (default: ${VERBOSE})
EOF
	exit "${exit_code}"
}

#
# backup_if_exists <path>
# If path already exists — e.g. this is a re-run for a date already processed, or
# a normal daily overwrite since output filenames are fixed — copies it to path.bak
# first, and does the same for path.date if it exists (a .bak without a matching
# .bak-of-the-date-it-was-generated-for isn't self-describing). Uses cp, not mv: the
# original must stay at `path` until the new content is actually ready to install
# via the final mv, so a failure between the backup step and the install step can
# never leave `path` missing entirely (mv-then-mv could; cp-then-mv can't, since the
# original is only ever replaced by one atomic rename). A single generation of
# backup, not accumulated history — each new backup overwrites the previous one.
#
backup_if_exists() {
	local path="$1"

	if [[ -e "${path}" ]]; then
		cp -- "${path}" "${path}.bak" || log_fatal "failed to back up ${path}"
		log_info 1 "backed up existing $(basename -- "${path}") -> $(basename -- "${path}").bak"
	fi
	if [[ -e "${path}.date" ]]; then
		cp -- "${path}.date" "${path}.date.bak" || log_fatal "failed to back up ${path}.date"
	fi
}

#
# install_date_sidecar <output_path> <date>
# Writes output_path.date via temp-then-rename, matching the same atomicity pattern
# already used for the main JSON output — a failure mid-write leaves the previous
# .date (if any) intact rather than a truncated one.
#
install_date_sidecar() {
	local output_path="$1"
	local date_value="$2"
	local tmp
	tmp="${output_path}.date.tmp"

	printf '%s\n' "${date_value}" > "${tmp}"
	mv -- "${tmp}" "${output_path}.date"
}

main() {
	parse_cmdline "$@"

	if ! "${DRY_RUN}"; then
		mkdir -p -- "${OUTPUT_DIR}"
	fi

	preflight_check_tables

	if "${DRY_RUN}"; then
		generate_ipv4_pds
		if [[ "${IPV6_FETCHED}" -eq 1 ]]; then
			generate_ipv6_pds
		fi
		echo "V4_PDS=0"
		echo "V6_PDS=0"
		echo "V4_DIFF_INSERT=0"
		echo "V4_DIFF_REMOVE=0"
		echo "V6_DIFF_INSERT=0"
		echo "V6_DIFF_REMOVE=0"
		return
	fi

	# Global, not OUTPUT_DIR-scoped: the actual shared resource this protects is
	# ClickHouse state (pd_baseline_v4/v6, pd_generation_state), not the output
	# directory — two invocations with DIFFERENT --output-dir values would still
	# race on the same baseline tables otherwise. This closes a real gap, not a
	# defensive restatement: pd_wrapper.sh's own lock only protects invocations
	# that go through the wrapper, but this script explicitly supports standalone
	# manual invocation (see its own header) as a first-class use case — a manual
	# run bypasses the wrapper's lock entirely and had no protection until now.
	local lock_file="/tmp/${PROG_NAME}.lock"
	if ! acquire_lock "${lock_file}"; then
		log_lock_details "${lock_file}"
		log_fatal "another PD generation is already running"
	fi

	ensure_generation_state_table
	check_date_not_older_than_committed v4

	# --- Prepare phase: compute everything, commit nothing yet ---
	generate_ipv4_pds
	local v4_pds="${GENERATED_ROWS}"
	local v4_insert="${DIFF_INSERT_COUNT}"
	local v4_remove="${DIFF_REMOVE_COUNT}"
	local v4_tmp_output="${TMP_FULL_OUTPUT}"
	local v4_diff_parts="${DIFF_PARTS_FILE}"
	local v4_staged_baseline="${STAGED_BASELINE_TABLE}"

	local v6_pds=0 v6_insert=0 v6_remove=0
	local v6_tmp_output="" v6_diff_parts="" v6_staged_baseline=""
	if [[ "${IPV6_FETCHED}" -eq 1 ]]; then
		check_date_not_older_than_committed v6
		generate_ipv6_pds
		v6_pds="${GENERATED_ROWS}"
		v6_insert="${DIFF_INSERT_COUNT}"
		v6_remove="${DIFF_REMOVE_COUNT}"
		v6_tmp_output="${TMP_FULL_OUTPUT}"
		v6_diff_parts="${DIFF_PARTS_FILE}"
		v6_staged_baseline="${STAGED_BASELINE_TABLE}"
	fi

	# --- Commit phase: install every durable artifact, THEN advance baselines ---
	local v4_output="${OUTPUT_DIR}/pds_v4.jsonl"
	backup_if_exists "${v4_output}"
	mv -- "${v4_tmp_output}" "${v4_output}"
	install_date_sidecar "${v4_output}" "${DATE}"

	if [[ -n "${v6_tmp_output}" ]]; then
		local v6_output="${OUTPUT_DIR}/pds_v6.jsonl"
		backup_if_exists "${v6_output}"
		mv -- "${v6_tmp_output}" "${v6_output}"
		install_date_sidecar "${v6_output}" "${DATE}"
	fi

	local diff_output="${OUTPUT_DIR}/pds_diff.jsonl"
	local tmp_diff_output
	tmp_diff_output=$(mktemp --tmpdir="${OUTPUT_DIR}" '.pds_diff.XXXXXX') ||
		log_fatal "failed to create temporary combined diff file"
	CLEANUP_TMP_FILES+=("${tmp_diff_output}")
	if [[ -n "${v6_diff_parts}" ]]; then
		cat -- "${v4_diff_parts}" "${v6_diff_parts}" > "${tmp_diff_output}"
	else
		cat -- "${v4_diff_parts}" > "${tmp_diff_output}"
	fi
	backup_if_exists "${diff_output}"
	mv -- "${tmp_diff_output}" "${diff_output}"
	install_date_sidecar "${diff_output}" "${DATE}"
	log_info 1 "combined diff -> ${diff_output}"

	# Only now — every file is durably installed — advance the baselines. This is
	# the point the Blocker review identified: everything above this line can fail
	# and be retried with nothing lost; a failure past this line is the narrow,
	# accepted residual risk described in this file's header comment.
	commit_baseline "pd_baseline_v4" "${v4_staged_baseline}" v4 "${DATE}"
	if [[ -n "${v6_staged_baseline}" ]]; then
		commit_baseline "pd_baseline_v6" "${v6_staged_baseline}" v6 "${DATE}"
	fi

	echo "V4_PDS=${v4_pds}"
	echo "V6_PDS=${v6_pds}"
	echo "V4_DIFF_INSERT=${v4_insert}"
	echo "V4_DIFF_REMOVE=${v4_remove}"
	echo "V6_DIFF_INSERT=${v6_insert}"
	echo "V6_DIFF_REMOVE=${v6_remove}"
}

#
# preflight_check_tables
# Confirms every input table this run needs actually exists before running the
# (large, slow) generation queries against them — a clear error here beats a
# ClickHouse "table doesn't exist" error surfacing from inside a 40-line query,
# especially for the standalone/manual-invocation case where --zeph-indices could
# reference a typo'd date or index. Skipped in dry-run, matching dry-run's existing
# "no ClickHouse queries at all" contract.
#
preflight_check_tables() {
	if "${DRY_RUN}"; then
		return
	fi

	check_database_engine

	local indices=()
	IFS=',' read -ra indices <<< "${ZEPH_INDICES}"
	local idx
	for idx in "${indices[@]}"; do
		require_table "iris_zeph__links__${DATE}_${idx}"
	done
	if [[ "${IPV6_FETCHED}" -eq 1 ]]; then
		require_table "iris_ipv6__links__${DATE}"
	fi
}

#
# check_database_engine
# EXCHANGE TABLES (used for the baseline commit) requires the Atomic or Shared
# database engine — fail clearly here rather than deep inside that call, once both
# AFIs' expensive generation work has already run.
#
check_database_engine() {
	local engine

	if ! engine=$(clickhouse client --query "SELECT engine FROM system.databases WHERE name = currentDatabase()"); then
		log_fatal "failed to query database engine — is ClickHouse reachable?"
	fi

	case "${engine}" in
	Atomic|Shared) ;;
	*) log_fatal "database engine must be Atomic or Shared, got: ${engine:-unknown}" ;;
	esac
}

#
# require_table <table>
#
require_table() {
	local table="$1"
	local exists

	if ! exists=$(clickhouse client --query "EXISTS TABLE ${table}"); then
		log_fatal "failed to check whether ClickHouse table exists: ${table}"
	fi
	if [[ "${exists}" != "1" ]]; then
		log_fatal "required ClickHouse table does not exist: ${table}"
	fi
}

#
# ensure_generation_state_table
# ReplacingMergeTree so a later commit for the same afi logically replaces the
# earlier row on read (via FINAL) — this table only ever needs "the latest
# committed date per afi", not a full history.
#
ensure_generation_state_table() {
	clickhouse client --query "
CREATE TABLE IF NOT EXISTS pd_generation_state (
    afi               String,
    last_committed_date String,
    committed_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(committed_at)
ORDER BY afi"
}

#
# check_date_not_older_than_committed <afi>
# Refuses to proceed if DATE is older than the last date this afi's baseline was
# actually committed for. Without this, a standalone/manual run for an old date
# (a real, intended use case for this tool — backfill or debugging) could silently
# rewind the baseline the live daily pipeline depends on, corrupting its next diff.
# Empty result (first run ever for this afi) is not an error — nothing to rewind yet.
#
check_date_not_older_than_committed() {
	local afi="$1"
	local last_committed

	last_committed=$(clickhouse client --query "
SELECT last_committed_date FROM pd_generation_state FINAL WHERE afi = '${afi}'")

	if [[ -n "${last_committed}" && "${DATE}" < "${last_committed}" ]]; then
		log_fatal "--date ${DATE} is older than the last committed ${afi} baseline date (${last_committed}) — refusing to rewind the live baseline. If this old-date run is intentional (backfill/debug), its diff must not be applied to the orchestrator."
	fi
}

#
# ensure_baseline_table <baseline_table>
# Empty on first run (CREATE TABLE IF NOT EXISTS) — everything in that day's result
# then correctly shows up as "insert" and nothing as "remove", no special first-run
# handling needed.
#
ensure_baseline_table() {
	local baseline_table="$1"

	clickhouse client --query "
CREATE TABLE IF NOT EXISTS ${baseline_table} (
    probing_directive_id UInt64
) ENGINE = MergeTree()
ORDER BY probing_directive_id"
}

#
# prepare_diff <result_table> <baseline_table> <diff_parts_output>
# Computes insert (full records, tagged "op":"insert" — what ApplyDiff's toInsert
# needs) and remove (just the ID, tagged "op":"remove" — the only field ApplyDiff's
# toRemove actually reads) into diff_parts_output, and STAGES (but does not exchange)
# a new baseline table containing result_table's ID set. Sets
# DIFF_INSERT_COUNT/DIFF_REMOVE_COUNT and STAGED_BASELINE_TABLE as side effects.
# Nothing here is visible/committed — see commit_baseline for that.
#
prepare_diff() {
	local result_table="$1"
	local baseline_table="$2"
	local diff_parts_output="$3"

	ensure_baseline_table "${baseline_table}"

	local insert_part
	insert_part=$(mktemp --tmpdir="${OUTPUT_DIR}" '.pds_diff_insert.XXXXXX') ||
		log_fatal "failed to create temporary diff-insert file"
	CLEANUP_TMP_FILES+=("${insert_part}")
	if ! clickhouse client --query "
SELECT 'insert' AS op, probing_directive_id, ip_version, protocol, agent_id, destination_address, near_ttl, next_header
FROM ${result_table}
WHERE probing_directive_id NOT IN (SELECT probing_directive_id FROM ${baseline_table})
ORDER BY probing_directive_id
FORMAT JSONEachRow" > "${insert_part}"; then
		log_fatal "failed to compute insert diff for ${result_table}"
	fi
	DIFF_INSERT_COUNT=$(wc -l < "${insert_part}" | tr -d '[:space:]')

	local remove_part
	remove_part=$(mktemp --tmpdir="${OUTPUT_DIR}" '.pds_diff_remove.XXXXXX') ||
		log_fatal "failed to create temporary diff-remove file"
	CLEANUP_TMP_FILES+=("${remove_part}")
	if ! clickhouse client --query "
SELECT 'remove' AS op, probing_directive_id
FROM ${baseline_table}
WHERE probing_directive_id NOT IN (SELECT probing_directive_id FROM ${result_table})
ORDER BY probing_directive_id
FORMAT JSONEachRow" > "${remove_part}"; then
		log_fatal "failed to compute remove diff for ${baseline_table}"
	fi
	DIFF_REMOVE_COUNT=$(wc -l < "${remove_part}" | tr -d '[:space:]')

	cat -- "${insert_part}" "${remove_part}" > "${diff_parts_output}"

	local baseline_staging="${baseline_table}_staging_$$"
	CLEANUP_TABLES+=("${baseline_staging}")
	clickhouse client --query "DROP TABLE IF EXISTS ${baseline_staging}"
	if ! clickhouse client --query "
CREATE TABLE ${baseline_staging}
ENGINE = MergeTree()
ORDER BY probing_directive_id
AS SELECT probing_directive_id FROM ${result_table}"; then
		log_fatal "failed to prepare staged baseline for ${baseline_table}"
	fi
	STAGED_BASELINE_TABLE="${baseline_staging}"
}

#
# commit_baseline <baseline_table> <staged_table> <afi> <date>
# The second, final phase — only ever called after the combined diff file has
# already been durably installed. Exchanges the staged baseline into place and
# records the commit date. Staging + EXCHANGE, matching the pattern already used
# elsewhere in this pipeline for ClickHouse table swaps.
#
commit_baseline() {
	local baseline_table="$1"
	local staged_table="$2"
	local afi="$3"
	local date="$4"

	if ! clickhouse client --query "EXCHANGE TABLES ${baseline_table} AND ${staged_table}"; then
		log_fatal "failed to commit baseline for ${baseline_table}"
	fi
	clickhouse client --query "DROP TABLE IF EXISTS ${staged_table}"
	if ! clickhouse client --query "INSERT INTO pd_generation_state (afi, last_committed_date) VALUES ('${afi}', '${date}')"; then
		log_fatal "baseline committed for ${afi}, but the committed date could not be recorded — the date-monotonicity guard's own state is now stale, treating this as a failed run so it gets noticed rather than silently leaving that protection unreliable"
	fi
}

#
# validate_result <result_table> <afi_label>
# Two cheap sanity checks on a freshly generated result set, before it's ever
# written anywhere: no probing_directive_id collisions (the ID is a content hash,
# not a guaranteed-unique key — this doesn't prove correctness, just catches an
# unexpected collision loudly instead of silently dropping/merging rows), and no
# row left with agent_id='unknown' (the source-IP-to-region mapping failed to match
# for that row — a directive the orchestrator likely can't use, better to fail than
# publish it silently).
#
validate_result() {
	local result_table="$1"
	local afi_label="$2"
	local total
	local distinct

	# Two separate single-value queries, not one multi-column query — matches how
	# the rest of this codebase queries counts elsewhere, and avoids depending on
	# an assumption about ClickHouse's default multi-column output separator that
	# isn't verified against a real instance from this environment.
	total=$(clickhouse client --query "SELECT count() FROM ${result_table}")
	distinct=$(clickhouse client --query "SELECT uniqExact(probing_directive_id) FROM ${result_table}")
	if [[ "${total}" != "${distinct}" ]]; then
		log_fatal "${afi_label}: probing_directive_id collision detected (${total} rows, ${distinct} distinct IDs) — refusing to publish"
	fi

	local unknown_count
	unknown_count=$(clickhouse client --query "SELECT count() FROM ${result_table} WHERE agent_id = 'unknown'")
	if [[ "${unknown_count}" -gt 0 ]]; then
		log_fatal "${afi_label}: ${unknown_count} row(s) have agent_id='unknown' (source address didn't match any known agent) — refusing to publish"
	fi
}

#
# generate_ipv4_pds
# Builds the stable-core UNION across every table named in ZEPH_INDICES, then
# generates PDs from the FIRST such table (matching the original script's
# ${zeph_indices[0]} — not necessarily table _0, since an empty measurement earlier
# in processing order can leave a gap; using the wrong "first" table here would
# silently read from an empty table and produce zero PDs, or skip real data further
# along) restricted to (prefix, src_addr, ttl) tuples seen in every one of them — a
# link is only "stable" if it showed up consistently across all fetched
# measurements for the date. TTL is widened by +/-2 around any hop where the
# near/far endpoint's tier-1 membership is ambiguous (differs between near_addr and
# far_addr), to give the orchestrator a small window to re-resolve it.
#
# The result is materialized into a scratch table (not written straight to a file)
# so it can be read multiple times without re-running this expensive query: once
# for the full file (written to a TEMP path — nothing is installed here, see
# main()'s commit phase), once for the diff/staged-baseline (prepare_diff). Sets
# GENERATED_ROWS, TMP_FULL_OUTPUT, DIFF_INSERT_COUNT, DIFF_REMOVE_COUNT, and
# STAGED_BASELINE_TABLE as side effects (bash has no clean multi-value return
# without a global or printing to stdout, and stdout here is reserved for the final
# tool-level summary).
#
generate_ipv4_pds() {
	local t_start
	t_start=$(date +%s)
	local indices=()
	IFS=',' read -ra indices <<< "${ZEPH_INDICES}"
	local n_zeph=${#indices[@]}

	if "${DRY_RUN}"; then
		log_info 1 "[dry-run] would generate IPv4 PDs (stable core from ${n_zeph} measurements) and compute its diff"
		GENERATED_ROWS=0
		DIFF_INSERT_COUNT=0
		DIFF_REMOVE_COUNT=0
		return
	fi

	local stable_core_union=""
	local meas_num=0
	local idx
	for idx in "${indices[@]}"; do
		local table="iris_zeph__links__${DATE}_${idx}"
		if [[ ${meas_num} -gt 0 ]]; then
			stable_core_union+=" UNION ALL "
		fi
		stable_core_union+="
			SELECT DISTINCT probe_dst_prefix, probe_src_addr, near_ttl, ${meas_num} AS meas
			FROM ${table}
			WHERE (dictGet('tier1_trie_v4', 'origin_asn', toIPv4OrDefault(near_addr)) != 0
				OR dictGet('tier1_trie_v4', 'origin_asn', toIPv4OrDefault(far_addr)) != 0)"
		meas_num=$((meas_num + 1))
	done

	# $$ (this script's PID) makes the scratch table name unique per invocation —
	# same reasoning as the mktemp-based temp filenames elsewhere in this pipeline:
	# a manual run overlapping with the cron-triggered one shouldn't clobber a
	# fixed shared table name. Doesn't replace real locking (still the wrapper's
	# job), just closes this one specific collision.
	local result_table="pd_result_v4_$$"
	CLEANUP_TABLES+=("${result_table}")
	clickhouse client --query "DROP TABLE IF EXISTS ${result_table}"
	if ! clickhouse client --query "
CREATE TABLE ${result_table}
ENGINE = MergeTree()
ORDER BY probing_directive_id
AS
SELECT
    cityHash64(agent_id, destination_address, near_ttl, protocol, source_port, destination_port) AS probing_directive_id,
    4                                                                    AS ip_version,
    protocol,
    agent_id,
    destination_address,
    near_ttl,
    if(protocol = 17,
        map('udp_next_header', map('source_port', source_port, 'destination_port', destination_port)),
        map('icmp_next_header', map('first_half_word', source_port, 'second_half_word', 0)))  AS next_header
FROM (
    SELECT
        replaceOne(IPv6NumToString(min(probe_dst_addr)), '::ffff:', '')  AS destination_address,
        CASE replaceOne(IPv6NumToString(min(probe_src_addr)), '::ffff:', '')
            WHEN '10.0.0.2' THEN 'retina-asia-east1'
            WHEN '10.0.1.2' THEN 'retina-asia-northeast1'
            WHEN '10.0.2.2' THEN 'retina-asia-south1'
            WHEN '10.0.3.2' THEN 'retina-asia-southeast1'
            WHEN '10.0.4.2' THEN 'retina-europe-north1'
            WHEN '10.0.5.2' THEN 'retina-europe-west6'
            WHEN '10.0.6.2' THEN 'retina-me-central1'
            WHEN '10.0.7.2' THEN 'retina-southamerica-east1'
            WHEN '10.0.8.2' THEN 'retina-us-east1'
            WHEN '10.0.9.2' THEN 'retina-us-west4'
            ELSE 'unknown'
        END                                                              AS agent_id,
        ttl                                                              AS near_ttl,
        if(cityHash64(probe_dst_prefix, ttl) % 2 = 0, 1, 17)            AS protocol,
        toUInt16(cityHash64(probe_dst_prefix, ttl) % 64512 + 1024)       AS source_port,
        if(protocol = 17, toUInt16(cityHash64(probe_dst_prefix, ttl, 1) % 64512 + 1024), toUInt16(0)) AS destination_port
    FROM (
        SELECT
            probe_dst_prefix,
            probe_dst_addr,
            probe_src_addr,
            near_ttl + arrayJoin(if(
                dictGet('tier1_trie_v4', 'origin_asn', toIPv4OrDefault(near_addr)) !=
                dictGet('tier1_trie_v4', 'origin_asn', toIPv4OrDefault(far_addr)),
                CAST([-2, -1, 0, 1, 2], 'Array(Int8)'),
                CAST([0], 'Array(Int8)')))                              AS ttl
        FROM iris_zeph__links__${DATE}_${indices[0]}
        WHERE (probe_dst_prefix, probe_src_addr, near_ttl) IN (
            SELECT probe_dst_prefix, probe_src_addr, near_ttl
            FROM (${stable_core_union})
            GROUP BY probe_dst_prefix, probe_src_addr, near_ttl
            HAVING countDistinct(meas) = ${n_zeph}
        )
        AND ttl > 0
    )
    GROUP BY probe_dst_prefix, probe_src_addr, ttl
)"; then
		log_fatal "IPv4 PD generation failed"
	fi

	validate_result "${result_table}" "IPv4"

	TMP_FULL_OUTPUT=$(mktemp --tmpdir="${OUTPUT_DIR}" '.pds_v4.XXXXXX') ||
		log_fatal "failed to create temporary IPv4 output file"
	CLEANUP_TMP_FILES+=("${TMP_FULL_OUTPUT}")
	if ! clickhouse client --query "
SELECT probing_directive_id, ip_version, protocol, agent_id, destination_address, near_ttl, next_header
FROM ${result_table}
ORDER BY probing_directive_id
FORMAT JSONEachRow" > "${TMP_FULL_OUTPUT}"; then
		log_fatal "failed to write full IPv4 PD file from ${result_table}"
	fi

	DIFF_PARTS_FILE=$(mktemp --tmpdir="${OUTPUT_DIR}" '.pds_diff_parts_v4.XXXXXX') ||
		log_fatal "failed to create temporary IPv4 diff-parts file"
	CLEANUP_TMP_FILES+=("${DIFF_PARTS_FILE}")
	prepare_diff "${result_table}" "pd_baseline_v4" "${DIFF_PARTS_FILE}"

	GENERATED_ROWS=$(wc -l < "${TMP_FULL_OUTPUT}" | tr -d '[:space:]')
	log_info 1 "IPv4 PDs: ${GENERATED_ROWS} rows ($(($(date +%s) - t_start))s)"
	log_info 1 "IPv4 diff (staged): +${DIFF_INSERT_COUNT}/-${DIFF_REMOVE_COUNT}"
}

#
# generate_ipv6_pds
# Same TTL-widening logic as generate_ipv4_pds, but no stable-core UNION step — only
# one IPv6 measurement is ever fetched per date (see fetch_iris_links.sh), so there's
# nothing to intersect against. Same scratch-table/staged-diff pattern as
# generate_ipv4_pds — see its doc comment for the full reasoning.
#
generate_ipv6_pds() {
	local t_start
	t_start=$(date +%s)

	if "${DRY_RUN}"; then
		log_info 1 "[dry-run] would generate IPv6 PDs and compute its diff"
		GENERATED_ROWS=0
		DIFF_INSERT_COUNT=0
		DIFF_REMOVE_COUNT=0
		return
	fi

	local result_table="pd_result_v6_$$"
	CLEANUP_TABLES+=("${result_table}")
	clickhouse client --query "DROP TABLE IF EXISTS ${result_table}"
	if ! clickhouse client --query "
CREATE TABLE ${result_table}
ENGINE = MergeTree()
ORDER BY probing_directive_id
AS
SELECT
    cityHash64(agent_id, destination_address, near_ttl, protocol, source_port, destination_port) AS probing_directive_id,
    6                                                                    AS ip_version,
    protocol,
    agent_id,
    destination_address,
    near_ttl,
    if(protocol = 17,
        map('udp_next_header', map('source_port', source_port, 'destination_port', destination_port)),
        map('icmpv6_next_header', map('first_half_word', source_port, 'second_half_word', 0)))  AS next_header
FROM (
    SELECT
        toString(min(probe_dst_addr))                                    AS destination_address,
        CASE cityHash64(probe_dst_prefix, ttl) % 10
            WHEN 0 THEN 'retina-asia-east1'
            WHEN 1 THEN 'retina-asia-northeast1'
            WHEN 2 THEN 'retina-asia-south1'
            WHEN 3 THEN 'retina-asia-southeast1'
            WHEN 4 THEN 'retina-europe-north1'
            WHEN 5 THEN 'retina-europe-west6'
            WHEN 6 THEN 'retina-me-central1'
            WHEN 7 THEN 'retina-southamerica-east1'
            WHEN 8 THEN 'retina-us-east1'
            WHEN 9 THEN 'retina-us-west4'
        END                                                              AS agent_id,
        ttl                                                              AS near_ttl,
        if(cityHash64(probe_dst_prefix, ttl) % 2 = 0, 58, 17)           AS protocol,
        toUInt16(cityHash64(probe_dst_prefix, ttl) % 64512 + 1024)       AS source_port,
        if(protocol = 17, toUInt16(cityHash64(probe_dst_prefix, ttl, 1) % 64512 + 1024), toUInt16(0)) AS destination_port
    FROM (
        SELECT
            probe_dst_prefix,
            probe_dst_addr,
            near_ttl + arrayJoin(if(
                dictGet('tier1_trie_v6', 'origin_asn', near_addr) !=
                dictGet('tier1_trie_v6', 'origin_asn', far_addr),
                CAST([-2, -1, 0, 1, 2], 'Array(Int8)'),
                CAST([0], 'Array(Int8)')))                     AS ttl
        FROM iris_ipv6__links__${DATE}
        WHERE (dictGet('tier1_trie_v6', 'origin_asn', near_addr) != 0
            OR dictGet('tier1_trie_v6', 'origin_asn', far_addr) != 0)
        AND ttl > 0
    )
    GROUP BY probe_dst_prefix, ttl
)"; then
		log_fatal "IPv6 PD generation failed"
	fi

	validate_result "${result_table}" "IPv6"

	TMP_FULL_OUTPUT=$(mktemp --tmpdir="${OUTPUT_DIR}" '.pds_v6.XXXXXX') ||
		log_fatal "failed to create temporary IPv6 output file"
	CLEANUP_TMP_FILES+=("${TMP_FULL_OUTPUT}")
	if ! clickhouse client --query "
SELECT probing_directive_id, ip_version, protocol, agent_id, destination_address, near_ttl, next_header
FROM ${result_table}
ORDER BY probing_directive_id
FORMAT JSONEachRow" > "${TMP_FULL_OUTPUT}"; then
		log_fatal "failed to write full IPv6 PD file from ${result_table}"
	fi

	DIFF_PARTS_FILE=$(mktemp --tmpdir="${OUTPUT_DIR}" '.pds_diff_parts_v6.XXXXXX') ||
		log_fatal "failed to create temporary IPv6 diff-parts file"
	CLEANUP_TMP_FILES+=("${DIFF_PARTS_FILE}")
	prepare_diff "${result_table}" "pd_baseline_v6" "${DIFF_PARTS_FILE}"

	GENERATED_ROWS=$(wc -l < "${TMP_FULL_OUTPUT}" | tr -d '[:space:]')
	log_info 1 "IPv6 PDs: ${GENERATED_ROWS} rows ($(($(date +%s) - t_start))s)"
	log_info 1 "IPv6 diff (staged): +${DIFF_INSERT_COUNT}/-${DIFF_REMOVE_COUNT}"
}

#
# Parse the command line.
#
parse_cmdline() {
	local args
	local arg

	if ! args="$(getopt \
			--options "d:hno:v:" \
			--longoptions "date: help zeph-indices: ipv6-fetched dry-run output-dir: verbose:" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"

	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-d|--date) DATE="$1"; shift 1;;
		-h|--help) usage 0;;
		--zeph-indices) ZEPH_INDICES="$1"; shift 1;;
		--ipv6-fetched) IPV6_FETCHED=1;;
		-n|--dry-run) DRY_RUN=true;;
		-o|--output-dir) OUTPUT_DIR="$1"; shift 1;;
		-v|--verbose)
			if [[ ! "$1" =~ ^[0-3]$ ]]; then
				log_fatal "verbosity must be an integer from 0 to 3, got: $1"
			fi
			VERBOSE="$1"
			shift 1
			;;
		--) break;;
		*) log_fatal "panic: error parsing arg=${arg}";;
		esac
	done

	if [[ $# -ne 0 ]]; then
		log_fatal "unexpected positional argument: $1"
	fi

	if [[ -z "${DATE}" ]]; then
		log_error "--date is required"
		usage 1
	fi
	if [[ ! "${DATE}" =~ ^[0-9]{8}$ ]]; then
		log_fatal "--date must use YYYYMMDD format"
	fi
	# Round-trip through `date` to reject syntactically-valid-but-nonexistent
	# calendar dates (e.g. 20261399) — the format regex alone wouldn't catch this,
	# and without this check such a date would only surface later as a confusing
	# "table does not exist" error from preflight_check_tables.
	if ! date -d "${DATE:0:4}-${DATE:4:2}-${DATE:6:2}" '+%Y%m%d' 2>/dev/null | grep -qx "${DATE}"; then
		log_fatal "--date is not a valid calendar date: ${DATE}"
	fi
	if [[ -z "${ZEPH_INDICES}" ]]; then
		log_fatal "--zeph-indices is required and must be non-empty"
	fi
	validate_zeph_indices
}

#
# validate_zeph_indices
# ZEPH_INDICES gets interpolated directly into ClickHouse table names — normally
# it's produced by fetch_iris_links.sh's own stdout, always well-formed, but this
# script is also meant to be run standalone/manually, where a hand-typed value is a
# real (not just theoretical) source of typos. A duplicate index specifically isn't
# just a cosmetic mistake: it would inflate the stable-core UNION's meas count,
# silently making the HAVING countDistinct(meas) = n_zeph check stricter than
# intended and corrupting the result, not just erroring loudly.
#
validate_zeph_indices() {
	local indices=()
	local idx
	local -A seen=()

	# Explicit boundary check before splitting — confirmed empirically that
	# `IFS=',' read -ra` catches a leading or embedded empty field ("0,,2"
	# splits to an entry that fails the regex below) but NOT a trailing comma
	# ("0," silently splits to just ["0"], quietly dropping the empty trailing
	# field instead of rejecting it) — which could mask a real typo like meaning
	# to type "0,1" and missing the second number.
	if [[ "${ZEPH_INDICES}" == ,* || "${ZEPH_INDICES}" == *, || "${ZEPH_INDICES}" == *,,* ]]; then
		log_fatal "--zeph-indices must be a comma-separated list of integers, no leading/trailing/double commas: ${ZEPH_INDICES}"
	fi

	IFS=',' read -ra indices <<< "${ZEPH_INDICES}"

	for idx in "${indices[@]}"; do
		if [[ ! "${idx}" =~ ^[0-9]+$ ]]; then
			log_fatal "invalid --zeph-indices entry (not a non-negative integer): ${idx}"
		fi
		if [[ -n "${seen[${idx}]:-}" ]]; then
			log_fatal "duplicate --zeph-indices entry: ${idx}"
		fi
		seen["${idx}"]=1
	done
}

main "$@"