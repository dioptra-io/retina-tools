#!/usr/bin/env bash
#
# Generates IPv4 and (if available) IPv6 probing directive (PD) files from the Iris
# links tables that pipeline/fetch_iris_links.sh loaded into ClickHouse for a given
# date. A standalone tool — usable manually or from pipeline/pd_pipeline.sh, which
# calls it as step 3 of the PD-generation pipeline. Design rationale (commit
# protocol, baseline tracking, IPv6 agent mapping): see generator_spec.md.
#
# Produces:
#   pds_v4.jsonl / pds_v6.jsonl   full snapshot, split by AFI — bootstrap/inspection.
#   pds_diff.jsonl                one combined file, op-tagged JSONL:
#                                 {"op":"insert", ...full fields...} or the
#                                 minimal {"op":"remove","probing_directive_id":<id>}.
#
# Baseline (pd_baseline_v4/v6) only ever advances after pds_diff.jsonl is durably
# installed — never call commit_baseline before that. A run for an older --date
# than what's already in pds_v{4,6}.jsonl.date is refused (see
# check_date_not_older_than_committed).
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
# Copies path (and path.date, if present) to .bak before it's overwritten. Uses cp,
# not mv, so the original stays at `path` until the final mv actually installs new
# content — a failure in between can never leave `path` missing entirely.
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

	mkdir -p -- "${OUTPUT_DIR}"
	preflight_check_tables

	# Protects ClickHouse state (baselines), not the output directory — must stay
	# global, not OUTPUT_DIR-scoped.
	local lock_file="/tmp/${PROG_NAME}.lock"
	if ! acquire_lock "${lock_file}"; then
		log_lock_details "${lock_file}"
		log_fatal "another PD generation is already running"
	fi

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

	# Must come after every file above is installed — never move earlier.
	commit_baseline "pd_baseline_v4" "${v4_staged_baseline}"
	if [[ -n "${v6_staged_baseline}" ]]; then
		commit_baseline "pd_baseline_v6" "${v6_staged_baseline}"
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
# Confirms every input table exists before running the slow generation queries —
# a clear error here beats a cryptic one from inside a 40-line query.
#
preflight_check_tables() {
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
# EXCHANGE TABLES needs Atomic or Shared — fail here, not deep inside that call.
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
# check_date_not_older_than_committed <afi>
# Refuses a run that would rewind an afi's baseline to an older date — a real risk
# given standalone/manual use (a backfill run for an old date). Reads the .date
# sidecar already written by install_date_sidecar in the prior successful run's
# commit phase — no separate state table needed, since that file already records
# exactly this. Missing file (first run) is fine, nothing to rewind yet.
#
check_date_not_older_than_committed() {
	local afi="$1"
	local date_file="${OUTPUT_DIR}/pds_${afi}.jsonl.date"
	local last_committed=""

	if [[ -f "${date_file}" ]]; then
		last_committed=$(<"${date_file}")
	fi

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
# Computes insert (full records) and remove (ID only) into diff_parts_output, and
# stages (but doesn't exchange) a new baseline. Nothing here is committed — see
# commit_baseline.
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
	# ORDER BY probing_directive_id: see generate_ipv4_pds's full-output query.
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
	# ORDER BY here mainly for consistency/reproducibility with the insert query
	# above — remove entries are just IDs, not new probing targets, so the
	# input-ordering concern doesn't really apply to this one.
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
# commit_baseline <baseline_table> <staged_table>
# Final phase — only called after the diff file is durably installed. Exchanges the
# staged baseline into place.
#
commit_baseline() {
	local baseline_table="$1"
	local staged_table="$2"

	if ! clickhouse client --query "EXCHANGE TABLES ${baseline_table} AND ${staged_table}"; then
		log_fatal "failed to commit baseline for ${baseline_table}"
	fi
	clickhouse client --query "DROP TABLE IF EXISTS ${staged_table}"
}

#
# validate_result <result_table> <afi_label>
# Two sanity checks before publishing: no probing_directive_id collisions (it's a
# content hash, not a guaranteed-unique key), and no agent_id='unknown' (source
# address didn't match any known agent).
#
validate_result() {
	local result_table="$1"
	local afi_label="$2"
	local total
	local distinct

	# Two single-value queries, not one multi-column query — avoids depending on
	# ClickHouse's default column-separator format.
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
# Stable-core UNION across every table in ZEPH_INDICES, then generates PDs from the
# first such table restricted to (prefix, src_addr, ttl) tuples seen in all of them
# — a link is "stable" only if consistent across every fetched measurement for the
# date. TTL is widened +/-2 around any hop where tier-1 membership differs between
# near_addr and far_addr.
#
# Materializes into a scratch table (not straight to a file) so it can be read
# twice without re-running the expensive query: once for the full file, once for
# the diff (prepare_diff). Sets GENERATED_ROWS, TMP_FULL_OUTPUT, DIFF_INSERT_COUNT,
# DIFF_REMOVE_COUNT, and STAGED_BASELINE_TABLE as side effects.
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

	# $$ makes the scratch table name unique per invocation, closing the same
	# collision the mktemp temp filenames do elsewhere.
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
	# ORDER BY probing_directive_id (a hash) is deliberate, not just a tie-breaker:
	# the source links tables come in a correlated/ordered form, and without this,
	# that ordering would leak into the output file's row order too — undesirable
	# for a probing schedule. A hash-based sort scatters output order independent
	# of any input ordering.
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
# Same TTL-widening as generate_ipv4_pds, no stable-core UNION (only one IPv6
# measurement per date). Same scratch-table/staged-diff pattern.
#
# agent_id is matched from probe_src_addr against each agent's external IPv6 /64
# — a different address space from v4's internal 10.0.x.2. Matched by prefix, not
# exact address, to tolerate whatever host bits appear in real data. See
# generator_spec.md for the mapping's derivation and rationale.
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
        CASE
            WHEN startsWith(IPv6NumToString(min(probe_src_addr)), '2600:1900:4030:7f37:') THEN 'retina-asia-east1'
            WHEN startsWith(IPv6NumToString(min(probe_src_addr)), '2600:1900:4050:1d6:')  THEN 'retina-asia-northeast1'
            WHEN startsWith(IPv6NumToString(min(probe_src_addr)), '2600:1900:40a0:3fa:')  THEN 'retina-asia-south1'
            WHEN startsWith(IPv6NumToString(min(probe_src_addr)), '2600:1900:4080:652:')  THEN 'retina-asia-southeast1'
            WHEN startsWith(IPv6NumToString(min(probe_src_addr)), '2600:1900:4150:693:')  THEN 'retina-europe-north1'
            WHEN startsWith(IPv6NumToString(min(probe_src_addr)), '2600:1900:4160:6a2:')  THEN 'retina-europe-west6'
            WHEN startsWith(IPv6NumToString(min(probe_src_addr)), '2600:1901:81c0:30a:')  THEN 'retina-me-central1'
            WHEN startsWith(IPv6NumToString(min(probe_src_addr)), '2600:1900:40f0:170:')  THEN 'retina-southamerica-east1'
            WHEN startsWith(IPv6NumToString(min(probe_src_addr)), '2600:1900:4020:2532:') THEN 'retina-us-east1'
            WHEN startsWith(IPv6NumToString(min(probe_src_addr)), '2600:1900:4180:68e:')  THEN 'retina-us-west4'
            ELSE 'unknown'
        END                                                              AS agent_id,
        ttl                                                              AS near_ttl,
        if(cityHash64(probe_dst_prefix, ttl) % 2 = 0, 58, 17)           AS protocol,
        toUInt16(cityHash64(probe_dst_prefix, ttl) % 64512 + 1024)       AS source_port,
        if(protocol = 17, toUInt16(cityHash64(probe_dst_prefix, ttl, 1) % 64512 + 1024), toUInt16(0)) AS destination_port
    FROM (
        SELECT
            probe_dst_prefix,
            probe_dst_addr,
            probe_src_addr,
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
    GROUP BY probe_dst_prefix, probe_src_addr, ttl
)"; then
		log_fatal "IPv6 PD generation failed"
	fi

	validate_result "${result_table}" "IPv6"

	TMP_FULL_OUTPUT=$(mktemp --tmpdir="${OUTPUT_DIR}" '.pds_v6.XXXXXX') ||
		log_fatal "failed to create temporary IPv6 output file"
	CLEANUP_TMP_FILES+=("${TMP_FULL_OUTPUT}")
	# ORDER BY probing_directive_id: see generate_ipv4_pds's identical query for why.
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
# ZEPH_INDICES is normally well-formed (from fetch_iris_links.sh's own stdout), but
# this script also runs standalone, where a hand-typed value is a real typo risk. A
# duplicate index isn't just cosmetic — it inflates the stable-core UNION's meas
# count, silently corrupting the result rather than erroring loudly.
#
validate_zeph_indices() {
	local indices=()
	local idx
	local -A seen=()

	# `IFS=',' read -ra` catches "0,,2" but silently drops a trailing comma
	# ("0," -> just ["0"]) instead of rejecting it — checked explicitly here.
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