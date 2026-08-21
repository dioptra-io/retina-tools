#!/usr/bin/env bash
#
# Generates IPv4 and (if available) IPv6 probing directive (PD) files from the Iris
# links tables that pipeline/fetch_iris_links.sh loaded into ClickHouse for a given
# date. A standalone tool — usable manually or from pipeline/pd_pipeline.sh, which
# calls it as step 3 of the PD-generation pipeline.
#
# Expects the input tables to already exist, named
# iris_zeph__links__<date>_<index> for each index in --zeph-indices (not
# necessarily 0..n-1 — see fetch_iris_links.sh), and (if ipv6-fetched=1)
# iris_ipv6__links__<date> — exactly what fetch_iris_links.sh produces.
#
# On success, prints to stdout:
#   V4_PDS=<row count>
#   V6_PDS=<row count, 0 if IPv6 wasn't generated>
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

	local v4_pds=0
	local v6_pds=0

	generate_ipv4_pds
	v4_pds="${GENERATED_ROWS}"

	if [[ "${IPV6_FETCHED}" -eq 1 ]]; then
		generate_ipv6_pds
		v6_pds="${GENERATED_ROWS}"
	fi

	echo "V4_PDS=${v4_pds}"
	echo "V6_PDS=${v6_pds}"
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
# require_table <table>
#
require_table() {
	local table="$1"
	local exists

	# Captured to a variable first, not tested inline via [[ "$(...)" ]] — a
	# command substitution's failure inside a [[ ]] test is NOT caught by set -e
	# (confirmed: this is one of the cases set -e doesn't apply to), so a failing
	# clickhouse client call would otherwise silently produce an empty string and
	# get reported as "table does not exist" — hiding that the real problem is
	# ClickHouse itself being unreachable, not a missing table.
	if ! exists=$(clickhouse client --query "EXISTS TABLE ${table}"); then
		log_fatal "failed to check whether ClickHouse table exists: ${table}"
	fi
	if [[ "${exists}" != "1" ]]; then
		log_fatal "required ClickHouse table does not exist: ${table}"
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
# far_addr), to give the orchestrator a small window to re-resolve it. Sets
# GENERATED_ROWS as a side effect (bash has no clean multi-value return without a
# global or printing to stdout, and stdout here is reserved for the final tool-level
# V4_PDS/V6_PDS summary).
#
generate_ipv4_pds() {
	local t_start
	t_start=$(date +%s)
	local v4_output="${OUTPUT_DIR}/pds_v4.jsonl"
	local indices=()
	IFS=',' read -ra indices <<< "${ZEPH_INDICES}"
	local n_zeph=${#indices[@]}

	if "${DRY_RUN}"; then
		log_info 1 "[dry-run] would generate IPv4 PDs -> ${v4_output} (stable core from ${n_zeph} measurements)"
		GENERATED_ROWS=0
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

	local tmp_v4_output
	# mktemp, not a fixed .tmp suffix — a deterministic temp name could collide if
	# a manual invocation of this script ever overlapped with another one (or with
	# the cron-triggered run); this doesn't replace real locking (still the
	# wrapper's job, not this standalone tool's — see cron/pd_wrapper.sh), but it
	# closes the specific "two processes clobber the same temp file" failure mode
	# cheaply, independent of whether locking is added here later.
	tmp_v4_output=$(mktemp --tmpdir="${OUTPUT_DIR}" '.pds_v4.XXXXXX') ||
		log_fatal "failed to create temporary IPv4 output file"
	if ! clickhouse client --query "
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
        if(protocol = 17, toUInt16(cityHash64(probe_dst_prefix, ttl) % 64512 + 1024), toUInt16(0)) AS destination_port
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
)
ORDER BY cityHash64(agent_id, destination_address, near_ttl, protocol, source_port, destination_port)
FORMAT JSONEachRow" > "${tmp_v4_output}"; then
		rm -f -- "${tmp_v4_output}"
		log_fatal "IPv4 PD generation failed"
	fi
	backup_if_exists "${v4_output}"
	mv -- "${tmp_v4_output}" "${v4_output}"
	install_date_sidecar "${v4_output}" "${DATE}"

	GENERATED_ROWS=$(wc -l < "${v4_output}" | tr -d '[:space:]')
	log_info 1 "IPv4 PDs: ${GENERATED_ROWS} rows -> ${v4_output} ($(($(date +%s) - t_start))s)"
}

#
# generate_ipv6_pds
# Same TTL-widening logic as generate_ipv4_pds, but no stable-core UNION step — only
# one IPv6 measurement is ever fetched per date (see fetch_iris_links.sh), so there's
# nothing to intersect against. Sets GENERATED_ROWS as a side effect.
#
generate_ipv6_pds() {
	local t_start
	t_start=$(date +%s)
	local v6_output="${OUTPUT_DIR}/pds_v6.jsonl"

	if "${DRY_RUN}"; then
		log_info 1 "[dry-run] would generate IPv6 PDs -> ${v6_output}"
		GENERATED_ROWS=0
		return
	fi

	local tmp_v6_output
	tmp_v6_output=$(mktemp --tmpdir="${OUTPUT_DIR}" '.pds_v6.XXXXXX') ||
		log_fatal "failed to create temporary IPv6 output file"
	if ! clickhouse client --query "
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
)
ORDER BY cityHash64(agent_id, destination_address, near_ttl, protocol, source_port, destination_port)
FORMAT JSONEachRow" > "${tmp_v6_output}"; then
		rm -f -- "${tmp_v6_output}"
		log_fatal "IPv6 PD generation failed"
	fi
	backup_if_exists "${v6_output}"
	mv -- "${tmp_v6_output}" "${v6_output}"
	install_date_sidecar "${v6_output}" "${DATE}"

	GENERATED_ROWS=$(wc -l < "${v6_output}" | tr -d '[:space:]')
	log_info 1 "IPv6 PDs: ${GENERATED_ROWS} rows -> ${v6_output} ($(($(date +%s) - t_start))s)"
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

	if [[ -z "${DATE}" ]]; then
		log_error "--date is required"
		usage 1
	fi
	if [[ ! "${DATE}" =~ ^[0-9]{8}$ ]]; then
		log_fatal "--date must use YYYYMMDD format"
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