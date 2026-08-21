#!/usr/bin/env bash
#
# Generates Retina probing directives (PDs) for a given date: checks the tier-1
# IP_TRIE dictionaries are populated, fetches that date's finished Iris measurements
# into ClickHouse (pipeline/fetch_iris_links.sh), then generates IPv4/IPv6 PD files
# from them (pipeline/generate_pds.sh). A standalone tool — usable manually or from
# cron/pd_wrapper.sh, which adds locking and failure tracking on top of this.
#
# Prerequisites:
#   - tier-1 IP_TRIE dictionaries must be loaded via retina-tools
#     (see pipeline/load_tier1_prefixes.sh)
#   - irisctl must be available and configured
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
DATE=""				# --date
DRY_RUN=false			# --dry-run
OUTPUT_DIR="${HOME}/pds"	# --output-dir
VERBOSE=1			# --verbose

#
# Print usage message and exit.
#
usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} -h
	${PROG_NAME} [-v <n>] [-n] [-o <dir>] -d <YYYYMMDD>

	-d, --date		date to generate PDs for, YYYYMMDD (required)
	-h, --help		print help message and exit
	-n, --dry-run		resolve measurements and print planned actions
				without fetching data or writing output files
	-o, --output-dir	directory for output PD files (default: ${OUTPUT_DIR})
	-v, --verbose		set the verbosity level, 0-3 (default: ${VERBOSE})

Note: --dry-run is read-only with respect to Iris data and output files, but
      does read ClickHouse tier-1 tables and fetch measurement metadata.
EOF
	exit "${exit_code}"
}

main() {
	local pipeline_start
	pipeline_start=$(date +%s)

	parse_cmdline "$@"
	if ! "${DRY_RUN}"; then
		mkdir -p -- "${OUTPUT_DIR}"
	fi

	log_info 1 "=== Retina PD Pipeline for ${DATE} ==="
	log_info 1 "dry-run: ${DRY_RUN}"

	log_line
	log_info 1 "[1/4] checking tier-1 dictionaries..."
	check_tier1_dictionaries

	log_line
	log_info 1 "[2/4] fetching Iris links tables..."
	local fetch_output zeph_indices ipv6_fetched
	local dry_run_flag=()
	"${DRY_RUN}" && dry_run_flag=(--dry-run)
	if ! fetch_output="$("${TOPLEVEL}/pipeline/fetch_iris_links.sh" --date "${DATE}" "${dry_run_flag[@]}")"; then
		log_fatal "step 2 failed: fetch_iris_links.sh"
	fi
	zeph_indices="$(grep '^ZEPH_INDICES=' <<< "${fetch_output}" | cut -d= -f2)"
	ipv6_fetched="$(grep '^IPV6_FETCHED=' <<< "${fetch_output}" | cut -d= -f2)"

	log_line
	log_info 1 "[3/4] generating PDs..."
	local gen_output v4_pds v6_pds
	local ipv6_flag=()
	[[ "${ipv6_fetched}" -eq 1 ]] && ipv6_flag=(--ipv6-fetched)
	if ! gen_output="$("${TOPLEVEL}/pipeline/generate_pds.sh" \
		--date "${DATE}" \
		--zeph-indices "${zeph_indices}" \
		--output-dir "${OUTPUT_DIR}" \
		"${ipv6_flag[@]}" \
		"${dry_run_flag[@]}")"; then
		log_fatal "step 3 failed: generate_pds.sh"
	fi
	v4_pds="$(grep '^V4_PDS=' <<< "${gen_output}" | cut -d= -f2)"
	v6_pds="$(grep '^V6_PDS=' <<< "${gen_output}" | cut -d= -f2)"

	log_line
	log_info 1 "[4/4] summary:"
	log_info 1 "  IPv4 PDs: ${v4_pds}"
	log_info 1 "  IPv6 PDs: ${v6_pds}"
	log_info 1 "  total pipeline time: $(($(date +%s) - pipeline_start))s"
	log_info 0 "=== done ==="
}

#
# check_tier1_dictionaries
# Fails fast if the tier-1 prefix tables are empty or missing — generate_pds.sh's
# dictGet calls would otherwise silently treat every address as non-tier-1 (origin_asn
# defaults to 0), producing PDs from an empty target space rather than a clear error.
# Checks two distinct things: the source TABLES have rows (via count), and the
# DICTIONARIES built from them are actually loaded and queryable (via a
# representative dictGet — a dictionary that failed to attach/load errors here
# rather than just returning a default value, so this is a genuinely different
# check from the table count, not a redundant one).
#
check_tier1_dictionaries() {
	local v4_count v6_count

	# Captured to variables first, not folded into `|| echo "0"` — that pattern
	# makes "ClickHouse query failed" and "genuinely zero rows" indistinguishable,
	# reporting a misleading "tables are empty" when the real problem could be
	# ClickHouse being unreachable entirely.
	if ! v4_count=$(clickhouse client --query "SELECT count() FROM tier1_prefixes_v4 WHERE origin_asn != 0"); then
		log_fatal "failed to query tier1_prefixes_v4 — is ClickHouse reachable?"
	fi
	if ! v6_count=$(clickhouse client --query "SELECT count() FROM tier1_prefixes_v6 WHERE origin_asn != 0"); then
		log_fatal "failed to query tier1_prefixes_v6 — is ClickHouse reachable?"
	fi

	if [[ "${v4_count}" -eq 0 || "${v6_count}" -eq 0 ]]; then
		log_fatal "tier-1 prefix tables are empty — run retina-tools first"
	fi

	if ! clickhouse client --query "SELECT dictGet('tier1_trie_v4', 'origin_asn', toIPv4('1.1.1.1'))" >/dev/null; then
		log_fatal "tier1_trie_v4 dictionary is not loaded or not queryable"
	fi
	if ! clickhouse client --query "SELECT dictGet('tier1_trie_v6', 'origin_asn', toIPv6('::1'))" >/dev/null; then
		log_fatal "tier1_trie_v6 dictionary is not loaded or not queryable"
	fi

	log_info 1 "tier1_prefixes_v4: ${v4_count} rows"
	log_info 1 "tier1_prefixes_v6: ${v6_count} rows"
}

#
# Parse the command line.
#
parse_cmdline() {
	local args
	local arg

	if ! args="$(getopt \
			--options "d:hno:v:" \
			--longoptions "date: help dry-run output-dir: verbose:" \
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
	if [[ "${OUTPUT_DIR}" != /* ]]; then
		log_fatal "--output-dir must be an absolute path, got: ${OUTPUT_DIR}"
	fi

	# Round-trip through `date` to reject syntactically-valid-but-nonexistent
	# calendar dates (e.g. 20260231) — the format regex alone wouldn't catch this.
	local normalized
	normalized="$(date -d "${DATE:0:4}-${DATE:4:2}-${DATE:6:2}" '+%Y%m%d')" || {
		log_fatal "invalid calendar date: ${DATE}"
	}
	[[ "${normalized}" == "${DATE}" ]] || {
		log_fatal "invalid calendar date: ${DATE}"
	}
}

main "$@"