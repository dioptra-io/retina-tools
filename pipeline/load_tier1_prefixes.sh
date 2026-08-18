#!/usr/bin/env bash
#
# Load tier-1 prefixes and exclusions into ClickHouse IP_TRIE tables.
#
# Schema: (prefix, origin_asn). A parent block is tagged with the tier-1 ASN it
# belongs to; an excluded (customer/other-network) prefix is tagged with origin_asn=0
# as a sentinel. AS 0 is reserved (RFC 7607) and never appears as a real BGP origin,
# and RPKI already uses it the same way (RFC 6491: "no AS authorized to originate
# this prefix") — so this isn't a fresh magic number, it's consistent with existing
# routing/RPKI convention. A trie lookup returning 0 means "known non-tier-1 space,
# do not treat as safe to probe."
#
# Loads into staging tables and swaps atomically — see create_tables_if_missing and
# the swap step in main() for how and why. The v4/v6 swaps are two separate
# EXCHANGE TABLES calls, not one transaction — if v4 succeeds and v6 fails, they'd
# briefly disagree on which generation is live. Accepted as a documented, low-
# probability, human-recoverable risk given this runs monthly with an operator
# watching, rather than built out into generation tables + a manifest system.
#

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1091"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
TOPLEVEL="$(git rev-parse --show-toplevel)"
readonly TOPLEVEL
source "${TOPLEVEL}/pipeline/common.sh"

#
# Global variables to support command line flags and arguments.
#
VERBOSE=1		# --verbose
V4_FILE=""		# <v4-file> (positional)
V6_FILE=""		# <v6-file> (positional)
POSITIONAL_ARGS=()

#
# Print usage message and exit.
#
usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} -h
	${PROG_NAME} [-v <n>] <v4-exclusions.json> <v6-exclusions.json>

	-h, --help	print help message and exit
	-v, --verbose	set the verbosity level, 0-3 (default: ${VERBOSE})
EOF
	exit "${exit_code}"
}

main() {
	parse_cmdline "$@"
	check_database_engine

	log_info 1 "creating tables (live + staging) if they don't exist..."
	create_tables_if_missing v4
	create_tables_if_missing v6

	log_info 1 "loading IPv4 into staging..."
	load_into_staging v4 "${V4_FILE}"
	log_info 1 "loading IPv6 into staging..."
	load_into_staging v6 "${V6_FILE}"

	# Only reached if both loads above succeeded (set -e stops the script on the
	# first failure) — this is the point of no return. Before this line, the live
	# tables have not been touched at all.
	log_info 1 "both loads succeeded — swapping staging into place..."
	clickhouse client --query "EXCHANGE TABLES tier1_prefixes_v4 AND tier1_prefixes_v4_staging"
	clickhouse client --query "EXCHANGE TABLES tier1_prefixes_v6 AND tier1_prefixes_v6_staging"
	# tier1_prefixes_v4/v6 now hold the new data; the _staging tables now hold last
	# month's old data. Deliberately NOT truncated here — that happens at the start
	# of the next run (see load_into_staging), so a failed future run always has
	# this month's data to fall back on in the meantime.

	log_info 1 "refreshing dictionaries..."
	refresh_dictionaries

	log_info 0 "done — dictionaries ready"
}

#
# check_database_engine
# EXCHANGE TABLES requires the Atomic (or Shared) database engine — fail clearly
# here rather than deep inside a swap call.
#
check_database_engine() {
	local engine

	engine="$(clickhouse client --query "
		SELECT engine FROM system.databases WHERE name = currentDatabase()
	")"

	case "${engine}" in
	Atomic|Shared) ;;
	*) log_fatal "database engine must be Atomic or Shared, got: ${engine:-unknown}" ;;
	esac
}

#
# create_tables_if_missing <v4|v6>
# Ensures both the live table and its staging counterpart exist, every run. This
# means EXCHANGE TABLES always has two real tables to swap — including on the very
# first run ever, where the live table just happens to be empty. No special-casing
# needed for "is this the first run."
#
create_tables_if_missing() {
	local suffix="$1"
	local name

	for name in "tier1_prefixes_${suffix}" "tier1_prefixes_${suffix}_staging"; do
		clickhouse client --query "
CREATE TABLE IF NOT EXISTS ${name} (
    prefix     String,
    origin_asn UInt32
) ENGINE = MergeTree()
ORDER BY prefix"
	done
}

#
# load_into_staging <v4|v6> <file>
# Truncates the staging table (so it starts clean, independent of whatever it held
# after the previous run's swap) and loads the given exclusion JSON into it. Parent
# blocks get the tier-1 ASN (the JSON's top-level key); exclusions get 0.
#
load_into_staging() {
	local suffix="$1"
	local file="$2"
	local staging="tier1_prefixes_${suffix}_staging"
	local row_count

	# Independent, correct count via jq itself — NOT `echo "$tsv" | wc -l`, which
	# reports 1 for empty input (echo "" still emits a newline) and would silently
	# let a malformed/empty file through as if it had one real row, truncating
	# staging and replacing it with nothing.
	row_count="$(jq '
		[
			to_entries[] |
			.key as $asn |
			.value[] |
			(.parent_block + "\t" + $asn),
			(.exclusions[] | . + "\t0")
		] | length
	' "${file}")"

	if [[ "${row_count}" -eq 0 ]]; then
		log_fatal "input file contains no prefixes: ${file}"
	fi

	clickhouse client --query "TRUNCATE TABLE ${staging}"

	# Streamed directly, not buffered into a bash variable first — safe here
	# because this is a plain two-stage pipe, which pipefail correctly monitors for
	# failures on either side (unlike a `tee >(...)` process substitution, which
	# pipefail cannot see into).
	jq -r '
		to_entries[] |
		.key as $asn |
		.value[] |
		(.parent_block + "\t" + $asn),
		(.exclusions[] | . + "\t0")
	' "${file}" | clickhouse client --query "INSERT INTO ${staging} FORMAT TSV"

	log_info 1 "inserted ${row_count} rows into ${staging}"
}

#
# refresh_dictionaries
# Recreates both IP_TRIE dictionaries so they pick up the swapped tables' contents.
# LIFETIME(0): no automatic periodic reload — this explicit recreate is what refreshes
# them, since the underlying data only changes once a month.
#
refresh_dictionaries() {
	clickhouse client --query "
CREATE OR REPLACE DICTIONARY tier1_trie_v4 (
    prefix     String,
    origin_asn UInt32
) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'tier1_prefixes_v4'))
LAYOUT(IP_TRIE())
LIFETIME(0)"
	clickhouse client --query "
CREATE OR REPLACE DICTIONARY tier1_trie_v6 (
    prefix     String,
    origin_asn UInt32
) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'tier1_prefixes_v6'))
LAYOUT(IP_TRIE())
LIFETIME(0)"
}

#
# Parse the command line.
#
parse_cmdline() {
	local args
	local arg

	if ! args="$(getopt \
			--options "hv:" \
			--longoptions "help verbose:" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"

	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-h|--help) usage 0;;
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

	POSITIONAL_ARGS=("$@")
	if [[ ${#POSITIONAL_ARGS[@]} -ne 2 ]]; then
		log_fatal "expected exactly 2 positional arguments (v4 file, v6 file), got ${#POSITIONAL_ARGS[@]}"
	fi
	V4_FILE="${POSITIONAL_ARGS[0]}"
	V6_FILE="${POSITIONAL_ARGS[1]}"

	if [[ ! -f "${V4_FILE}" ]]; then
		log_fatal "input file not found: ${V4_FILE}"
	fi
	if [[ ! -f "${V6_FILE}" ]]; then
		log_fatal "input file not found: ${V6_FILE}"
	fi
}

main "$@"