#!/usr/bin/env bash
#
# Runs tier1exclusions to generate fresh tier-1 BGP exclusion lists, then loads the
# result into ClickHouse via load_tier1_prefixes.sh. A standalone tool — usable
# manually or from cron/tier1_wrapper.sh, which adds locking and failure tracking on
# top of this.
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
CONFIG_FILE="${TOPLEVEL}/tier1exclusions/prod.conf.json"	# --config
DATA_DIR="${TOPLEVEL}/tier1exclusions/output"			# --output-dir
RIB_DATE_FLAG=""						# --rib-date (default: today, UTC)
VERBOSE=1							# --verbose

#
# Print usage message and exit.
#
usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} -h
	${PROG_NAME} [-v <n>] [-c <config>] [-o <dir>] [-r <date>]

	-c, --config		tier1exclusions config file (default ${CONFIG_FILE})
	-h, --help		print help message and exit
	-o, --output-dir	directory for this run's output files
				(default ${DATA_DIR})
	-r, --rib-date		RIB snapshot date, YYYY-MM-DD (default: today, UTC)
	-v, --verbose		set the verbosity level (default: ${VERBOSE})
EOF
	exit "${exit_code}"
}

main() {
	local rib_date
	local v4_file
	local v6_file

	parse_cmdline "$@"

	log_info 1 "checking ClickHouse connectivity..."
	if ! clickhouse client --query "SELECT 1" >/dev/null; then
		log_fatal "cannot reach ClickHouse — check before running tier1exclusions, not after"
	fi

	rib_date="${RIB_DATE_FLAG:-$(date -u +%Y-%m-%d)}"
	mkdir -p "${DATA_DIR}"
	log_info 1 "RIB_DATE=${rib_date} DATA_DIR=${DATA_DIR}"

	log_line
	log_info 1 "step 1/2: running tier1exclusions..."
	run_tier1exclusions "${rib_date}"
	log_info 1 "tier1exclusions completed successfully"

	v4_file="${DATA_DIR}/tier1_exclusions_v4_${rib_date}.json"
	v6_file="${DATA_DIR}/tier1_exclusions_v6_${rib_date}.json"

	# tier1exclusions exiting 0 means it ran without error, not necessarily that it
	# wrote what we expect at the path we expect — check explicitly so a mismatch
	# fails clearly here, not with a confusing error one layer down inside the
	# loading script.
	if [[ ! -s "${v4_file}" ]]; then
		log_fatal "IPv4 output missing or empty: ${v4_file}"
	fi
	if [[ ! -s "${v6_file}" ]]; then
		log_fatal "IPv6 output missing or empty: ${v6_file}"
	fi

	log_line
	log_info 1 "step 2/2: loading into ClickHouse..."
	"${TOPLEVEL}/pipeline/load_tier1_prefixes.sh" "${v4_file}" "${v6_file}"
	log_info 1 "ClickHouse load completed successfully"

	log_line
	log_info 0 "pipeline completed successfully for RIB_DATE=${rib_date}"
}

#
# run_tier1exclusions <rib_date>
# Requires BGP_API_KEYS to already be set in the environment — this script does not
# manage secrets itself.
#
run_tier1exclusions() {
	local rib_date="$1"

	if [[ -z "${BGP_API_KEYS:-}" ]]; then
		log_fatal "BGP_API_KEYS is not set in the environment"
	fi

	RIB_DATE="${rib_date}" OUTPUT_DIR="${DATA_DIR}" tier1exclusions --config "${CONFIG_FILE}"
}

#
# Parse the command line.
#
parse_cmdline() {
	local args
	local arg

	if ! args="$(getopt \
			--options "c:ho:r:v:" \
			--longoptions "config: help output-dir: rib-date: verbose:" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"

	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-c|--config) CONFIG_FILE="$1"; shift 1;;
		-h|--help) usage 0;;
		-o|--output-dir) DATA_DIR="$1"; shift 1;;
		-r|--rib-date) RIB_DATE_FLAG="$1"; shift 1;;
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

	if [[ ! -f "${CONFIG_FILE}" ]]; then
		log_fatal "config file not found: ${CONFIG_FILE}"
	fi
}

main "$@"