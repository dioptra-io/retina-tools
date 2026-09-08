#!/usr/bin/env bash
#
# Cron entry point for pipeline/pd_pipeline.sh: locking, failure marker, pruning.
#

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1091"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
TOPLEVEL="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
readonly TOPLEVEL
source "${TOPLEVEL}/pipeline/common.sh"
source "${TOPLEVEL}/conf/pd_settings.conf"

readonly VERBOSE=1
readonly LOCK_FILE="/tmp/${PROG_NAME}.lock"
readonly FAILURE_MARKER="${LOG_DIR}/last_failure.log"

# No lock-file deletion here — see tier1_wrapper.sh for why (stale lock files are
# harmless with flock).
cleanup() {
	local exit_code="$?"

	log_info 1 "exited with status ${exit_code}"
	log_line

	return "${exit_code}"
}
trap cleanup EXIT

main() {
	local date
	# Yesterday: today's measurements aren't finished yet when this runs.
	date="$(date -u -d 'yesterday' +%Y%m%d)"

	mkdir -p "${LOG_DIR}"
	log_info 1 "started VERBOSE=${VERBOSE} date=${date}"

	if ! acquire_lock "${LOCK_FILE}"; then
		log_lock_details "${LOCK_FILE}"
		exit 1
	fi

	if "${TOPLEVEL}/pipeline/pd_pipeline.sh" --date "${date}" --output-dir "${OUTPUT_DIR}"; then
		log_info 0 "pd_pipeline.sh succeeded"
	else
		fail_run "pd_pipeline.sh" "$?"
	fi

	rm -f "${FAILURE_MARKER}"

	prune_old_output
	prune_old_iris_tables

	log_info 0 "daily PD generation completed successfully"
}

#
# prune_old_output
# Fixed filenames mean this directory doesn't accumulate under normal operation —
# this only catches orphaned mktemp files from a killed/crashed run.
#
prune_old_output() {
	local removed

	if [[ ! "${PRUNE_DAYS}" =~ ^[0-9]+$ ]]; then
		log_fatal "PRUNE_DAYS must be a non-negative integer, got: ${PRUNE_DAYS}"
	fi
	if [[ "${OUTPUT_DIR}" != /* ]]; then
		log_fatal "OUTPUT_DIR must be an absolute path, got: ${OUTPUT_DIR}"
	fi

	removed=$(find "${OUTPUT_DIR}" -maxdepth 1 -type f -mtime "+${PRUNE_DAYS}" -print -delete | wc -l)
	if [[ "${removed}" -gt 0 ]]; then
		log_info 1 "pruned ${removed} output file(s) older than ${PRUNE_DAYS} days from ${OUTPUT_DIR}"
	fi
}

#
# prune_old_iris_tables
# Drops iris_zeph__links__<date>_N / iris_ipv6__links__<date> tables older than
# IRIS_TABLE_RETENTION_DAYS. Table date is parsed from the table name (fixed-width
# YYYYMMDD, so plain string comparison against the cutoff is safe).
#
prune_old_iris_tables() {
	if [[ ! "${IRIS_TABLE_RETENTION_DAYS}" =~ ^[0-9]+$ ]]; then
		log_fatal "IRIS_TABLE_RETENTION_DAYS must be a non-negative integer, got: ${IRIS_TABLE_RETENTION_DAYS}"
	fi

	local cutoff_date
	cutoff_date=$(date -u -d "-${IRIS_TABLE_RETENTION_DAYS} days" +%Y%m%d)

	# Captured to variables first, not read via `< <(...)` — a failure inside
	# process substitution is invisible to the parent shell.
	local zeph_tables
	local ipv6_tables
	if ! zeph_tables=$(clickhouse client --query "SHOW TABLES LIKE 'iris_zeph__links__%'"); then
		log_error "failed to list iris_zeph__links__* tables — skipping table pruning this run"
		return
	fi
	if ! ipv6_tables=$(clickhouse client --query "SHOW TABLES LIKE 'iris_ipv6__links__%'"); then
		log_error "failed to list iris_ipv6__links__* tables — skipping table pruning this run"
		return
	fi

	local table
	local table_date
	local removed=0
	local failed=0
	while IFS= read -r table; do
		[[ -z "${table}" ]] && continue
		table_date=$(grep -oP '\d{8}' <<< "${table}" | head -1)
		if [[ -n "${table_date}" && "${table_date}" < "${cutoff_date}" ]]; then
			# One failed DROP shouldn't kill an otherwise-successful run.
			if clickhouse client --query "DROP TABLE IF EXISTS ${table}"; then
				removed=$((removed + 1))
			else
				log_error "failed to drop old iris link table: ${table}"
				failed=$((failed + 1))
			fi
		fi
	done <<< "$(printf '%s\n%s' "${zeph_tables}" "${ipv6_tables}")"

	if [[ ${removed} -gt 0 ]]; then
		log_info 1 "dropped ${removed} iris link table(s) older than ${IRIS_TABLE_RETENTION_DAYS} days"
	fi
	if [[ ${failed} -gt 0 ]]; then
		log_error "${failed} iris link table(s) failed to drop — will be retried on the next successful run"
	fi
}

#
# fail_run <step> <exit_code>
# Writes the failure marker (Grafana/Loki can alert on this or on [ERROR] in logs)
# and exits non-zero.
#
fail_run() {
	local step="$1"
	local exit_code="$2"
	local timestamp
	timestamp=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

	echo "${timestamp} ${PROG_NAME}: FAILED at step=${step} exit_code=${exit_code}" > "${FAILURE_MARKER}"

	log_error "step ${step} failed (exit ${exit_code}) — see ${FAILURE_MARKER}"
	exit "${exit_code}"
}

main "$@"