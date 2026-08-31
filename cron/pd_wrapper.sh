#!/usr/bin/env bash
#
# This wrapper script is invoked by cron to run pipeline/pd_pipeline.sh (which
# checks tier-1 dictionaries, fetches Iris links, and generates PDs) for today's
# date, and to handle everything specific to running that as a scheduled,
# unattended job: locking against overlapping runs, recording failures where
# they'll actually be seen, and pruning old output files so ${OUTPUT_DIR} doesn't
# grow forever.
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

# No lock-file deletion here, deliberately — see tier1_wrapper.sh for the full
# reasoning: with flock (no noclobber), a stale lock FILE is harmless, and deleting
# it unconditionally risks unlinking a file a different, still-running instance has
# open.
cleanup() {
	local exit_code="$?"

	log_info 1 "exited with status ${exit_code}"
	log_line

	return "${exit_code}"
}
trap cleanup EXIT

main() {
	local date
	# Yesterday, not today: this wrapper runs early in the day (per crontab.txt),
	# before today's measurements have finished — the confirmed, previously
	# established decision is to always process the prior day's finished
	# measurements, not attempt to find today's (which wouldn't exist yet).
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

	# Clear any stale failure marker from a previous failed run — this run
	# succeeded end to end.
	rm -f "${FAILURE_MARKER}"

	prune_old_output
	prune_old_iris_tables

	log_info 0 "daily PD generation completed successfully"
}

#
# prune_old_output
# With fixed output filenames (pds_v4.jsonl/pds_v6.jsonl, overwritten daily, not
# date-named), this directory no longer accumulates files the way it used to — so
# this is now just a general safety net for any stray/orphaned files that end up
# here (manual test output, leftover .bak from a period of failed runs, etc.),
# not the primary defense against unbounded growth it originally was.
#
prune_old_output() {
	local removed

	# Cheap guard against a config-file typo before a destructive find -delete —
	# not defending against external input (PRUNE_DAYS/OUTPUT_DIR are only ever
	# set in conf/pd_settings.conf, which we control), but a human editing that
	# file by hand is a real, plausible way to end up with an empty or malformed
	# value, and the failure mode here (pruning far more than intended) is bad
	# enough to be worth two cheap checks.
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
# Drops iris_zeph__links__<date>_N and iris_ipv6__links__<date> tables older than
# IRIS_TABLE_RETENTION_DAYS. Unlike prune_old_output above, this addresses a real,
# currently-unbounded growth problem: fetch_iris_links.sh creates a brand new set of
# these tables every single day (confirmed 500M+ rows per zeph table in production),
# and their per-table `TTL fetched_at + INTERVAL 30 DAY` only prunes ROWS within an
# existing table — it does nothing about new dated tables piling up daily as full
# table objects. This drops the table objects themselves, on a much shorter
# retention than that 30-day row TTL.
#
# Table dates are parsed from the table name itself (e.g. the "20260825" in
# iris_zeph__links__20260825_0), not from any timestamp column — YYYYMMDD is a
# fixed-width, zero-padded format, so a plain string comparison against the cutoff
# date is safe and correctly matches chronological order.
#
prune_old_iris_tables() {
	if [[ ! "${IRIS_TABLE_RETENTION_DAYS}" =~ ^[0-9]+$ ]]; then
		log_fatal "IRIS_TABLE_RETENTION_DAYS must be a non-negative integer, got: ${IRIS_TABLE_RETENTION_DAYS}"
	fi

	local cutoff_date
	cutoff_date=$(date -u -d "-${IRIS_TABLE_RETENTION_DAYS} days" +%Y%m%d)

	# Captured into a variable first, not read directly from a `< <(...)` process
	# substitution — a failure inside process substitution is invisible to the
	# parent shell (pipefail can't see into it, same issue as elsewhere in this
	# codebase), so a failing SHOW TABLES would otherwise silently look like "no
	# tables to prune" instead of a real, reportable error.
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
			# A single failed DROP shouldn't kill the whole (otherwise-successful)
			# wrapper run — log it clearly and keep going with the rest, rather
			# than letting set -e abort mid-loop with no clear attribution.
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
# Writes the small failure marker and exits non-zero. Called instead of log_fatal so
# the marker file (not just stderr) records what failed — stderr output disappears
# into cron's default mail-on-error behavior, the marker doesn't.
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