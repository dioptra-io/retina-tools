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