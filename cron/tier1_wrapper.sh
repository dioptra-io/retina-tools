#!/usr/bin/env bash
#
# This wrapper script is invoked by cron to run pipeline/tier1_pipeline.sh (which
# generates fresh tier-1 exclusion lists and loads them into ClickHouse), and to
# handle everything specific to running that as a scheduled, unattended job: locking
# against overlapping runs, recording failures where they'll actually be seen, and
# pruning old output files so ${DATA_DIR} doesn't grow forever.
#

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1091"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
TOPLEVEL="$(git rev-parse --show-toplevel)"
readonly TOPLEVEL
source "${TOPLEVEL}/pipeline/common.sh"
source "${TOPLEVEL}/conf/tier1_settings.conf"

readonly VERBOSE=1
readonly LOCK_FILE="/tmp/${PROG_NAME}.lock"
readonly FAILURE_MARKER="${LOG_DIR}/last_failure.log"

# No lock-file deletion here, deliberately: with flock (no noclobber), a stale lock
# FILE is harmless — the kernel lock itself releases when the owning process exits,
# regardless of whether the file remains on disk. The next run's acquire_lock just
# reopens and re-locks it. Deleting it would only be cosmetic, and doing so
# unconditionally would risk unlinking a file a DIFFERENT, still-running instance
# has open — letting a third racing instance acquire a fresh inode's lock while the
# second instance still believes it holds the original one.
cleanup() {
	local exit_code="$?"

	log_info 1 "exited with status ${exit_code}"
	log_line

	return "${exit_code}"
}
trap cleanup EXIT

main() {
	mkdir -p "${LOG_DIR}"
	log_info 1 "started VERBOSE=${VERBOSE}"

	if ! acquire_lock "${LOCK_FILE}"; then
		log_lock_details "${LOCK_FILE}"
		exit 1
	fi

	if "${TOPLEVEL}/pipeline/tier1_pipeline.sh" --output-dir "${DATA_DIR}"; then
		log_info 0 "tier1_pipeline.sh succeeded"
	else
		fail_run "tier1_pipeline.sh" "$?"
	fi

	# Clear any stale failure marker from a previous failed run — this run
	# succeeded end to end.
	rm -f "${FAILURE_MARKER}"

	prune_old_output

	log_info 0 "monthly refresh completed successfully"
}

#
# prune_old_output
# Removes output files older than PRUNE_DAYS from DATA_DIR, so a directory that
# accumulates one run's worth of files every month doesn't grow unbounded forever.
# Only reached after a successful run — never prunes anything if the run failed, so
# a failed run always still has whatever prior months' files were already there.
#
prune_old_output() {
	local removed

	removed=$(find "${DATA_DIR}" -maxdepth 1 -type f -mtime "+${PRUNE_DAYS}" -print -delete | wc -l)
	if [[ "${removed}" -gt 0 ]]; then
		log_info 1 "pruned ${removed} output file(s) older than ${PRUNE_DAYS} days from ${DATA_DIR}"
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