#!/usr/bin/env bash
#
# Cron entry point for pipeline/tier1_pipeline.sh: locking, failure marker, pruning.
#

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1091"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
TOPLEVEL="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
readonly TOPLEVEL
source "${TOPLEVEL}/pipeline/common.sh"
source "${TOPLEVEL}/conf/tier1_settings.conf"

readonly VERBOSE=1
readonly LOCK_FILE="/tmp/${PROG_NAME}.lock"
readonly FAILURE_MARKER="${LOG_DIR}/last_failure.log"

# No lock-file deletion here: with flock (no noclobber), a stale lock file is
# harmless — the kernel lock releases when the owning process exits. Deleting it
# unconditionally would risk unlinking a file a different, still-running instance
# has open, letting a third racing instance bypass the lock via a fresh inode.
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

	rm -f "${FAILURE_MARKER}"

	prune_old_output

	log_info 0 "refresh completed successfully"
}

#
# prune_old_output
# Removes output files older than PRUNE_DAYS from DATA_DIR. Only reached after a
# successful run, so a failed run always still has prior runs' files.
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