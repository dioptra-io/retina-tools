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
readonly FAILURE_MARKER="${LOG_DIR}/pd_last_failure.log"

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

	fetch_iris_password

	if "${TOPLEVEL}/pipeline/pd_pipeline.sh" --date "${date}" --output-dir "${OUTPUT_DIR}"; then
		log_info 0 "pd_pipeline.sh succeeded"
	else
		fail_run "pd_pipeline.sh" "$?"
	fi

	rm -f "${FAILURE_MARKER}"

	prune_old_output
	prune_old_iris_tables
	prune_orphaned_irisctl_files

	# Disabled until the orchestrator can handle the reload.
	# if ! docker compose --project-directory "${ORCHESTRATOR_COMPOSE_DIR}" kill -s HUP orchestrator; then
	# 	log_error "failed to send SIGHUP to orchestrator via docker compose (project dir: ${ORCHESTRATOR_COMPOSE_DIR}) — diff installed but reload not triggered"
	# fi

	log_info 0 "daily PD generation completed successfully"
}

# Exported so pd_pipeline.sh and irisctl inherit it; never stored in the crontab or on disk.
fetch_iris_password() {
	if ! IRIS_PASSWORD=$(gcloud secrets versions access latest --secret="${IRIS_PASSWORD_SECRET_NAME}"); then
		log_fatal "failed to fetch secret ${IRIS_PASSWORD_SECRET_NAME} from GCP Secret Manager"
	fi
	export IRIS_PASSWORD
}

# Filenames are fixed, so this only catches orphaned mktemp files from a crashed run.
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

# Drops iris_{zeph,ipv6}__links__<date> tables past IRIS_TABLE_RETENTION_DAYS.
# Fixed-width YYYYMMDD makes the string comparison safe.
prune_old_iris_tables() {
	if [[ ! "${IRIS_TABLE_RETENTION_DAYS}" =~ ^[0-9]+$ ]]; then
		log_fatal "IRIS_TABLE_RETENTION_DAYS must be a non-negative integer, got: ${IRIS_TABLE_RETENTION_DAYS}"
	fi

	local cutoff_date
	cutoff_date=$(date -u -d "-${IRIS_TABLE_RETENTION_DAYS} days" +%Y%m%d)

	# Not `< <(...)`: failures inside process substitution are invisible to the parent.
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

# irisctl never cleans up its /tmp/irisctl-clickhouse-* buffers (up to a few GB each),
# even after successful runs. The 3h cutoff is well past a single fetch (~10 min), and
# the prefix is exact because /tmp is shared.
prune_orphaned_irisctl_files() {
	local removed
	removed=$(find /tmp -maxdepth 1 -type f -name 'irisctl-clickhouse-*' -mmin +180 -print -delete | wc -l)
	if [[ "${removed}" -gt 0 ]]; then
		log_info 1 "removed ${removed} orphaned irisctl temp file(s) from /tmp"
	fi
}

# fail_run <step> <exit_code>: writes the failure marker and exits non-zero.
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