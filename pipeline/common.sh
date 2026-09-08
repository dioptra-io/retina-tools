#!/usr/bin/env bash
#
# Shared helpers for retina-tools/pipeline and retina-tools/cron scripts.
# Source with: source "${TOPLEVEL}/pipeline/common.sh"
#
# Callers are expected to set PROG_NAME and VERBOSE before calling log_info, and
# their own set -euo pipefail — this library doesn't set shell options itself,
# since sourcing runs in the caller's shell, not a scoped subshell.
#
# No setup_environment/irisctl_auth (present in iris-tools' common.sh) — those are
# specific to Iris's sops-based credential flow, which retina-tools doesn't have.
# Add deliberately if/when needed, don't assume this is a gap to fill silently.
#

readonly START_MAGENTA='\033[0;35m'
readonly START_BLUE='\033[0;34m'
readonly START_YELLOW='\033[0;33m'
readonly START_RED='\033[0;31m'
readonly END_COLOR='\033[0m'

#
# Acquire a non-blocking lock. Stale lock files are harmless with flock (the kernel
# lock releases on process exit regardless of the file), so no noclobber check.
#
acquire_lock() {
	local lock="$1"
	local lock_fd

	exec {lock_fd}>>"${lock}"
	if ! flock -n "${lock_fd}"; then
		log_info 1 "another instance of ${PROG_NAME} must be running because ${lock} is locked"
		exec {lock_fd}>&-
		return 1
	fi
	printf '%s\n' "$$" > "${lock}"
	log_info 1 "pid $$ acquired lock on ${lock}"
}

#
# Log an informative message for easier tracking and debugging.
#
log_info() {
	local level="$1"
	local verbosity="${VERBOSE:-1}"

	if [[ ! "${level}" =~ ^[0-3]$ ]]; then
		log_fatal "invalid verbosity level: ${level}"
	fi
	if [[ ! "${verbosity}" =~ ^[0-3]$ ]]; then
		log_fatal "invalid VERBOSE value: ${verbosity}"
	fi
	if ((level > verbosity)); then
		return
	fi
	shift 1
	log_message "INFO" "$*"
}

#
# Log a warning — doesn't stop the script, distinct from log_error/log_fatal.
#
log_warn() {
	log_message "WARN" "$*"
}

#
# Log an error message.
#
log_error() {
	log_message "ERROR" "$*"
}

#
# Log a fatal error message and terminate the program with a non-zero exit code.
#
log_fatal() {
	log_message "ERROR" "$*"
	exit 1
}

#
# Log a message (common code for INFO, WARN, and ERROR).
#
log_message() {
	local type="$1"
	local prog_color
	local msg_color
	local timestamp

	shift 1
	case "${type}" in
	ERROR)
		prog_color="${START_RED}"
		msg_color="${START_RED}"
		;;
	WARN)
		prog_color="${START_YELLOW}"
		msg_color="${START_YELLOW}"
		;;
	*)
		prog_color="${START_MAGENTA}"
		msg_color="${START_BLUE}"
		;;
	esac
	timestamp=$(date +'%Y-%m-%dT%H:%M:%SZ')
	# %b (not %s) for the color-code args: they're literal '\033[...]' text, and
	# only %b interprets that into a real escape byte. Message content ($*) stays
	# on %s — must not have its own backslash sequences reinterpreted.
	printf '%b%s %s: %b%b[%s] %s%b\n' \
		"${prog_color}" \
		"${timestamp}" \
		"${PROG_NAME}" \
		"${END_COLOR}" \
		"${msg_color}" \
		"${type}" \
		"$*" \
		"${END_COLOR}" >&2
}

#
# Log lock file details to aid in debugging.
#
log_lock_details() {
	local lock_file="$1"

	ls -li -- "${lock_file}" >&2 || true
	cat -- "${lock_file}" >&2 || true
}

#
# Log a separator line to visually distinguish between different sections of the logs.
#
log_line() {
	printf '%*s\n' 72 '' | tr ' ' '-' >&2
}