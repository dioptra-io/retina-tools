#!/usr/bin/env bash
#
# Shared helpers for retina-tools/pipeline and retina-tools/cron scripts.
# Source with: source "${TOPLEVEL}/pipeline/common.sh"
#
# Callers are expected to set PROG_NAME and VERBOSE before calling log_info.
#
# Trimmed from iris-tools' pipeline/common.sh: no setup_environment/irisctl_auth
# (those are specific to Iris's sops-based credential flow, which retina-tools
# doesn't have yet — add it deliberately if/when ClickHouse needs the same kind of
# authenticated access, don't inherit it silently).
#

# Callers are responsible for their own strict-mode settings (set -euo pipefail) —
# this library doesn't set them itself, since sourcing a file with `set` runs in the
# caller's own shell, not a scoped subshell, and a helper library shouldn't silently
# change the caller's shell options out from under it.

source "${TOPLEVEL}/conf/common_settings.conf"

#
# Acquire lock before proceeding to avoid running multiple instances of the caller.
# Uses flock alone (no noclobber existence-check) — a stale lock FILE is harmless
# with flock, since the kernel lock itself is released when the owning process
# exits, regardless of whether the file remains on disk. Uses a dynamically
# allocated fd rather than a hardcoded number to avoid any collision with a caller
# using the same fixed fd for something else.
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
	# Truncate-then-write, not append — otherwise the file accumulates every past
	# holder's PID forever. Safe to do while holding the lock: we're the exclusive
	# holder, and truncating via a fresh open of the same path doesn't invalidate
	# the already-open lock_fd's flock (same inode, not a new one).
	: > "${lock}"
	printf '%s\n' "$$" >> "${lock}"
	log_info 1 "pid $$ acquired lock on ${lock}"
}

#
# Log an informative message for easier tracking and debugging.
#
log_info() {
	local level="$1"

	if [[ ${level} -lt 0 || ${level} -gt 3 ]]; then
		log_fatal "invalid verbosity level: ${level}"
	fi
	if [[ ${level} -gt ${VERBOSE} ]]; then
		return
	fi
	shift 1
	log_message "INFO" "$*"
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
# Log a message (common code for INFO and ERROR).
#
log_message() {
	local type="$1"
	local prog_color
	local msg_color
	local timestamp

	shift 1
	if [[ "${type}" != "ERROR" ]]; then
		prog_color="${START_MAGENTA}"
		msg_color="${START_BLUE}"
	else
		prog_color="${START_RED}"
		msg_color="${START_RED}"
	fi
	timestamp=$(date +'%Y-%m-%dT%H:%M:%SZ')
	(1>&2 echo -n -e "${prog_color}${timestamp} ${PROG_NAME}: ${END_COLOR}")
	(1>&2 echo -e "${msg_color}[${type}] $*${END_COLOR}")
}

#
# Log lock file details to aid in debugging.
#
log_lock_details() {
	local lock_file="$1"

	(1>&2 ls -li "${lock_file}")
	(1>&2 cat "${lock_file}")
}

#
# Log a separator line to visually distinguish between different sections of the logs.
#
log_line() {
	(printf '%*s\n' 72 '' | tr ' ' '-') 1>&2
}