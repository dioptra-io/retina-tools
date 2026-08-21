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

readonly START_MAGENTA='\033[0;35m'
readonly START_BLUE='\033[0;34m'
readonly START_YELLOW='\033[0;33m'
readonly START_RED='\033[0;31m'
readonly END_COLOR='\033[0m'

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
	# Direct overwrite (not append) — we're already the exclusive holder at this
	# point, so this simply expresses "the file's contents are just this PID,"
	# and avoids accumulating every past holder's PID forever the way >> would.
	printf '%s\n' "$$" > "${lock}"
	log_info 1 "pid $$ acquired lock on ${lock}"
}

#
# Log an informative message for easier tracking and debugging.
#
log_info() {
	local level="$1"
	# Falls back to 1 if VERBOSE is unset — this library documents VERBOSE as a
	# precondition callers must set, and every current caller does, but a shared
	# library is safer defaulting for a future caller that forgets than crashing
	# on an unbound variable under set -u.
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
# Log a warning message — a problem worth flagging that doesn't stop the script,
# distinct from log_error (which also doesn't stop the script, but signals
# something more serious) and log_fatal (which does).
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
	# %b, not %s, for the four color-code args specifically — they're defined with
	# literal '\033[...]' text (regular single quotes, not $'...' ANSI-C quoting),
	# so something has to interpret that escape sequence into a real ESC byte at
	# print time. %s never does this for any argument; %b does, for arguments that
	# contain it. Message content ($*) stays on %s deliberately — it must NOT have
	# its own backslash sequences reinterpreted, which was the actual point of
	# moving off echo -e in the first place.
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