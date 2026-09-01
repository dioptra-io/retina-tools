#!/usr/bin/env bash
#
# Fetches finished Iris measurements (zeph/IPv4 and ipv6-hitlist) for a given date and
# loads their links tables into ClickHouse, named iris_zeph__links__<date>_<n> and
# iris_ipv6__links__<date>. A standalone tool — usable manually or from
# pipeline/pd_pipeline.sh, which calls it as step 2 of the PD-generation pipeline.
#
# On success, prints exactly two lines to stdout (nothing else goes to stdout — all
# progress/diagnostic output goes to stderr via log_info/log_warn/log_error), so a
# caller can capture them directly:
#   ZEPH_INDICES=<comma-separated indices of the zeph tables that were actually
#                 fetched with data, e.g. "0,2,3" — NOT necessarily contiguous, since
#                 an empty measurement still consumes a numeric slot (its table gets
#                 created but stays empty) without being included here>
#   IPV6_FETCHED=<0 or 1>
#

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1091"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
TOPLEVEL="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
readonly TOPLEVEL
source "${TOPLEVEL}/pipeline/common.sh"

#
# Internal constants.
#
# CH_USER_FILES must match wherever clickhouse-server is actually configured to read
# from — its user_files_path setting. ClickHouse's file() table function (used below
# to load fetched Parquet data) can only read from that one designated directory, as
# a security restriction, regardless of how the server itself is deployed.
#
# No config file exists for this ClickHouse instance, so user_files_path defaults to
# something relative to wherever `clickhouse server --daemon` was last launched
# FROM — not a fixed location. This is not theoretical: it has already changed once
# in production. Originally confirmed as
# /opt/retina/retina-tools/tier1exclusions/logs/user_files on 2026-08-21; after a
# later server restart (from $HOME), it silently became /opt/retina/user_files
# instead (confirmed 2026-09-01, same DATABASE_ACCESS_DENIED error pattern) — the
# hardcoded value below went stale the moment the server was restarted from a
# different directory, with no error until the next fetch actually ran. This WILL
# happen again on the next restart from yet another directory. Give clickhouse-
# server an explicit --user_files_path (or a real config file) before this recurs a
# third time — patching this constant again is treating the symptom, not the cause.
# If it fails again before that's done: the DATABASE_ACCESS_DENIED error reports the
# real current path directly, don't guess.
readonly CH_USER_FILES="/opt/retina/user_files"
TMP_DIR="$(mktemp -d)"
readonly TMP_DIR
readonly MEAS_MD_ALL="${TMP_DIR}/meas_md_all"

# Retry settings for fetch_table's download+load step — see attempt_fetch_and_load's
# doc comment for why this exists (a confirmed, not hypothetical, transient failure
# in production). 30s, not a few seconds — each fetch already takes 500-600s, so a
# longer cooldown before retrying is still negligible overhead, but gives an actual
# transient network/service issue real time to clear rather than immediately
# re-hammering whatever just failed.
readonly FETCH_MAX_ATTEMPTS=3
readonly FETCH_RETRY_DELAY=30

#
# Global variables to support command line flags and arguments.
#
DATE=""			# --date
DRY_RUN=false		# --dry-run
VERBOSE=1		# --verbose

cleanup() {
	local status=$?

	if [[ -n "${TMP_DIR:-}" && -d "${TMP_DIR}" ]]; then
		log_info 1 "removing ${TMP_DIR}"
		rm -rf -- "${TMP_DIR}" || true
	fi

	exit "${status}"
}
trap cleanup EXIT

#
# Print usage message and exit.
#
usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} -h
	${PROG_NAME} [-v <n>] [-n] -d <YYYYMMDD>

	-d, --date	date to fetch links for, YYYYMMDD (required)
	-h, --help	print help message and exit
	-n, --dry-run	resolve measurements and print planned actions without
			fetching data
	-v, --verbose	set the verbosity level, 0-3 (default: ${VERBOSE})

Note: --dry-run is read-only with respect to Iris data, but does fetch
      measurement metadata.
EOF
	exit "${exit_code}"
}

main() {
	local zeph_indices=()
	local zeph_seq=0
	local ipv6_fetched=0

	parse_cmdline "$@"

	if ! clickhouse client --query "SELECT 1" >/dev/null; then
		log_fatal "cannot reach ClickHouse — check before fetching, not after"
	fi

	mkdir -p "${CH_USER_FILES}"

	fetch_meas_metadata

	while IFS= read -r line; do
		[[ -z "${line}" ]] && continue
		local uuid
		read -r uuid _ <<< "${line}"
		if ! check_meas_agents "${uuid}"; then
			continue
		fi
		local fetch_status
		fetch_table "${uuid}" "iris_zeph__links__${DATE}_${zeph_seq}" 1 && fetch_status=0 || fetch_status=$?
		case ${fetch_status} in
		0) zeph_indices+=("${zeph_seq}"); zeph_seq=$((zeph_seq + 1)) ;;
		2) log_warn "skipping empty measurement ${uuid}"; zeph_seq=$((zeph_seq + 1)) ;;
		*) log_fatal "failed to fetch zeph measurement ${uuid}" ;;
		esac
	done < <(list_meas_for_date "${DATE}" "zeph-gcp-daily.json")

	while IFS= read -r line; do
		[[ -z "${line}" ]] && continue
		local uuid
		read -r uuid _ <<< "${line}"
		if ! check_meas_agents "${uuid}"; then
			continue
		fi
		local fetch_status
		fetch_table "${uuid}" "iris_ipv6__links__${DATE}" 58 && fetch_status=0 || fetch_status=$?
		case ${fetch_status} in
		0) ipv6_fetched=1; break ;;
		2) log_warn "skipping empty IPv6 measurement ${uuid}" ;;
		*) log_fatal "failed to fetch IPv6 measurement ${uuid}" ;;
		esac
	done < <(list_meas_for_date "${DATE}" "ipv6-hitlist.json")

	log_info 1 "fetched ${#zeph_indices[@]} zeph measurements, ${ipv6_fetched} IPv6 measurement"

	if [[ ${#zeph_indices[@]} -eq 0 ]]; then
		log_fatal "no zeph measurements found for ${DATE}"
	fi
	if [[ ${ipv6_fetched} -eq 0 ]]; then
		log_warn "no IPv6 measurement found for ${DATE} — skipping IPv6 PDs"
	fi

	echo "ZEPH_INDICES=$(IFS=,; echo "${zeph_indices[*]}")"
	echo "IPV6_FETCHED=${ipv6_fetched}"
}

#
# fetch_meas_metadata
# Downloads all measurement metadata via irisctl into MEAS_MD_ALL.
#
fetch_meas_metadata() {
	local output output_path

	log_info 1 "fetching all measurement metadata..."

	if ! output="$(irisctl meas --all-users 2>&1)"; then
		log_error "failed to fetch measurement metadata"
		log_error "${output}"
		log_fatal "cannot proceed without measurement metadata"
	fi

	output_path="$(awk '/saving in/ {print $NF; exit}' <<< "${output}")"

	if [[ -z "${output_path}" || ! -f "${output_path}" ]]; then
		log_error "could not identify downloaded metadata file"
		log_error "${output}"
		log_fatal "cannot proceed without measurement metadata"
	fi

	mv -- "${output_path}" "${MEAS_MD_ALL}"
	log_info 1 "metadata saved to ${MEAS_MD_ALL}"
}

#
# list_meas_for_date <date> <measurement_file>
# Lists finished measurements matching measurement_file for the given YYYYMMDD date.
#
list_meas_for_date() {
	local date="$1"
	local measurement_file="${2:?measurement file is required}"
	local iso_date="${date:0:4}-${date:4:2}-${date:6:2}"
	local after="${iso_date}T00:00:00.000000"
	local before

	before="$(date -d "${iso_date} + 1 day" '+%Y-%m-%d')T00:00:00.000000"
	log_info 1 "listing measurements for ${date} file=${measurement_file} (after=${after} before=${before})"
	irisctl list \
		-s finished \
		--after "${after}" \
		--before "${before}" \
		"${MEAS_MD_ALL}" | awk -v file="${measurement_file}" '$0 ~ file'
}

#
# check_meas_agents <uuid>
# Confirms every agent in the measurement succeeded (all but one "finished" state
# line correspond to a real agent — the extra one is the measurement itself).
#
check_meas_agents() {
	local uuid="$1"
	local tmp_file

	tmp_file=$(mktemp "${TMP_DIR}/meas_agent_check_XXXXXX")

	log_info 1 "irisctl meas --uuid ${uuid} -o > ${tmp_file}"
	if ! irisctl meas --uuid "${uuid}" -o > "${tmp_file}"; then
		log_error "failed to fetch metadata for ${uuid}"
		rm -f -- "${tmp_file}"
		return 1
	fi

	local num_agents num_state
	# `|| true` guards against grep -c's own quirk: it exits 1 (not 0) when the
	# count is genuinely zero, which set -e would otherwise treat as a real
	# failure and kill the script — a legitimately-zero count isn't an error here.
	num_agents="$(grep -c '"tool_parameters"' "${tmp_file}" || true)"
	num_state="$(grep -c '"state": "finished"' "${tmp_file}" || true)"
	rm -f -- "${tmp_file}"

	if [[ $((num_state - 1)) -ne ${num_agents} ]]; then
		log_warn "ignoring ${uuid}: only $((num_state - 1)) of ${num_agents} agents succeeded"
		return 1
	fi

	log_info 1 "all agents succeeded for ${uuid}"
	return 0
}

#
# attempt_fetch_and_load <uuid> <uuid_clean> <protocol_filter> <staging>
# One attempt at downloading a measurement's links and loading them into staging.
# Returns 0 (success), 1 (failure — retryable by the caller), or 2 (genuinely empty
# result — not retryable, since retrying wouldn't change a legitimately-zero-row
# outcome). Truncates staging first so a partial row set left behind by a prior
# failed attempt can't leak into this one.
#
attempt_fetch_and_load() {
	local uuid="$1"
	local uuid_clean="$2"
	local protocol_filter="$3"
	local staging="$4"
	local tmpfile

	clickhouse client --query "TRUNCATE TABLE ${staging}"

	tmpfile=$(mktemp "${CH_USER_FILES}/links_XXXXXX.parquet")

	if ! irisctl clickhouse --query "SELECT
		probe_src_addr, probe_dst_prefix, probe_dst_addr,
		probe_src_port, near_ttl,
		near_addr, far_addr
		FROM merge(currentDatabase(), '^links__${uuid_clean}.*')
		WHERE near_addr != '::' AND far_addr != '::'
		AND probe_protocol = ${protocol_filter}
		FORMAT PARQUET" > "${tmpfile}" 2>"${TMP_DIR}/irisctl_err_${uuid_clean}"; then
		log_error "failed to fetch links for ${uuid}"
		cat -- "${TMP_DIR}/irisctl_err_${uuid_clean}" >&2
		rm -f -- "${tmpfile}"
		return 1
	fi

	if [[ ! -s "${tmpfile}" ]]; then
		log_warn "empty output for ${uuid}, skipping"
		rm -f -- "${tmpfile}"
		return 2
	fi

	# irisctl appends a trailing newline after the Parquet footer which breaks
	# ClickHouse's Parquet reader (wrong magic bytes). Strip the last byte — but
	# only if the file is at least large enough to plausibly hold real Parquet
	# content (magic bytes alone are 4+4=8 bytes); a suspiciously tiny "non-empty"
	# file is a sign something else is wrong, not a file this workaround is safe
	# to blindly truncate.
	local size
	size=$(stat -c '%s' "${tmpfile}")
	if ((size < 9)); then
		log_error "Parquet output unexpectedly short (${size} bytes) for ${uuid}"
		rm -f -- "${tmpfile}"
		return 1
	fi
	truncate -s -1 "${tmpfile}"

	if ! clickhouse client --query "INSERT INTO ${staging} SELECT * FROM file('${tmpfile}', Parquet)"; then
		log_error "failed to load ${tmpfile} into ${staging}"
		rm -f -- "${tmpfile}"
		return 1
	fi
	rm -f -- "${tmpfile}"
	return 0
}

#
# fetch_table <uuid> <dest> <protocol_filter>
# Fetches one measurement's links table from Iris into a ClickHouse table named
# dest, filtered by protocol_filter (1=ICMP, 58=ICMPv6). Returns 2 (not an error) if
# the measurement produced no rows.
#
# Retries FETCH_MAX_ATTEMPTS times on a genuine failure (network blip during the
# irisctl download, a resulting corrupted/truncated Parquet file, etc.) — confirmed
# not hypothetical: a production run hit exactly this ("wrong magic bytes at the end
# of file") on one measurement out of four, while the other three succeeded via the
# identical code path, pointing at a transient issue with that one fetch rather than
# a systematic bug. Does NOT retry a genuinely empty result (return 2) — that's a
# legitimate outcome, not a failure, and retrying it would just waste time confirming
# the same true zero again.
#
fetch_table() {
	local uuid="$1"
	local dest="$2"
	local protocol_filter="$3"

	[[ "${uuid}" =~ ^[[:xdigit:]]{8}(-[[:xdigit:]]{4}){3}-[[:xdigit:]]{12}$ ]] || {
		log_error "invalid measurement UUID: ${uuid}"
		return 1
	}
	local uuid_clean="${uuid//-/_}"
	local t_start
	t_start=$(date +%s)

	if "${DRY_RUN}"; then
		log_info 1 "[dry-run] would fetch ${uuid} -> ${dest} (protocol=${protocol_filter})"
		return
	fi

	log_info 1 "fetching ${uuid} -> ${dest}"

	local staging="${dest}__staging"
	clickhouse client --query "DROP TABLE IF EXISTS ${staging}"
	clickhouse client --query "
		CREATE TABLE ${staging} (
			probe_src_addr   IPv6,
			probe_dst_prefix IPv6,
			probe_dst_addr   IPv6,
			probe_src_port   UInt16,
			near_ttl         UInt8,
			near_addr        IPv6,
			far_addr         IPv6,
			fetched_at       DateTime DEFAULT now()
		) ENGINE = MergeTree()
		ORDER BY (probe_dst_prefix, near_ttl, probe_src_addr)
		TTL fetched_at + INTERVAL 30 DAY"

	local attempt
	local status
	for ((attempt = 1; attempt <= FETCH_MAX_ATTEMPTS; attempt++)); do
		# && / || here, not a bare call — a bare nonzero-returning statement
		# under set -e would kill the script before "status" is ever assigned.
		attempt_fetch_and_load "${uuid}" "${uuid_clean}" "${protocol_filter}" "${staging}" && status=0 || status=$?
		if [[ ${status} -ne 1 ]]; then
			break # 0 (success) or 2 (empty) — stop retrying either way
		fi
		if [[ ${attempt} -lt ${FETCH_MAX_ATTEMPTS} ]]; then
			log_warn "fetch attempt ${attempt}/${FETCH_MAX_ATTEMPTS} failed for ${uuid}, retrying in ${FETCH_RETRY_DELAY}s..."
			sleep "${FETCH_RETRY_DELAY}"
		fi
	done

	if [[ ${status} -eq 1 ]]; then
		log_error "all ${FETCH_MAX_ATTEMPTS} attempts failed for ${uuid}"
		clickhouse client --query "DROP TABLE IF EXISTS ${staging}"
		return 1
	fi
	if [[ ${status} -eq 2 ]]; then
		clickhouse client --query "DROP TABLE IF EXISTS ${staging}"
		return 2
	fi

	local n_rows
	if ! n_rows=$(clickhouse client --query "SELECT count() FROM ${staging}"); then
		log_error "failed to count rows in ${staging}"
		clickhouse client --query "DROP TABLE IF EXISTS ${staging}"
		return 1
	fi

	# Only now — new data fetched, loaded, and counted successfully — replace
	# whatever dest previously held.
	clickhouse client --query "DROP TABLE IF EXISTS ${dest}"
	clickhouse client --query "RENAME TABLE ${staging} TO ${dest}"

	log_info 1 "fetched ${dest}: ${n_rows} rows ($(($(date +%s) - t_start))s)"
}

#
# Parse the command line.
#
parse_cmdline() {
	local args
	local arg

	if ! args="$(getopt \
			--options "d:hnv:" \
			--longoptions "date: help dry-run verbose:" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"

	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-d|--date) DATE="$1"; shift 1;;
		-h|--help) usage 0;;
		-n|--dry-run) DRY_RUN=true;;
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

	if [[ -z "${DATE}" ]]; then
		log_error "--date is required"
		usage 1
	fi
	if [[ ! "${DATE}" =~ ^[0-9]{8}$ ]]; then
		log_fatal "--date must use YYYYMMDD format"
	fi
}

main "$@"