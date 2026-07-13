#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
SCENARIO="${SCRIPT_DIR}/scenarios/odds_read.js"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
ODDS_PORT=${ODDS_PORT:-8085}
BASE_URL="http://localhost:${ODDS_PORT}"
REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6392}
KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9096}
MAVEN_REPO_LOCAL=${MAVEN_REPO_LOCAL:-"${REPO_ROOT}/../../.m2-release"}
BOOTSTRAP_TICK_INTERVAL_MS=500
FROZEN_TICK_INTERVAL_MS=900000
FROZEN_MINUTES_PER_SECOND=0.001
FROZEN_RANDOM_SEED=7050
LOAD_OTEL_SAMPLING_PROBABILITY=${LOAD_OTEL_SAMPLING_PROBABILITY:-0}
RUN_ID=${RUN_ID:-$(date '+%Y%m%d-%H%M%S')}
RESULT_ROOT=${RESULT_ROOT:-"${SCRIPT_DIR}/results/gate/${RUN_ID}"}
SERVICE_PID=''
CURRENT_OUTPUT_DIR=''
LOAD_PIDS=()
TERMINATION_GRACE_SECONDS=10
TERMINATION_KILL_WAIT_SECONDS=5
EVIDENCE_BINDING_READY=false

source_worktree_status() {
  local result_relative

  if [[ "${RESULT_ROOT}" == "${REPO_ROOT}/"* ]]; then
    result_relative=${RESULT_ROOT#"${REPO_ROOT}/"}
    git -C "${REPO_ROOT}" status --porcelain --untracked-files=all -- . \
      ":(exclude)${result_relative}" ":(exclude)${result_relative}/**"
  else
    git -C "${REPO_ROOT}" status --porcelain --untracked-files=all
  fi
}

for command in cmp curl docker git java jq k6 nc ps redis-cli shasum; do
  if ! command -v "${command}" > /dev/null 2>&1; then
    echo "Missing required command: ${command}" >&2
    exit 2
  fi
done

if [[ ! "${LOAD_OTEL_SAMPLING_PROBABILITY}" \
  =~ ^(0(\.[0-9]+)?|1(\.0+)?)$ ]]; then
  echo "LOAD_OTEL_SAMPLING_PROBABILITY must be between 0 and 1" >&2
  exit 2
fi

WORKTREE_STATUS=$(source_worktree_status)
if [[ -n "${WORKTREE_STATUS}" && "${ALLOW_DIRTY_CHARACTERIZATION:-0}" != "1" ]]; then
  echo "Final gate requires a clean worktree; refusing dirty source or harness inputs" >&2
  printf '%s\n' "${WORKTREE_STATUS}" >&2
  exit 2
fi
if [[ -n "${WORKTREE_STATUS}" ]]; then
  WORKTREE_CLEAN=false
else
  WORKTREE_CLEAN=true
fi

if [[ -e "${RESULT_ROOT}" ]]; then
  echo "Refusing to overwrite an existing evidence directory: ${RESULT_ROOT}" >&2
  exit 2
fi
mkdir -p "${RESULT_ROOT}"

verify_end_evidence_binding() {
  if [[ "${EVIDENCE_BINDING_READY}" != "true" ]]; then
    return 0
  fi

  local final_commit final_tree final_status final_clean
  local final_jar_sha final_runner_sha final_scenario_sha final_compose_sha
  local hash_output worktree_status_equal binding_status
  local binding_failed=0
  if ! final_commit=$(git -C "${REPO_ROOT}" rev-parse HEAD); then
    final_commit=ERROR
    binding_failed=1
  fi
  if ! final_tree=$(git -C "${REPO_ROOT}" rev-parse 'HEAD^{tree}'); then
    final_tree=ERROR
    binding_failed=1
  fi
  if ! final_status=$(source_worktree_status); then
    final_status=ERROR
    binding_failed=1
  fi
  if [[ -n "${final_status}" ]]; then
    final_clean=false
  else
    final_clean=true
  fi
  if hash_output=$(shasum -a 256 "${JAR_PATH}"); then
    final_jar_sha=${hash_output%% *}
  else
    final_jar_sha=ERROR
    binding_failed=1
  fi
  if hash_output=$(shasum -a 256 "${BASH_SOURCE[0]}"); then
    final_runner_sha=${hash_output%% *}
  else
    final_runner_sha=ERROR
    binding_failed=1
  fi
  if hash_output=$(shasum -a 256 "${SCENARIO}"); then
    final_scenario_sha=${hash_output%% *}
  else
    final_scenario_sha=ERROR
    binding_failed=1
  fi
  if hash_output=$(shasum -a 256 "${COMPOSE_FILE}"); then
    final_compose_sha=${hash_output%% *}
  else
    final_compose_sha=ERROR
    binding_failed=1
  fi

  [[ "${final_commit}" == "${INITIAL_SOURCE_COMMIT}" ]] || binding_failed=1
  [[ "${final_tree}" == "${INITIAL_SOURCE_TREE}" ]] || binding_failed=1
  [[ "${final_clean}" == "${WORKTREE_CLEAN}" ]] || binding_failed=1
  [[ "${final_status}" == "${WORKTREE_STATUS}" ]] || binding_failed=1
  [[ "${final_jar_sha}" == "${INITIAL_JAR_SHA}" ]] || binding_failed=1
  [[ "${final_runner_sha}" == "${INITIAL_RUNNER_SHA}" ]] || binding_failed=1
  [[ "${final_scenario_sha}" == "${INITIAL_SCENARIO_SHA}" ]] || binding_failed=1
  [[ "${final_compose_sha}" == "${INITIAL_COMPOSE_SHA}" ]] || binding_failed=1

  if [[ "${final_status}" == "${WORKTREE_STATUS}" ]]; then
    worktree_status_equal=true
  else
    worktree_status_equal=false
  fi
  if ! printf '%s' "${final_status}" > "${RESULT_ROOT}/end-worktree-status.txt"; then
    echo "Could not write the final worktree status" >&2
    binding_failed=1
  fi
  if [[ "${binding_failed}" == "0" ]]; then
    binding_status=PASS
  else
    binding_status=FAIL
  fi

  if ! {
    echo "initial_source_commit=${INITIAL_SOURCE_COMMIT}"
    echo "final_source_commit=${final_commit}"
    echo "initial_source_tree=${INITIAL_SOURCE_TREE}"
    echo "final_source_tree=${final_tree}"
    echo "initial_worktree_clean=${WORKTREE_CLEAN}"
    echo "final_worktree_clean=${final_clean}"
    echo "worktree_status_equal=${worktree_status_equal}"
    echo "initial_jar_sha256=${INITIAL_JAR_SHA}"
    echo "final_jar_sha256=${final_jar_sha}"
    echo "initial_runner_sha256=${INITIAL_RUNNER_SHA}"
    echo "final_runner_sha256=${final_runner_sha}"
    echo "initial_scenario_sha256=${INITIAL_SCENARIO_SHA}"
    echo "final_scenario_sha256=${final_scenario_sha}"
    echo "initial_compose_sha256=${INITIAL_COMPOSE_SHA}"
    echo "final_compose_sha256=${final_compose_sha}"
    echo "binding_status=${binding_status}"
  } > "${RESULT_ROOT}/end-binding.txt"; then
    echo "Could not write the end-of-run binding manifest" >&2
    binding_failed=1
  fi

  if [[ "${binding_failed}" != "0" ]]; then
    echo "End-of-run source/evidence binding diverged from the initial manifest" >&2
    return 1
  fi
}

cleanup() {
  local exit_code=$?
  local cleanup_code=0
  local binding_code=0
  local cleanup_dir
  trap - EXIT
  set +e
  if [[ -n "${CURRENT_OUTPUT_DIR}" ]]; then
    cleanup_dir=${CURRENT_OUTPUT_DIR}
  else
    cleanup_dir="${RESULT_ROOT}/final-cleanup"
  fi
  mkdir -p "${cleanup_dir}"
  stop_endpoint_infrastructure "${cleanup_dir}" "exit" || cleanup_code=$?
  verify_end_evidence_binding || binding_code=$?
  if [[ "${exit_code}" == "0" && "${cleanup_code}" != "0" ]]; then
    exit_code=${cleanup_code}
  fi
  if [[ "${exit_code}" == "0" && "${binding_code}" != "0" ]]; then
    exit_code=${binding_code}
  fi
  exit "${exit_code}"
}

process_has_exited() {
  local pid=$1
  local state

  if ! kill -0 "${pid}" > /dev/null 2>&1; then
    return 0
  fi
  if ! state=$(ps -o stat= -p "${pid}" 2> /dev/null); then
    return 0
  fi
  state=${state#"${state%%[![:space:]]*}"}
  [[ -z "${state}" || "${state}" == Z* ]]
}

terminate_pid() {
  local pid=$1
  local label=$2
  local deadline

  if process_has_exited "${pid}"; then
    wait "${pid}" > /dev/null 2>&1 || true
    return 0
  fi

  kill -TERM "${pid}" > /dev/null 2>&1 || true
  deadline=$((SECONDS + TERMINATION_GRACE_SECONDS))
  while ! process_has_exited "${pid}" && (( SECONDS < deadline )); do
    sleep 0.25
  done
  if ! process_has_exited "${pid}"; then
    echo "${label} process ${pid} did not stop after ${TERMINATION_GRACE_SECONDS}s; sending KILL" >&2
    kill -KILL "${pid}" > /dev/null 2>&1 || true
    deadline=$((SECONDS + TERMINATION_KILL_WAIT_SECONDS))
    while ! process_has_exited "${pid}" && (( SECONDS < deadline )); do
      sleep 0.25
    done
  fi
  if ! process_has_exited "${pid}"; then
    echo "${label} process ${pid} survived TERM, KILL, and the bounded post-KILL poll" >&2
    return 1
  fi
  wait "${pid}" > /dev/null 2>&1 || true
}

stop_service() {
  local termination_failed=0
  if [[ -n "${SERVICE_PID}" ]] && kill -0 "${SERVICE_PID}" > /dev/null 2>&1; then
    terminate_pid "${SERVICE_PID}" service || termination_failed=1
  fi
  SERVICE_PID=''
  return "${termination_failed}"
}

stop_load_generators() {
  local pid
  local termination_failed=0
  for pid in "${LOAD_PIDS[@]}"; do
    terminate_pid "${pid}" "load generator" || termination_failed=1
  done
  LOAD_PIDS=()
  return "${termination_failed}"
}

prove_endpoint_cleanup() {
  local output_dir=$1
  local prefix=$2
  local inventory_failed=0

  if ! docker ps -aq --filter label=com.docker.compose.project=odds-load-gate \
    > "${output_dir}/${prefix}-containers.txt" \
    2> "${output_dir}/${prefix}-containers.err"; then
    echo "Failed to inventory odds gate containers" >&2
    inventory_failed=1
  fi
  if ! docker volume ls -q --filter label=com.docker.compose.project=odds-load-gate \
    > "${output_dir}/${prefix}-volumes.txt" \
    2> "${output_dir}/${prefix}-volumes.err"; then
    echo "Failed to inventory odds gate volumes" >&2
    inventory_failed=1
  fi
  if ! docker network ls -q --filter label=com.docker.compose.project=odds-load-gate \
    > "${output_dir}/${prefix}-networks.txt" \
    2> "${output_dir}/${prefix}-networks.err"; then
    echo "Failed to inventory odds gate networks" >&2
    inventory_failed=1
  fi

  local ports_clean=true
  if nc -z localhost "${ODDS_PORT}"; then
    echo "occupied ${ODDS_PORT}" > "${output_dir}/${prefix}-ports.txt"
    ports_clean=false
  else
    echo "free ${ODDS_PORT}" > "${output_dir}/${prefix}-ports.txt"
  fi
  if nc -z localhost "${REDIS_PORT}"; then
    echo "occupied ${REDIS_PORT}" >> "${output_dir}/${prefix}-ports.txt"
    ports_clean=false
  else
    echo "free ${REDIS_PORT}" >> "${output_dir}/${prefix}-ports.txt"
  fi
  if nc -z localhost 9096; then
    echo "occupied 9096" >> "${output_dir}/${prefix}-ports.txt"
    ports_clean=false
  else
    echo "free 9096" >> "${output_dir}/${prefix}-ports.txt"
  fi

  if [[ "${inventory_failed}" != "0" \
    || -s "${output_dir}/${prefix}-containers.txt" \
    || -s "${output_dir}/${prefix}-volumes.txt" \
    || -s "${output_dir}/${prefix}-networks.txt" \
    || "${ports_clean}" != "true" ]]; then
    echo "Odds gate cleanup left a container, volume, network, or bound port" >&2
    return 1
  fi
}

stop_endpoint_infrastructure() {
  local output_dir=$1
  local prefix=${2:-endpoint}
  local cleanup_failed=0

  stop_load_generators || cleanup_failed=1
  stop_service || cleanup_failed=1
  docker compose -f "${COMPOSE_FILE}" ps --all \
    > "${output_dir}/${prefix}-compose-ps-before-down.txt" 2>&1 || cleanup_failed=1
  docker compose -f "${COMPOSE_FILE}" logs --no-color \
    > "${output_dir}/${prefix}-compose.log" 2>&1 || cleanup_failed=1
  docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans \
    > "${output_dir}/${prefix}-compose-down.log" 2>&1 || cleanup_failed=1
  prove_endpoint_cleanup "${output_dir}" "${prefix}-post-down" || cleanup_failed=1
  return "${cleanup_failed}"
}

trap cleanup EXIT
trap 'exit 130' INT TERM

"${REPO_ROOT}/mvnw" -B -Dmaven.repo.local="${MAVEN_REPO_LOCAL}" clean verify \
  > "${RESULT_ROOT}/maven-clean-verify.log" 2>&1

JAR_PATH="${REPO_ROOT}/target/odds-feed-service-0.1.0-SNAPSHOT.jar"
if [[ ! -f "${JAR_PATH}" ]]; then
  echo "Expected jar was not built: ${JAR_PATH}" >&2
  exit 1
fi

INITIAL_SOURCE_COMMIT=$(git -C "${REPO_ROOT}" rev-parse HEAD)
INITIAL_SOURCE_TREE=$(git -C "${REPO_ROOT}" rev-parse 'HEAD^{tree}')
INITIAL_JAR_SHA=$(shasum -a 256 "${JAR_PATH}")
INITIAL_JAR_SHA=${INITIAL_JAR_SHA%% *}
INITIAL_RUNNER_SHA=$(shasum -a 256 "${BASH_SOURCE[0]}")
INITIAL_RUNNER_SHA=${INITIAL_RUNNER_SHA%% *}
INITIAL_SCENARIO_SHA=$(shasum -a 256 "${SCENARIO}")
INITIAL_SCENARIO_SHA=${INITIAL_SCENARIO_SHA%% *}
INITIAL_COMPOSE_SHA=$(shasum -a 256 "${COMPOSE_FILE}")
INITIAL_COMPOSE_SHA=${INITIAL_COMPOSE_SHA%% *}
EVIDENCE_BINDING_READY=true

{
  echo "source_commit=${INITIAL_SOURCE_COMMIT}"
  echo "source_tree=${INITIAL_SOURCE_TREE}"
  echo "worktree_clean=${WORKTREE_CLEAN}"
  echo "maven_repository=${MAVEN_REPO_LOCAL}"
  echo "maven_clean_verify=true"
  echo "release_gate_endpoint_groups=events,odds"
  echo "simultaneous_observation_endpoint_groups=events,odds"
  echo "simultaneous_observation_release_blocking=false"
  echo "cold_service_restart_per_group=true"
  echo "fresh_redis_kafka_volumes_per_group=true"
  echo "mock_auto_rotate=false"
  echo "mock_minutes_per_second=${FROZEN_MINUTES_PER_SECOND}"
  echo "mock_random_seed=${FROZEN_RANDOM_SEED}"
  echo "bootstrap_tick_interval_ms=${BOOTSTRAP_TICK_INTERVAL_MS}"
  echo "frozen_tick_interval_ms=${FROZEN_TICK_INTERVAL_MS}"
  echo "odds_bootstrap_then_frozen=true"
  echo "odds_baseline_captured_after_bootstrap_stop=true"
  echo "frozen_value_verified_after_readiness=true"
  echo "events_canonical_list_baseline=true"
  echo "events_every_k6_response_exact=true"
  echo "contract_checkpoints=pre-warmup,post-warmup,run-N-before,run-N-after"
  echo "warmup_rps=1000"
  echo "warmup_seconds=60"
  echo "measured_runs_per_endpoint=5"
  echo "measured_rps=1000"
  echo "measured_seconds=60"
  echo "threshold_p99_ms_lt=50"
  echo "threshold_error_rate_lt=0.001"
  echo "threshold_checks_rate_gt=0.999"
  echo "threshold_dropped_iterations_eq=0"
  echo "observation_rps_per_endpoint=1000"
  echo "observation_seconds=60"
  echo "phase_threshold_semantics=measure-only"
  echo "observation_contract_checkpoints=pre-observation,post-observation"
  echo "load_otel_sampling_probability=${LOAD_OTEL_SAMPLING_PROBABILITY}"
  echo "jfr_output_dir=${JFR_OUTPUT_DIR:-disabled}"
  echo "jar_sha256=${INITIAL_JAR_SHA}"
  echo "runner_sha256=${INITIAL_RUNNER_SHA}"
  echo "scenario_sha256=${INITIAL_SCENARIO_SHA}"
  echo "compose_sha256=${INITIAL_COMPOSE_SHA}"
} > "${RESULT_ROOT}/manifest.txt"

start_service() {
  local output_dir=$1
  local phase=$2
  local tick_interval_ms=$3
  local java_options=()

  if nc -z localhost "${ODDS_PORT}"; then
    echo "Service port ${ODDS_PORT} is occupied before ${phase} start" >&2
    return 1
  fi

  if [[ -n "${JFR_OUTPUT_DIR:-}" ]]; then
    if [[ "${JFR_OUTPUT_DIR}" != /* || ! -d "${JFR_OUTPUT_DIR}" \
      || ! -w "${JFR_OUTPUT_DIR}" ]]; then
      echo "JFR_OUTPUT_DIR must be an existing writable absolute directory" >&2
      return 1
    fi
    java_options+=(
      "-XX:StartFlightRecording=settings=profile,disk=true,dumponexit=true,maxsize=256m,maxage=20m,filename=${JFR_OUTPUT_DIR}/$(basename "${output_dir}")-${phase}.jfr"
    )
  fi

  SERVER_PORT="${ODDS_PORT}" \
    REDIS_HOST="${REDIS_HOST}" \
    REDIS_PORT="${REDIS_PORT}" \
    KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}" \
    SPRING_PROFILES_ACTIVE=mock \
    ODDSFEED_MOCK_MINUTES_PER_SECOND="${FROZEN_MINUTES_PER_SECOND}" \
    ODDSFEED_MOCK_SCENARIOS_AUTO_ROTATE=false \
    ODDSFEED_MOCK_RANDOM_SEED="${FROZEN_RANDOM_SEED}" \
    ODDSFEED_MOCK_TICK_INTERVAL_MS="${tick_interval_ms}" \
    OTEL_SAMPLING_PROBABILITY="${LOAD_OTEL_SAMPLING_PROBABILITY}" \
    java "${java_options[@]}" -jar "${JAR_PATH}" \
      > "${output_dir}/service-${phase}.log" 2>&1 &
  SERVICE_PID=$!

  local readiness_deadline=$((SECONDS + 180))
  until curl -fsS "${BASE_URL}/actuator/health/readiness" \
    > "${output_dir}/readiness-${phase}.json" 2> /dev/null; do
    if ! kill -0 "${SERVICE_PID}" > /dev/null 2>&1; then
      echo "odds-feed-service exited before ${phase} readiness" >&2
      return 1
    fi
    if (( SECONDS >= readiness_deadline )); then
      echo "odds-feed-service did not become ${phase}-ready within 180 seconds" >&2
      return 1
    fi
    sleep 1
  done

  if ! jq -e '.status == "UP"' "${output_dir}/readiness-${phase}.json" > /dev/null; then
    echo "${phase} readiness endpoint did not report UP" >&2
    return 1
  fi
}

wait_for_event() {
  local output_dir=$1
  local phase=$2
  local event_id=''
  local deadline=$((SECONDS + 60))

  while (( SECONDS < deadline )); do
    if curl -fsS "${BASE_URL}/api/v1/events?size=20" \
      > "${output_dir}/events-${phase}.json" \
      && event_id=$(jq -er '.items[0].eventId' "${output_dir}/events-${phase}.json"); then
      printf '%s\n' "${event_id}"
      return 0
    fi
    sleep 1
  done
  echo "No event became available during ${phase} within 60 seconds" >&2
  return 1
}

wait_for_open_odds() {
  local deadline=$((SECONDS + 60))

  while (( SECONDS < deadline )); do
    while IFS= read -r odds_key; do
      local key_body=${odds_key#odds:}
      local candidate_event candidate_market candidate_selection extra
      local market_status candidate_odds
      IFS=: read -r candidate_event candidate_market candidate_selection extra <<< "${key_body}"
      [[ -z "${extra}" ]] || continue
      market_status=$(redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
        GET "market:${candidate_event}:${candidate_market}")
      if [[ "${market_status}" != "OPEN" ]]; then
        continue
      fi
      candidate_odds=$(redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" GET "${odds_key}")
      if [[ -n "${candidate_odds}" ]]; then
        printf '%s\t%s\t%s\n' \
          "${candidate_event}" "${candidate_market}" "${candidate_selection}"
        return 0
      fi
    done < <(redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" --scan --pattern 'odds:*')
    sleep 1
  done
  echo "No OPEN odds key became available within 60 seconds" >&2
  return 1
}

capture_events_baseline() {
  local output_dir=$1
  local event_id=$2
  local list_raw="${output_dir}/frozen-events-list-raw.json"
  local list_canonical="${output_dir}/frozen-events-list-canonical.json"
  local event_canonical="${output_dir}/frozen-event-canonical.json"
  local redis_raw="${output_dir}/frozen-event-redis-raw.json"
  local redis_canonical="${output_dir}/frozen-event-redis-canonical.json"

  if ! curl -fsS "${BASE_URL}/api/v1/events?size=20" > "${list_raw}"; then
    echo "Could not capture the frozen events list" >&2
    return 1
  fi
  if ! jq -e --arg event_id "${event_id}" \
    '[.items[] | select(.eventId == $event_id)] | length == 1' \
    "${list_raw}" > /dev/null; then
    echo "Frozen events list did not contain exactly one ${event_id}" >&2
    return 1
  fi
  if ! jq -cS . "${list_raw}" > "${list_canonical}"; then
    echo "Could not canonicalize the frozen events list" >&2
    return 1
  fi
  if ! jq -ecS --arg event_id "${event_id}" \
    '.items[] | select(.eventId == $event_id)' \
    "${list_raw}" > "${event_canonical}"; then
    echo "Could not canonicalize frozen event ${event_id}" >&2
    return 1
  fi
  if ! redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
    GET "event:${event_id}" > "${redis_raw}"; then
    echo "Could not capture Redis event:${event_id}" >&2
    return 1
  fi
  if ! jq -e --arg event_id "${event_id}" '.eventId == $event_id' \
    "${redis_raw}" > /dev/null; then
    echo "Redis event:${event_id} was missing or malformed during baseline capture" >&2
    return 1
  fi
  if ! jq -cS . "${redis_raw}" > "${redis_canonical}"; then
    echo "Could not canonicalize Redis event:${event_id}" >&2
    return 1
  fi
  if ! cmp -s "${event_canonical}" "${redis_canonical}"; then
    echo "Frozen HTTP event ${event_id} diverged from its Redis baseline" >&2
    return 1
  fi
}

capture_open_odds() {
  local event_id=$1
  local market_id=$2
  local selection_id=$3
  local market_status
  local actual_odds

  market_status=$(redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
    GET "market:${event_id}:${market_id}")
  actual_odds=$(redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
    GET "odds:${event_id}:${market_id}:${selection_id}")
  if [[ "${market_status}" != "OPEN" || -z "${actual_odds}" ]]; then
    echo "Selected Redis odds were not OPEN after frozen-service readiness" >&2
    return 1
  fi
  printf '%s\n' "${actual_odds}"
}

assert_events_contract() {
  local output_dir=$1
  local label=$2
  local event_id=$3
  local expected_list_file=$4
  local expected_event_file=$5
  local list_file="${output_dir}/${label}-events-http.json"
  local canonical_list_file="${output_dir}/${label}-events-canonical.json"
  local http_event_file="${output_dir}/${label}-event-http.json"
  local redis_event_file="${output_dir}/${label}-event-redis.json"

  if ! curl -fsS "${BASE_URL}/api/v1/events?size=20" > "${list_file}"; then
    echo "Events list request failed at ${label}" >&2
    return 1
  fi
  if ! jq -e --arg event_id "${event_id}" \
    '[.items[] | select(.eventId == $event_id)] | length == 1' \
    "${list_file}" > /dev/null; then
    echo "Events list did not contain exactly one ${event_id} at ${label}" >&2
    return 1
  fi
  if ! jq -cS . "${list_file}" > "${canonical_list_file}"; then
    echo "Could not canonicalize the events list at ${label}" >&2
    return 1
  fi
  if ! cmp -s "${canonical_list_file}" "${expected_list_file}"; then
    echo "Events list changed from the frozen canonical baseline at ${label}" >&2
    return 1
  fi
  if ! jq -ecS --arg event_id "${event_id}" \
    '.items[] | select(.eventId == $event_id)' \
    "${list_file}" > "${http_event_file}"; then
    echo "Could not extract event ${event_id} from the HTTP list at ${label}" >&2
    return 1
  fi
  if ! cmp -s "${http_event_file}" "${expected_event_file}"; then
    echo "Event ${event_id} changed from the frozen canonical baseline at ${label}" >&2
    return 1
  fi
  if ! redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
    GET "event:${event_id}" > "${redis_event_file}"; then
    echo "Redis event:${event_id} read failed at ${label}" >&2
    return 1
  fi
  if ! jq -ecS --arg event_id "${event_id}" \
    'select(.eventId == $event_id)' "${redis_event_file}" \
    > "${redis_event_file}.canonical"; then
    echo "Redis event:${event_id} was missing or malformed at ${label}" >&2
    return 1
  fi
  if ! cmp -s "${redis_event_file}.canonical" "${expected_event_file}"; then
    echo "Redis event:${event_id} changed from the frozen canonical baseline at ${label}" >&2
    return 1
  fi
}

assert_odds_contract() {
  local output_dir=$1
  local label=$2
  local event_id=$3
  local market_id=$4
  local selection_id=$5
  local expected_odds=$6
  local status_file="${output_dir}/${label}-market-redis.txt"
  local odds_file="${output_dir}/${label}-odds-redis.txt"
  local body_file="${output_dir}/${label}-odds-http.json"

  redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
    GET "market:${event_id}:${market_id}" > "${status_file}"
  redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
    GET "odds:${event_id}:${market_id}:${selection_id}" > "${odds_file}"
  if [[ "$(< "${status_file}")" != "OPEN" || "$(< "${odds_file}")" != "${expected_odds}" ]]; then
    echo "Frozen OPEN odds key changed at ${label}" >&2
    return 1
  fi

  curl -fsS "${BASE_URL}/api/v1/odds/${event_id}/${market_id}/${selection_id}" \
    > "${body_file}"
  jq -e \
    --arg event_id "${event_id}" \
    --arg market_id "${market_id}" \
    --arg selection_id "${selection_id}" \
    --arg odds "${expected_odds}" '
      keys == ["eventId", "marketId", "odds", "selectionId"] and
      .eventId == $event_id and
      .marketId == $market_id and
      .selectionId == $selection_id and
      (.odds | type) == "number" and
      .odds == ($odds | tonumber)
    ' "${body_file}" > /dev/null
}

assert_endpoint_contract() {
  local endpoint=$1
  local output_dir=$2
  local label=$3
  local event_id=$4
  local market_id=$5
  local selection_id=$6
  local expected_odds=$7
  local expected_events_list_file=$8
  local expected_event_file=$9

  if [[ "${endpoint}" == "events" ]]; then
    assert_events_contract \
      "${output_dir}" "${label}" "${event_id}" \
      "${expected_events_list_file}" "${expected_event_file}"
  else
    assert_odds_contract \
      "${output_dir}" "${label}" "${event_id}" "${market_id}" "${selection_id}" "${expected_odds}"
  fi
}

run_endpoint_group() {
  local endpoint=$1
  local output_dir="${RESULT_ROOT}/${endpoint}"
  local event_id=''
  local market_id=''
  local selection_id=''
  local expected_odds=''
  local frozen_baseline_odds=''
  local post_restart_odds=''
  local expected_events_json=''
  local expected_events_list_file="${output_dir}/frozen-events-list-canonical.json"
  local expected_event_file="${output_dir}/frozen-event-canonical.json"
  local odds_tuple=''
  mkdir -p "${output_dir}"
  CURRENT_OUTPUT_DIR=${output_dir}

  docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans \
    > "${output_dir}/compose-cold-down.log" 2>&1
  prove_endpoint_cleanup "${output_dir}" "cold"

  docker compose -f "${COMPOSE_FILE}" up -d --wait --wait-timeout 180 \
    > "${output_dir}/compose-up.log" 2>&1

  if [[ "${endpoint}" == "events" ]]; then
    start_service "${output_dir}" "frozen" "${FROZEN_TICK_INTERVAL_MS}"
    event_id=$(wait_for_event "${output_dir}" "frozen")
    capture_events_baseline "${output_dir}" "${event_id}"
    expected_events_json=$(< "${expected_events_list_file}")
  else
    start_service "${output_dir}" "bootstrap" "${BOOTSTRAP_TICK_INTERVAL_MS}"
    odds_tuple=$(wait_for_open_odds)
    IFS=$'\t' read -r event_id market_id selection_id <<< "${odds_tuple}"
    if [[ -z "${event_id}" || -z "${market_id}" \
      || -z "${selection_id}" ]]; then
      echo "Bootstrap did not return a complete OPEN odds tuple" >&2
      return 1
    fi
    stop_service
    if nc -z localhost "${ODDS_PORT}"; then
      echo "Bootstrap service did not release port ${ODDS_PORT}" >&2
      return 1
    fi
    if [[ "$(redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
      GET "market:${event_id}:${market_id}")" != "OPEN" ]]; then
      echo "Selected market was no longer OPEN after bootstrap stopped" >&2
      return 1
    fi
    frozen_baseline_odds=$(redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
      GET "odds:${event_id}:${market_id}:${selection_id}")
    if [[ -z "${frozen_baseline_odds}" ]]; then
      echo "Selected odds disappeared after bootstrap stopped" >&2
      return 1
    fi
    expected_odds=${frozen_baseline_odds}
    start_service "${output_dir}" "frozen" "${FROZEN_TICK_INTERVAL_MS}"
    post_restart_odds=$(capture_open_odds "${event_id}" "${market_id}" "${selection_id}")
    if [[ "${post_restart_odds}" != "${frozen_baseline_odds}" ]]; then
      echo "Selected odds changed across the frozen service restart" >&2
      return 1
    fi
  fi

  {
    echo "endpoint=${endpoint}"
    echo "event_id=${event_id}"
    if [[ "${endpoint}" == "odds" ]]; then
      echo "market_id=${market_id}"
      echo "selection_id=${selection_id}"
      echo "frozen_baseline_odds=${frozen_baseline_odds}"
      echo "post_restart_odds=${post_restart_odds}"
      echo "expected_odds=${expected_odds}"
    else
      echo "frozen_events_list_file=${expected_events_list_file}"
      echo "frozen_event_file=${expected_event_file}"
    fi
  } > "${output_dir}/data-precondition.txt"

  assert_endpoint_contract \
    "${endpoint}" "${output_dir}" "pre-warmup" \
    "${event_id}" "${market_id}" "${selection_id}" "${expected_odds}" \
    "${expected_events_list_file}" "${expected_event_file}"

  local warmup_summary="${output_dir}/warmup-summary.json"
  if ! k6 run --quiet --summary-mode=full \
    --summary-export "${warmup_summary}" \
    -e "BASE_URL=${BASE_URL}" \
    -e "ENDPOINT=${endpoint}" \
    -e PHASE=warmup \
    -e "EVENT_ID=${event_id}" \
    -e "MARKET_ID=${market_id}" \
    -e "SELECTION_ID=${selection_id}" \
    -e "EXPECTED_ODDS=${expected_odds}" \
    -e "EXPECTED_EVENTS_JSON=${expected_events_json}" \
    "${SCENARIO}" > "${output_dir}/warmup-k6.log" 2>&1; then
    echo "${endpoint} warm-up execution failed" >&2
    return 1
  fi
  if ! jq -e '
    .metrics.http_req_failed.value < 0.001 and
    .metrics.checks.value > 0.999 and
    ((.metrics.dropped_iterations.count // 0) == 0)
  ' "${warmup_summary}" > /dev/null; then
    echo "${endpoint} warm-up violated correctness or delivery invariants" >&2
    return 1
  fi
  assert_endpoint_contract \
    "${endpoint}" "${output_dir}" "post-warmup" \
    "${event_id}" "${market_id}" "${selection_id}" "${expected_odds}" \
    "${expected_events_list_file}" "${expected_event_file}"

  printf 'run\tstatus\tp99_ms\terror_rate\tchecks_rate\tdropped_iterations\n' \
    > "${output_dir}/gate.tsv"
  local gate_failed=0
  local run
  for run in 1 2 3 4 5; do
    local summary="${output_dir}/run-${run}-summary.json"
    local status=FAIL
    assert_endpoint_contract \
      "${endpoint}" "${output_dir}" "run-${run}-before" \
      "${event_id}" "${market_id}" "${selection_id}" "${expected_odds}" \
      "${expected_events_list_file}" "${expected_event_file}"
    if k6 run \
      --summary-export "${summary}" \
      -e "BASE_URL=${BASE_URL}" \
      -e "ENDPOINT=${endpoint}" \
      -e PHASE=measure \
      -e "EVENT_ID=${event_id}" \
      -e "MARKET_ID=${market_id}" \
      -e "SELECTION_ID=${selection_id}" \
      -e "EXPECTED_ODDS=${expected_odds}" \
      -e "EXPECTED_EVENTS_JSON=${expected_events_json}" \
      "${SCENARIO}" > "${output_dir}/run-${run}-k6.log" 2>&1 \
      && jq -e '
        .metrics.http_req_duration["p(99)"] < 50 and
        .metrics.http_req_failed.value < 0.001 and
        .metrics.checks.value > 0.999 and
        ((.metrics.dropped_iterations.count // 0) == 0)
      ' "${summary}" > /dev/null; then
      status=PASS
    else
      gate_failed=1
    fi
    assert_endpoint_contract \
      "${endpoint}" "${output_dir}" "run-${run}-after" \
      "${event_id}" "${market_id}" "${selection_id}" "${expected_odds}" \
      "${expected_events_list_file}" "${expected_event_file}"
    if [[ -f "${summary}" ]]; then
      jq -r --arg run "${run}" --arg status "${status}" '
        [
          $run,
          $status,
          .metrics.http_req_duration["p(99)"],
          .metrics.http_req_failed.value,
          .metrics.checks.value,
          (.metrics.dropped_iterations.count // 0)
        ] | @tsv
      ' "${summary}" >> "${output_dir}/gate.tsv"
    else
      printf '%s\t%s\tNA\tNA\tNA\tNA\n' "${run}" "${status}" \
        >> "${output_dir}/gate.tsv"
    fi
  done

  stop_endpoint_infrastructure "${output_dir}"
  CURRENT_OUTPUT_DIR=''
  if [[ "${gate_failed}" != "0" ]]; then
    echo "${endpoint} endpoint gate failed; evidence: ${output_dir}" >&2
    return 1
  fi
  echo "${endpoint} endpoint gate passed; evidence: ${output_dir}"
}

append_observation_row() {
  local endpoint=$1
  local summary=$2
  local process_exit=$3
  local pre_contract=$4
  local post_contract=$5
  local combined_file=$6
  local observation_status=INCOMPLETE

  if [[ "${process_exit}" == "0" \
    && "${pre_contract}" == "PASS" \
    && "${post_contract}" == "PASS" \
    && -f "${summary}" ]] \
    && jq -e '
      .metrics.http_req_duration["p(99)"] != null and
      .metrics.http_req_failed.value != null and
      .metrics.checks.value != null
    ' "${summary}" > /dev/null; then
    observation_status=COMPLETE
    jq -r \
      --arg endpoint "${endpoint}" \
      --arg observation_status "${observation_status}" \
      --arg process_exit "${process_exit}" \
      --arg pre_contract "${pre_contract}" \
      --arg post_contract "${post_contract}" '
        [
          $endpoint,
          $observation_status,
          $process_exit,
          $pre_contract,
          $post_contract,
          .metrics.http_req_duration["p(99)"],
          .metrics.http_req_failed.value,
          .metrics.checks.value,
          (.metrics.dropped_iterations.count // 0)
        ] | @tsv
      ' "${summary}" >> "${combined_file}"
  else
    printf '%s\t%s\t%s\t%s\t%s\tNA\tNA\tNA\tNA\n' \
      "${endpoint}" "${observation_status}" "${process_exit}" \
      "${pre_contract}" "${post_contract}" >> "${combined_file}"
    return 1
  fi
}

run_simultaneous_observation() {
  local output_dir="${RESULT_ROOT}/simultaneous-observation"
  local failure_file="${output_dir}/observation-failures.txt"
  local odds_tuple=''
  local events_event_id=''
  local odds_event_id=''
  local market_id=''
  local selection_id=''
  local expected_odds=''
  local frozen_baseline_odds=''
  local post_restart_odds=''
  local expected_events_json=''
  local expected_events_list_file="${output_dir}/frozen-events-list-canonical.json"
  local expected_event_file="${output_dir}/frozen-event-canonical.json"
  local events_pre=FAIL
  local odds_pre=FAIL
  local events_post=FAIL
  local odds_post=FAIL
  local events_exit=125
  local odds_exit=125
  local events_pid=''
  local odds_pid=''
  local stopped_market=''
  local stopped_odds=''
  local setup_ready=true
  local observation_failed=0
  local cleanup_failed=0
  local combined_file="${output_dir}/combined.tsv"
  mkdir -p "${output_dir}"
  : > "${failure_file}"
  CURRENT_OUTPUT_DIR=${output_dir}

  if ! docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans \
    > "${output_dir}/compose-cold-down.log" 2>&1; then
    echo "Cold-down command failed" >> "${failure_file}"
    observation_failed=1
    cleanup_failed=1
    setup_ready=false
  fi
  if ! prove_endpoint_cleanup "${output_dir}" "cold"; then
    echo "Cold cleanup proof failed" >> "${failure_file}"
    observation_failed=1
    cleanup_failed=1
    setup_ready=false
  fi
  if [[ "${setup_ready}" == "true" ]] \
    && ! docker compose -f "${COMPOSE_FILE}" up -d --wait --wait-timeout 180 \
      > "${output_dir}/compose-up.log" 2>&1; then
    echo "Infrastructure startup failed" >> "${failure_file}"
    observation_failed=1
    setup_ready=false
  fi

  if [[ "${setup_ready}" == "true" ]] \
    && ! start_service "${output_dir}" "bootstrap" "${BOOTSTRAP_TICK_INTERVAL_MS}"; then
    echo "Bootstrap service startup failed" >> "${failure_file}"
    observation_failed=1
    setup_ready=false
  fi
  if [[ "${setup_ready}" == "true" ]]; then
    if odds_tuple=$(wait_for_open_odds); then
      IFS=$'\t' read -r odds_event_id market_id selection_id \
        <<< "${odds_tuple}"
      if [[ -z "${odds_event_id}" || -z "${market_id}" \
        || -z "${selection_id}" ]]; then
        echo "Bootstrap returned an incomplete OPEN odds tuple" >> "${failure_file}"
        observation_failed=1
        setup_ready=false
      fi
    else
      echo "No OPEN odds tuple became available" >> "${failure_file}"
      observation_failed=1
      setup_ready=false
    fi
  fi
  if [[ "${setup_ready}" == "true" ]] && ! stop_service; then
    echo "Bootstrap service did not terminate cleanly" >> "${failure_file}"
    observation_failed=1
    setup_ready=false
  fi
  if [[ "${setup_ready}" == "true" ]] && nc -z localhost "${ODDS_PORT}"; then
    echo "Bootstrap service did not release port ${ODDS_PORT}" >> "${failure_file}"
    observation_failed=1
    setup_ready=false
  fi
  if [[ "${setup_ready}" == "true" ]]; then
    if ! stopped_market=$(redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
      GET "market:${odds_event_id}:${market_id}"); then
      echo "Could not read the selected market after bootstrap" >> "${failure_file}"
      observation_failed=1
      setup_ready=false
    elif [[ "${stopped_market}" != "OPEN" ]]; then
      echo "Selected market was not OPEN after bootstrap" >> "${failure_file}"
      observation_failed=1
      setup_ready=false
    fi
  fi
  if [[ "${setup_ready}" == "true" ]]; then
    if ! stopped_odds=$(redis-cli --raw -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
      GET "odds:${odds_event_id}:${market_id}:${selection_id}"); then
      echo "Could not read the selected odds after bootstrap" >> "${failure_file}"
      observation_failed=1
      setup_ready=false
    elif [[ -z "${stopped_odds}" ]]; then
      echo "Selected odds disappeared after bootstrap" >> "${failure_file}"
      observation_failed=1
      setup_ready=false
    else
      frozen_baseline_odds=${stopped_odds}
      expected_odds=${frozen_baseline_odds}
    fi
  fi
  if [[ "${setup_ready}" == "true" ]] \
    && ! start_service "${output_dir}" "frozen" "${FROZEN_TICK_INTERVAL_MS}"; then
    echo "Frozen service startup failed" >> "${failure_file}"
    observation_failed=1
    setup_ready=false
  fi
  if [[ "${setup_ready}" == "true" ]]; then
    if ! events_event_id=$(wait_for_event "${output_dir}" "observation-frozen"); then
      echo "No event became available after frozen restart" >> "${failure_file}"
      observation_failed=1
      setup_ready=false
    fi
  fi
  if [[ "${setup_ready}" == "true" ]]; then
    if ! capture_events_baseline "${output_dir}" "${events_event_id}"; then
      echo "Could not capture the frozen events baseline" >> "${failure_file}"
      observation_failed=1
      setup_ready=false
    else
      expected_events_json=$(< "${expected_events_list_file}")
    fi
  fi
  if [[ "${setup_ready}" == "true" ]]; then
    if ! post_restart_odds=$(capture_open_odds \
      "${odds_event_id}" "${market_id}" "${selection_id}"); then
      echo "Could not capture OPEN odds after frozen restart" >> "${failure_file}"
      observation_failed=1
      setup_ready=false
    elif [[ "${post_restart_odds}" != "${frozen_baseline_odds}" ]]; then
      echo "Selected odds changed across the frozen restart" >> "${failure_file}"
      observation_failed=1
      setup_ready=false
    else
      expected_odds=${frozen_baseline_odds}
    fi
  fi

  {
    echo "release_gate_impact=none"
    echo "events_rps=1000"
    echo "odds_rps=1000"
    echo "duration_seconds=60"
    echo "thresholds_applied=false"
    echo "concurrent=true"
    echo "events_event_id=${events_event_id}"
    echo "odds_event_id=${odds_event_id}"
    echo "market_id=${market_id}"
    echo "selection_id=${selection_id}"
    echo "frozen_baseline_odds=${frozen_baseline_odds}"
    echo "post_restart_odds=${post_restart_odds}"
    echo "expected_odds=${expected_odds}"
    echo "frozen_events_list_file=${expected_events_list_file}"
    echo "frozen_event_file=${expected_event_file}"
  } > "${output_dir}/data-precondition.txt"

  if [[ "${setup_ready}" == "true" ]] \
    && assert_events_contract \
      "${output_dir}" "pre-observation" "${events_event_id}" \
      "${expected_events_list_file}" "${expected_event_file}"; then
    events_pre=PASS
  else
    echo "Events pre-observation contract failed or was skipped" >> "${failure_file}"
    observation_failed=1
  fi
  if [[ "${setup_ready}" == "true" ]] && assert_odds_contract \
    "${output_dir}" "pre-observation" \
    "${odds_event_id}" "${market_id}" "${selection_id}" "${expected_odds}"; then
    odds_pre=PASS
  else
    echo "Odds pre-observation contract failed or was skipped" >> "${failure_file}"
    observation_failed=1
  fi

  if [[ "${events_pre}" == "PASS" && "${odds_pre}" == "PASS" ]]; then
    k6 run \
      --summary-export "${output_dir}/events-summary.json" \
      -e "BASE_URL=${BASE_URL}" \
      -e ENDPOINT=events \
      -e PHASE=observe \
      -e "EVENT_ID=${events_event_id}" \
      -e "EXPECTED_EVENTS_JSON=${expected_events_json}" \
      "${SCENARIO}" > "${output_dir}/events-k6.log" 2>&1 &
    events_pid=$!
    k6 run \
      --summary-export "${output_dir}/odds-summary.json" \
      -e "BASE_URL=${BASE_URL}" \
      -e ENDPOINT=odds \
      -e PHASE=observe \
      -e "EVENT_ID=${odds_event_id}" \
      -e "MARKET_ID=${market_id}" \
      -e "SELECTION_ID=${selection_id}" \
      -e "EXPECTED_ODDS=${expected_odds}" \
      -e "EXPECTED_EVENTS_JSON=${expected_events_json}" \
      "${SCENARIO}" > "${output_dir}/odds-k6.log" 2>&1 &
    odds_pid=$!
    LOAD_PIDS=("${events_pid}" "${odds_pid}")

    if wait "${events_pid}"; then
      events_exit=0
    else
      events_exit=$?
      echo "Events observation k6 exited ${events_exit}" >> "${failure_file}"
      observation_failed=1
    fi
    if wait "${odds_pid}"; then
      odds_exit=0
    else
      odds_exit=$?
      echo "Odds observation k6 exited ${odds_exit}" >> "${failure_file}"
      observation_failed=1
    fi
    LOAD_PIDS=()
  else
    echo "Observation skipped because a pre-contract check failed" \
      > "${output_dir}/events-k6.log"
    cp "${output_dir}/events-k6.log" "${output_dir}/odds-k6.log"
  fi

  if [[ "${setup_ready}" == "true" ]] \
    && assert_events_contract \
      "${output_dir}" "post-observation" "${events_event_id}" \
      "${expected_events_list_file}" "${expected_event_file}"; then
    events_post=PASS
  else
    echo "Events post-observation contract failed" >> "${failure_file}"
    observation_failed=1
  fi
  if [[ "${setup_ready}" == "true" ]] && assert_odds_contract \
    "${output_dir}" "post-observation" \
    "${odds_event_id}" "${market_id}" "${selection_id}" "${expected_odds}"; then
    odds_post=PASS
  else
    echo "Odds post-observation contract failed" >> "${failure_file}"
    observation_failed=1
  fi

  printf 'endpoint\tobservation_status\tprocess_exit\tpre_contract\tpost_contract\tp99_ms\terror_rate\tchecks_rate\tdropped_iterations\n' \
    > "${combined_file}"
  if ! append_observation_row \
    events "${output_dir}/events-summary.json" "${events_exit}" \
    "${events_pre}" "${events_post}" "${combined_file}"; then
    echo "Events observation row is incomplete" >> "${failure_file}"
    observation_failed=1
  fi
  if ! append_observation_row \
    odds "${output_dir}/odds-summary.json" "${odds_exit}" \
    "${odds_pre}" "${odds_post}" "${combined_file}"; then
    echo "Odds observation row is incomplete" >> "${failure_file}"
    observation_failed=1
  fi

  if ! stop_endpoint_infrastructure "${output_dir}"; then
    echo "Observation cleanup or cleanup inventory failed" >> "${failure_file}"
    cleanup_failed=1
  fi
  CURRENT_OUTPUT_DIR=''

  {
    echo "release_gate_impact=none"
    echo "observation_status=$([[ "${observation_failed}" == "0" ]] && echo COMPLETE || echo FAILED_NONBLOCKING)"
    echo "cleanup_status=$([[ "${cleanup_failed}" == "0" ]] && echo PASS || echo FAIL)"
    echo "events_process_exit=${events_exit}"
    echo "odds_process_exit=${odds_exit}"
    echo "events_pre_contract=${events_pre}"
    echo "events_post_contract=${events_post}"
    echo "odds_pre_contract=${odds_pre}"
    echo "odds_post_contract=${odds_post}"
  } > "${output_dir}/observation-status.txt"

  if [[ "${cleanup_failed}" != "0" ]]; then
    echo "Simultaneous observation cleanup failed; evidence: ${output_dir}" >&2
    return 1
  fi
  if [[ "${observation_failed}" != "0" ]]; then
    echo "Simultaneous observation failed nonblockingly; evidence: ${output_dir}" >&2
  else
    echo "Simultaneous endpoint observation completed without release thresholds: ${output_dir}"
  fi
  return 0
}

run_endpoint_group events
run_endpoint_group odds
run_simultaneous_observation
