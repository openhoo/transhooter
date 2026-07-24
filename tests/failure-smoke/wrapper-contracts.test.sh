#!/bin/sh
set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
ROOT=$PROJECT_ROOT
. "$ROOT/deploy/scripts/compose-lib.sh"

fail() {
  printf 'wrapper contract failure: %s\n' "$1" >&2
  exit 1
}

owned=transhooter-failure-123-456
failure_smoke_project_is_owned "$owned" "$owned" || fail 'generated project was not owned'
if failure_smoke_project_is_owned transhooter "$owned"; then
  fail 'generic project was classified as owned'
fi
if failure_smoke_project_is_owned transhooter-failure-123-456 transhooter-failure-999-456; then
  fail 'another harness project was classified as owned'
fi
if failure_smoke_project_is_owned transhooter-failure-pid-456 transhooter-failure-pid-456; then
  fail 'malformed harness project was classified as owned'
fi
failure_smoke_resource_is_owned "$owned" "$owned" "$owned" || fail 'matching resource label was not owned'
if failure_smoke_resource_is_owned "$owned" "$owned" external-project; then
  fail 'external resource label was classified as owned'
fi
current_pid_project="transhooter-failure-$$-456"
if failure_smoke_project_is_stale "$current_pid_project" transhooter-failure-999-456; then
  fail 'live harness owner was classified as stale'
fi
stale_pid=99999999
while kill -0 "$stale_pid" 2>/dev/null; do
  stale_pid=$((stale_pid + 1))
done
stale_project="transhooter-failure-$stale_pid-456"
failure_smoke_project_is_stale "$stale_project" transhooter-failure-999-456 ||
  fail 'dead harness owner was not classified as stale'
if failure_smoke_project_is_stale transhooter transhooter-failure-999-456; then
  fail 'generic project was classified as stale failure-smoke state'
fi
if failure_smoke_project_is_stale "$stale_project" "$stale_project"; then
  fail 'current harness project was classified as stale'
fi


TEST_DIRECTORY=$(mktemp -d "${TMPDIR:-/tmp}/failure-wrapper-contract.XXXXXXXX")
trap 'rm -rf -- "$TEST_DIRECTORY"' 0 HUP INT TERM
mkdir "$TEST_DIRECTORY/bin"
CONTRACT_SENTINEL_DIRECTORY="$TEST_DIRECTORY/contract-sentinel"
mkdir "$CONTRACT_SENTINEL_DIRECTORY"
printf '#!/bin/sh\nprintf "lightweight\\n" >> "$CONTRACT_CALLS"\n' > \
  "$CONTRACT_SENTINEL_DIRECTORY/lightweight-contracts.test.sh"
printf '#!/bin/sh\nprintf "integration\\n" >> "$CONTRACT_CALLS"\n' > \
  "$CONTRACT_SENTINEL_DIRECTORY/postgres-integration.test.sh"
chmod +x "$CONTRACT_SENTINEL_DIRECTORY/lightweight-contracts.test.sh" \
  "$CONTRACT_SENTINEL_DIRECTORY/postgres-integration.test.sh"
CONTRACT_SCRIPT_DIRECTORY="$CONTRACT_SENTINEL_DIRECTORY" \
  "$ROOT/scripts/run-tests" --contract-discovery-only ||
  fail 'registered lightweight and infrastructure-only contracts were not classified'
CONTRACT_CALLS="$TEST_DIRECTORY/contract-calls"
export CONTRACT_CALLS
: > "$CONTRACT_CALLS"
CONTRACT_SCRIPT_DIRECTORY="$CONTRACT_SENTINEL_DIRECTORY" \
  "$ROOT/scripts/run-tests" --contract-scripts-only ||
  fail 'registered lightweight contract execution failed'
[ "$(cat "$CONTRACT_CALLS")" = lightweight ] ||
  fail 'lightweight contracts did not execute exactly once or an integration contract ran'

printf '#!/bin/sh\nexit 0\n' > "$CONTRACT_SENTINEL_DIRECTORY/forgotten-contract.test.sh"
chmod +x "$CONTRACT_SENTINEL_DIRECTORY/forgotten-contract.test.sh"
if CONTRACT_SCRIPT_DIRECTORY="$CONTRACT_SENTINEL_DIRECTORY" \
  "$ROOT/scripts/run-tests" --contract-discovery-only \
  > "$TEST_DIRECTORY/contract-discovery-output" 2>&1; then
  fail 'an omitted contract script passed discovery'
fi
grep -F 'Contract script exists but is omitted from lightweight discovery' \
  "$TEST_DIRECTORY/contract-discovery-output" >/dev/null ||
  fail 'omitted contract script did not emit the discovery sentinel diagnostic'
rm "$CONTRACT_SENTINEL_DIRECTORY/forgotten-contract.test.sh"

printf '#!/bin/sh\nexit 0\n' > "$CONTRACT_SENTINEL_DIRECTORY/not-executable-contracts.test.sh"
if CONTRACT_SCRIPT_DIRECTORY="$CONTRACT_SENTINEL_DIRECTORY" \
  "$ROOT/scripts/run-tests" --contract-discovery-only \
  > "$TEST_DIRECTORY/contract-executable-output" 2>&1; then
  fail 'a non-executable lightweight contract passed discovery'
fi
grep -F 'Lightweight contract script is not executable' \
  "$TEST_DIRECTORY/contract-executable-output" >/dev/null ||
  fail 'non-executable contract did not emit the discovery sentinel diagnostic'
rm "$CONTRACT_SENTINEL_DIRECTORY/not-executable-contracts.test.sh"

CALLS="$TEST_DIRECTORY/calls"
export CALLS
cat > "$TEST_DIRECTORY/bin/docker" <<'MOCK'
#!/bin/sh
printf 'APP_ENV=%s SMTP_URL=%s RTC_ADVERTISED_IP=%s S3_PUBLIC_ENDPOINT=%s PUBLIC_BASE_URL=%s PUBLIC_LIVEKIT_URL=%s S3_KMS_KEY_ID=%s %s RTC_SUBNET=%s\n' \
  "${APP_ENV:-}" "${SMTP_URL:-}" "${RTC_ADVERTISED_IP:-}" "${S3_PUBLIC_ENDPOINT:-}" "${PUBLIC_BASE_URL:-}" "${PUBLIC_LIVEKIT_URL:-}" "${S3_KMS_KEY_ID:-}" "$*" "${RTC_SUBNET:-}" >> "$CALLS"
case " $* " in
  *" compose version "*) exit 0 ;;
esac
case " $* " in
  *" config --environment "*)
    previous=
    for argument do
      if [ "$previous" = --env-file ]; then
        cat "$argument"
      fi
      previous=$argument
    done
    [ -z "${PUBLIC_BASE_URL:-}" ] || printf 'PUBLIC_BASE_URL=%s\n' "$PUBLIC_BASE_URL"
    [ -z "${PUBLIC_LIVEKIT_URL:-}" ] || printf 'PUBLIC_LIVEKIT_URL=%s\n' "$PUBLIC_LIVEKIT_URL"
    [ -z "${S3_PUBLIC_ENDPOINT:-}" ] || printf 'S3_PUBLIC_ENDPOINT=%s\n' "$S3_PUBLIC_ENDPOINT"
    exit 0
    ;;
esac
case " $* " in
  *" ps --all --filter label=com.docker.compose.project --format "* | \
    *" network ls --filter label=com.docker.compose.project --format "* | \
    *" volume ls --filter label=com.docker.compose.project --format "*)
    [ "${MOCK_MODE:-}" = stale ] && printf '%s\n' "$STALE_PROJECT"
    exit 0
    ;;
esac
case "${1:-}:${2:-}" in
  ps:*)
    [ "${MOCK_MODE:-}" = ambiguous ] && printf 'container|foreign-container|external-project\n'
    exit 0
    ;;
  network:ls | volume:ls) exit 0 ;;
esac
case " $* " in
  *" run --rm --no-deps secrets-init "*)
    case "${MOCK_MODE:-}" in
      timeout) exec sleep 5 ;;
      signal)
        printf 'ready\n' > "$READY_FIFO"
        exec sleep 30
        ;;
      ambiguous) exit 7 ;;
    esac
    ;;
  *" build minio egress-ready translation-worker spool-drainer web failure-smoke "*)
    if [ "${MOCK_MODE:-}" = deadline ]; then
      sleep 2
    fi
    ;;
  *" build minio egress-ready translation-worker spool-drainer web e2e "*)
    if [ "${MOCK_MODE:-}" = consultation-deadline ]; then
      sleep 2
    fi
    ;;
  *" down --volumes --remove-orphans "*) exit 0 ;;
esac
case " $* " in
  *" failure-smoke bun ../failure-smoke/failure-smoke.mjs "*)
    proof_directory=
    previous=
    for argument do
      if [ "$previous" = -v ]; then
        case "$argument" in
          *:/proof) proof_directory=${argument%:/proof} ;;
        esac
      fi
      previous=$argument
    done
    if [ -n "$proof_directory" ]; then
      mkdir -p "$proof_directory"
      printf '{"scenarios":[],"shard":"mock","totalDurationMs":1,"scenarioDurationsMs":{}}\n' > "$proof_directory/proof.json"
    fi
    if [ "${MOCK_MODE:-}" = scenario-failure ]; then
      printf 'full-log-first-line\n'
      line_number=1
      while [ "$line_number" -le 205 ]; do
        printf 'full-log-filler-%s\n' "$line_number"
        line_number=$((line_number + 1))
      done
      printf 'full-log-last-line\n'
      exit 42
    fi
    if [ "${MOCK_MODE:-}" = scenario-signal ]; then
      printf 'signal-log-first-line\n'
      printf 'signal-log-last-line\n'
      printf 'ready\n' > "$READY_FIFO"
      exec sleep 30
    fi
    ;;
esac
exit 0
MOCK
chmod +x "$TEST_DIRECTORY/bin/docker"

COMPOSE_LIBRARY_ROOT="$TEST_DIRECTORY/compose-library-root"
mkdir -p "$COMPOSE_LIBRARY_ROOT/deploy/scripts"
cp "$ROOT/deploy/scripts/compose-lib.sh" "$COMPOSE_LIBRARY_ROOT/deploy/scripts/compose-lib.sh"
resolved_root=$(
  ROOT="$COMPOSE_LIBRARY_ROOT"
  TRANSHOOTER_ROOT=/not/the/selected/root
  . "$PROJECT_ROOT/deploy/scripts/compose-lib.sh"
  printf '%s\n' "$ROOT"
)
[ "$resolved_root" = "$COMPOSE_LIBRARY_ROOT" ] ||
  fail 'compose library replaced an explicit valid caller root'
resolved_root=$(
  ROOT=/not/a/repository
  TRANSHOOTER_ROOT="$COMPOSE_LIBRARY_ROOT"
  . "$PROJECT_ROOT/deploy/scripts/compose-lib.sh"
  printf '%s\n' "$ROOT"
)
[ "$resolved_root" = "$COMPOSE_LIBRARY_ROOT" ] ||
  fail 'compose library did not retain the TRANSHOOTER_ROOT direct-invocation fallback'

: > "$CALLS"
(
  PATH="$TEST_DIRECTORY/bin:$PATH"
  ROOT="$PROJECT_ROOT"
  unset COMPOSE_COMMAND_AVAILABLE
  . "$PROJECT_ROOT/deploy/scripts/compose-lib.sh"
  select_compose_command
  select_compose_command
)
[ "$(grep -c ' compose version ' "$CALLS")" -eq 1 ] ||
  fail 'successful Docker Compose detection was not cached in the wrapper process'
if [ "${WRAPPER_CONTRACT_SCOPE:-}" = compose-lib ]; then
  printf 'compose library wrapper contracts passed\n'
  exit 0
fi
cat > "$TEST_DIRECTORY/bin/mv" <<'MOCK'
#!/bin/sh
if [ "${MOCK_ARTIFACT_RENAME_FAILURE:-}" = true ]; then
  exit 73
fi
exec /usr/bin/mv "$@"
MOCK
chmod +x "$TEST_DIRECTORY/bin/mv"

run_wrapper() {
  mode=$1
  output=$2
  shift 2
  PATH="$TEST_DIRECTORY/bin:$PATH" \
    MOCK_MODE="$mode" \
    TMPDIR="$TEST_DIRECTORY" \
    COMPOSE_DETECTION_TIMEOUT_SECONDS=1 \
    COMPOSE_SETUP_TIMEOUT_SECONDS=1 \
    COMPOSE_BUILD_TIMEOUT_SECONDS=1 \
    COMPOSE_SCENARIO_TIMEOUT_SECONDS=1 \
    COMPOSE_DIAGNOSTIC_TIMEOUT_SECONDS=1 \
    COMPOSE_OWNERSHIP_TIMEOUT_SECONDS=1 \
    COMPOSE_CLEANUP_TIMEOUT_SECONDS=1 \
    timeout 15 "$ROOT/scripts/failure-smoke" "$@" > "$output" 2>&1
}

STALE_PROJECT="transhooter-failure-$stale_pid-456"
export STALE_PROJECT

: > "$CALLS"
if run_wrapper timeout "$TEST_DIRECTORY/timeout-output"; then
  fail 'timed-out setup phase succeeded'
else
  timeout_status=$?
fi
[ "$timeout_status" -eq 124 ] || fail "timed-out setup returned $timeout_status instead of 124"
grep -F 'Phase "Initializing failure-smoke runtime secrets" exceeded its absolute 1s deadline' \
  "$TEST_DIRECTORY/timeout-output" >/dev/null || fail 'timeout diagnostic was not emitted'
grep -F ' down --volumes --remove-orphans' "$CALLS" >/dev/null ||
  fail 'bounded cleanup did not run after timeout'

: > "$CALLS"
if ! run_wrapper stale "$TEST_DIRECTORY/stale-output" --no-build; then
  fail 'stale-stack cleanup wrapper run failed'
fi
grep -F " -p $STALE_PROJECT " "$CALLS" >/dev/null ||
  fail 'stale failure-smoke project was not targeted'
grep -F ' down --volumes --remove-orphans' "$CALLS" >/dev/null ||
  fail 'stale failure-smoke project was not removed before the new run'

: > "$CALLS"
if run_wrapper ambiguous "$TEST_DIRECTORY/ambiguous-output"; then
  fail 'ambiguous stack run succeeded'
else
  ambiguous_status=$?
fi
[ "$ambiguous_status" -eq 7 ] || fail "ambiguous stack preserved wrong status $ambiguous_status"
grep -F 'external or ambiguous project label external-project' \
  "$TEST_DIRECTORY/ambiguous-output" >/dev/null || fail 'ambiguous label was not rejected'
if grep -F ' down --volumes --remove-orphans' "$CALLS" >/dev/null; then
  fail 'destructive cleanup ran for an ambiguous stack'
fi

: > "$CALLS"
READY_FIFO="$TEST_DIRECTORY/ready"
export READY_FIFO
mkfifo "$READY_FIFO"
PATH="$TEST_DIRECTORY/bin:$PATH" \
  MOCK_MODE=signal \
  TMPDIR="$TEST_DIRECTORY" \
  COMPOSE_DETECTION_TIMEOUT_SECONDS=1 \
  COMPOSE_SETUP_TIMEOUT_SECONDS=10 \
  COMPOSE_BUILD_TIMEOUT_SECONDS=1 \
  COMPOSE_SCENARIO_TIMEOUT_SECONDS=1 \
  COMPOSE_DIAGNOSTIC_TIMEOUT_SECONDS=1 \
  COMPOSE_OWNERSHIP_TIMEOUT_SECONDS=1 \
  COMPOSE_CLEANUP_TIMEOUT_SECONDS=1 \
  timeout 15 "$ROOT/scripts/failure-smoke" > "$TEST_DIRECTORY/signal-output" 2>&1 &
wrapper_pid=$!
IFS= read -r ready < "$READY_FIFO"
[ "$ready" = ready ] || fail 'signal test did not reach active Compose phase'
kill -TERM "$wrapper_pid"
if wait "$wrapper_pid"; then
  fail 'signalled wrapper succeeded'
else
  signal_status=$?
fi
[ "$signal_status" -eq 143 ] || fail "SIGTERM returned $signal_status instead of 143"
grep -F ' down --volumes --remove-orphans' "$CALLS" >/dev/null ||
  fail 'cleanup did not run after SIGTERM'


: > "$CALLS"
deadline_test_started_ms=$(($(date +%s) * 1000))
TEST_BUILD_TIMEOUT_SECONDS=3
TEST_FAILURE_DEADLINE_SECONDS=1
export TEST_BUILD_TIMEOUT_SECONDS TEST_FAILURE_DEADLINE_SECONDS
if ! PATH="$TEST_DIRECTORY/bin:$PATH" \
  MOCK_MODE=deadline \
  TMPDIR="$TEST_DIRECTORY" \
  FAILURE_SMOKE_DEADLINE_SECONDS="$TEST_FAILURE_DEADLINE_SECONDS" \
  COMPOSE_DETECTION_TIMEOUT_SECONDS=1 \
  COMPOSE_SETUP_TIMEOUT_SECONDS=1 \
  COMPOSE_BUILD_TIMEOUT_SECONDS="$TEST_BUILD_TIMEOUT_SECONDS" \
  COMPOSE_SCENARIO_TIMEOUT_SECONDS=1 \
  COMPOSE_DIAGNOSTIC_TIMEOUT_SECONDS=1 \
  COMPOSE_OWNERSHIP_TIMEOUT_SECONDS=1 \
  COMPOSE_CLEANUP_TIMEOUT_SECONDS=1 \
  timeout 15 "$ROOT/scripts/failure-smoke" > "$TEST_DIRECTORY/deadline-output" 2>&1; then
  fail 'deadline timing wrapper run failed'
fi
deadline_test_finished_ms=$(($(date +%s) * 1000))
generated_deadline=$(
  grep -F 'failure-smoke.mjs' "$CALLS" |
    sed -n 's/.*--deadline-epoch-ms \([0-9][0-9]*\).*/\1/p'
)
[ -n "$generated_deadline" ] || fail 'generated scenario deadline was not forwarded'
[ "$generated_deadline" -ge "$((deadline_test_started_ms + 1000))" ] ||
  fail 'generated scenario deadline was shorter than configured'
[ "$generated_deadline" -le "$deadline_test_finished_ms" ] ||
  fail 'generated scenario deadline excluded setup/build elapsed time'

: > "$CALLS"
build_metrics="$TEST_DIRECTORY/build-only.metrics.jsonl"
if ! run_wrapper normal "$TEST_DIRECTORY/build-only-output" \
  --build-only --metrics-file "$build_metrics"; then
  fail 'build-only wrapper run failed'
fi
grep -F ' build minio egress-ready translation-worker spool-drainer web failure-smoke' "$CALLS" >/dev/null ||
  fail 'build-only did not build the two role-specific Python images and shared smoke images'
if grep -F 'failure-smoke.mjs' "$CALLS" >/dev/null; then
  fail 'build-only unexpectedly ran failure scenarios'
fi
grep -F '"name":"Building failure-smoke application and harness images"' "$build_metrics" >/dev/null ||
  fail 'build-only metrics omitted the image build phase'
if [ -e "$TEST_DIRECTORY/build-only.scenario.log" ]; then
  fail 'build-only unexpectedly created a failure scenario log'
fi

: > "$CALLS"
no_build_proof="$TEST_DIRECTORY/no-build.proof.json"
no_build_metrics="$TEST_DIRECTORY/no-build.metrics.jsonl"
if ! run_wrapper normal "$TEST_DIRECTORY/no-build-output" \
  --no-build --proof-file "$no_build_proof" --metrics-file "$no_build_metrics" \
  --shard provider; then
  fail 'no-build shard wrapper run failed'
fi
if grep -F ' build minio egress-ready translation-worker spool-drainer web failure-smoke' "$CALLS" >/dev/null; then
  fail 'no-build unexpectedly rebuilt smoke images'
fi
grep -F -- '--shard provider' "$CALLS" >/dev/null ||
  fail 'no-build did not forward the selected shard'
grep -F ':/proof' "$CALLS" >/dev/null || fail 'no-build did not mount the private proof directory'
[ -s "$no_build_proof" ] || fail 'no-build did not copy the harness proof'
grep -F '"name":"Running failure injection scenarios"' "$no_build_metrics" >/dev/null ||
  fail 'no-build metrics omitted scenario execution'
if [ -e "$TEST_DIRECTORY/no-build.scenario.log" ]; then
  fail 'successful scenario run unexpectedly created a failure scenario log'
fi

: > "$CALLS"
if ! run_wrapper normal "$TEST_DIRECTORY/crash-point-output" \
  --no-build --scenarios spool-drainer-crash-replay --spool-crash-point archive-registration; then
  fail 'spool crash-point wrapper run failed'
fi
grep -F -- '--scenarios spool-drainer-crash-replay --spool-crash-point archive-registration' \
  "$CALLS" >/dev/null || fail 'wrapper did not forward the exact spool crash subcase'

: > "$CALLS"
if ! run_wrapper normal "$TEST_DIRECTORY/seal-point-output" \
  --no-build --scenarios spool-drainer-seal-race --spool-seal-point committed; then
  fail 'spool seal-point wrapper run failed'
fi
grep -F -- '--scenarios spool-drainer-seal-race --spool-seal-point committed' \
  "$CALLS" >/dev/null || fail 'wrapper did not forward the exact spool seal subcase'

: > "$CALLS"
failed_scenario_log="$TEST_DIRECTORY/failed-scenario.log"
if run_wrapper scenario-failure "$TEST_DIRECTORY/scenario-failure-output" \
  --no-build --scenario-log-file "$failed_scenario_log"; then
  fail 'failed scenario wrapper run succeeded'
else
  failed_scenario_status=$?
fi
[ "$failed_scenario_status" -eq 42 ] ||
  fail "failed scenario returned $failed_scenario_status instead of 42"
[ "$(wc -l < "$failed_scenario_log")" -ge 207 ] ||
  fail 'failed scenario artifact did not preserve the complete log'
grep -Fx 'full-log-first-line' "$failed_scenario_log" >/dev/null ||
  fail 'failed scenario artifact omitted the initial log context'
grep -Fx 'full-log-last-line' "$failed_scenario_log" >/dev/null ||
  fail 'failed scenario artifact omitted the terminal log context'
grep -F "Complete failure-smoke scenario log: $failed_scenario_log" \
  "$TEST_DIRECTORY/scenario-failure-output" >/dev/null ||
  fail 'failed scenario did not report the caller-visible artifact'
if grep -F 'full-log-first-line' "$TEST_DIRECTORY/scenario-failure-output" >/dev/null; then
  fail 'failed scenario terminal output was not limited to the concise tail'
fi
grep -F 'full-log-last-line' "$TEST_DIRECTORY/scenario-failure-output" >/dev/null ||
  fail 'failed scenario terminal tail omitted the final log context'
grep -F ' down --volumes --remove-orphans' "$CALLS" >/dev/null ||
  fail 'failed scenario did not clean the owned Compose stack'
for remaining_run_directory in "$TEST_DIRECTORY"/transhooter-failure-smoke.*; do
  [ ! -e "$remaining_run_directory" ] ||
    fail 'failed scenario retained its private run directory after artifact copy'
done

: > "$CALLS"
signal_scenario_log="$TEST_DIRECTORY/signal-scenario.log"
READY_FIFO="$TEST_DIRECTORY/scenario-ready"
export READY_FIFO
mkfifo "$READY_FIFO"
PATH="$TEST_DIRECTORY/bin:$PATH" \
  MOCK_MODE=scenario-signal \
  TMPDIR="$TEST_DIRECTORY" \
  COMPOSE_DETECTION_TIMEOUT_SECONDS=1 \
  COMPOSE_SETUP_TIMEOUT_SECONDS=1 \
  COMPOSE_BUILD_TIMEOUT_SECONDS=1 \
  COMPOSE_SCENARIO_TIMEOUT_SECONDS=30 \
  COMPOSE_DIAGNOSTIC_TIMEOUT_SECONDS=1 \
  COMPOSE_OWNERSHIP_TIMEOUT_SECONDS=1 \
  COMPOSE_CLEANUP_TIMEOUT_SECONDS=1 \
  "$ROOT/scripts/failure-smoke" --no-build \
    --scenario-log-file "$signal_scenario_log" \
    > "$TEST_DIRECTORY/scenario-signal-output" 2>&1 &
wrapper_pid=$!
IFS= read -r ready < "$READY_FIFO"
[ "$ready" = ready ] || fail 'scenario signal test did not produce log output'
kill -TERM "$wrapper_pid"
if wait "$wrapper_pid"; then
  fail 'scenario-signalled wrapper succeeded'
else
  signal_status=$?
fi
[ "$signal_status" -eq 143 ] || fail "scenario SIGTERM returned $signal_status instead of 143"
grep -Fx 'signal-log-first-line' "$signal_scenario_log" >/dev/null ||
  fail 'scenario SIGTERM artifact omitted initial log output'
grep -Fx 'signal-log-last-line' "$signal_scenario_log" >/dev/null ||
  fail 'scenario SIGTERM artifact omitted terminal log output'

: > "$CALLS"
failed_atomic_log="$TEST_DIRECTORY/failed-atomic.log"
printf 'existing-artifact-content\n' > "$failed_atomic_log"
if PATH="$TEST_DIRECTORY/bin:$PATH" \
  MOCK_MODE=scenario-failure \
  MOCK_ARTIFACT_RENAME_FAILURE=true \
  TMPDIR="$TEST_DIRECTORY" \
  COMPOSE_DETECTION_TIMEOUT_SECONDS=1 \
  COMPOSE_SETUP_TIMEOUT_SECONDS=1 \
  COMPOSE_BUILD_TIMEOUT_SECONDS=1 \
  COMPOSE_SCENARIO_TIMEOUT_SECONDS=1 \
  COMPOSE_DIAGNOSTIC_TIMEOUT_SECONDS=1 \
  COMPOSE_OWNERSHIP_TIMEOUT_SECONDS=1 \
  COMPOSE_CLEANUP_TIMEOUT_SECONDS=1 \
  timeout 15 "$ROOT/scripts/failure-smoke" --no-build \
    --scenario-log-file "$failed_atomic_log" \
    > "$TEST_DIRECTORY/atomic-failure-output" 2>&1; then
  fail 'failed destination preservation wrapper run succeeded'
else
  atomic_failure_status=$?
fi
[ "$atomic_failure_status" -eq 42 ] ||
  fail "failed destination preservation returned $atomic_failure_status instead of 42"
[ "$(cat "$failed_atomic_log")" = existing-artifact-content ] ||
  fail 'failed atomic preservation truncated or replaced the existing artifact'
retained_source=$(
  sed -n 's/.*source retained at \([^ ]*\)\.$/\1/p' \
    "$TEST_DIRECTORY/atomic-failure-output"
)
[ -n "$retained_source" ] || fail 'failed preservation did not report its retained source path'
[ -f "$retained_source" ] || fail 'failed preservation deleted its retained source log'
grep -Fx 'full-log-first-line' "$retained_source" >/dev/null ||
  fail 'retained source log omitted the initial context'
grep -Fx 'full-log-last-line' "$retained_source" >/dev/null ||
  fail 'retained source log omitted the terminal context'

parallel_output="$TEST_DIRECTORY/parallel-artifacts"
: > "$CALLS"
if ! PATH="$TEST_DIRECTORY/bin:$PATH" \
  TMPDIR="$TEST_DIRECTORY" \
  FAILURE_SMOKE_OUTPUT_DIR="$parallel_output" \
  COMPOSE_DETECTION_TIMEOUT_SECONDS=1 \
  COMPOSE_SETUP_TIMEOUT_SECONDS=1 \
  COMPOSE_BUILD_TIMEOUT_SECONDS=1 \
  COMPOSE_SCENARIO_TIMEOUT_SECONDS=1 \
  COMPOSE_DIAGNOSTIC_TIMEOUT_SECONDS=1 \
  COMPOSE_OWNERSHIP_TIMEOUT_SECONDS=1 \
  COMPOSE_CLEANUP_TIMEOUT_SECONDS=1 \
  timeout 30 "$ROOT/scripts/failure-smoke-parallel" > "$TEST_DIRECTORY/parallel-output" 2>&1; then
  fail 'parallel shard wrapper run failed'
fi
for shard in control provider spool storage; do
  [ -s "$parallel_output/$shard.proof.json" ] || fail "parallel run omitted $shard proof"
  [ -s "$parallel_output/$shard.metrics.jsonl" ] || fail "parallel run omitted $shard metrics"
  grep -F -- "--shard $shard" "$CALLS" >/dev/null || fail "parallel run omitted $shard shard"
done
for subnet in 10.254.233.0/24 10.254.234.0/24 10.254.235.0/24 10.254.236.0/24; do
  grep -F "RTC_SUBNET=$subnet" "$CALLS" >/dev/null || fail "parallel run omitted RTC subnet $subnet"
done


: > "$CALLS"
consultation_started_ms=$(($(date +%s) * 1000))
if ! PATH="$TEST_DIRECTORY/bin:$PATH" \
  MOCK_MODE=consultation-deadline \
  TMPDIR="$TEST_DIRECTORY" \
  CONSULTATION_SMOKE_DEADLINE_SECONDS=1 \
  COMPOSE_PHASE_TIMEOUT_SECONDS=3 \
  COMPOSE_HARNESS_LOCK_WAIT_SECONDS=1 \
  timeout 15 "$ROOT/scripts/smoke-consultation" --harness-owned \
    > "$TEST_DIRECTORY/consultation-deadline-output" 2>&1; then
  fail 'consultation smoke deadline timing wrapper run failed'
fi
consultation_finished_ms=$(($(date +%s) * 1000))
consultation_deadline=$(
  grep -F 'smoke:consultation' "$CALLS" |
    sed -n 's/.*--deadline-epoch-ms \([0-9][0-9]*\).*/\1/p'
)
[ -n "$consultation_deadline" ] || fail 'consultation smoke generated deadline was not forwarded'
[ "$consultation_deadline" -ge "$((consultation_started_ms + 1000))" ] ||
  fail 'consultation smoke deadline was shorter than configured'
[ "$consultation_deadline" -le "$consultation_finished_ms" ] ||
  fail 'consultation smoke deadline excluded lock/setup/build elapsed time'

: > "$CALLS"
caller_deadline=4102444800000
if ! PATH="$TEST_DIRECTORY/bin:$PATH" \
  TMPDIR="$TEST_DIRECTORY" \
  COMPOSE_PHASE_TIMEOUT_SECONDS=1 \
  COMPOSE_HARNESS_LOCK_WAIT_SECONDS=1 \
  timeout 15 "$ROOT/scripts/smoke-consultation" --harness-owned \
    --deadline-epoch-ms "$caller_deadline" > "$TEST_DIRECTORY/consultation-caller-output" 2>&1; then
  fail 'consultation smoke caller-deadline wrapper run failed'
fi
grep -F -- "--deadline-epoch-ms $caller_deadline" "$CALLS" >/dev/null ||
  fail 'consultation smoke did not preserve caller-supplied deadline'

DEV_ROOT="$TEST_DIRECTORY/dev-project"
mkdir -p "$DEV_ROOT/scripts" "$DEV_ROOT/deploy/scripts" "$DEV_ROOT/deploy/compose" "$DEV_ROOT/.secrets"
cp "$ROOT/scripts/dev-up" "$DEV_ROOT/scripts/dev-up"
cp "$ROOT/deploy/scripts/compose-lib.sh" "$DEV_ROOT/deploy/scripts/compose-lib.sh"
printf '{}\n' > "$DEV_ROOT/.secrets/google-adc.json"
: > "$DEV_ROOT/deploy/compose/compose.yml"
: > "$DEV_ROOT/deploy/compose/compose.google.yml"
: > "$DEV_ROOT/deploy/compose/compose.google-speech.yml"
: > "$DEV_ROOT/deploy/compose/compose.test.yml"
: > "$CALLS"

: > "$CALLS"
if ! APP_ENV=test \
  PROVIDER_PROFILE=fixture \
  TRANSHOOTER_ROOT="$DEV_ROOT" \
  PATH="$TEST_DIRECTORY/bin:$PATH" "$DEV_ROOT/scripts/dev-up" --provider-profile fixture \
  > "$TEST_DIRECTORY/dev-up-fixture-output" 2>&1; then
  fail 'fixture dev-up local-default wrapper run failed'
fi
grep -F 'APP_ENV=test SMTP_URL= RTC_ADVERTISED_IP=10.254.231.10 S3_PUBLIC_ENDPOINT=http://localhost:9000 PUBLIC_BASE_URL=http://app.localhost:3000 PUBLIC_LIVEKIT_URL=ws://rtc.localhost:7880 S3_KMS_KEY_ID=' \
  "$CALLS" >/dev/null || fail 'fixture dev-up did not export explicit local-only endpoint defaults'

: > "$CALLS"
if ! PATH="$TEST_DIRECTORY/bin:$PATH" \
  APP_ENV=development \
  PROVIDER_PROFILE=google-eu \
  TRANSHOOTER_ROOT="$DEV_ROOT" \
  SMTP_URL=smtp://custom-mail:2525 \
  RTC_ADVERTISED_IP=192.0.2.44 \
  S3_PUBLIC_ENDPOINT=https://objects.dev.example \
  PUBLIC_BASE_URL=https://app.dev.example \
  PUBLIC_LIVEKIT_URL=wss://rtc.dev.example \
  "$DEV_ROOT/scripts/dev-up" --provider-profile google-eu > "$TEST_DIRECTORY/dev-up-custom-output" 2>&1; then
  fail 'dev-up configured-endpoint wrapper run failed'
fi
grep -F 'APP_ENV=development SMTP_URL=smtp://custom-mail:2525 RTC_ADVERTISED_IP=192.0.2.44 S3_PUBLIC_ENDPOINT=https://objects.dev.example PUBLIC_BASE_URL=https://app.dev.example PUBLIC_LIVEKIT_URL=wss://rtc.dev.example S3_KMS_KEY_ID=' \
  "$CALLS" >/dev/null || fail 'dev-up overwrote configured endpoints or required a KMS key for bundled MinIO'

: > "$CALLS"
if ! PATH="$TEST_DIRECTORY/bin:$PATH" \
  APP_ENV=development \
  PROVIDER_PROFILE=google-speech-eu \
  TRANSHOOTER_ROOT="$DEV_ROOT" \
  PUBLIC_BASE_URL=https://app.dev.example \
  PUBLIC_LIVEKIT_URL=wss://rtc.dev.example \
  S3_PUBLIC_ENDPOINT=https://objects.dev.example \
  "$DEV_ROOT/scripts/dev-up" --provider-profile google-speech-eu \
  > "$TEST_DIRECTORY/dev-up-google-speech-output" 2>&1; then
  fail 'google-speech-eu dev-up wrapper run failed with ADC present'
fi
grep -F 'compose.google-speech.yml' "$CALLS" >/dev/null ||
  fail 'google-speech-eu dev-up did not select its Compose overlay'

mv "$DEV_ROOT/.secrets/google-adc.json" "$DEV_ROOT/.secrets/google-adc.missing"
: > "$CALLS"
if PATH="$TEST_DIRECTORY/bin:$PATH" \
  APP_ENV=development \
  PROVIDER_PROFILE=google-speech-eu \
  TRANSHOOTER_ROOT="$DEV_ROOT" \
  PUBLIC_BASE_URL=https://app.dev.example \
  PUBLIC_LIVEKIT_URL=wss://rtc.dev.example \
  S3_PUBLIC_ENDPOINT=https://objects.dev.example \
  "$DEV_ROOT/scripts/dev-up" --provider-profile google-speech-eu \
  > "$TEST_DIRECTORY/dev-up-google-speech-missing-adc-output" 2>&1; then
  fail 'google-speech-eu dev-up accepted a missing ADC file'
fi
grep -F 'Missing .secrets/google-adc.json' \
  "$TEST_DIRECTORY/dev-up-google-speech-missing-adc-output" >/dev/null ||
  fail 'google-speech-eu missing ADC failure lacked the expected diagnostic'
[ ! -s "$CALLS" ] || fail 'google-speech-eu missing ADC failure invoked Compose'
mv "$DEV_ROOT/.secrets/google-adc.missing" "$DEV_ROOT/.secrets/google-adc.json"
cat > "$DEV_ROOT/.env" <<'ENV'
PUBLIC_BASE_URL=https://app.env.example
PUBLIC_LIVEKIT_URL=wss://rtc.env.example
S3_PUBLIC_ENDPOINT=https://objects.env.example
ENV
if ! env -u PUBLIC_BASE_URL -u PUBLIC_LIVEKIT_URL -u S3_PUBLIC_ENDPOINT \
  APP_ENV=development \
  PROVIDER_PROFILE=google-eu \
  TRANSHOOTER_ROOT="$DEV_ROOT" \
  PATH="$TEST_DIRECTORY/bin:$PATH" "$DEV_ROOT/scripts/dev-up" --provider-profile google-eu \
  > "$TEST_DIRECTORY/dev-up-env-file-output" 2>&1; then
  fail 'dev-up rejected production URLs loaded only from the repository .env'
fi
rm "$DEV_ROOT/.env"
for invalid_case in base livekit s3-insecure s3-localhost s3-omitted; do
  invalid_base=https://app.dev.example
  invalid_livekit=wss://rtc.dev.example
  invalid_s3=https://objects.dev.example
  case "$invalid_case" in
    base) invalid_base=http://app.dev.example ;;
    livekit) invalid_livekit=ws://rtc.dev.example ;;
    s3-insecure) invalid_s3=http://objects.dev.example ;;
    s3-localhost) invalid_s3=https://localhost:9000 ;;
    s3-omitted) invalid_s3= ;;
  esac
  if PATH="$TEST_DIRECTORY/bin:$PATH" \
    APP_ENV=development \
    PROVIDER_PROFILE=google-eu \
    TRANSHOOTER_ROOT="$DEV_ROOT" \
    PUBLIC_BASE_URL="$invalid_base" \
    PUBLIC_LIVEKIT_URL="$invalid_livekit" \
    S3_PUBLIC_ENDPOINT="$invalid_s3" \
    "$DEV_ROOT/scripts/dev-up" --provider-profile google-eu > "$TEST_DIRECTORY/dev-up-invalid-$invalid_case-output" 2>&1; then
    fail "dev-up accepted invalid production $invalid_case URL"
  fi
done


EMPTY_ENV="$TEST_DIRECTORY/empty.env"
: > "$EMPTY_ENV"
if ! docker compose version >/dev/null 2>&1; then
  fail 'Docker Compose plugin is required for production configuration contracts'
fi
config_compose() { docker compose "$@"; }

render_google_config() (
  export PROVIDER_PROFILE=google-eu
  export BOOTSTRAP_ADMIN_EMAIL=admin@example.test
  export GOOGLE_CLOUD_PROJECT=example-project
  export GOOGLE_QUOTA_PROJECT=example-quota
  export SMTP_URL=smtp://smtp.example.test:2525
  export RTC_ADVERTISED_IP=203.0.113.10
  export S3_PUBLIC_ENDPOINT=https://objects.example.test
  export PUBLIC_BASE_URL=https://app.example.test
  export PUBLIC_LIVEKIT_URL=wss://rtc.example.test
  config_compose --env-file "$EMPTY_ENV" \
    -f "$ROOT/deploy/compose/compose.yml" \
    -f "$ROOT/deploy/compose/compose.google.yml" config "$@"
)

render_google_speech_config() (
  export PROVIDER_PROFILE=google-speech-eu
  export BOOTSTRAP_ADMIN_EMAIL=admin@example.test
  export GOOGLE_CLOUD_PROJECT=example-project
  export GOOGLE_QUOTA_PROJECT=example-quota
  export SMTP_URL=smtp://smtp.example.test:2525
  export RTC_ADVERTISED_IP=203.0.113.10
  export S3_PUBLIC_ENDPOINT=https://objects.example.test
  export PUBLIC_BASE_URL=https://app.example.test
  export PUBLIC_LIVEKIT_URL=wss://rtc.example.test
  config_compose --env-file "$EMPTY_ENV" \
    -f "$ROOT/deploy/compose/compose.yml" \
    -f "$ROOT/deploy/compose/compose.google-speech.yml" config "$@"
)

for overlay in compose.google.yml compose.providers.yml; do
  for required in SMTP_URL RTC_ADVERTISED_IP S3_PUBLIC_ENDPOINT PUBLIC_BASE_URL PUBLIC_LIVEKIT_URL; do
    output="$TEST_DIRECTORY/${overlay}.${required}.output"
    if (
      export PROVIDER_PROFILE=deepgram-deepl-eu
      export BOOTSTRAP_ADMIN_EMAIL=admin@example.test
      export GOOGLE_CLOUD_PROJECT=example-project
      export GOOGLE_QUOTA_PROJECT=example-quota
      export DEEPGRAM_STREAMS=10
      export DEEPGRAM_AUDIO_SECONDS_MINUTE=600
      export DEEPL_REQUESTS_MINUTE=100
      export DEEPL_CHARACTERS_MINUTE=100000
      export SMTP_URL=smtp://smtp.example.test:2525
      export RTC_ADVERTISED_IP=203.0.113.10
      export S3_PUBLIC_ENDPOINT=https://objects.example.test
      export PUBLIC_BASE_URL=https://app.example.test
      export PUBLIC_LIVEKIT_URL=wss://rtc.example.test
      unset "$required"
      config_compose --env-file "$EMPTY_ENV" \
        -f "$ROOT/deploy/compose/compose.yml" \
        -f "$ROOT/deploy/compose/$overlay" config --quiet
    ) > "$output" 2>&1; then
      fail "$overlay rendered without required $required"
    fi
    grep -F "set $required" "$output" >/dev/null ||
      fail "$overlay missing-$required error was not actionable"
  done
done

render_google_speech_config > "$TEST_DIRECTORY/google-speech-config"
grep -F 'GOOGLE_SPEECH_LOCATION: europe-west3' \
  "$TEST_DIRECTORY/google-speech-config" >/dev/null ||
  fail 'google-speech-eu did not default to the reference Speech location'
grep -F 'GOOGLE_SPEECH_MODEL: long' \
  "$TEST_DIRECTORY/google-speech-config" >/dev/null ||
  fail 'google-speech-eu did not default to the reference Speech model'
grep -F 'GOOGLE_TRANSLATION_LOCATION: europe-west1' \
  "$TEST_DIRECTORY/google-speech-config" >/dev/null ||
  fail 'google-speech-eu did not default to the reference Translation location'
grep -F 'GOOGLE_TRANSLATION_MODEL: general/base' \
  "$TEST_DIRECTORY/google-speech-config" >/dev/null ||
  fail 'google-speech-eu did not default to the reference Translation model'
grep -F 'GOOGLE_TTS_LOCATION: eu' \
  "$TEST_DIRECTORY/google-speech-config" >/dev/null ||
  fail 'google-speech-eu did not default to the reference TTS location'
grep -F 'GOOGLE_TTS_VOICE: en-US-Chirp3-HD-Charon' \
  "$TEST_DIRECTORY/google-speech-config" >/dev/null ||
  fail 'google-speech-eu did not default to the reference TTS voice'

unset APP_ENV
render_google_config > "$TEST_DIRECTORY/production-config"
grep -F 'APP_ENV: production' "$TEST_DIRECTORY/production-config" >/dev/null ||
  fail 'direct production overlay use did not default APP_ENV to production'
grep -F 'S3_PUBLIC_ENDPOINT: https://objects.example.test' \
  "$TEST_DIRECTORY/production-config" >/dev/null ||
  fail 'configured external S3 endpoint did not propagate'
grep -F 'ARCHIVE_REQUIRE_KMS: "false"' "$TEST_DIRECTORY/production-config" >/dev/null ||
  fail 'production bundled MinIO archive did not explicitly disable unsupported KMS enforcement'
if grep -F 'S3_KMS_KEY_ID:' "$TEST_DIRECTORY/production-config" >/dev/null; then
  fail 'production bundled MinIO archive unexpectedly received a KMS key'
fi
grep -F 'PUBLIC_BASE_URL: https://app.example.test' "$TEST_DIRECTORY/production-config" >/dev/null ||
  fail 'production public HTTPS URL did not propagate'
grep -F 'PUBLIC_LIVEKIT_URL: wss://rtc.example.test' "$TEST_DIRECTORY/production-config" >/dev/null ||
  fail 'production public WSS URL did not propagate'
for role in migrator web control translation; do
  grep -F "database-$role-url" "$TEST_DIRECTORY/production-config" >/dev/null ||
    fail "distinct PostgreSQL $role URL was absent from production config"
done
for service in web control spool-drainer; do
  grep -F "minio-$service-credentials" "$TEST_DIRECTORY/production-config" >/dev/null ||
    fail "scoped MinIO $service credentials were absent from production config"
done
COMPOSE_PROFILES=benchmark render_google_config > "$TEST_DIRECTORY/benchmark-config"
grep -F 'minio-provider-diagnostics-credentials' "$TEST_DIRECTORY/benchmark-config" >/dev/null ||
  fail 'provider-diagnostics credentials were absent from benchmark profile config'
if grep -F 'minio-translation-credentials' "$TEST_DIRECTORY/production-config" >/dev/null; then
  fail 'long-running translation worker retained production S3 credentials'
fi
grep -F 'stop_grace_period: 1h0m0s' "$TEST_DIRECTORY/production-config" >/dev/null ||
  fail 'Egress graceful stop window was not one hour'

APP_ENV=development render_google_config > "$TEST_DIRECTORY/development-config"
grep -F 'APP_ENV: development' "$TEST_DIRECTORY/development-config" >/dev/null ||
  fail 'production overlay replaced dev-up APP_ENV'

(
  export PROVIDER_PROFILE=fixture
  export BOOTSTRAP_ADMIN_EMAIL=admin@example.test
  unset S3_PUBLIC_ENDPOINT
  config_compose --env-file "$EMPTY_ENV" \
    -f "$ROOT/deploy/compose/compose.yml" config
) > "$TEST_DIRECTORY/base-config"
grep -F 'S3_PUBLIC_ENDPOINT: http://localhost:9000' "$TEST_DIRECTORY/base-config" >/dev/null ||
  fail 'base Compose did not retain its explicit local S3 endpoint default'
grep -F -- '- transhooter-translation-worker' "$TEST_DIRECTORY/base-config" >/dev/null ||
  fail 'translation worker did not use its installed console command'
grep -F -- '- transhooter-spool-drainer' "$TEST_DIRECTORY/base-config" >/dev/null ||
  fail 'spool drainer did not use its installed console command'
grep -A80 '^  spool-drainer:$' "$TEST_DIRECTORY/base-config" | grep -m1 -F 'restart: unless-stopped' >/dev/null ||
  fail 'spool drainer did not retain crash-recovery restart policy'
COMPOSE_PROFILES=e2e config_compose --env-file "$EMPTY_ENV" -f "$ROOT/deploy/compose/compose.yml" config > "$TEST_DIRECTORY/e2e-config"
grep -A80 '^  e2e:$' "$TEST_DIRECTORY/e2e-config" | grep -m1 -F 'spool-drainer:' >/dev/null ||
  fail 'consultation smoke did not start the spool drainer'
grep -F 'internal-translation-token' "$TEST_DIRECTORY/base-config" >/dev/null ||
  fail 'translation worker secret projection omitted its role token'
grep -F 'internal-spool-drainer-token' "$TEST_DIRECTORY/base-config" >/dev/null ||
  fail 'spool drainer secret projection omitted its role token'
if grep -F 'translation-runtime.Dockerfile' "$TEST_DIRECTORY/base-config" >/dev/null; then
  fail 'Compose retained the deleted combined Python image'
fi
printf 'failure-smoke wrapper contracts passed\n'
