#!/bin/sh
set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
. "$PROJECT_ROOT/deploy/scripts/compose-lib.sh"
ROOT=$PROJECT_ROOT

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

TEST_DIRECTORY=$(mktemp -d "${TMPDIR:-/tmp}/failure-wrapper-contract.XXXXXXXX")
trap 'rm -rf -- "$TEST_DIRECTORY"' 0 HUP INT TERM
mkdir "$TEST_DIRECTORY/bin"
CALLS="$TEST_DIRECTORY/calls"
export CALLS
cat > "$TEST_DIRECTORY/bin/docker" <<'MOCK'
#!/bin/sh
printf 'APP_ENV=%s SMTP_URL=%s RTC_ADVERTISED_IP=%s S3_PUBLIC_ENDPOINT=%s %s\n' \
  "${APP_ENV:-}" "${SMTP_URL:-}" "${RTC_ADVERTISED_IP:-}" "${S3_PUBLIC_ENDPOINT:-}" "$*" >> "$CALLS"
case " $* " in
  *" compose version "*) exit 0 ;;
esac
case "${1:-}:${2:-}" in
  ps:*)
    if [ "${MOCK_MODE:-}" = ambiguous ]; then
      printf 'container|foreign-container|external-project\n'
    fi
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
  *" build egress-ready translation-worker web failure-smoke "*)
    if [ "${MOCK_MODE:-}" = deadline ]; then
      sleep 2
    fi
    ;;
  *" build egress-ready translation-worker web e2e "*)
    if [ "${MOCK_MODE:-}" = consultation-deadline ]; then
      sleep 2
    fi
    ;;
  *" down --volumes --remove-orphans "*) exit 0 ;;
esac
exit 0
MOCK
chmod +x "$TEST_DIRECTORY/bin/docker"

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
generated_deadline=$(
  grep -F 'failure-smoke.mjs' "$CALLS" |
    sed -n 's/.*--deadline-epoch-ms \([0-9][0-9]*\).*/\1/p'
)
[ -n "$generated_deadline" ] || fail 'generated scenario deadline was not forwarded'
[ "$generated_deadline" -ge "$((deadline_test_started_ms + 2000))" ] ||
  fail 'generated scenario deadline included setup/build elapsed time'


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
consultation_deadline=$(
  grep -F 'smoke:consultation' "$CALLS" |
    sed -n 's/.*--deadline-epoch-ms \([0-9][0-9]*\).*/\1/p'
)
[ -n "$consultation_deadline" ] || fail 'consultation smoke generated deadline was not forwarded'
[ "$consultation_deadline" -ge "$((consultation_started_ms + 2000))" ] ||
  fail 'consultation smoke deadline included lock/setup/build elapsed time'

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
: > "$DEV_ROOT/deploy/compose/compose.test.yml"
: > "$CALLS"

: > "$CALLS"
if ! PATH="$TEST_DIRECTORY/bin:$PATH" "$DEV_ROOT/scripts/dev-up" --provider-profile fixture \
  > "$TEST_DIRECTORY/dev-up-fixture-output" 2>&1; then
  fail 'fixture dev-up local-default wrapper run failed'
fi
grep -F 'APP_ENV=test SMTP_URL= RTC_ADVERTISED_IP=10.254.231.10 S3_PUBLIC_ENDPOINT=http://localhost:9000' \
  "$CALLS" >/dev/null || fail 'fixture dev-up did not export explicit local-only endpoint defaults'

: > "$CALLS"
if ! PATH="$TEST_DIRECTORY/bin:$PATH" \
  SMTP_URL=smtp://custom-mail:2525 \
  RTC_ADVERTISED_IP=192.0.2.44 \
  S3_PUBLIC_ENDPOINT=https://objects.dev.example \
  "$DEV_ROOT/scripts/dev-up" > "$TEST_DIRECTORY/dev-up-custom-output" 2>&1; then
  fail 'dev-up configured-endpoint wrapper run failed'
fi
grep -F 'APP_ENV=development SMTP_URL=smtp://custom-mail:2525 RTC_ADVERTISED_IP=192.0.2.44 S3_PUBLIC_ENDPOINT=https://objects.dev.example' \
  "$CALLS" >/dev/null || fail 'dev-up overwrote configured endpoints'

EMPTY_ENV="$TEST_DIRECTORY/empty.env"
: > "$EMPTY_ENV"
if docker compose version >/dev/null 2>&1; then
  config_compose() { docker compose "$@"; }
elif docker-compose version >/dev/null 2>&1; then
  config_compose() { docker-compose "$@"; }
else
  fail 'Docker Compose is required for production configuration contracts'
fi

render_google_config() (
  export PROVIDER_PROFILE=google-eu
  export BOOTSTRAP_ADMIN_EMAIL=admin@example.test
  export GOOGLE_CLOUD_PROJECT=example-project
  export GOOGLE_QUOTA_PROJECT=example-quota
  export SMTP_URL=smtp://smtp.example.test:2525
  export RTC_ADVERTISED_IP=203.0.113.10
  export S3_PUBLIC_ENDPOINT=https://objects.example.test
  config_compose --env-file "$EMPTY_ENV" \
    -f "$ROOT/deploy/compose/compose.yml" \
    -f "$ROOT/deploy/compose/compose.google.yml" config "$@"
)

for overlay in compose.google.yml compose.providers.yml; do
  for required in SMTP_URL RTC_ADVERTISED_IP S3_PUBLIC_ENDPOINT; do
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

unset APP_ENV
render_google_config > "$TEST_DIRECTORY/production-config"
grep -F 'APP_ENV: production' "$TEST_DIRECTORY/production-config" >/dev/null ||
  fail 'direct production overlay use did not default APP_ENV to production'
grep -F 'S3_PUBLIC_ENDPOINT: https://objects.example.test' \
  "$TEST_DIRECTORY/production-config" >/dev/null ||
  fail 'configured external S3 endpoint did not propagate'

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
printf 'failure-smoke wrapper contracts passed\n'
