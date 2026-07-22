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
    exit 0
    ;;
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
  *" build minio egress-ready translation-worker web failure-smoke "*)
    if [ "${MOCK_MODE:-}" = deadline ]; then
      sleep 2
    fi
    ;;
  *" build minio egress-ready translation-worker web e2e "*)
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
    ;;
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
grep -F ' build minio egress-ready translation-worker web failure-smoke' "$CALLS" >/dev/null ||
  fail 'build-only did not build the shared smoke images'
if grep -F 'failure-smoke.mjs' "$CALLS" >/dev/null; then
  fail 'build-only unexpectedly ran failure scenarios'
fi
grep -F '"name":"Building failure-smoke application and harness images"' "$build_metrics" >/dev/null ||
  fail 'build-only metrics omitted the image build phase'

: > "$CALLS"
no_build_proof="$TEST_DIRECTORY/no-build.proof.json"
no_build_metrics="$TEST_DIRECTORY/no-build.metrics.jsonl"
if ! run_wrapper normal "$TEST_DIRECTORY/no-build-output" \
  --no-build --proof-file "$no_build_proof" --metrics-file "$no_build_metrics" \
  --shard provider; then
  fail 'no-build shard wrapper run failed'
fi
if grep -F ' build minio egress-ready translation-worker web failure-smoke' "$CALLS" >/dev/null; then
  fail 'no-build unexpectedly rebuilt smoke images'
fi
grep -F -- '--shard provider' "$CALLS" >/dev/null ||
  fail 'no-build did not forward the selected shard'
grep -F ':/proof' "$CALLS" >/dev/null || fail 'no-build did not mount the private proof directory'
[ -s "$no_build_proof" ] || fail 'no-build did not copy the harness proof'
grep -F '"name":"Running failure injection scenarios"' "$no_build_metrics" >/dev/null ||
  fail 'no-build metrics omitted scenario execution'

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
: > "$DEV_ROOT/deploy/compose/compose.test.yml"
: > "$CALLS"

: > "$CALLS"
if ! PATH="$TEST_DIRECTORY/bin:$PATH" "$DEV_ROOT/scripts/dev-up" --provider-profile fixture \
  > "$TEST_DIRECTORY/dev-up-fixture-output" 2>&1; then
  fail 'fixture dev-up local-default wrapper run failed'
fi
grep -F 'APP_ENV=test SMTP_URL= RTC_ADVERTISED_IP=10.254.231.10 S3_PUBLIC_ENDPOINT=http://localhost:9000 PUBLIC_BASE_URL=http://app.localhost:3000 PUBLIC_LIVEKIT_URL=ws://rtc.localhost:7880 S3_KMS_KEY_ID=' \
  "$CALLS" >/dev/null || fail 'fixture dev-up did not export explicit local-only endpoint defaults'

: > "$CALLS"
if ! PATH="$TEST_DIRECTORY/bin:$PATH" \
  SMTP_URL=smtp://custom-mail:2525 \
  RTC_ADVERTISED_IP=192.0.2.44 \
  S3_PUBLIC_ENDPOINT=https://objects.dev.example \
  PUBLIC_BASE_URL=https://app.dev.example \
  PUBLIC_LIVEKIT_URL=wss://rtc.dev.example \
  "$DEV_ROOT/scripts/dev-up" > "$TEST_DIRECTORY/dev-up-custom-output" 2>&1; then
  fail 'dev-up configured-endpoint wrapper run failed'
fi
grep -F 'APP_ENV=development SMTP_URL=smtp://custom-mail:2525 RTC_ADVERTISED_IP=192.0.2.44 S3_PUBLIC_ENDPOINT=https://objects.dev.example PUBLIC_BASE_URL=https://app.dev.example PUBLIC_LIVEKIT_URL=wss://rtc.dev.example S3_KMS_KEY_ID=' \
  "$CALLS" >/dev/null || fail 'dev-up overwrote configured endpoints or required a KMS key for bundled MinIO'
cat > "$DEV_ROOT/.env" <<'ENV'
PUBLIC_BASE_URL=https://app.env.example
PUBLIC_LIVEKIT_URL=wss://rtc.env.example
ENV
if ! env -u PUBLIC_BASE_URL -u PUBLIC_LIVEKIT_URL PATH="$TEST_DIRECTORY/bin:$PATH" "$DEV_ROOT/scripts/dev-up" \
  > "$TEST_DIRECTORY/dev-up-env-file-output" 2>&1; then
  fail 'dev-up rejected production URLs loaded only from the repository .env'
fi
rm "$DEV_ROOT/.env"
for invalid_case in base livekit; do
  if [ "$invalid_case" = base ]; then
    invalid_base=http://app.dev.example
    invalid_livekit=wss://rtc.dev.example
  else
    invalid_base=https://app.dev.example
    invalid_livekit=ws://rtc.dev.example
  fi
  if PATH="$TEST_DIRECTORY/bin:$PATH" \
    PUBLIC_BASE_URL="$invalid_base" \
    PUBLIC_LIVEKIT_URL="$invalid_livekit" \
    "$DEV_ROOT/scripts/dev-up" > "$TEST_DIRECTORY/dev-up-invalid-$invalid_case-output" 2>&1; then
    fail "dev-up accepted insecure production $invalid_case URL"
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
for service in web control translation spool-drainer; do
  grep -F "minio-$service-credentials" "$TEST_DIRECTORY/production-config" >/dev/null ||
    fail "scoped MinIO $service credentials were absent from production config"
done
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
printf 'failure-smoke wrapper contracts passed\n'
