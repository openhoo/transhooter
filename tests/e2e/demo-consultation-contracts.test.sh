#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
WRAPPER="$ROOT/scripts/demo-consultation"
fail() { printf 'demo wrapper contract failure: %s\n' "$1" >&2; exit 1; }
assert_contains() { grep -F -- "$2" "$1" >/dev/null || fail "missing '$2' in $1"; }
assert_count() {
  actual=$(grep -F -c -- "$2" "$1" || true)
  [ "$actual" -eq "$3" ] || fail "expected $3 occurrences of '$2' in $1, found $actual"
}
sh -n "$WRAPPER" || fail 'wrapper syntax is invalid'
TEST_DIRECTORY=$(mktemp -d "${TMPDIR:-/tmp}/demo-wrapper-contract.XXXXXXXX")
trap 'rm -rf -- "$TEST_DIRECTORY"' 0 HUP INT TERM
SANDBOX="$TEST_DIRECTORY/repo"
mkdir -p "$SANDBOX/scripts" "$SANDBOX/tests/e2e" "$SANDBOX/deploy/compose" "$SANDBOX/.secrets" "$TEST_DIRECTORY/bin"
cp "$WRAPPER" "$SANDBOX/scripts/demo-consultation"
for file in compose.yml compose.test.yml compose.demo.yml compose.demo.fixture.yml compose.google.yml compose.demo.google.yml compose.providers.yml compose.demo.providers.yml; do
  : > "$SANDBOX/deploy/compose/$file"
done
: > "$SANDBOX/tests/e2e/demo-consultation.mjs"
printf input > "$TEST_DIRECTORY/employee.mp4"
printf input > "$TEST_DIRECTORY/customer.mp4"
printf '{}\n' > "$SANDBOX/.secrets/google-adc.json"
printf key > "$SANDBOX/.secrets/deepgram-api-key"
printf key > "$SANDBOX/.secrets/deepl-api-key"
CALLS="$TEST_DIRECTORY/calls"
export CALLS

cat > "$TEST_DIRECTORY/bin/bun" <<'STUB'
#!/bin/sh
printf 'bun %s\n' "$*" >> "$CALLS"
if [ "${1:-}" = -e ]; then
  case "${2:-}" in
    *'JSON.parse'*)
      cat >/dev/null
      case "${MOCK_PROBE:-}" in
        no-video) printf 'no-video audio duration 12.5\n' ;;
        no-audio) printf 'video no-audio duration 12.5\n' ;;
        invalid-duration | non-numeric-duration) printf 'video audio invalid-duration\n' ;;
        malformed) exit 1 ;;
        short) printf 'video audio duration 0.5\n' ;;
        *) printf 'video audio duration 12.5\n' ;;
      esac
      exit 0
      ;;
  esac
  [ "${MOCK_BROWSER_MISSING:-false}" = true ] && exit 1
  printf '%s\n' "${MOCK_CHROMIUM_PATH:?}"
  exit 0
fi
printf 'demo-env profile=%s base=%s livekit=%s mailpit=%s employee=%s/%s customer=%s/%s proof=%s deadline=%s\n' \
  "${EXPECTED_PROFILE:-}" "${BASE_URL:-}" "${LIVEKIT_URL:-}" "${MAILPIT_URL:-}" \
  "${E2E_EMPLOYEE_VIDEO_FILE:-}" "${E2E_EMPLOYEE_AUDIO_FILE:-}" \
  "${E2E_CUSTOMER_VIDEO_FILE:-}" "${E2E_CUSTOMER_AUDIO_FILE:-}" \
  "${EMIT_PROOF_JSON:-}" "${SCENARIO_DEADLINE_EPOCH_MS:-}" >> "$CALLS"
exit "${MOCK_DEMO_STATUS:-0}"
STUB
cat > "$TEST_DIRECTORY/bin/docker" <<'STUB'
#!/bin/sh
printf 'docker PROVIDER_PROFILE=%s PUBLIC_BASE_URL=%s PUBLIC_LIVEKIT_URL=%s S3_PUBLIC_ENDPOINT=%s SMTP_URL=%s %s\n' \
  "${PROVIDER_PROFILE:-}" "${PUBLIC_BASE_URL:-}" "${PUBLIC_LIVEKIT_URL:-}" "${S3_PUBLIC_ENDPOINT:-}" "${SMTP_URL:-}" "$*" >> "$CALLS"
case " $* " in
  *' compose version '*) exit 0 ;;
  *' compose '*ps*'-q mailpit'*) printf 'mailpit-container\n' ;;
  *' inspect '*) printf '10.254.232.2\n' ;;
  *' config '*)
    project=
    previous=
    for argument do
      [ "$previous" = -p ] && project=$argument
      previous=$argument
    done
    printf 'services:\n  web:\n    environment:\n      PROVIDER_PROFILE: %s\nvolumes:\n  test-secrets:\n    name: %s_test-secrets\n' "${PROVIDER_PROFILE:-}" "$project"
    ;;
esac
exit 0
STUB
cat > "$TEST_DIRECTORY/bin/ffprobe" <<'STUB'
#!/bin/sh
printf 'ffprobe %s\n' "$*" >> "$CALLS"
case "${MOCK_PROBE:-}" in
  decode) exit 1 ;;
  malformed) printf '{not-json\n' ;;
  no-video) printf '{"streams":[{"codec_type":"audio"}],"format":{"duration":"12.5"}}\n' ;;
  no-audio) printf '{"streams":[{"codec_type":"video"}],"format":{"duration":"12.5"}}\n' ;;
  invalid-duration) printf '{"streams":[{"codec_type":"video"},{"codec_type":"audio"}],"format":{}}\n' ;;
  non-numeric-duration) printf '{"streams":[{"codec_type":"video"},{"codec_type":"audio"}],"format":{"duration":"unknown"}}\n' ;;
  short) printf '{"streams":[{"codec_type":"video"},{"codec_type":"audio"}],"format":{"duration":"0.5"}}\n' ;;
  *) printf '{"streams":[{"codec_type":"video"},{"codec_type":"audio"}],"format":{"duration":"12.5"}}\n' ;;
esac
exit 0
STUB
cat > "$TEST_DIRECTORY/bin/ffmpeg" <<'STUB'
#!/bin/sh
printf 'ffmpeg %s\n' "$*" >> "$CALLS"
[ "${MOCK_FFMPEG_FAIL:-false}" = true ] && exit 1
for output do :; done
printf converted > "$output"
STUB
cat > "$TEST_DIRECTORY/bin/curl" <<'STUB'
#!/bin/sh
case " $* " in
  *'/api/v1/messages'*) printf '{"messages":[]}\n' ;;
  *) printf '{"status":"ready"}\n' ;;
esac
STUB
cat > "$TEST_DIRECTORY/bin/ss" <<'STUB'
#!/bin/sh
exit 0
STUB
cat > "$TEST_DIRECTORY/bin/timeout" <<'STUB'
#!/bin/sh
case " $* " in *'/dev/tcp/'*) exit 0 ;; esac
shift
exec "$@"
STUB
cat > "$TEST_DIRECTORY/bin/setsid" <<'STUB'
#!/bin/sh
exec "$@"
STUB
chmod +x "$TEST_DIRECTORY/bin/"*
MOCK_CHROMIUM_PATH="$TEST_DIRECTORY/chromium"
printf '#!/bin/sh\nexit 0\n' > "$MOCK_CHROMIUM_PATH"
chmod +x "$MOCK_CHROMIUM_PATH"
export MOCK_CHROMIUM_PATH

run_demo() {
  output=$1
  shift
  : > "$CALLS"
  PATH="$TEST_DIRECTORY/bin:$PATH" TRANSHOOTER_ROOT="$SANDBOX" TMPDIR="$TEST_DIRECTORY" DISPLAY=:99 \
    "$SANDBOX/scripts/demo-consultation" "$@" > "$output" 2>&1
}
expect_failure() {
  label=$1
  diagnostic=$2
  shift 2
  output="$TEST_DIRECTORY/$label.out"
  if run_demo "$output" "$@"; then fail "$label unexpectedly succeeded"; fi
  assert_contains "$output" "$diagnostic"
}

expect_failure missing-customer 'missing required --customer-video option' --employee-video "$TEST_DIRECTORY/employee.mp4"
expect_failure duplicate-employee 'duplicate --employee-video option' --employee-video "$TEST_DIRECTORY/employee.mp4" --employee-video "$TEST_DIRECTORY/customer.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4"
expect_failure unknown-option 'unknown option: --bogus' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4" --bogus value
expect_failure positional 'unexpected positional argument: stray' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4" stray
expect_failure same-input 'must be different files' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/employee.mp4"
expect_failure bad-profile 'unsupported provider profile: invalid' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4" --provider-profile invalid

MOCK_BROWSER_MISSING=true
export MOCK_BROWSER_MISSING
expect_failure browser-missing 'Run: bunx playwright install chromium' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4"
unset MOCK_BROWSER_MISSING

MOCK_PROBE=decode; export MOCK_PROBE
expect_failure probe-failure 'employee input cannot be decoded' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4"
MOCK_PROBE=malformed; export MOCK_PROBE
expect_failure malformed-probe 'employee input cannot be decoded' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4"
MOCK_PROBE=invalid-duration; export MOCK_PROBE
expect_failure invalid-duration 'employee input has invalid or non-numeric duration' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4"
MOCK_PROBE=non-numeric-duration; export MOCK_PROBE
expect_failure non-numeric-duration 'employee input has invalid or non-numeric duration' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4"
MOCK_PROBE=no-video; export MOCK_PROBE
expect_failure no-video 'employee input has no decodable video stream' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4"
MOCK_PROBE=no-audio; export MOCK_PROBE
expect_failure no-audio 'employee input has no decodable audio stream' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4"
MOCK_PROBE=short; export MOCK_PROBE
expect_failure short 'employee input is shorter than one second' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4"
unset MOCK_PROBE

mv "$SANDBOX/.secrets/google-adc.json" "$SANDBOX/.secrets/google-adc.missing"
expect_failure google-credential 'missing provider credential: .secrets/google-adc.json' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4" --provider-profile google-eu
mv "$SANDBOX/.secrets/google-adc.missing" "$SANDBOX/.secrets/google-adc.json"
expect_failure google-setting 'missing provider setting: GOOGLE_CLOUD_PROJECT' --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4" --provider-profile google-eu

output="$TEST_DIRECTORY/happy.out"
run_demo "$output" --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4"
assert_contains "$CALLS" 'PROVIDER_PROFILE=fixture'
assert_count "$CALLS" 'ffprobe ' 2
assert_count "$CALLS" '-show_entries stream=codec_type:format=duration -of json' 2
assert_contains "$CALLS" "-t 30 -an -vf scale=w='min(640,iw)':h='min(360,ih)':force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2,fps=15,format=yuv420p -f yuv4mpegpipe"
assert_contains "$CALLS" '-t 30 -vn -ac 1 -ar 48000 -c:a pcm_s16le'
assert_contains "$CALLS" 'compose.demo.fixture.yml -f'
assert_contains "$CALLS" 'run --rm --no-deps secrets-init'
assert_contains "$CALLS" 'up --build --wait --remove-orphans otel-collector mailpit web'
assert_contains "$CALLS" 'mailpit=http://10.254.232.2:8025'
assert_contains "$CALLS" 'employee.y4m/'
assert_contains "$CALLS" 'employee.wav customer='
assert_contains "$CALLS" 'proof=true deadline='
assert_contains "$CALLS" 'down --volumes --remove-orphans'
[ -f "$TEST_DIRECTORY/employee.mp4" ] && [ -f "$TEST_DIRECTORY/customer.mp4" ] || fail 'source files were removed'

GOOGLE_CLOUD_PROJECT=project GOOGLE_QUOTA_PROJECT=quota; export GOOGLE_CLOUD_PROJECT GOOGLE_QUOTA_PROJECT
run_demo "$TEST_DIRECTORY/google.out" --employee-video "$TEST_DIRECTORY/employee.mp4" --customer-video "$TEST_DIRECTORY/customer.mp4" --provider-profile google-eu
assert_contains "$CALLS" 'PUBLIC_BASE_URL=http://localhost:3000 PUBLIC_LIVEKIT_URL=ws://localhost:7880 S3_PUBLIC_ENDPOINT=http://localhost:9000 SMTP_URL=smtp://mailpit:1025'
assert_contains "$CALLS" 'compose.google.yml -f'
assert_contains "$CALLS" 'compose.demo.google.yml -f'

assert_contains "$WRAPPER" 'flock -n 9'
assert_contains "$WRAPPER" ': > "$RUN_DIRECTORY/compose.env"'
assert_contains "$WRAPPER" 'chmod 0700 "$RUN_DIRECTORY"'
assert_contains "$WRAPPER" 'setsid bun "$ROOT/tests/e2e/demo-consultation.mjs"'
printf 'demo consultation wrapper contracts passed\n'
