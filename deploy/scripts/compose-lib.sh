#!/bin/sh
set -eu
# ROOT is based on the top-level lifecycle script that sources this file.
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
STATE_FILE="$ROOT/.secrets/compose-state"
PROJECT_NAME=${COMPOSE_PROJECT_NAME:-transhooter}
BASE_FILE="$ROOT/deploy/compose/compose.yml"

announce_phase() {
  printf '\n==> %s\n' "$1" >&2
}

write_compose_state() {
  mkdir -p "$ROOT/.secrets"
  tmp="$STATE_FILE.tmp.$$"
  if ! (umask 077 && printf 'profile=%s\nproject=%s\n' "$profile" "$PROJECT_NAME" > "$tmp"); then
    rm -f "$tmp"
    return 1
  fi
  if ! chmod 0600 "$tmp" || ! mv "$tmp" "$STATE_FILE"; then
    rm -f "$tmp"
    return 1
  fi
}

invalid_state() {
  echo "Invalid compose state file" >&2
  exit 1
}

unsupported_profile() {
  echo "Unsupported provider profile: $1" >&2
  return 2
}

resolve_profile() {
  profile=$1
  case "$profile" in
    fixture)
      overlay=deploy/compose/compose.test.yml
      APP_ENV=test
      RTC_SUBNET=${RTC_SUBNET:-${TEST_RTC_SUBNET:-10.254.232.0/24}}
      RTC_NODE_IP=${RTC_NODE_IP:-${TEST_RTC_NODE_IP:-10.254.232.250}}
      export RTC_SUBNET RTC_NODE_IP
      ;;
    google-eu)
      overlay=deploy/compose/compose.google.yml
      APP_ENV=development
      ;;
    deepgram-deepl-eu)
      overlay=deploy/compose/compose.providers.yml
      APP_ENV=development
      ;;
    *)
      unsupported_profile "$profile"
      return $?
      ;;
  esac
  OVERLAY_FILE="$ROOT/$overlay"
  export PROVIDER_PROFILE=$profile
  export APP_ENV
}

profile_configuration_value() {
  environment=$1
  requested=$2
  value=
  while IFS='=' read -r name candidate; do
    if [ "$name" = "$requested" ]; then
      value=$candidate
    fi
  done <<EOF
$environment
EOF
  printf '%s\n' "$value"
}

validate_profile_configuration() {
  [ "$profile" = fixture ] && return 0

  environment=${1:-}
  public_base_url=${PUBLIC_BASE_URL:-}
  public_livekit_url=${PUBLIC_LIVEKIT_URL:-}
  if [ -n "$environment" ]; then
    public_base_url=$(profile_configuration_value "$environment" PUBLIC_BASE_URL)
    public_livekit_url=$(profile_configuration_value "$environment" PUBLIC_LIVEKIT_URL)
  fi

  case "$public_base_url" in
    https://?*) ;;
    *) echo "PUBLIC_BASE_URL must be an explicit https:// URL for production provider profiles" >&2; return 1 ;;
  esac
  case "$public_livekit_url" in
    wss://?*) ;;
    *) echo "PUBLIC_LIVEKIT_URL must be an explicit wss:// URL for production provider profiles" >&2; return 1 ;;
  esac
}

validate_project_name() {
  case "$PROJECT_NAME" in
    "" | -* | *[!a-zA-Z0-9_.-]*)
      invalid_state
      ;;
  esac
}

volume_deletion_requested() {
  for argument do
    case "$argument" in
      -v | -v=1 | -v=[tT] | -v=true | -v=True | -v=TRUE | \
        --volumes | --volumes=1 | --volumes=[tT] | --volumes=true | --volumes=True | --volumes=TRUE)
        return 0
        ;;
    esac
  done
  return 1
}

compose_help_requested() {
  for argument do
    case "$argument" in
      -h | --help)
        return 0
        ;;
    esac
  done
  return 1
}

confirm_volume_deletion() {
  printf '%s\n' 'WARNING: this permanently removes Transhooter persistent volumes.' >&2
  printf '%s' 'Type RESET to continue: ' >&2
  IFS= read -r answer
  test "$answer" = RESET || { echo "Volume deletion cancelled." >&2; exit 1; }
}

load_state() {
  if [ ! -s "$STATE_FILE" ]; then
    echo "No recorded transhooter stack. Run ./scripts/dev-up first." >&2
    exit 1
  fi
  # A recorded stack must not be redirected by the caller's current shell.
  PROJECT_NAME=transhooter

  profile=
  recorded_project=
  seen_profile=false
  seen_project=false

  while IFS='=' read -r key value || [ -n "$key$value" ]; do
    case "$key" in
      profile)
        $seen_profile && invalid_state
        profile=$value
        seen_profile=true
        ;;
      project)
        $seen_project && invalid_state
        recorded_project=$value
        seen_project=true
        ;;
      *)
        invalid_state
        ;;
    esac
  done < "$STATE_FILE"

  if [ -z "$profile" ] || ! $seen_project; then
    invalid_state
  fi
  PROJECT_NAME=$recorded_project

  validate_project_name
  export COMPOSE_PROJECT_NAME=$PROJECT_NAME
  resolve_profile "$profile"
}

select_compose_command() {
  if ! docker compose version >/dev/null 2>&1; then
    echo "Docker Compose plugin is required." >&2
    return 127
  fi
}

invoke_compose() {
  docker compose "$@"
}

compose() {
  select_compose_command || return $?

  set -- \
    -p "$PROJECT_NAME" \
    -f "$BASE_FILE" \
    -f "$OVERLAY_FILE" \
    "$@"

  if [ -f "$ROOT/.env" ]; then
    set -- --env-file "$ROOT/.env" "$@"
  fi

  invoke_compose "$@"
}

failure_smoke_project_is_owned() {
  candidate=$1
  owner=$2
  [ "$candidate" = "$owner" ] || return 1

  case "$candidate" in
    transhooter-failure-*-*) ;;
    *) return 1 ;;
  esac
  identity=${candidate#transhooter-failure-}
  process_id=${identity%%-*}
  started_at=${identity#*-}
  case "$process_id:$started_at" in
    :* | *: | *[!0-9:]* | *:*:*) return 1 ;;
  esac
}

failure_smoke_project_owner_pid() {
  candidate=$1
  case "$candidate" in
    transhooter-failure-*-*) ;;
    *) return 1 ;;
  esac
  identity=${candidate#transhooter-failure-}
  process_id=${identity%%-*}
  started_at=${identity#*-}
  case "$process_id:$started_at" in
    :* | *: | *[!0-9:]* | *:*:*) return 1 ;;
  esac
  printf '%s\n' "$process_id"
}

failure_smoke_project_is_stale() {
  candidate=$1
  current_project=$2
  [ "$candidate" != "$current_project" ] || return 1
  process_id=$(failure_smoke_project_owner_pid "$candidate") || return 1
  ! kill -0 "$process_id" 2>/dev/null
}

failure_smoke_resource_is_owned() {
  project=$1
  owner=$2
  resource_project_label=$3
  failure_smoke_project_is_owned "$project" "$owner" &&
    [ "$resource_project_label" = "$owner" ]
}

build_runtime_and_harness_images() {
  compose build \
    minio \
    egress-ready \
    translation-worker \
    web \
    "$@"
}
