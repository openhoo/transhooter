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

validate_project_name() {
  case "$PROJECT_NAME" in
    "" | -* | *[!a-zA-Z0-9_.-]*)
      invalid_state
      ;;
  esac
}

load_state() {
  if [ ! -s "$STATE_FILE" ]; then
    echo "No recorded transhooter stack. Run ./scripts/dev-up first." >&2
    exit 1
  fi

  profile=
  recorded_overlay=
  recorded_project=
  seen_profile=false
  seen_overlay=false
  seen_project=false

  while IFS='=' read -r key value || [ -n "$key$value" ]; do
    case "$key" in
      profile)
        $seen_profile && invalid_state
        profile=$value
        seen_profile=true
        ;;
      overlay)
        $seen_overlay && invalid_state
        recorded_overlay=$value
        seen_overlay=true
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

  if [ -z "$profile" ]; then
    invalid_state
  fi
  if $seen_project; then
    PROJECT_NAME=$recorded_project
  fi

  validate_project_name
  resolve_profile "$profile"
  if $seen_overlay; then
    $seen_project || invalid_state
    case "$recorded_overlay" in
      "$overlay" | "$OVERLAY_FILE")
        ;;
      *)
        echo "Legacy compose state overlay does not match profile $profile." >&2
        exit 1
        ;;
    esac

    write_compose_state
  fi
}

select_compose_command() {
  if docker compose version >/dev/null 2>&1; then
    compose_implementation=plugin
  elif docker-compose version >/dev/null 2>&1; then
    compose_implementation=legacy
  else
    echo "Docker Compose is required (plugin or docker-compose executable)." >&2
    return 127
  fi
}

invoke_compose() {
  case "$compose_implementation" in
    plugin)
      docker compose "$@"
      ;;
    legacy)
      docker-compose "$@"
      ;;
  esac
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

build_runtime_and_harness_images() {
  compose build \
    egress-ready \
    translation-worker \
    web \
    "$@"
}
