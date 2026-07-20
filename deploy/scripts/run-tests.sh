#!/bin/sh
set -eu
workspace=/workspace
translation_worker_directory="$workspace/services/translation-worker"
phase_deadline_seconds=${TEST_GATE_PHASE_DEADLINE_SECONDS:-600}
case "$phase_deadline_seconds" in
  "" | *[!0-9]* | 0)
    printf 'TEST_GATE_PHASE_DEADLINE_SECONDS must be a positive integer, got %s.\n' "$phase_deadline_seconds" >&2
    exit 2
    ;;
esac

run_phase() {
  phase=$1
  shift
  printf '\n==> %s (deadline: %ss)\n' "$phase" "$phase_deadline_seconds" >&2
  if timeout --signal=TERM --kill-after=10s "${phase_deadline_seconds}s" "$@"; then
    return 0
  else
    status=$?
  fi
  if [ "$status" -eq 124 ] || [ "$status" -eq 137 ]; then
    printf 'Gate phase "%s" exceeded its absolute %ss deadline; its process tree was terminated.\n' "$phase" "$phase_deadline_seconds" >&2
  else
    printf 'Gate phase "%s" failed with status %s.\n' "$phase" "$status" >&2
  fi
  return "$status"
}


run_typescript_gates() {
  cd "$workspace"
  run_phase "Checking generated cross-runtime contracts" bun run contracts:check
  run_phase "Type-checking TypeScript workspaces" bun run typecheck
  run_phase "Running repository lint and boundary checks" bun run lint
  run_phase "Running scoped TypeScript tests" \
    bun test packages/contracts/test packages/server-core/tests packages/telemetry/test apps/control-worker/test apps/web/lib
}

run_python_gates() {
  cd "$translation_worker_directory"
  run_phase "Type-checking the translation worker" mypy src
  run_phase "Linting the translation worker" ruff check
  run_phase "Checking translation-worker import boundaries" lint-imports
  run_phase "Running translation-worker tests" pytest
}

main() {
  run_typescript_gates
  run_python_gates
}

main "$@"
