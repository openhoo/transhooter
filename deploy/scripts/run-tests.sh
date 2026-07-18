#!/bin/sh
set -eu
workspace=/workspace
translation_worker_directory="$workspace/services/translation-worker"
run_phase() {
  phase=$1
  shift
  printf '\n==> %s\n' "$phase" >&2
  "$@"
}


run_typescript_gates() {
  cd "$workspace"
  run_phase "Checking generated cross-runtime contracts" bun run contracts:check
  run_phase "Type-checking TypeScript workspaces" bun run typecheck
  run_phase "Running repository lint and boundary checks" bun run lint
  run_phase "Running scoped TypeScript tests" \
    bun test packages/contracts/test packages/server-core/tests apps/control-worker/test apps/web/lib
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
