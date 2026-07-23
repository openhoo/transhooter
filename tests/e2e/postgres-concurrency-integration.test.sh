#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$ROOT"

if [ -n "${DATABASE_URL:-}" ]; then
  :
elif [ -n "${DATABASE_URL_FILE:-}" ] && [ -r "$DATABASE_URL_FILE" ]; then
  :
else
  printf '%s\n' 'DATABASE_URL or a readable DATABASE_URL_FILE is required for the hermetic PostgreSQL integration contract.' >&2
  exit 2
fi

POSTGRES_CONCURRENCY_INTEGRATION=1 \
  bun test packages/server-core/tests/auth-persistence.test.ts \
    --test-name-pattern 'PostgreSQL migration and terminal settlement integration'

POSTGRES_CONCURRENCY_INTEGRATION=1 \
  exec bun test apps/control-worker/test/postgres-effect-claim-integration.test.ts \
    --test-name-pattern 'PostgreSQL effect claim fairness'
