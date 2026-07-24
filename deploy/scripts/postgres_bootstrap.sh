#!/bin/sh
set -eu

read_secret() {
  value=$(cat "$1")
  [ -n "$value" ] || { echo "empty secret file: $1" >&2; exit 1; }
  printf '%s' "$value"
}

owner_password=$(read_secret /run/secrets/postgres-owner-password)
export PGPASSWORD="$owner_password"

attempt=0
until pg_isready --host=postgres --username=transhooter_owner --dbname=transhooter >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "postgres did not accept network connections within 60 seconds" >&2
    exit 1
  fi
  sleep 1
done

if [ "${POSTGRES_BOOTSTRAP_PHASE:-roles}" = capability-grants ]; then
  psql --host=postgres --username=transhooter_owner --dbname=transhooter \
    --set=ON_ERROR_STOP=1 <<'SQL'
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM transhooter_capability;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM transhooter_capability;
GRANT SELECT, INSERT, UPDATE ON TABLE provider_profiles TO transhooter_capability;
GRANT SELECT, INSERT ON TABLE provider_profile_revisions TO transhooter_capability;
GRANT SELECT, INSERT, UPDATE ON TABLE language_capabilities TO transhooter_capability;
SQL
  exit 0
fi
migrator_password=$(read_secret /run/secrets/postgres-migrator-password)
web_password=$(read_secret /run/secrets/postgres-web-password)
control_password=$(read_secret /run/secrets/postgres-control-password)
translation_password=$(read_secret /run/secrets/postgres-translation-password)
capability_password=$(read_secret /run/secrets/postgres-capability-password)
export POSTGRES_MIGRATOR_PASSWORD="$migrator_password"
export POSTGRES_WEB_PASSWORD="$web_password"
export POSTGRES_CONTROL_PASSWORD="$control_password"
export POSTGRES_TRANSLATION_PASSWORD="$translation_password"
export POSTGRES_CAPABILITY_PASSWORD="$capability_password"

psql --host=postgres --username=transhooter_owner --dbname=transhooter \
  --set=ON_ERROR_STOP=1 <<'SQL'
\getenv migrator_password POSTGRES_MIGRATOR_PASSWORD
\getenv web_password POSTGRES_WEB_PASSWORD
\getenv control_password POSTGRES_CONTROL_PASSWORD
\getenv translation_password POSTGRES_TRANSLATION_PASSWORD
\getenv capability_password POSTGRES_CAPABILITY_PASSWORD
SELECT format('CREATE ROLE transhooter_migrator LOGIN PASSWORD %L', :'migrator_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'transhooter_migrator')\gexec
SELECT format('CREATE ROLE transhooter_web LOGIN PASSWORD %L', :'web_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'transhooter_web')\gexec
SELECT format('CREATE ROLE transhooter_control LOGIN PASSWORD %L', :'control_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'transhooter_control')\gexec
SELECT format('CREATE ROLE transhooter_translation LOGIN PASSWORD %L', :'translation_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'transhooter_translation')\gexec
SELECT format('CREATE ROLE transhooter_capability LOGIN PASSWORD %L', :'capability_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'transhooter_capability')\gexec

ALTER ROLE transhooter_migrator PASSWORD :'migrator_password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
ALTER ROLE transhooter_web PASSWORD :'web_password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
ALTER ROLE transhooter_control PASSWORD :'control_password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
ALTER ROLE transhooter_translation PASSWORD :'translation_password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
ALTER ROLE transhooter_capability PASSWORD :'capability_password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;


ALTER ROLE transhooter_migrator IN DATABASE transhooter SET search_path = public, pg_catalog;
ALTER ROLE transhooter_web IN DATABASE transhooter SET search_path = public, pg_catalog;
ALTER ROLE transhooter_control IN DATABASE transhooter SET search_path = public, pg_catalog;
ALTER ROLE transhooter_translation IN DATABASE transhooter SET search_path = public, pg_catalog;
ALTER ROLE transhooter_capability IN DATABASE transhooter SET search_path = public, pg_catalog;

REVOKE CONNECT, CREATE, TEMPORARY ON DATABASE transhooter FROM PUBLIC;
GRANT CONNECT, CREATE ON DATABASE transhooter TO transhooter_migrator;
GRANT CONNECT ON DATABASE transhooter TO transhooter_web, transhooter_control, transhooter_translation, transhooter_capability;

ALTER SCHEMA public OWNER TO transhooter_migrator;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO transhooter_migrator;
GRANT USAGE ON SCHEMA public TO transhooter_web, transhooter_control, transhooter_translation, transhooter_capability;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO transhooter_web, transhooter_control, transhooter_translation;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO transhooter_web, transhooter_control, transhooter_translation;
ALTER DEFAULT PRIVILEGES FOR ROLE transhooter_migrator IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO transhooter_web, transhooter_control, transhooter_translation;
ALTER DEFAULT PRIVILEGES FOR ROLE transhooter_migrator IN SCHEMA public
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO transhooter_web, transhooter_control, transhooter_translation;
SQL

integration_database=${POSTGRES_INTEGRATION_DATABASE:-}
if [ -z "$integration_database" ]; then
  exit 0
fi
if [ "$integration_database" != transhooter_integration ]; then
  echo "unsupported integration database name: $integration_database" >&2
  exit 2
fi

psql --host=postgres --username=transhooter_owner --dbname=transhooter \
  --set=ON_ERROR_STOP=1 <<'SQL'
SELECT format('CREATE DATABASE transhooter_integration OWNER transhooter_migrator')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'transhooter_integration')\gexec
ALTER DATABASE transhooter_integration OWNER TO transhooter_migrator;
ALTER ROLE transhooter_migrator IN DATABASE transhooter_integration SET search_path = public, pg_catalog;
REVOKE CONNECT, CREATE, TEMPORARY ON DATABASE transhooter_integration FROM PUBLIC;
GRANT CONNECT, CREATE ON DATABASE transhooter_integration TO transhooter_migrator;
SQL

psql --host=postgres --username=transhooter_owner --dbname=transhooter_integration \
  --set=ON_ERROR_STOP=1 <<'SQL'
ALTER SCHEMA public OWNER TO transhooter_migrator;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO transhooter_migrator;
SQL
