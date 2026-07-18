#!/bin/sh
set -eu
umask 027
secrets_directory=/secrets
runtime_gid=${RUNTIME_GID:-10001}
temporary_file=

mkdir -p "$secrets_directory"

generate_hex() {
  byte_count=$1
  od -An -N"$byte_count" -tx1 /dev/urandom | tr -d ' \n'
}

generate_base64() {
  byte_count=$1
  od -An -N"$byte_count" -tu1 /dev/urandom |
    awk '{ for (index = 1; index <= NF; index++) printf "%c", $index }' |
    base64 |
    tr -d '\n'
}

path_exists() {
  [ -e "$1" ] || [ -L "$1" ]
}

prepare_secret_path() {
  secret_name=$1
  secret_target="$secrets_directory/$secret_name"

  if [ -d "$secret_target" ]; then
    if ! rmdir "$secret_target"; then
      echo "Secret path is a non-empty directory: $secret_target" >&2
      exit 1
    fi
  fi

  path_exists "$secret_target"
}

remove_temporary_file() {
  if [ -n "$temporary_file" ]; then
    rm -f "$temporary_file"
    temporary_file=
  fi
}

publish_new_secret() {
  secret_name=$1
  secret_value=$2
  secret_target="$secrets_directory/$secret_name"
  temporary_file=$(mktemp "$secrets_directory/.${secret_name}.XXXXXX")

  printf '%s\n' "$secret_value" > "$temporary_file"
  chown 0:"$runtime_gid" "$temporary_file"
  chmod 0440 "$temporary_file"

  if ln "$temporary_file" "$secret_target" 2>/dev/null; then
    remove_temporary_file
    return
  fi

  if path_exists "$secret_target"; then
    remove_temporary_file
    return
  fi

  echo "Unable to publish secret: $secret_name" >&2
  exit 1
}

install_secret_value() {
  secret_name=$1
  secret_value=$2

  if prepare_secret_path "$secret_name"; then
    return
  fi
  publish_new_secret "$secret_name" "$secret_value"
}

install_generated_hex_secret() {
  secret_name=$1
  byte_count=$2
  prefix=${3:-}

  if prepare_secret_path "$secret_name"; then
    return
  fi
  generated_value="${prefix}$(generate_hex "$byte_count")"
  publish_new_secret "$secret_name" "$generated_value"
}

install_primitive_secrets() {
  install_generated_hex_secret postgres-password 32
  install_generated_hex_secret redis-password 32

  install_generated_hex_secret minio-access-key 10 ts
  install_generated_hex_secret minio-secret-key 32

  install_generated_hex_secret livekit-api-key 12 lk_
  install_generated_hex_secret livekit-api-secret 32

  install_generated_hex_secret session-secret 48
  install_generated_hex_secret csrf-secret 48
  install_generated_hex_secret egress-layout-signing-key 32

  install_generated_hex_secret internal-control-token 32
  install_generated_hex_secret internal-translation-token 32
  install_generated_hex_secret internal-spool-drainer-token 32
}

load_persisted_primitives() {
  postgres_password=$(cat "$secrets_directory/postgres-password")
  redis_password=$(cat "$secrets_directory/redis-password")
  minio_access_key=$(cat "$secrets_directory/minio-access-key")
  minio_secret_key=$(cat "$secrets_directory/minio-secret-key")
  livekit_api_key=$(cat "$secrets_directory/livekit-api-key")
  livekit_api_secret=$(cat "$secrets_directory/livekit-api-secret")
}

install_derived_credentials() {
  database_url="postgresql://transhooter:${postgres_password}@postgres:5432/transhooter"
  redis_url="redis://:${redis_password}@redis:6379/0"
  livekit_credentials=$(
    printf '{"apiKey":"%s","apiSecret":"%s"}' \
      "$livekit_api_key" \
      "$livekit_api_secret"
  )
  minio_credentials=$(
    printf '{"accessKeyId":"%s","secretAccessKey":"%s"}' \
      "$minio_access_key" \
      "$minio_secret_key"
  )

  install_secret_value database-url "$database_url"
  install_secret_value redis-url "$redis_url"
  install_secret_value livekit-credentials "$livekit_credentials"
  install_secret_value minio-credentials "$minio_credentials"
}

install_spool_keyring() {
  if prepare_secret_path spool-keyring; then
    return
  fi

  spool_key=$(generate_base64 32)
  spool_keyring=$(
    printf '{"active":"local-v1","keys":{"local-v1":"%s"}}' "$spool_key"
  )
  publish_new_secret spool-keyring "$spool_keyring"
}

main() {
  trap remove_temporary_file 0
  trap 'exit 1' HUP INT TERM

  install_primitive_secrets
  load_persisted_primitives
  install_derived_credentials
  install_spool_keyring
}

main "$@"
