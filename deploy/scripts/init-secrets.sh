#!/bin/sh
set -eu
umask 027

secrets_directory=/secrets
runtime_gid=${RUNTIME_GID:-10001}
temporary_generation=
temporary_link=

primitive_secrets='postgres-password redis-password minio-access-key minio-secret-key livekit-api-key livekit-api-secret session-secret csrf-secret egress-layout-signing-key internal-control-token internal-translation-token internal-spool-drainer-token spool-keyring magic-link-seal-keys'
derived_secrets='database-url redis-url livekit-credentials minio-credentials'
managed_secrets="$primitive_secrets $derived_secrets"

mkdir -p "$secrets_directory"

generate_hex() {
  byte_count=$1
  od -An -N"$byte_count" -tx1 /dev/urandom | tr -d ' \n'
}

generate_base64() {
  byte_count=$1
  dd if=/dev/urandom bs=1 count="$byte_count" 2>/dev/null |
    base64 |
    tr -d '\n'
}

path_exists() {
  [ -e "$1" ] || [ -L "$1" ]
}

fail_invalid_path() {
  secret_name=$1
  secret_target="$secrets_directory/$secret_name"

  if [ -d "$secret_target" ]; then
    echo "Secret path is a directory: $secret_target" >&2
  else
    echo "Existing secret is empty or unreadable: $secret_target" >&2
  fi
  exit 1
}

validate_existing_secrets() {
  for secret_name in $managed_secrets; do
    secret_target="$secrets_directory/$secret_name"

    # Compose can materialize a missing bind-mounted file as an empty directory
    # before this one-shot service runs. Preserve the established recovery for
    # that case, but never remove a non-empty directory or follow a symlink.
    if [ -d "$secret_target" ] && [ ! -L "$secret_target" ]; then
      if ! rmdir "$secret_target"; then
        echo "Secret path is a non-empty directory: $secret_target" >&2
        exit 1
      fi
    fi

    if path_exists "$secret_target" && [ ! -s "$secret_target" ]; then
      fail_invalid_path "$secret_name"
    fi
  done
}

read_existing_secret() {
  secret_name=$1
  secret_target="$secrets_directory/$secret_name"

  [ -f "$secret_target" ] && [ -s "$secret_target" ] || fail_invalid_path "$secret_name"
  cat "$secret_target"
}

write_generation_secret() {
  secret_name=$1
  secret_value=$2
  secret_target="$temporary_generation/$secret_name"

  [ -n "$secret_value" ] || {
    echo "Refusing to generate an empty secret: $secret_name" >&2
    exit 1
  }
  printf '%s\n' "$secret_value" > "$secret_target"
  chown 0:"$runtime_gid" "$secret_target"
  chmod 0440 "$secret_target"
}

load_or_generate_hex() {
  secret_name=$1
  byte_count=$2
  prefix=${3:-}
  secret_target="$secrets_directory/$secret_name"

  if path_exists "$secret_target"; then
    read_existing_secret "$secret_name"
  else
    printf '%s%s' "$prefix" "$(generate_hex "$byte_count")"
  fi
}

load_or_generate_spool_keyring() {
  secret_target="$secrets_directory/spool-keyring"

  if path_exists "$secret_target"; then
    read_existing_secret spool-keyring
    return
  fi

  spool_key=$(generate_base64 32)
  printf '{"active":"local-v1","keys":{"local-v1":"%s"}}' "$spool_key"
}

validate_magic_link_keyring() {
  keyring=$1
  compact_keyring=$(printf '%s' "$keyring" | tr -d '[:space:]')

  if ! printf '%s' "$compact_keyring" |
    grep -Eq '^\{"currentKeyId":"[A-Za-z0-9._-]+","keys":\{"[A-Za-z0-9._-]+":"[A-Za-z0-9+/]{43}="(,"[A-Za-z0-9._-]+":"[A-Za-z0-9+/]{43}=")*\}\}$'; then
    echo "Invalid magic-link seal keyring JSON" >&2
    exit 1
  fi

  current_key_id=$(printf '%s' "$compact_keyring" |
    sed -n 's/^{"currentKeyId":"\([A-Za-z0-9._-]*\)","keys":{.*$/\1/p')
  key_entries=$(printf '%s' "$compact_keyring" |
    sed -n 's/^{"currentKeyId":"[^"]*","keys":{\(.*\)}}$/\1/p')
  current_key_present=false
  seen_key_ids=
  old_ifs=$IFS
  IFS=,
  for key_entry in $key_entries; do
    key_id=$(printf '%s' "$key_entry" | sed -n 's/^"\([A-Za-z0-9._-]*\)":".*"$/\1/p')
    encoded_key=$(printf '%s' "$key_entry" | sed -n 's/^"[^"]*":"\([^"]*\)"$/\1/p')
    case ",$seen_key_ids," in
      *,"$key_id",*)
        echo "Magic-link seal keyring contains duplicate key id '$key_id'" >&2
        exit 1
        ;;
    esac
    seen_key_ids="${seen_key_ids}${seen_key_ids:+,}${key_id}"
    decoded_length=$(printf '%s' "$encoded_key" | base64 -d 2>/dev/null | wc -c | tr -d ' ')
    [ "$decoded_length" = 32 ] || {
      echo "Magic-link seal key '$key_id' is not a base64-encoded 32-byte key" >&2
      exit 1
    }
    if [ "$key_id" = "$current_key_id" ]; then
      current_key_present=true
    fi
  done
  IFS=$old_ifs

  [ "$current_key_present" = true ] || {
    echo "Magic-link seal currentKeyId is absent from keys" >&2
    exit 1
  }
}

load_or_generate_magic_link_keyring() {
  secret_target="$secrets_directory/magic-link-seal-keys"

  if path_exists "$secret_target"; then
    keyring=$(read_existing_secret magic-link-seal-keys)
  else
    current_key_id="local-$(generate_hex 8)"
    current_key=$(generate_base64 32)
    keyring=$(printf '{"currentKeyId":"%s","keys":{"%s":"%s"}}' \
      "$current_key_id" "$current_key_id" "$current_key")
  fi

  validate_magic_link_keyring "$keyring"
  printf '%s' "$compact_keyring"
}

url_encode() {
  printf '%s' "$1" | od -An -v -tx1 |
    awk '
      function hex_digit(character) {
        return index("0123456789ABCDEF", character) - 1
      }
      {
        for (field_index = 1; field_index <= NF; field_index++) {
          byte = toupper($field_index)
          decimal = (16 * hex_digit(substr(byte, 1, 1))) + hex_digit(substr(byte, 2, 1))
          character = sprintf("%c", decimal)
          if ((decimal >= 48 && decimal <= 57) ||
              (decimal >= 65 && decimal <= 90) ||
              (decimal >= 97 && decimal <= 122) ||
              character == "-" || character == "." || character == "_" || character == "~") {
            printf "%s", character
          } else {
            printf "%%%s", byte
          }
        }
      }
    '
}

json_escape() {
  printf '%s' "$1" | od -An -v -tx1 |
    awk '
      function hex_digit(character) {
        return index("0123456789ABCDEF", character) - 1
      }
      {
        for (field_index = 1; field_index <= NF; field_index++) {
          byte = toupper($field_index)
          decimal = (16 * hex_digit(substr(byte, 1, 1))) + hex_digit(substr(byte, 2, 1))
          if (decimal == 34) {
            printf "\\\""
          } else if (decimal == 92) {
            printf "\\\\"
          } else if (decimal >= 32 && decimal <= 126) {
            printf "%c", decimal
          } else {
            printf "\\u00%s", byte
          }
        }
      }
    '
}

build_generation() {
  temporary_generation=$(mktemp -d "$secrets_directory/.tmp-generation.XXXXXX")
  chown 0:"$runtime_gid" "$temporary_generation"
  chmod 0750 "$temporary_generation"

  postgres_password=$(load_or_generate_hex postgres-password 32)
  redis_password=$(load_or_generate_hex redis-password 32)
  minio_access_key=$(load_or_generate_hex minio-access-key 10 ts)
  minio_secret_key=$(load_or_generate_hex minio-secret-key 32)
  livekit_api_key=$(load_or_generate_hex livekit-api-key 12 lk_)
  livekit_api_secret=$(load_or_generate_hex livekit-api-secret 32)
  session_secret=$(load_or_generate_hex session-secret 48)
  csrf_secret=$(load_or_generate_hex csrf-secret 48)
  egress_layout_signing_key=$(load_or_generate_hex egress-layout-signing-key 32)
  internal_control_token=$(load_or_generate_hex internal-control-token 32)
  internal_translation_token=$(load_or_generate_hex internal-translation-token 32)
  internal_spool_drainer_token=$(load_or_generate_hex internal-spool-drainer-token 32)
  spool_keyring=$(load_or_generate_spool_keyring)
  magic_link_seal_keys=$(load_or_generate_magic_link_keyring)

  write_generation_secret postgres-password "$postgres_password"
  write_generation_secret redis-password "$redis_password"
  write_generation_secret minio-access-key "$minio_access_key"
  write_generation_secret minio-secret-key "$minio_secret_key"
  write_generation_secret livekit-api-key "$livekit_api_key"
  write_generation_secret livekit-api-secret "$livekit_api_secret"
  write_generation_secret session-secret "$session_secret"
  write_generation_secret csrf-secret "$csrf_secret"
  write_generation_secret egress-layout-signing-key "$egress_layout_signing_key"
  write_generation_secret internal-control-token "$internal_control_token"
  write_generation_secret internal-translation-token "$internal_translation_token"
  write_generation_secret internal-spool-drainer-token "$internal_spool_drainer_token"
  write_generation_secret spool-keyring "$spool_keyring"
  write_generation_secret magic-link-seal-keys "$magic_link_seal_keys"

  database_password=$(url_encode "$postgres_password")
  redis_url_password=$(url_encode "$redis_password")
  livekit_key_json=$(json_escape "$livekit_api_key")
  livekit_secret_json=$(json_escape "$livekit_api_secret")
  minio_access_json=$(json_escape "$minio_access_key")
  minio_secret_json=$(json_escape "$minio_secret_key")

  write_generation_secret database-url "postgresql://transhooter:${database_password}@postgres:5432/transhooter"
  write_generation_secret redis-url "redis://:${redis_url_password}@redis:6379/0"
  write_generation_secret livekit-credentials "{\"apiKey\":\"${livekit_key_json}\",\"apiSecret\":\"${livekit_secret_json}\"}"
  write_generation_secret minio-credentials "{\"accessKeyId\":\"${minio_access_json}\",\"secretAccessKey\":\"${minio_secret_json}\"}"

  for secret_name in $managed_secrets; do
    [ -f "$temporary_generation/$secret_name" ] && [ -s "$temporary_generation/$secret_name" ] || {
      echo "Incomplete secret generation: $secret_name" >&2
      exit 1
    }
  done

  # Alpine's sync supports file arguments inconsistently across versions. A full
  # sync provides the durability fence before the generation becomes visible.
  sync
}

publish_public_links() {
  for secret_name in $managed_secrets; do
    secret_target="$secrets_directory/$secret_name"
    if [ -d "$secret_target" ]; then
      echo "Secret path is a directory: $secret_target" >&2
      exit 1
    fi

    temporary_link="$secrets_directory/.tmp-link.${secret_name}.$$"
    rm -f "$temporary_link"
    ln -s ".current/$secret_name" "$temporary_link"
    mv -fT "$temporary_link" "$secret_target"
    temporary_link=
  done
}

publish_generation() {
  generation_suffix=${temporary_generation##*.tmp-generation.}
  generation_name=".generation.$generation_suffix"
  generation_path="$secrets_directory/$generation_name"

  mv -T "$temporary_generation" "$generation_path"
  temporary_generation=

  publish_public_links
  sync

  temporary_link="$secrets_directory/.tmp-current.$$"
  rm -f "$temporary_link"
  ln -s "$generation_name" "$temporary_link"
  mv -fT "$temporary_link" "$secrets_directory/.current"
  temporary_link=
  sync
}

cleanup() {
  if [ -n "$temporary_link" ]; then
    rm -f "$temporary_link"
  fi
  if [ -n "$temporary_generation" ]; then
    rm -rf "$temporary_generation"
  fi
}

main() {
  trap cleanup 0
  trap 'exit 1' HUP INT TERM

  validate_existing_secrets
  build_generation
  publish_generation
}

main "$@"
