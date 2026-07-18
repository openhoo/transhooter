#!/bin/sh
set -eu
umask 027
required_secret_files="livekit-api-key livekit-api-secret redis-password"
temporary_file=

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

validate_inputs() {
  for secret_file in $required_secret_files; do
    if [ ! -s "/secrets/$secret_file" ]; then
      fail "missing required secret: $secret_file"
    fi
  done

  if [ ! -s /runtime/rtc-host ]; then
    fail "RTC host resolution missing"
  fi

  : "${RTC_UDP_PORT_START:?RTC_UDP_PORT_START is required}"
  : "${RTC_UDP_PORT_END:?RTC_UDP_PORT_END is required}"
}

load_inputs() {
  livekit_api_key=$(cat /secrets/livekit-api-key)
  livekit_api_secret=$(cat /secrets/livekit-api-secret)
  redis_password=$(cat /secrets/redis-password)
  rtc_host=$(cat /runtime/rtc-host)
  rtc_udp_port_start=$RTC_UDP_PORT_START
  rtc_udp_port_end=$RTC_UDP_PORT_END
}

escape_sed_replacement() {
  printf '%s' "$1" | sed 's/[\\&|]/\\&/g'
}

prepare_replacements() {
  escaped_livekit_api_key=$(escape_sed_replacement "$livekit_api_key")
  escaped_livekit_api_secret=$(escape_sed_replacement "$livekit_api_secret")
  escaped_redis_password=$(escape_sed_replacement "$redis_password")
  escaped_rtc_host=$(escape_sed_replacement "$rtc_host")
  escaped_rtc_udp_port_start=$(escape_sed_replacement "$rtc_udp_port_start")
  escaped_rtc_udp_port_end=$(escape_sed_replacement "$rtc_udp_port_end")
}

remove_temporary_file() {
  if [ -n "$temporary_file" ]; then
    rm -f -- "$temporary_file"
    temporary_file=
  fi
}

render_template() {
  template_file=$1
  output_file=$2
  temporary_file=$(mktemp "${output_file}.tmp.XXXXXX")

  sed \
    -e "s|__LIVEKIT_API_KEY__|$escaped_livekit_api_key|g" \
    -e "s|__LIVEKIT_API_SECRET__|$escaped_livekit_api_secret|g" \
    -e "s|__REDIS_PASSWORD__|$escaped_redis_password|g" \
    -e "s|__RTC_HOST__|$escaped_rtc_host|g" \
    -e "s|__RTC_UDP_PORT_START__|$escaped_rtc_udp_port_start|g" \
    -e "s|__RTC_UDP_PORT_END__|$escaped_rtc_udp_port_end|g" \
    "$template_file" > "$temporary_file"

  chmod 0440 "$temporary_file"
  mv -f "$temporary_file" "$output_file"
  temporary_file=
}

main() {
  trap remove_temporary_file 0
  trap 'remove_temporary_file; exit 1' HUP INT TERM
  validate_inputs
  load_inputs
  prepare_replacements
  render_template /templates/livekit.yaml /runtime/livekit.yaml
  render_template /templates/egress.yaml /runtime/egress.yaml
}

main "$@"
