#!/bin/sh
set -eu

read_secret() {
  value=$(cat "$1")
  [ -n "$value" ] || { echo "empty secret file: $1" >&2; exit 1; }
  printf '%s' "$value"
}

endpoint=${S3_ENDPOINT:?S3_ENDPOINT is required}
root_access=$(read_secret /run/secrets/minio-root-access-key)
root_secret=$(read_secret /run/secrets/minio-root-secret-key)

mc alias set bootstrap "$endpoint" "$root_access" "$root_secret"

write_policy() {
  service=$1
  actions=$2
  resource=${3:-both}
  case "$resource" in
    both) resources="[\"arn:aws:s3:::${S3_BUCKET}\",\"arn:aws:s3:::${S3_BUCKET}/*\"]" ;;
    objects) resources="[\"arn:aws:s3:::${S3_BUCKET}/*\"]" ;;
    *) echo "unsupported policy resource scope: $resource" >&2; exit 1 ;;
  esac
  cat > "/tmp/$service-policy.json" <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":$actions,"Resource":$resources}]}
EOF
}

install_identity() {
  service=$1
  policy=$2
  access=$(read_secret "/run/secrets/minio-$service-access-key")
  secret=$(read_secret "/run/secrets/minio-$service-secret-key")
  mc admin policy create bootstrap "transhooter-$service" "$policy"
  mc admin user add bootstrap "$access" "$secret"
  mc admin policy attach bootstrap "transhooter-$service" --user "$access"
}

runtime_actions='["s3:AbortMultipartUpload","s3:DeleteObject","s3:DeleteObjectVersion","s3:GetBucketLocation","s3:GetObject","s3:GetObjectAttributes","s3:GetObjectLegalHold","s3:GetObjectRetention","s3:GetObjectVersion","s3:ListBucket","s3:ListBucketMultipartUploads","s3:ListBucketVersions","s3:ListMultipartUploadParts","s3:PutObject","s3:PutObjectLegalHold","s3:PutObjectRetention"]'
archive_writer_actions='["s3:PutObject","s3:AbortMultipartUpload","s3:ListMultipartUploadParts","s3:GetObject","s3:GetObjectVersion","s3:GetObjectAttributes"]'
write_policy web "$runtime_actions"
write_policy control "$runtime_actions"
write_policy provider-diagnostics "$archive_writer_actions" objects
write_policy spool-drainer "$archive_writer_actions" objects

install_identity web /tmp/web-policy.json
install_identity control /tmp/control-policy.json
install_identity provider-diagnostics /tmp/provider-diagnostics-policy.json
install_identity spool-drainer /tmp/spool-drainer-policy.json
