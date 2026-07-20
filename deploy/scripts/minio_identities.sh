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
  cat > "/tmp/$service-policy.json" <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":$actions,"Resource":["arn:aws:s3:::${S3_BUCKET}","arn:aws:s3:::${S3_BUCKET}/*"]}]}
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
write_policy web "$runtime_actions"
write_policy control "$runtime_actions"
write_policy translation "$runtime_actions"
write_policy spool-drainer "$runtime_actions"

install_identity web /tmp/web-policy.json
install_identity control /tmp/control-policy.json
install_identity translation /tmp/translation-policy.json
install_identity spool-drainer /tmp/spool-drainer-policy.json
