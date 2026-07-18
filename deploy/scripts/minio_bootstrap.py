from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError


def secret(name: str) -> str:
    value = Path(os.environ[name]).read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"empty secret file: {name}")
    return value


endpoint = os.environ["S3_ENDPOINT"]
region = os.environ["S3_REGION"]
bucket = os.environ["S3_BUCKET"]
s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    region_name=region,
    aws_access_key_id=secret("S3_ACCESS_KEY_FILE"),
    aws_secret_access_key=secret("S3_SECRET_KEY_FILE"),
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

for attempt in range(60):
    try:
        s3.list_buckets()
        break
    except Exception:
        if attempt == 59:
            raise
        time.sleep(1)

try:
    s3.create_bucket(Bucket=bucket, ObjectLockEnabledForBucket=True)
except ClientError as exc:
    if exc.response["Error"]["Code"] not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
        raise
ownership_mode = "BucketOwnerEnforced"
try:
    s3.put_bucket_ownership_controls(
        Bucket=bucket,
        OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
    )
except ClientError as exc:
    error_code = exc.response["Error"]["Code"]
    server = exc.response.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("server", "")
    if error_code not in {"MalformedXML", "NotImplemented", "XNotImplemented"} or not server.lower().startswith(
        "minio"
    ):
        raise
    ownership_mode = "MinIOImplicitBucketOwner"

s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
s3.put_object_lock_configuration(
    Bucket=bucket,
    ObjectLockConfiguration={"ObjectLockEnabled": "Enabled"},
)
try:
    s3.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
except ClientError as exc:
    if exc.response["Error"]["Code"] not in {"MalformedXML", "NotImplemented", "XNotImplemented"}:
        raise

anonymous_s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    region_name=region,
    config=Config(signature_version=UNSIGNED, s3={"addressing_style": "path"}),
)
try:
    anonymous_s3.list_objects_v2(Bucket=bucket, MaxKeys=1)
except ClientError as exc:
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status not in {401, 403}:
        raise
else:
    raise RuntimeError("anonymous bucket access is not blocked")
lifecycle_supported = True
try:
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "abort-incomplete-multipart-after-seven-days",
                    "Status": "Enabled",
                    "Prefix": "",
                    "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
                }
            ]
        },
    )
except ClientError as exc:
    if exc.response["Error"]["Code"] != "InvalidArgument":
        raise
    lifecycle_supported = False
if not lifecycle_supported and os.environ.get("MINIO_STALE_UPLOADS_EXPIRY") != "168h":
    raise RuntimeError("seven-day incomplete multipart cleanup is not configured")

if ownership_mode == "BucketOwnerEnforced":
    ownership = s3.get_bucket_ownership_controls(Bucket=bucket)
    ownership_rules = ownership.get("OwnershipControls", {}).get("Rules", [])
    if ownership_rules != [{"ObjectOwnership": "BucketOwnerEnforced"}]:
        raise RuntimeError("bucket ownership controls are not BucketOwnerEnforced")
else:
    acl = s3.get_bucket_acl(Bucket=bucket)
    grants = acl.get("Grants", [])
    if not grants or any(grant.get("Permission") != "FULL_CONTROL" for grant in grants):
        raise RuntimeError("MinIO bucket does not expose owner-only full-control semantics")

versioning = s3.get_bucket_versioning(Bucket=bucket)
lock = s3.get_object_lock_configuration(Bucket=bucket)
if versioning.get("Status") != "Enabled" or lock["ObjectLockConfiguration"].get("ObjectLockEnabled") != "Enabled":
    raise RuntimeError("bucket versioning or Object Lock is not enabled")

key = f"bootstrap/probe-{uuid.uuid4()}"
payload = b"transhooter-s3-semantic-probe-v1"
first = s3.put_object(
    Bucket=bucket,
    Key=key,
    Body=payload,
    IfNoneMatch="*",
    ChecksumAlgorithm="CRC64NVME",
    ContentType="application/octet-stream",
)
try:
    s3.put_object(Bucket=bucket, Key=key, Body=b"duplicate", IfNoneMatch="*")
except ClientError as exc:
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status != 412 and exc.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
        raise
else:
    raise RuntimeError("S3 conditional create probe unexpectedly overwrote an object")

attrs = s3.get_object_attributes(
    Bucket=bucket,
    Key=key,
    VersionId=first["VersionId"],
    ObjectAttributes=["Checksum", "ObjectSize"],
)
if attrs.get("ObjectSize") != len(payload) or not attrs.get("Checksum", {}).get("ChecksumCRC64NVME"):
    raise RuntimeError("S3 CRC64NVME checksum/size verification failed")
s3.delete_object(Bucket=bucket, Key=key, VersionId=first["VersionId"])
try:
    s3.head_object(Bucket=bucket, Key=key, VersionId=first["VersionId"])
except ClientError as exc:
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status != 404 and exc.response["Error"]["Code"] not in {"NoSuchKey", "NoSuchVersion", "404"}:
        raise
else:
    raise RuntimeError("deleted S3 probe version is still readable")
remaining = s3.list_object_versions(Bucket=bucket, Prefix=key)
residue = [
    item
    for group in ("Versions", "DeleteMarkers")
    for item in remaining.get(group, [])
    if item.get("Key") == key
]
if residue:
    raise RuntimeError("S3 probe cleanup left object versions or delete markers")
print(
    json.dumps(
        {
            "bucket": bucket,
            "objectLock": "Enabled",
            "versioning": "Enabled",
            "ownership": ownership_mode,
            "crc64nvme": True,
            "abortIncompleteMultipart": "lifecycle-rule"
            if lifecycle_supported
            else "minio-server-168h",
        }
    )
)
