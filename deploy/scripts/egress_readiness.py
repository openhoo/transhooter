from __future__ import annotations

import asyncio
import os
import threading
import time
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from livekit import api


def wait_http(url: str) -> None:
    for attempt in range(60):
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status < 500:
                    return
        except Exception:
            if attempt == 59:
                raise
            time.sleep(1)


def value(path_variable: str) -> str:
    result = Path(os.environ[path_variable]).read_text(encoding="utf-8").strip()
    if not result:
        raise RuntimeError(f"empty secret file: {path_variable}")
    return result


def assert_deleted(s3: object, bucket: str, key: str, version_id: str) -> None:
    try:
        s3.head_object(Bucket=bucket, Key=key, VersionId=version_id)  # type: ignore[attr-defined]
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 404 and exc.response["Error"]["Code"] not in {
            "NoSuchKey",
            "NoSuchVersion",
            "404",
        }:
            raise
    else:
        raise RuntimeError("deleted readiness version is still readable")
    remaining = s3.list_object_versions(Bucket=bucket, Prefix=key)  # type: ignore[attr-defined]
    residue = [
        item
        for group in ("Versions", "DeleteMarkers")
        for item in remaining.get(group, [])
        if item.get("Key") == key
    ]
    if residue:
        raise RuntimeError("readiness cleanup left object versions or delete markers")


class ProbePage(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = b"<html><body style='background:#101B35;color:white'>egress readiness</body></html>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: object) -> None:
        return


async def prove_egress_control(s3: object, bucket: str) -> tuple[str, str]:
    output_key = f"bootstrap/egress-control-{uuid.uuid4()}.mp4"
    client = api.LiveKitAPI(
        os.environ["LIVEKIT_HTTP_URL"],
        value("LIVEKIT_API_KEY_FILE"),
        value("LIVEKIT_API_SECRET_FILE"),
    )
    try:
        info = await client.egress.start_web_egress(
            api.WebEgressRequest(
                url="http://egress-ready:8090/",
                file_outputs=[
                    api.EncodedFileOutput(
                        file_type=api.EncodedFileType.MP4,
                        filepath=output_key,
                        s3=api.S3Upload(
                            access_key=value("S3_ACCESS_KEY_FILE"),
                            secret=value("S3_SECRET_KEY_FILE"),
                            region=os.environ["S3_REGION"],
                            endpoint=os.environ["S3_ENDPOINT"],
                            bucket=bucket,
                            force_path_style=True,
                        ),
                    )
                ],
            )
        )
        active = False
        for _ in range(90):
            listed = await client.egress.list_egress(api.ListEgressRequest(egress_id=info.egress_id))
            current = listed.items[0] if listed.items else None
            if current and current.status == api.EgressStatus.EGRESS_ACTIVE:
                active = True
                break
            if current and current.status in {
                api.EgressStatus.EGRESS_FAILED,
                api.EgressStatus.EGRESS_ABORTED,
                api.EgressStatus.EGRESS_LIMIT_REACHED,
            }:
                raise RuntimeError(f"Egress control probe failed before active: {current.error}")
            await asyncio.sleep(1)
        if not active:
            raise RuntimeError("Egress control probe never became active")
        await asyncio.sleep(2)
        await client.egress.stop_egress(api.StopEgressRequest(egress_id=info.egress_id))
        terminal = None
        for _ in range(90):
            listed = await client.egress.list_egress(api.ListEgressRequest(egress_id=info.egress_id))
            terminal = listed.items[0] if listed.items else None
            if terminal and terminal.status in {
                api.EgressStatus.EGRESS_COMPLETE,
                api.EgressStatus.EGRESS_FAILED,
                api.EgressStatus.EGRESS_ABORTED,
                api.EgressStatus.EGRESS_LIMIT_REACHED,
            }:
                break
            await asyncio.sleep(1)
        if terminal is None or terminal.status != api.EgressStatus.EGRESS_COMPLETE:
            raise RuntimeError(f"Egress control probe did not complete: {terminal}")
    finally:
        await client.aclose()
    head = s3.head_object(Bucket=bucket, Key=output_key)  # type: ignore[attr-defined]
    if head["ContentLength"] <= 0 or not head.get("VersionId"):
        raise RuntimeError("Egress control output was not durably visible in S3")
    version_id = str(head["VersionId"])
    s3.delete_object(Bucket=bucket, Key=output_key, VersionId=version_id)  # type: ignore[attr-defined]
    assert_deleted(s3, bucket, output_key, version_id)
    return info.egress_id, version_id


async def main() -> None:
    wait_http("http://egress:8080/")
    wait_http("http://livekit:7880/")
    server = ThreadingHTTPServer(("0.0.0.0", 8090), ProbePage)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    bucket = os.environ["S3_BUCKET"]
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        region_name=os.environ["S3_REGION"],
        aws_access_key_id=value("S3_ACCESS_KEY_FILE"),
        aws_secret_access_key=value("S3_SECRET_KEY_FILE"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    key = f"bootstrap/egress-readiness-{uuid.uuid4()}"
    body = b"egress-readiness-v1"
    created = s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        IfNoneMatch="*",
        ChecksumAlgorithm="CRC64NVME",
    )
    head = s3.head_object(
        Bucket=bucket,
        Key=key,
        VersionId=created["VersionId"],
        ChecksumMode="ENABLED",
    )
    if head["ContentLength"] != len(body) or not head.get("ChecksumCRC64NVME"):
        raise RuntimeError("Egress S3 put/head checksum probe failed")
    s3.delete_object(Bucket=bucket, Key=key, VersionId=created["VersionId"])
    assert_deleted(s3, bucket, key, created["VersionId"])
    try:
        egress_id, _version_id = await prove_egress_control(s3, bucket)
    finally:
        server.shutdown()
        thread.join(timeout=5)
    print(f"Egress control job {egress_id} and archive put/head/delete are ready")


if __name__ == "__main__":
    asyncio.run(main())
