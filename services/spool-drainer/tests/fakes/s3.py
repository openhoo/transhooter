from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError


class RecordingS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, dict[str, str], str, str]] = {}
        self.parts: dict[str, dict[int, tuple[bytes, str, str]]] = {}
        self.aborted: list[str] = []
        self.multipart: dict[str, Any] = {}
        self.completion_error: ClientError | None = None
        self.active_uploads: dict[str, str] = {}
        self.next_upload = 1
        self.completion_calls = 0
        self.upload_calls: list[tuple[str, int]] = []
        self.upload_finish_order: list[int] = []
        self.completion_part_numbers: list[int] = []
        self.head_calls = 0

    def put_object(self, **kwargs: Any) -> dict[str, str]:
        self.objects[(kwargs["Key"], "v1")] = (
            bytes(kwargs["Body"]),
            kwargs["Metadata"],
            "crc",
            kwargs["ContentType"],
        )
        return {"VersionId": "v1", "ChecksumCRC64NVME": "crc"}

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        self.head_calls += 1
        if "VersionId" in kwargs:
            version = kwargs["VersionId"]
        else:
            version = "v2" if (kwargs["Key"], "v2") in self.objects else "v1"
        body, metadata, checksum, content_type = self.objects[(kwargs["Key"], version)]
        return {
            "ContentLength": len(body),
            "Metadata": metadata,
            "ChecksumCRC64NVME": checksum,
            "ContentType": content_type,
            "VersionId": version,
        }

    def create_multipart_upload(self, **kwargs: Any) -> dict[str, str]:
        self.multipart = kwargs
        self.parts[kwargs["Key"]] = {}
        upload_id = f"upload-{self.next_upload}"
        self.next_upload += 1
        self.active_uploads[upload_id] = kwargs["Key"]
        return {"UploadId": upload_id}

    def list_multipart_uploads(self, **_: Any) -> dict[str, Any]:
        return {
            "Uploads": [
                {"Key": key, "UploadId": upload_id}
                for upload_id, key in self.active_uploads.items()
            ]
        }

    def list_parts(self, **kwargs: Any) -> dict[str, list[dict[str, Any]]]:
        return {
            "Parts": [
                {
                    "PartNumber": number,
                    "ETag": part[1],
                    "ChecksumCRC64NVME": part[2],
                    "Size": len(part[0]),
                }
                for number, part in self.parts.get(kwargs["Key"], {}).items()
            ]
        }

    def upload_part(self, **kwargs: Any) -> dict[str, str]:
        part_number = kwargs["PartNumber"]
        self.upload_calls.append((str(kwargs["Key"]), int(part_number)))
        etag = f"etag-{part_number}"
        self.upload_finish_order.append(int(part_number))
        checksum = str(kwargs["ChecksumCRC64NVME"])
        self.parts[kwargs["Key"]][part_number] = (
            bytes(kwargs["Body"]),
            etag,
            checksum,
        )
        return {"ETag": etag, "ChecksumCRC64NVME": checksum}

    def complete_multipart_upload(self, **kwargs: Any) -> dict[str, str]:
        self.completion_calls += 1
        self.completion_part_numbers = [
            int(part["PartNumber"]) for part in kwargs["MultipartUpload"]["Parts"]
        ]
        key = kwargs["Key"]
        uploaded_parts = self.parts[key]
        body = b"".join(uploaded_parts[number][0] for number in sorted(uploaded_parts))
        self.objects[(key, "v2")] = (
            body,
            self.multipart["Metadata"],
            "crc-multi",
            self.multipart["ContentType"],
        )
        self.active_uploads.pop(str(kwargs["UploadId"]), None)
        if self.completion_error is not None:
            raise self.completion_error
        return {"VersionId": "v2", "ChecksumCRC64NVME": "crc-multi"}

    def abort_multipart_upload(self, **kwargs: Any) -> None:
        upload_id = str(kwargs["UploadId"])
        self.aborted.append(upload_id)
        self.active_uploads.pop(upload_id, None)

