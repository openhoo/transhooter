from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Literal
from uuid import UUID

from jsonschema import Draft202012Validator, FormatChecker  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, model_validator


class WireModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=lambda name: "".join(
            [name.split("_")[0], *(part.title() for part in name.split("_")[1:])]
        ),
        validate_by_alias=True,
        validate_by_name=True,
        extra="forbid",
    )


class CredentialReference(WireModel):
    reference: str = Field(min_length=1)
    version: str = Field(min_length=1)


class FrozenStage(WireModel):
    provider: str = Field(min_length=1)
    endpoint: str = Field(min_length=1)
    region: str = Field(min_length=1)
    model: str = Field(min_length=1)
    adapter_build: str = Field(min_length=1)
    policy: str = Field(min_length=1)
    credential: CredentialReference
    limits: dict[str, int]


class SttStage(FrozenStage):
    locale: str = Field(min_length=1)
    encoding: str = Field(min_length=1)


class TranslationStage(FrozenStage):
    source_code: str = Field(min_length=1)
    target_code: str = Field(min_length=1)


class TtsStage(FrozenStage):
    locale: str = Field(min_length=1)
    voice: str = Field(min_length=1)
    encoding: str = Field(min_length=1)
    sample_rate: int = Field(gt=0)


class DirectionMetadata(WireModel):
    mode: Literal["translated", "same_language"]
    source_participant_id: UUID
    destination_participant_id: UUID
    capability_row_id: UUID
    stt: SttStage
    bypass: Literal[True] | None = None
    target_code: str | None = None
    translation: TranslationStage | None = None
    tts: TtsStage | None = None

    @model_validator(mode="after")
    def validate_mode(self) -> DirectionMetadata:
        translated = self.mode == "translated"
        if translated != (
            self.translation is not None and self.tts is not None and self.target_code is not None
        ):
            raise ValueError("translated direction stages are incomplete")
        if translated == (self.bypass is True):
            raise ValueError("same-language bypass shape is inconsistent")
        return self

    @property
    def source_language(self) -> str:
        return self.stt.locale

    @property
    def target_language(self) -> str:
        return self.tts.locale if self.tts is not None else self.stt.locale

    @property
    def voice(self) -> str | None:
        return self.tts.voice if self.tts is not None else None


class RoomProviderSelectionMetadata(WireModel):
    profile_id: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
    profile_revision: int = Field(ge=1)
    capability_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    participant_ids: tuple[UUID, UUID]
    directions: tuple[DirectionMetadata, DirectionMetadata]


class JobMetadata(WireModel):
    schema_version: Literal[1]
    consultation_id: UUID
    generation: int = Field(ge=1)
    room_name: UUID
    worker_identity: UUID
    worker_epoch: int = Field(ge=1)
    write_epoch: int = Field(ge=0)
    expected_participant_ids: tuple[UUID, UUID]
    expected_livekit_identities: tuple[UUID, UUID]
    selection: RoomProviderSelectionMetadata = Field(alias="providerSelection")
    snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    adoption_id: UUID | None = None

    @model_validator(mode="after")
    def validate_bindings(self) -> JobMetadata:
        if self.expected_participant_ids != self.selection.participant_ids:
            raise ValueError("worker participant order must match provider selection")
        canonical = json.dumps(
            self.selection.model_dump(mode="json", by_alias=True, exclude_none=True),
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if hashlib.sha256(canonical).hexdigest() != self.snapshot_hash:
            raise ValueError("provider selection snapshot hash mismatch")
        return self


class CaptionPacket(BaseModel):
    schemaVersion: Literal[1]
    consultationId: UUID
    destinationParticipantId: UUID
    sourceParticipantId: UUID
    utteranceId: UUID
    revision: int = Field(ge=1)
    finality: Literal["provisional", "final"]
    sourceLanguage: str
    targetLanguage: str
    sourceText: str
    translatedText: str
    sourceSampleStart: int = Field(ge=0)
    sourceSampleEnd: int = Field(gt=0)
    occurredAtMs: int = Field(ge=0)


def _validated_job_metadata(payload: str) -> JobMetadata:
    path = Path(
        os.environ.get("CONTRACTS_SCHEMA_FILE", "/workspace/contracts/contracts.schema.json")
    )
    try:
        schema = json.loads(path.read_text("utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("generated contracts schema is required") from exc
    definitions = schema.get("schemas")
    if not isinstance(definitions, dict) or not isinstance(
        definitions.get("WorkerJobMetadata"), dict
    ):
        raise RuntimeError("generated WorkerJobMetadata schema is absent")
    candidate = json.loads(payload)
    Draft202012Validator(definitions["WorkerJobMetadata"], format_checker=FormatChecker()).validate(
        candidate
    )
    return JobMetadata.model_validate(candidate, by_alias=True, by_name=False)
