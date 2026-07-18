from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID


class Finality(StrEnum):
    PROVISIONAL = "provisional"
    SPAN_FINAL = "span_final"


class Lifecycle(StrEnum):
    OPEN = "open"
    DRAINING = "draining"
    TERMINAL = "terminal"


class ErrorKind(StrEnum):
    AUTHENTICATION = "authentication"
    QUOTA = "quota"
    RATE_LIMIT = "rate_limit"
    TRANSPORT = "transport"
    INVALID_REQUEST = "invalid_request"
    PROVIDER = "provider"
    CANCELLED = "cancelled"
    INTERNAL = "internal"


class RetryAdvice(StrEnum):
    NEVER = "never"
    RETRY_AFTER = "retry_after"
    UNSPECIFIED = "unspecified"


class RetryAction(StrEnum):
    RETRY = "retry"
    DEGRADE = "degrade"
    STOP = "stop"


class Outcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Transport(StrEnum):
    HTTP = "http"
    WEBSOCKET = "websocket"
    GRPC = "grpc"


@dataclass(frozen=True, slots=True)
class SampleRange:
    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start < 0 or self.end <= self.start:
            raise ValueError("sample range must be non-empty and inclusive-exclusive")

    @property
    def length(self) -> int:
        return self.end - self.start


@dataclass(frozen=True, slots=True)
class RawRef:
    object_id: UUID
    ordinal: int
    sha256: str
    size: int
    media_type: str


@dataclass(frozen=True, slots=True)
class AudioChunk:
    operation_id: UUID
    sequence: int
    samples: SampleRange
    pcm: bytes
    sample_rate: int = 16_000
    channels: int = 1
    encoding: str = "LINEAR16"


@dataclass(frozen=True, slots=True)
class WordTiming:
    text: str
    samples: SampleRange
    confidence: float | None


@dataclass(frozen=True, slots=True)
class TranscriptEvent:
    samples: SampleRange
    revision: int
    finality: Finality
    text: str
    words: tuple[WordTiming, ...]
    confidence: float | None
    raw_ref: RawRef


@dataclass(frozen=True, slots=True)
class BoundaryEvent:
    boundary_id: UUID
    committed_through: int
    raw_ref: RawRef


@dataclass(frozen=True, slots=True)
class TranslationRequest:
    operation_id: UUID
    attempt_id: UUID
    purpose: str
    source_language: str
    target_language: str
    text: str
    source_range: SampleRange


@dataclass(frozen=True, slots=True)
class TranslationResult:
    operation_id: UUID
    attempt_id: UUID
    text: str
    source_range: SampleRange
    raw_ref: RawRef


@dataclass(frozen=True, slots=True)
class SynthesisUtterance:
    operation_id: UUID
    attempt_id: UUID
    text: str
    language: str
    voice: str
    source_range: SampleRange


@dataclass(frozen=True, slots=True)
class AudioEvent:
    operation_id: UUID
    sequence: int
    samples: SampleRange
    pcm: bytes
    sample_rate: int
    channels: int
    raw_ref: RawRef


@dataclass(frozen=True, slots=True)
class SynthesisBoundary:
    operation_id: UUID
    samples: SampleRange
    raw_ref: RawRef


@dataclass(frozen=True, slots=True)
class StageCapabilities:
    provider: str
    stage: str
    endpoint: str
    regions: tuple[str, ...]
    languages: tuple[str, ...]
    models: tuple[str, ...]
    limits: tuple[tuple[str, int], ...]
    evidence: RawRef | None
    voices: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ProviderHealth:
    healthy: bool
    checked_at_ms: int
    reason: str | None
    evidence: RawRef | None


@dataclass(frozen=True, slots=True)
class ProviderError:
    kind: ErrorKind
    scope: str
    provider_retry_advice: RetryAdvice
    provider_code: str | None
    provider_request_id: str | None
    retry_delay_ms: int | None
    attempt_id: UUID
    raw_refs: tuple[RawRef, ...]
    message: str


@dataclass(frozen=True, slots=True)
class RetryDecision:
    action: RetryAction
    delay_ms: int | None
    reason: str
    previous_attempt_id: UUID | None


@dataclass(frozen=True, slots=True)
class OperationTerminal:
    terminal_id: UUID
    operation_id: UUID
    attempt_id: UUID
    outcome: Outcome
    error: ProviderError | None
    retry: RetryDecision
    accepted_input: int
    received_output: int
    emitted_output: int
    transport: Transport
    raw_refs: tuple[RawRef, ...]
    credential_fingerprint: str


@dataclass(frozen=True, slots=True)
class SessionTerminal:
    terminal_id: UUID
    session_id: UUID
    outcome: Outcome
    error: ProviderError | None
    accepted_input: int
    received_output: int
    emitted_output: int
    transport: Transport
    raw_refs: tuple[RawRef, ...]


@dataclass(frozen=True, slots=True)
class BoundaryReceipt:
    accepted: bool
    boundary_id: UUID


@dataclass(frozen=True, slots=True)
class TranslationOutcome:
    result: TranslationResult | None
    terminal: OperationTerminal


@dataclass(frozen=True, slots=True)
class OperationTerminalEvent:
    terminal: OperationTerminal


@dataclass(frozen=True, slots=True)
class SessionTerminalEvent:
    terminal: SessionTerminal
