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
    INVALID_RESPONSE = "invalid_response"
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
    STOP = "do_not_retry"


class Outcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


_RETRYABLE_ERROR_KINDS = frozenset(
    {
        ErrorKind.QUOTA,
        ErrorKind.RATE_LIMIT,
        ErrorKind.TRANSPORT,
        ErrorKind.PROVIDER,
        ErrorKind.INVALID_RESPONSE,
        ErrorKind.INTERNAL,
    }
)


def _validate_terminal_error(
    outcome: Outcome, error: ProviderError | None, attempt_id: UUID
) -> None:
    if outcome is Outcome.SUCCEEDED and error is not None:
        raise ValueError("successful terminals cannot carry an error")
    if outcome is Outcome.FAILED and (error is None or error.kind is ErrorKind.CANCELLED):
        raise ValueError("failed terminals require a non-cancellation error")
    if outcome is Outcome.CANCELLED and error is not None and error.kind is not ErrorKind.CANCELLED:
        raise ValueError("cancelled terminals can carry only a cancellation error")
    if error is not None and error.attempt_id != attempt_id:
        raise ValueError("error attempt_id must match the terminal attempt")


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

    def __post_init__(self) -> None:
        retries = self.action is RetryAction.RETRY
        if retries != (self.delay_ms is not None):
            raise ValueError("only retries have a delay")
        if retries and self.previous_attempt_id is None:
            raise ValueError("retry decisions require the terminal attempt link")


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

    def __post_init__(self) -> None:
        _validate_terminal_error(self.outcome, self.error, self.attempt_id)
        if self.outcome is not Outcome.FAILED and self.retry.action is not RetryAction.STOP:
            raise ValueError("successful and cancelled terminals cannot carry retry advice")
        if self.outcome is not Outcome.FAILED and self.retry.previous_attempt_id is not None:
            raise ValueError("successful and cancelled terminals cannot link a retry decision")
        if self.retry.previous_attempt_id not in {None, self.attempt_id}:
            raise ValueError("retry decision must link to the terminal attempt")
        if self.retry.action is RetryAction.RETRY and (
            self.error is None
            or self.error.provider_retry_advice is RetryAdvice.NEVER
            or self.error.kind not in _RETRYABLE_ERROR_KINDS
        ):
            raise ValueError("retries require a retryable failed provider error")


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

    def __post_init__(self) -> None:
        _validate_terminal_error(self.outcome, self.error, self.session_id)


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
