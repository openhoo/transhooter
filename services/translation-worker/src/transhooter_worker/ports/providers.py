from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol
from uuid import UUID

from transhooter_worker.domain.models import (
    AudioChunk,
    AudioEvent,
    BoundaryEvent,
    BoundaryReceipt,
    OperationTerminal,
    OperationTerminalEvent,
    ProviderHealth,
    SessionTerminal,
    SessionTerminalEvent,
    StageCapabilities,
    SynthesisBoundary,
    SynthesisUtterance,
    TranscriptEvent,
    TranslationOutcome,
    TranslationRequest,
)

SttEvent = TranscriptEvent | BoundaryEvent | SessionTerminalEvent
TtsEvent = AudioEvent | SynthesisBoundary | OperationTerminalEvent


class SttSession(Protocol):
    async def send_audio(self, chunk: AudioChunk) -> None: ...
    def events(self) -> AsyncIterator[SttEvent]: ...
    async def request_boundary(self, boundary_id: UUID) -> BoundaryReceipt: ...
    async def finish(self) -> SessionTerminal: ...
    async def cancel(self) -> SessionTerminal: ...


class StreamingSttProvider(Protocol):
    async def capabilities(self) -> StageCapabilities: ...
    async def health(self, snapshot: str) -> ProviderHealth: ...
    async def open(
        self,
        session_id: UUID,
        language: str,
        *,
        resume_at_sample: int = 0,
        commit_watermark: int = 0,
    ) -> SttSession: ...


class TranslationAttempt(Protocol):
    async def result(self) -> TranslationOutcome: ...
    async def cancel(self) -> OperationTerminal: ...


class TextTranslationProvider(Protocol):
    async def capabilities(self) -> StageCapabilities: ...
    async def health(self, snapshot: str) -> ProviderHealth: ...
    async def start(self, request: TranslationRequest) -> TranslationAttempt: ...


class SynthesisAttempt(Protocol):
    def events(self) -> AsyncIterator[TtsEvent]: ...
    async def finish(self) -> OperationTerminal: ...
    async def cancel(self) -> OperationTerminal: ...


class TtsSession(Protocol):
    async def start(self, utterance: SynthesisUtterance) -> SynthesisAttempt: ...
    def session_events(self) -> AsyncIterator[SessionTerminalEvent]: ...
    async def finish(self) -> SessionTerminal: ...
    async def cancel(self) -> SessionTerminal: ...


class StreamingTtsProvider(Protocol):
    async def capabilities(self) -> StageCapabilities: ...
    async def health(self, snapshot: str) -> ProviderHealth: ...
    async def open(self, session_id: UUID, language: str, voice: str) -> TtsSession: ...
