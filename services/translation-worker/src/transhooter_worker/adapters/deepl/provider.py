from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import httpx

from transhooter_worker.adapters.terminal import terminal_bytes
from transhooter_worker.domain.models import (
    ErrorKind,
    OperationTerminal,
    Outcome,
    ProviderError,
    ProviderHealth,
    RawRef,
    RetryAction,
    RetryAdvice,
    RetryDecision,
    SampleRange,
    StageCapabilities,
    TranslationOutcome,
    TranslationRequest,
    TranslationResult,
    Transport,
)
from transhooter_worker.ports.exchange_journal import ExchangeJournal


@dataclass(frozen=True, slots=True)
class DeepLConfig:
    api_key: str
    meeting_id: UUID
    credential_fingerprint: str = "deepl-key-v1"
    endpoint: str = "https://api.deepl.com"
    model_type: str = "latency_optimized"
    requests_minute: int = 1
    characters_minute: int = 1

    def __post_init__(self) -> None:
        if not self.api_key.strip():
            raise ValueError("DeepL API key is required")
        if self.endpoint != "https://api.deepl.com":
            raise ValueError("DeepL EU endpoint must be https://api.deepl.com")
        if self.model_type not in {"latency_optimized", "quality_optimized"}:
            raise ValueError("unsupported DeepL model_type")
        if self.requests_minute <= 0 or self.characters_minute <= 0:
            raise ValueError("DeepL effective quota limits are required")


def _redacted_headers(request: httpx.Request) -> tuple[tuple[str, str], ...]:
    return tuple(
        (
            name,
            "[REDACTED:deepl-api-key]" if name.lower() == "authorization" else value,
        )
        for name, value in request.headers.multi_items()
    )


def _record_http_request(
    config: DeepLConfig,
    journal: ExchangeJournal,
    attempt_id: UUID,
    stage: str,
    request: httpx.Request,
    *,
    sample_range: SampleRange | None = None,
) -> RawRef:
    return journal.append(
        meeting_id=config.meeting_id,
        attempt_id=attempt_id,
        stage=stage,
        transport="http",
        direction="out",
        media_type=(
            "application/json"
            if request.headers.get("Content-Type") == "application/json"
            else "application/octet-stream"
        ),
        payload=request.content,
        sample_range=sample_range,
        metadata=(
            (":method", request.method),
            (":url", str(request.url)),
            *_redacted_headers(request),
        ),
    )


def _record_http_response(
    config: DeepLConfig,
    journal: ExchangeJournal,
    attempt_id: UUID,
    stage: str,
    response: httpx.Response,
    *,
    sample_range: SampleRange | None = None,
) -> RawRef:
    return journal.append(
        meeting_id=config.meeting_id,
        attempt_id=attempt_id,
        stage=stage,
        transport="http",
        direction="in",
        media_type="application/json",
        payload=response.content,
        sample_range=sample_range,
        metadata=(
            (":status", str(response.status_code)),
            *tuple(response.headers.multi_items()),
        ),
    )


def _terminal_evidence(
    journal: ExchangeJournal,
    attempt_id: UUID,
    outcome: str,
    references: list[RawRef],
) -> None:
    journal.terminal(
        attempt_id,
        json.dumps(
            {
                "outcome": outcome,
                "transport": "http",
                "rawRefs": [str(reference.object_id) for reference in references],
            },
            separators=(",", ":"),
        ).encode(),
    )


def _parse_capability_languages(payload: bytes) -> tuple[str, ...]:
    data: Any = json.loads(payload)
    if not isinstance(data, list) or not data:
        raise RuntimeError("DeepL returned an incomplete language capability set")

    languages: set[str] = set()
    for row in data:
        if not isinstance(row, dict):
            raise RuntimeError("DeepL returned an incomplete language capability row")
        language = row.get("lang")
        if not isinstance(language, str) or not language.strip():
            raise RuntimeError("DeepL returned an incomplete language capability row")
        languages.add(language)
    return tuple(sorted(languages))


def _translation_request_body(
    request: TranslationRequest,
    model_type: str,
) -> bytes:
    body = json.dumps(
        {
            "text": [request.text],
            "source_lang": request.source_language,
            "target_lang": request.target_language,
            "model_type": model_type,
            "show_billed_characters": True,
        },
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    if len(body) >= 128 * 1024:
        raise ValueError("DeepL request must be below 128 KiB")
    return body


def _parse_translation_text(payload: bytes) -> str:
    parsed: Any = json.loads(payload)
    if not isinstance(parsed, dict):
        raise ValueError("DeepL response cardinality must be exactly one")
    translations = parsed.get("translations")
    if not isinstance(translations, list) or len(translations) != 1:
        raise ValueError("DeepL response cardinality must be exactly one")
    translation = translations[0]
    if not isinstance(translation, dict):
        raise ValueError("DeepL response cardinality must be exactly one")
    translated_text = translation.get("text")
    if not isinstance(translated_text, str):
        raise ValueError("DeepL response cardinality must be exactly one")
    return translated_text


class DeepLProvider:
    def __init__(
        self, config: DeepLConfig, journal: ExchangeJournal, client: httpx.AsyncClient | None = None
    ) -> None:
        self._config = config
        self._journal = journal
        self._client = client or httpx.AsyncClient(timeout=20, follow_redirects=False)

    async def capabilities(self) -> StageCapabilities:
        attempt_id = uuid4()
        request = self._client.build_request(
            "GET",
            self._config.endpoint + "/v3/languages?resource=translate_text",
            headers={"Authorization": f"DeepL-Auth-Key {self._config.api_key}"},
        )
        references = [
            _record_http_request(
                self._config,
                self._journal,
                attempt_id,
                "capabilities",
                request,
            )
        ]
        try:
            response = await self._client.send(request)
            evidence = _record_http_response(
                self._config,
                self._journal,
                attempt_id,
                "capabilities",
                response,
            )
            references.append(evidence)
            response.raise_for_status()
            languages = _parse_capability_languages(response.content)
            _terminal_evidence(
                self._journal,
                attempt_id,
                "succeeded",
                references,
            )
            return StageCapabilities(
                "deepl",
                "translation",
                self._config.endpoint,
                ("eu",),
                languages,
                (self._config.model_type,),
                (
                    ("request_bytes", 131072),
                    ("requests_minute", self._config.requests_minute),
                    ("characters_minute", self._config.characters_minute),
                ),
                evidence,
            )
        except BaseException as error:
            failure = self._journal.append(
                meeting_id=self._config.meeting_id,
                attempt_id=attempt_id,
                stage="capabilities",
                transport="http",
                direction="status-in",
                media_type="application/json",
                payload=json.dumps(
                    {
                        "outcome": "failed",
                        "errorType": type(error).__name__,
                    },
                    separators=(",", ":"),
                ).encode(),
            )
            references.append(failure)
            _terminal_evidence(
                self._journal,
                attempt_id,
                "failed",
                references,
            )
            raise

    async def health(self, snapshot: str) -> ProviderHealth:
        try:
            capability = await self.capabilities()
            return ProviderHealth(
                bool(capability.languages), int(time.time() * 1000), None, capability.evidence
            )
        except Exception as exc:
            return ProviderHealth(False, int(time.time() * 1000), type(exc).__name__, None)

    async def start(self, request: TranslationRequest) -> DeepLAttempt:
        body = _translation_request_body(request, self._config.model_type)
        prepared_request = self._client.build_request(
            "POST",
            self._config.endpoint + "/v2/translate",
            content=body,
            headers={
                "Authorization": f"DeepL-Auth-Key {self._config.api_key}",
                "Content-Type": "application/json",
            },
        )
        outbound = _record_http_request(
            self._config,
            self._journal,
            request.attempt_id,
            "translation",
            prepared_request,
            sample_range=request.source_range,
        )
        return DeepLAttempt(
            self._config,
            self._journal,
            self._client,
            request,
            prepared_request,
            outbound,
        )


class DeepLAttempt:
    def __init__(
        self,
        config: DeepLConfig,
        journal: ExchangeJournal,
        client: httpx.AsyncClient,
        request: TranslationRequest,
        prepared: httpx.Request,
        outbound: RawRef,
    ) -> None:
        self._config = config
        self._journal = journal
        self._client = client
        self._request = request
        self._prepared = prepared
        self._outbound = outbound
        self._task: asyncio.Task[TranslationOutcome] | None = None
        self._terminal: OperationTerminal | None = None
        self._lock = asyncio.Lock()
        self._cancel_lock = asyncio.Lock()
        self._cancel_task: asyncio.Task[OperationTerminal] | None = None
        self._inbound: RawRef | None = None
        self._cancelling = False
        self._cancelled_terminal_ready = asyncio.Event()
        self._credential = config.credential_fingerprint

    async def result(self) -> TranslationOutcome:
        async with self._lock:
            if self._task is not None:
                task = self._task
            elif self._terminal:
                return TranslationOutcome(None, self._terminal)
            elif self._cancelling:
                task = None
            else:
                self._task = asyncio.create_task(self._execute())
                task = self._task
        if task is None:
            await self._cancelled_terminal_ready.wait()
            assert self._terminal is not None
            return TranslationOutcome(None, self._terminal)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            async with self._lock:
                cancelling = self._cancelling
                terminal = self._terminal
            if terminal is not None:
                return TranslationOutcome(None, terminal)
            if cancelling:
                await self._cancelled_terminal_ready.wait()
                assert self._terminal is not None
                return TranslationOutcome(None, self._terminal)
            raise

    async def cancel(self) -> OperationTerminal:
        async with self._cancel_lock:
            async with self._lock:
                if self._terminal is not None:
                    return self._terminal
                if self._cancel_task is None:
                    self._cancelling = True
                    task = self._task
                    if task is not None:
                        task.cancel()
                    self._cancel_task = asyncio.create_task(self._finish_cancellation(task))
                cancellation = self._cancel_task
        assert cancellation is not None
        return await asyncio.shield(cancellation)

    async def _finish_cancellation(
        self, task: asyncio.Task[TranslationOutcome] | None
    ) -> OperationTerminal:
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        references = (
            (self._outbound, self._inbound) if self._inbound is not None else (self._outbound,)
        )
        terminal = await self._accept_terminal(
            Outcome.CANCELLED,
            None,
            references,
            1 if self._inbound is not None else 0,
        )
        self._cancelled_terminal_ready.set()
        return terminal

    async def _execute(self) -> TranslationOutcome:
        inbound: RawRef | None = None
        try:
            response = await self._client.send(self._prepared)
            inbound = _record_http_response(
                self._config,
                self._journal,
                self._request.attempt_id,
                "translation",
                response,
                sample_range=self._request.source_range,
            )
            self._inbound = inbound
            references: tuple[RawRef, ...] = (self._outbound, inbound)
            if response.status_code != 200:
                error = self._http_error(response, references)
                terminal = await self._accept_terminal(
                    Outcome.FAILED,
                    error,
                    references,
                    0,
                )
                return TranslationOutcome(None, terminal)

            translated_text = _parse_translation_text(response.content)
            result = TranslationResult(
                self._request.operation_id,
                self._request.attempt_id,
                translated_text,
                self._request.source_range,
                inbound,
            )
            terminal = await self._accept_terminal(
                Outcome.SUCCEEDED,
                None,
                references,
                1,
            )
            accepted_result = result if terminal.outcome is Outcome.SUCCEEDED else None
            return TranslationOutcome(accepted_result, terminal)
        except (httpx.TransportError, json.JSONDecodeError, ValueError) as error:
            references = (self._outbound, inbound) if inbound is not None else (self._outbound,)
            error_kind = (
                ErrorKind.TRANSPORT
                if isinstance(error, httpx.TransportError)
                else ErrorKind.PROVIDER
            )
            provider_error = ProviderError(
                error_kind,
                "operation",
                RetryAdvice.UNSPECIFIED,
                type(error).__name__,
                None,
                None,
                self._request.attempt_id,
                references,
                str(error),
            )
            terminal = await self._accept_terminal(
                Outcome.FAILED,
                provider_error,
                references,
                1 if inbound is not None else 0,
            )
            return TranslationOutcome(None, terminal)

    def _http_error(
        self,
        response: httpx.Response,
        references: tuple[RawRef, ...],
    ) -> ProviderError:
        status = response.status_code
        if status == 456:
            error_kind = ErrorKind.QUOTA
        elif status in {429, 529}:
            error_kind = ErrorKind.RATE_LIMIT
        elif status in {500, 504}:
            error_kind = ErrorKind.TRANSPORT
        else:
            error_kind = ErrorKind.PROVIDER

        if error_kind in {ErrorKind.RATE_LIMIT, ErrorKind.TRANSPORT}:
            retry_advice = RetryAdvice.RETRY_AFTER
        else:
            retry_advice = RetryAdvice.NEVER

        retry_after = response.headers.get("Retry-After", "")
        retry_delay_ms = None
        if retry_after.replace(".", "", 1).isdigit():
            retry_delay_ms = int(float(retry_after) * 1000)

        return ProviderError(
            error_kind,
            "operation",
            retry_advice,
            str(status),
            response.headers.get("X-Trace-ID"),
            retry_delay_ms,
            self._request.attempt_id,
            references,
            f"DeepL HTTP {status}",
        )

    async def _accept_terminal(
        self,
        outcome: Outcome,
        error: ProviderError | None,
        references: tuple[RawRef, ...],
        received: int,
    ) -> OperationTerminal:
        async with self._lock:
            if self._terminal is not None:
                return self._terminal
            if self._cancelling and outcome is not Outcome.CANCELLED:
                outcome = Outcome.CANCELLED
                error = None
            self._terminal = OperationTerminal(
                uuid4(),
                self._request.operation_id,
                self._request.attempt_id,
                outcome,
                error,
                RetryDecision(
                    RetryAction.STOP, None, "adapter terminal; application decides replay", None
                ),
                1,
                received,
                0,
                Transport.HTTP,
                references,
                self._credential,
            )
            terminal = self._terminal
        self._journal.terminal(terminal.attempt_id, terminal_bytes(terminal))
        return terminal
