from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx

from transhooter_worker.adapters.spool import EncryptedSpool

_DEFAULT_RETRY_DELAYS = (0.25, 0.5, 1.0, 2.0, 2.0)


class ControlRequestError(RuntimeError):
    pass


class RetryableControlRequestError(ControlRequestError):
    pass


class PermanentControlRequestError(ControlRequestError):
    pass


class ControlClient:
    def __init__(
        self,
        base_url: str,
        bearer_file: Path,
        meeting_id: UUID,
        generation: int,
        worker_id: UUID,
        worker_epoch: int,
        spool: EncryptedSpool,
        client: httpx.AsyncClient | None = None,
        *,
        retry_delays: tuple[float, ...] = _DEFAULT_RETRY_DELAYS,
    ) -> None:
        self._bearer_file = bearer_file
        if not self._read_bearer():
            raise RuntimeError("worker internal bearer is empty")
        self._base = base_url.rstrip("/")
        self._meeting = meeting_id
        self._generation = generation
        self._worker_id = worker_id
        self._epoch = worker_epoch
        self._spool = spool
        self._owns_client = client is None
        self._client = (
            client if client is not None else httpx.AsyncClient(timeout=10, follow_redirects=False)
        )
        self._retry_delays = retry_delays

    async def __aenter__(self) -> ControlClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def heartbeat(self, health: dict[str, Any], *, event_id: UUID | None = None) -> None:
        await self._post("heartbeat", health, event_id=event_id)

    async def checkpoint(self, payload: dict[str, Any], *, event_id: UUID | None = None) -> None:
        await self._post("checkpoint", payload, event_id=event_id)

    async def record_object(self, payload: dict[str, Any], *, event_id: UUID | None = None) -> None:
        await self._post("archive-object", payload, event_id=event_id)

    async def finalize(self, inventory: dict[str, Any], *, event_id: UUID | None = None) -> None:
        await self._post("finalize", {"inventory": inventory}, event_id=event_id)

    async def report_failure(
        self, payload: dict[str, Any], *, event_id: UUID | None = None
    ) -> None:
        await self._post("failure", payload, event_id=event_id)

    async def provider_attempt(
        self, payload: dict[str, Any], *, event_id: UUID | None = None
    ) -> None:
        await self._post("provider-attempt", {"report": payload}, event_id=event_id)

    def _read_bearer(self) -> str:
        try:
            bearer = self._bearer_file.read_text("utf-8").strip()
        except OSError as error:
            raise RuntimeError("worker internal bearer is unavailable") from error
        if not bearer:
            raise RuntimeError("worker internal bearer is empty")
        return bearer

    async def _post(
        self, kind: str, payload: dict[str, Any], *, event_id: UUID | None = None
    ) -> None:
        event_id = event_id or uuid4()
        body = json.dumps(
            {
                **payload,
                "consultationId": str(self._meeting),
                "generation": self._generation,
                "workerId": str(self._worker_id),
                "epoch": self._epoch,
                "eventId": str(event_id),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        url = f"{self._base}/api/internal/{kind}"
        for attempt in range(len(self._retry_delays) + 1):
            prepared = self._client.build_request(
                "POST",
                url,
                content=body,
                headers={
                    "Authorization": f"Bearer {self._read_bearer()}",
                    "Content-Type": "application/json",
                },
            )
            headers = tuple(
                (
                    name,
                    "[REDACTED:worker-internal-bearer]"
                    if name.lower() == "authorization"
                    else value,
                )
                for name, value in prepared.headers.multi_items()
            )
            if kind != "archive-object":
                self._spool.append(
                    meeting_id=self._meeting,
                    attempt_id=event_id,
                    stage=f"control-{kind}",
                    transport="http",
                    direction="out",
                    media_type="application/json",
                    payload=prepared.content,
                    metadata=((":method", prepared.method), (":url", str(prepared.url)), *headers),
                )
            try:
                response = await self._client.send(prepared)
            except httpx.TransportError:
                if attempt == len(self._retry_delays):
                    raise
                await asyncio.sleep(self._retry_delays[attempt])
                continue
            if kind != "archive-object":
                self._spool.append(
                    meeting_id=self._meeting,
                    attempt_id=event_id,
                    stage=f"control-{kind}",
                    transport="http",
                    direction="in",
                    media_type="application/json",
                    payload=response.content,
                    metadata=(
                        (":status", str(response.status_code)),
                        *tuple(response.headers.multi_items()),
                    ),
                )
            if response.status_code // 100 == 2:
                return
            retryable = response.status_code in {408, 425, 429} or response.status_code >= 500
            if retryable and attempt < len(self._retry_delays):
                await asyncio.sleep(self._retry_delays[attempt])
                continue
            error_type = RetryableControlRequestError if retryable else PermanentControlRequestError
            raise error_type(f"internal {kind} rejected with HTTP {response.status_code}")
