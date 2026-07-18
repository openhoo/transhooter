from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx

from transhooter_worker.adapters.spool import EncryptedSpool


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
    ) -> None:
        bearer = bearer_file.read_text("utf-8").strip()
        if not bearer:
            raise RuntimeError("worker internal bearer is empty")
        self._base = base_url.rstrip("/")
        self._bearer = bearer
        self._meeting = meeting_id
        self._generation = generation
        self._worker_id = worker_id
        self._epoch = worker_epoch
        self._spool = spool
        self._client = client or httpx.AsyncClient(timeout=10, follow_redirects=False)

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
        for attempt in range(3):
            prepared = self._client.build_request(
                "POST",
                url,
                content=body,
                headers={
                    "Authorization": f"Bearer {self._bearer}",
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
                if attempt == 2:
                    raise
                await asyncio.sleep(0.1 * (2**attempt))
                continue
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
            if response.status_code // 100 == 5 and attempt < 2:
                await asyncio.sleep(0.1 * (2**attempt))
                continue
            raise RuntimeError(f"internal {kind} rejected with HTTP {response.status_code}")
