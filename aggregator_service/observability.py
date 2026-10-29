"""Publicación no bloqueante de eventos hacia el servicio de observabilidad."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import httpx


class ObservabilityPublisher:
    """Adaptador HTTP opcional; los fallos de telemetría nunca detienen el dominio."""

    def __init__(self, base_url: str, source: str, timeout_seconds: float = 3.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.source = source
        self.timeout_seconds = timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    async def publish(
        self,
        *,
        category: str,
        event_type: str,
        payload: dict[str, Any],
        application_id: str = "default",
        severity: str = "info",
        service_id: str | None = None,
        batch_id: str | None = None,
        timestamp: str | None = None,
    ) -> None:
        if not self.enabled:
            return
        event = {
            "event_id": str(uuid4()),
            "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
            "application_id": application_id,
            "source": self.source,
            "category": category,
            "event_type": event_type,
            "severity": severity,
            "service_id": service_id,
            "batch_id": batch_id,
            "payload": payload,
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds, trust_env=False) as client:
                response = await client.post(f"{self.base_url}/events", json=event)
                response.raise_for_status()
        except Exception:
            return

    def publish_later(self, **kwargs: Any) -> None:
        if not self.enabled:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self.publish(**kwargs))
