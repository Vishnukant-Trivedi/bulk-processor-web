from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

import httpx

from app.core.settings import settings


class UpstreamAPIError(RuntimeError):
    pass


class HospitalDirectoryClient:
    def __init__(self, base_url: Optional[str] = None) -> None:
        self.base_url = (base_url or settings.hospital_api_base_url).rstrip("/")

    def _client(self) -> httpx.AsyncClient:
        timeout = httpx.Timeout(settings.request_timeout_seconds)
        limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
        return httpx.AsyncClient(base_url=self.base_url, timeout=timeout, limits=limits)

    async def create_hospital(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        async with self._client() as client:
            return await self._request_with_retry(
                client, "POST", "/hospitals/", json=payload
            )

    async def activate_batch(self, batch_id: str) -> Dict[str, Any]:
        async with self._client() as client:
            return await self._request_with_retry(
                client, "PATCH", f"/hospitals/batch/{batch_id}/activate"
            )

    async def delete_batch(self, batch_id: str) -> Dict[str, Any]:
        async with self._client() as client:
            return await self._request_with_retry(
                client, "DELETE", f"/hospitals/batch/{batch_id}"
            )

    async def _request_with_retry(
        self,
        client: httpx.AsyncClient,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        for attempt in range(1, settings.request_max_attempts + 1):
            try:
                response = await client.request(method, path, **kwargs)
                if response.status_code >= 500:
                    raise UpstreamAPIError(
                        f"upstream server error {response.status_code}: {response.text}"
                    )
                if response.status_code in {429}:
                    retry_after = response.headers.get("Retry-After")
                    await asyncio.sleep(float(retry_after) if retry_after else min(2**attempt, 5))
                    continue
                response.raise_for_status()
                return response.json()
            except (httpx.TimeoutException, httpx.NetworkError, UpstreamAPIError) as exc:
                last_error = exc
                if attempt >= settings.request_max_attempts:
                    break
                await asyncio.sleep(min(2 ** (attempt - 1), 8))
        raise UpstreamAPIError(str(last_error) if last_error else "unknown upstream failure")
