import json
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, aclosing
from dataclasses import dataclass
from enum import StrEnum, auto, unique
from types import TracebackType
from typing import Any

import aiohttp
from multidict import MultiDict
from yarl import URL


class ClientError(Exception):
    pass


class IllegalArgumentError(ValueError):
    pass


class ResourceNotFoundError(ValueError):
    pass


@dataclass(frozen=True)
class Job:
    @unique
    class Status(StrEnum):
        PENDING = auto()
        SUSPENDED = auto()
        RUNNING = auto()
        SUCCEEDED = auto()
        FAILED = auto()
        CANCELLED = auto()
        UNKNOWN = auto()

    id: str
    uri: URL
    status: Status
    created_at: str
    namespace: str | None = None
    name: str | None = None


def _create_job(payload: dict[str, Any]) -> Job:
    return Job(
        id=payload["id"],
        status=_create_job_status(payload["history"].get("status", "unknown")),
        uri=URL(payload["uri"]),
        name=payload.get("name"),
        created_at=payload["history"].get("created_at"),
        namespace=payload.get("namespace"),
    )


def _create_job_status(value: str) -> Job.Status:
    try:
        return Job.Status(value)
    except Exception:
        return Job.Status.UNKNOWN


class ApiClient:
    _client: aiohttp.ClientSession

    def __init__(
        self,
        url: URL,
        token: str | None = None,
        timeout: aiohttp.ClientTimeout = aiohttp.client.DEFAULT_TIMEOUT,
        trace_configs: list[aiohttp.TraceConfig] | None = None,
    ):
        super().__init__()

        self._base_url = url / "api/v1"
        self._token = token
        self._timeout = timeout
        self._trace_configs = trace_configs

    async def __aenter__(self) -> "ApiClient":
        self._client = self._create_http_client()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def _create_http_client(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            headers=self._create_default_headers(),
            timeout=self._timeout,
            trace_configs=self._trace_configs,
        )

    async def aclose(self) -> None:
        assert self._client
        await self._client.close()

    def _create_default_headers(self) -> dict[str, str]:
        result = {}
        if self._token:
            result["Authorization"] = f"Bearer {self._token}"
        return result

    def get_jobs(
        self,
        *,
        cluster_name: str,
        being_dropped: bool | None = None,
        logs_removed: bool | None = None,
    ) -> AbstractAsyncContextManager[AsyncGenerator[Job]]:
        return aclosing(
            self._get_jobs(
                cluster_name=cluster_name,
                being_dropped=being_dropped,
                logs_removed=logs_removed,
            )
        )

    async def _get_jobs(
        self,
        *,
        cluster_name: str,
        being_dropped: bool | None = None,
        logs_removed: bool | None = None,
    ) -> AsyncGenerator[Job]:
        headers = {"Accept": "application/x-ndjson"}
        params: MultiDict[str] = MultiDict()
        params["cluster_name"] = cluster_name
        if being_dropped is not None:
            params.add("being_dropped", str(being_dropped))
        if logs_removed is not None:
            params.add("logs_removed", str(logs_removed))
        async with self._client.get(
            self._base_url / "jobs", headers=headers, params=params
        ) as response:
            await self._raise_for_status(response)
            if response.headers.get("Content-Type", "").startswith(
                "application/x-ndjson"
            ):
                async for line in response.content:
                    payload = json.loads(line)
                    if "error" in payload:
                        raise Exception(payload["error"])
                    yield _create_job(payload)
            else:
                response_json = await response.json()
                for j in response_json["jobs"]:
                    yield _create_job(j)

    async def get_job(self, job_id: str) -> Job:
        async with self._client.get(self._base_url / "jobs" / job_id) as response:
            await self._raise_for_status(response)
            response_json = await response.json()
            return _create_job(response_json)

    async def mark_job_logs_dropped(self, job_id: str) -> None:
        async with self._client.post(
            self._base_url / "jobs" / job_id / "drop_progress",
            json={"logs_removed": True},
        ) as response:
            await self._raise_for_status(response)

    async def _raise_for_status(self, response: aiohttp.ClientResponse) -> None:
        if response.ok:
            return

        text = await response.text()
        if response.status == 404:
            raise ResourceNotFoundError(text)
        if 400 <= response.status < 500:
            raise IllegalArgumentError(text)

        try:
            response.raise_for_status()
        except aiohttp.ClientResponseError as exc:
            msg = f"{str(exc)}, body={text!r}"
            raise ClientError(msg) from exc
