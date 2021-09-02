import logging
from asyncio.locks import Lock
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional, Union

import aiohttp
from yarl import URL

from .utils import asyncgeneratorcontextmanager


logger = logging.getLogger(__name__)


class ContainerRuntimeError(Exception):
    pass


class ContainerNotFoundError(ContainerRuntimeError):
    def __init__(self, id: str) -> None:
        super().__init__(f"Container {id!r} not found")


class ContainerRuntimeClient:
    def __init__(self, client: aiohttp.ClientSession, url: Union[URL, str]) -> None:
        self._client = client
        self._containers_url = URL(url) / "api/v1/containers"

    @asynccontextmanager
    async def attach(
        self,
        container_id: str,
        *,
        tty: bool = False,
        stdin: bool = False,
        stdout: bool = True,
        stderr: bool = True,
    ) -> AsyncIterator[aiohttp.ClientWebSocketResponse]:
        url = self._containers_url / _encode_container_id(container_id) / "attach"

        try:
            async with self._client.ws_connect(
                url.with_query(
                    tty=_bool_to_str(tty),
                    stdin=_bool_to_str(stdin),
                    stdout=_bool_to_str(stdout),
                    stderr=_bool_to_str(stderr),
                ),
                timeout=None,  # type: ignore
                receive_timeout=None,
                heartbeat=30,
            ) as ws:
                yield ws
        except aiohttp.WSServerHandshakeError as ex:
            if ex.status == 404:
                logger.warning("Container %r not found", container_id)
                raise ContainerNotFoundError(container_id)
            raise ContainerRuntimeError(
                f"Attach container {container_id!r} error: {ex}"
            )

    @asynccontextmanager
    async def exec(
        self,
        container_id: str,
        cmd: str,
        *,
        tty: bool = False,
        stdin: bool = False,
        stdout: bool = True,
        stderr: bool = True,
    ) -> AsyncIterator[aiohttp.ClientWebSocketResponse]:
        url = self._containers_url / _encode_container_id(container_id) / "exec"

        try:
            async with self._client.ws_connect(
                url.with_query(
                    cmd=cmd,
                    tty=_bool_to_str(tty),
                    stdin=_bool_to_str(stdin),
                    stdout=_bool_to_str(stdout),
                    stderr=_bool_to_str(stderr),
                ),
                timeout=None,  # type: ignore
                receive_timeout=None,
                heartbeat=30,
            ) as ws:
                yield ws
        except aiohttp.WSServerHandshakeError as ex:
            if ex.status == 404:
                logger.warning("Container %r not found", container_id)
                raise ContainerNotFoundError(container_id)
            raise ContainerRuntimeError(f"Exec container {container_id!r} error: {ex}")

    async def kill(self, container_id: str) -> None:
        async with self._client.post(
            self._containers_url / _encode_container_id(container_id) / "kill"
        ) as resp:
            if resp.status == 404:
                raise ContainerNotFoundError(container_id)
            if resp.status >= 400:
                error = await _get_error(resp)
                raise ContainerRuntimeError(
                    f"Kill container {container_id!r} error: {error}"
                )

    @asyncgeneratorcontextmanager
    async def commit(
        self, container_id: str, image: str, username: str = "", password: str = ""
    ) -> AsyncIterator[bytes]:
        payload = {"image": image, "push": True}
        if username and password:
            payload["auth"] = {"username": username, "password": password}

        async with self._client.post(
            self._containers_url / _encode_container_id(container_id) / "commit",
            json=payload,
        ) as resp:
            if resp.status == 404:
                raise ContainerNotFoundError(container_id)
            if resp.status >= 400:
                error = await _get_error(resp)
                raise ContainerRuntimeError(
                    f"Commit container {container_id!r} error: {error}"
                )
            async for chunk in resp.content:
                yield chunk


def _encode_container_id(id: str) -> str:
    return id.replace("/", "%2F")


def _bool_to_str(value: bool) -> str:
    return str(value).lower()


async def _get_error(resp: aiohttp.ClientResponse) -> str:
    try:
        payload = await resp.json()
        return payload["error"]
    except Exception:
        return await resp.text()


class ContainerRuntimeClientRegistry:
    def __init__(
        self,
        container_runtime_port: int,
        trace_configs: Optional[List[aiohttp.TraceConfig]] = None,
    ) -> None:
        self._port = container_runtime_port
        self._exit_stack = AsyncExitStack()
        self._registry: Dict[str, ContainerRuntimeClient] = {}
        self._trace_configs = trace_configs
        self._lock = Lock()

    async def __aenter__(self) -> "ContainerRuntimeClientRegistry":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self._exit_stack.aclose()

    async def get(self, host: str) -> "ContainerRuntimeClient":
        client = self._registry.get(host)

        if client:
            return client

        async with self._lock:
            session = await self._exit_stack.enter_async_context(
                aiohttp.ClientSession(trace_configs=self._trace_configs)
            )
            self._registry[host] = ContainerRuntimeClient(
                session, f"http://{host}:{self._port}"
            )

        return self._registry[host]
