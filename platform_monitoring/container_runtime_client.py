import logging
from asyncio.locks import Lock
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

import aiohttp
from yarl import URL

from .utils import asyncgeneratorcontextmanager


logger = logging.getLogger(__name__)


class ContainerRuntimeError(Exception):
    pass


class ContainerRuntimeClientError(ContainerRuntimeError):
    pass


class ContainerNotFoundError(ContainerRuntimeClientError):
    def __init__(self, id_: str) -> None:
        super().__init__(f"Container {id_!r} not found")


class ContainerRuntimeClient:
    def __init__(self, client: aiohttp.ClientSession, url: URL | str) -> None:
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
                timeout=aiohttp.ClientWSTimeout(),
                heartbeat=30,
            ) as ws:
                yield ws
        except aiohttp.WSServerHandshakeError as ex:
            if ex.status == 404:
                logger.warning("Container %r not found", container_id)
                raise ContainerNotFoundError(container_id) from ex
            if 400 <= ex.status < 500:
                msg = f"Attach container {container_id!r} client error: {ex}"
                raise ContainerRuntimeClientError(msg) from ex
            msg = f"Attach container {container_id!r} error: {ex}"
            raise ContainerRuntimeError(msg) from ex

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
                timeout=aiohttp.ClientWSTimeout(),
                heartbeat=30,
            ) as ws:
                yield ws
        except aiohttp.WSServerHandshakeError as ex:
            if ex.status == 404:
                logger.warning("Container %r not found", container_id)
                raise ContainerNotFoundError(container_id) from ex
            if 400 <= ex.status < 500:
                msg = f"Exec container {container_id!r} client error: {ex}"
                raise ContainerRuntimeClientError(msg) from ex
            msg = f"Exec container {container_id!r} error: {ex}"
            raise ContainerRuntimeError(msg) from ex

    async def kill(self, container_id: str) -> None:
        async with self._client.post(
            self._containers_url / _encode_container_id(container_id) / "kill"
        ) as resp:
            if resp.status == 404:
                raise ContainerNotFoundError(container_id)
            if 400 <= resp.status < 500:
                error = await _get_error(resp)
                msg = f"Kill container {container_id!r} client error: {error}"
                raise ContainerRuntimeClientError(msg)
            if resp.status >= 500:
                error = await _get_error(resp)
                msg = f"Kill container {container_id!r} error: {error}"
                raise ContainerRuntimeError(msg)

    @asyncgeneratorcontextmanager
    async def commit(
        self, container_id: str, image: str, username: str = "", password: str = ""
    ) -> AsyncGenerator[bytes]:
        payload = {"image": image, "push": True}
        if username and password:
            payload["auth"] = {"username": username, "password": password}

        async with self._client.post(
            self._containers_url / _encode_container_id(container_id) / "commit",
            json=payload,
        ) as resp:
            if resp.status == 404:
                raise ContainerNotFoundError(container_id)
            if 400 <= resp.status < 500:
                error = await _get_error(resp)
                msg = f"Commit container {container_id!r} client error: {error}"
                raise ContainerRuntimeClientError(msg)
            if resp.status >= 500:
                error = await _get_error(resp)
                msg = f"Commit container {container_id!r} error: {error}"
                raise ContainerRuntimeError(msg)
            async for chunk in resp.content:
                yield chunk


def _encode_container_id(id_: str) -> str:
    return id_.replace("/", "%2F")


def _bool_to_str(value: bool) -> str:  # noqa: FBT001
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
        container_runtime_host: str | None = None,
        trace_configs: list[aiohttp.TraceConfig] | None = None,
    ) -> None:
        self._container_runtime_host = container_runtime_host
        self._port = container_runtime_port
        self._exit_stack = AsyncExitStack()
        self._registry: dict[str, ContainerRuntimeClient] = {}
        self._trace_configs = trace_configs
        self._lock = Lock()

    async def __aenter__(self) -> "ContainerRuntimeClientRegistry":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self._exit_stack.aclose()

    async def get(self, host: str) -> "ContainerRuntimeClient":
        host = self._container_runtime_host or host
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
