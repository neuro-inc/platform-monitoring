import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterator, List
from unittest import mock
from uuid import uuid4

import aiohttp
import pytest
from aiohttp import WSServerHandshakeError
from aiohttp.web import HTTPOk
from aiohttp.web_exceptions import (
    HTTPAccepted,
    HTTPBadRequest,
    HTTPForbidden,
    HTTPNoContent,
    HTTPUnauthorized,
)
from async_timeout import timeout
from platform_monitoring.api import create_app
from platform_monitoring.config import (
    DOCKER_API_VERSION,
    Config,
    DockerConfig,
    PlatformApiConfig,
)
from platform_monitoring.docker_client import Docker
from yarl import URL

from tests.integration.conftest_kube import MyKubeClient

from .conftest import ApiAddress, create_local_app_server
from .conftest_auth import _User


async def expect_prompt(ws: aiohttp.ClientWebSocketResponse) -> bytes:
    _ansi_re = re.compile(br"\033\[[;?0-9]*[a-zA-Z]")
    _exit_re = re.compile(br"exit \d+\Z")
    try:
        ret: bytes = b""
        async with timeout(3):
            while not ret.strip().endswith(b"#") and not _exit_re.match(ret.strip()):
                msg = await ws.receive()
                if msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.CLOSED,
                ):
                    break
                assert msg.data[0] == 1
                ret += _ansi_re.sub(b"", msg.data[1:])
            return ret
    except asyncio.TimeoutError:
        raise AssertionError(f"[Timeout] {ret!r}")


@dataclass(frozen=True)
class MonitoringApiEndpoints:
    address: ApiAddress

    @property
    def api_v1_endpoint(self) -> URL:
        return URL(f"http://{self.address.host}:{self.address.port}/api/v1")

    @property
    def ping_url(self) -> URL:
        return self.api_v1_endpoint / "ping"

    @property
    def secured_ping_url(self) -> URL:
        return self.api_v1_endpoint / "secured-ping"

    @property
    def endpoint(self) -> URL:
        return self.api_v1_endpoint / "jobs"

    def generate_top_url(self, job_id: str) -> URL:
        return self.endpoint / job_id / "top"

    def generate_log_url(self, job_id: str) -> URL:
        return self.endpoint / job_id / "log"

    def generate_save_url(self, job_id: str) -> URL:
        return self.endpoint / job_id / "save"

    def generate_attach_url(
        self,
        job_id: str,
        *,
        stdin: bool = False,
        stdout: bool = True,
        stderr: bool = True,
        logs: bool = False,
    ) -> URL:
        url = self.endpoint / job_id / "attach"
        return url.with_query(
            stdin=int(stdin), stdout=int(stdout), stderr=int(stderr), logs=int(logs)
        )

    def generate_resize_url(self, job_id: str, *, w: int, h: int) -> URL:
        url = self.endpoint / job_id / "resize"
        return url.with_query(w=w, h=h)

    def generate_exec_create_url(self, job_id: str) -> URL:
        return self.endpoint / job_id / "exec_create"

    def generate_exec_resize_url(
        self, job_id: str, exec_id: str, *, w: int, h: int
    ) -> URL:
        url = self.endpoint / job_id / exec_id / "exec_resize"
        return url.with_query(w=w, h=h)

    def generate_exec_inspect_url(self, job_id: str, exec_id: str) -> URL:
        return self.endpoint / job_id / exec_id / "exec_inspect"

    def generate_exec_start_url(self, job_id: str, exec_id: str) -> URL:
        return self.endpoint / job_id / exec_id / "exec_start"


@dataclass(frozen=True)
class PlatformApiEndpoints:
    url: URL

    @property
    def endpoint(self) -> URL:
        return self.url

    @property
    def platform_config_url(self) -> URL:
        return self.endpoint / "config"

    @property
    def jobs_base_url(self) -> URL:
        return self.endpoint / "jobs"

    def generate_job_url(self, job_id: str) -> URL:
        return self.jobs_base_url / job_id


@pytest.fixture
async def monitoring_api(config: Config) -> AsyncIterator[MonitoringApiEndpoints]:
    app = await create_app(config)
    async with create_local_app_server(app, port=8080) as address:
        yield MonitoringApiEndpoints(address=address)


@pytest.fixture
async def platform_api(
    platform_api_config: PlatformApiConfig,
) -> AsyncIterator[PlatformApiEndpoints]:
    yield PlatformApiEndpoints(url=platform_api_config.url)


class JobsClient:
    def __init__(
        self,
        platform_api: PlatformApiEndpoints,
        client: aiohttp.ClientSession,
        user: _User,
    ) -> None:
        self._platform_api = platform_api
        self._client = client
        self._user = user

    @property
    def user(self) -> _User:
        return self._user

    @property
    def headers(self) -> Dict[str, str]:
        return self._user.headers

    async def get_job_by_id(self, job_id: str) -> Dict[str, Any]:
        url = self._platform_api.generate_job_url(job_id)
        async with self._client.get(url, headers=self.headers) as response:
            response_text = await response.text()
            assert response.status == HTTPOk.status_code, response_text
            result = await response.json()
        return result

    async def long_polling_by_job_id(
        self, job_id: str, status: str, interval_s: float = 0.5, max_time: float = 180
    ) -> Dict[str, Any]:
        t0 = time.monotonic()
        while True:
            response = await self.get_job_by_id(job_id)
            if response["status"] == status:
                return response
            await asyncio.sleep(max(interval_s, time.monotonic() - t0))
            current_time = time.monotonic() - t0
            if current_time > max_time:
                pytest.fail(f"too long: {current_time:.3f} sec; resp: {response}")
            interval_s *= 1.5

    async def delete_job(self, job_id: str, assert_success: bool = True) -> None:
        url = self._platform_api.generate_job_url(job_id)
        async with self._client.delete(url, headers=self.headers) as response:
            if assert_success:
                assert response.status == HTTPNoContent.status_code


@pytest.fixture
def jobs_client_factory(
    platform_api: PlatformApiEndpoints, client: aiohttp.ClientSession
) -> Iterator[Callable[[_User], JobsClient]]:
    def impl(user: _User) -> JobsClient:
        return JobsClient(platform_api, client, user=user)

    yield impl


@pytest.fixture
async def jobs_client(
    regular_user_factory: Callable[..., Awaitable[_User]],
    jobs_client_factory: Callable[[_User], JobsClient],
) -> JobsClient:
    regular_user = await regular_user_factory()
    return jobs_client_factory(regular_user)


@pytest.fixture
def job_request_factory() -> Callable[[], Dict[str, Any]]:
    def _factory() -> Dict[str, Any]:
        return {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
            }
        }

    return _factory


@pytest.fixture
async def job_submit(
    job_request_factory: Callable[[], Dict[str, Any]]
) -> Dict[str, Any]:
    return job_request_factory()


@pytest.fixture
async def job_factory(
    platform_api: PlatformApiEndpoints,
    client: aiohttp.ClientSession,
    jobs_client: JobsClient,
    job_request_factory: Callable[[], Dict[str, Any]],
) -> AsyncIterator[Callable[[str], Awaitable[str]]]:
    jobs: List[str] = []

    async def _f(command: str) -> str:
        request_payload = job_request_factory()
        request_payload["container"]["command"] = command
        async with client.post(
            platform_api.jobs_base_url,
            headers=jobs_client.headers,
            json=request_payload,
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            job_id = result["id"]
            jobs.append(job_id)
            await jobs_client.long_polling_by_job_id(job_id, status="running")

        return job_id

    yield _f

    for job_id in jobs:
        await jobs_client.delete_job(job_id)


@pytest.fixture
async def infinite_job(job_factory: Callable[[str], Awaitable[str]]) -> str:
    return await job_factory("tail -f /dev/null")


@pytest.fixture
async def wait_for_job_docker_client(
    kube_client: MyKubeClient,
    docker_config: DockerConfig,
    job_factory: Callable[[str], Awaitable[str]],
) -> None:
    timeout_s: float = 60
    interval_s: float = 1

    job_id = await job_factory("sleep 5m")
    pod_name = job_id
    async with timeout(timeout_s):
        pod = await kube_client.get_pod(pod_name)
        async with kube_client.get_node_proxy_client(
            pod.node_name, docker_config.docker_engine_api_port
        ) as proxy_client:
            docker = Docker(
                url=str(proxy_client.url),
                session=proxy_client.session,
                connector=proxy_client.session.connector,
                api_version=DOCKER_API_VERSION,
            )
            while True:
                try:
                    await docker.ping()
                    return
                except aiohttp.ClientError as e:
                    logging.info(
                        f"Failed to ping docker client: {proxy_client.url}: {e}"
                    )
                    await asyncio.sleep(interval_s)


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(
        self, monitoring_api: MonitoringApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(monitoring_api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == "Pong"

    @pytest.mark.asyncio
    async def test_secured_ping(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
    ) -> None:
        headers = jobs_client.headers
        async with client.get(monitoring_api.secured_ping_url, headers=headers) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == "Secured Pong"

    @pytest.mark.asyncio
    async def test_secured_ping_no_token_provided_unauthorized(
        self, monitoring_api: MonitoringApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        url = monitoring_api.secured_ping_url
        async with client.get(url) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    @pytest.mark.asyncio
    async def test_secured_ping_non_existing_token_unauthorized(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        token_factory: Callable[[str], str],
    ) -> None:
        url = monitoring_api.secured_ping_url
        token = token_factory("non-existing-user")
        headers = {"Authorization": f"Bearer {token}"}
        async with client.get(url, headers=headers) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    @pytest.mark.asyncio
    async def test_ping_unknown_origin(
        self, monitoring_api: MonitoringApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(
            monitoring_api.ping_url, headers={"Origin": "http://unknown"}
        ) as response:
            assert response.status == HTTPOk.status_code, await response.text()
            assert "Access-Control-Allow-Origin" not in response.headers

    @pytest.mark.asyncio
    async def test_ping_allowed_origin(
        self, monitoring_api: MonitoringApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(
            monitoring_api.ping_url, headers={"Origin": "https://neu.ro"}
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert resp.headers["Access-Control-Allow-Origin"] == "https://neu.ro"
            assert resp.headers["Access-Control-Allow-Credentials"] == "true"
            assert resp.headers["Access-Control-Expose-Headers"] == ""

    @pytest.mark.asyncio
    async def test_ping_options_no_headers(
        self, monitoring_api: MonitoringApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.options(monitoring_api.ping_url) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            assert await resp.text() == (
                "CORS preflight request failed: "
                "origin header is not specified in the request"
            )

    @pytest.mark.asyncio
    async def test_ping_options_unknown_origin(
        self, monitoring_api: MonitoringApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.options(
            monitoring_api.ping_url,
            headers={
                "Origin": "http://unknown",
                "Access-Control-Request-Method": "GET",
            },
        ) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            assert await resp.text() == (
                "CORS preflight request failed: "
                "origin 'http://unknown' is not allowed"
            )

    @pytest.mark.asyncio
    async def test_ping_options(
        self, monitoring_api: MonitoringApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.options(
            monitoring_api.ping_url,
            headers={
                "Origin": "https://neu.ro",
                "Access-Control-Request-Method": "GET",
            },
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert resp.headers["Access-Control-Allow-Origin"] == "https://neu.ro"
            assert resp.headers["Access-Control-Allow-Credentials"] == "true"
            assert resp.headers["Access-Control-Allow-Methods"] == "GET"


class TestTopApi:
    @pytest.mark.asyncio
    async def test_top_ok(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        num_request = 2
        records = []

        url = monitoring_api.generate_top_url(job_id=infinite_job)
        async with client.ws_connect(url, headers=jobs_client.headers) as ws:
            # TODO move this ws communication to JobClient
            while True:
                msg = await ws.receive()
                if msg.type == aiohttp.WSMsgType.CLOSE:
                    break
                else:
                    records.append(json.loads(msg.data))

                if len(records) == num_request:
                    # TODO (truskovskiyk 09/12/18) do not use protected prop
                    # https://github.com/aio-libs/aiohttp/issues/3443
                    proto = ws._writer.protocol
                    assert proto.transport is not None
                    proto.transport.close()
                    break

        assert len(records) == num_request
        for message in records:
            assert message == {
                "cpu": mock.ANY,
                "memory": mock.ANY,
                "timestamp": mock.ANY,
            }

    @pytest.mark.asyncio
    async def test_top_no_permissions_unauthorized(
        self,
        monitoring_api: MonitoringApiEndpoints,
        platform_api: PlatformApiEndpoints,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        regular_user_factory: Callable[..., Awaitable[_User]],
    ) -> None:
        user1 = await regular_user_factory()
        user2 = await regular_user_factory()

        url = platform_api.jobs_base_url
        async with client.post(url, headers=user1.headers, json=job_submit) as resp:
            assert resp.status == HTTPAccepted.status_code
            payload = await resp.json()
            job_id = payload["id"]

        url = monitoring_api.generate_top_url(job_id)
        with pytest.raises(WSServerHandshakeError, match="Invalid response status"):
            async with client.ws_connect(url, headers=user2.headers):
                pass

    @pytest.mark.asyncio
    async def test_top_no_auth_token_provided_unauthorized(
        self,
        monitoring_api: MonitoringApiEndpoints,
        platform_api: PlatformApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:

        url = monitoring_api.generate_top_url(job_id=infinite_job)
        with pytest.raises(WSServerHandshakeError, match="Invalid response status"):
            async with client.ws_connect(url):
                pass

    @pytest.mark.asyncio
    async def test_top_non_running_job(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        job = infinite_job
        await jobs_client.delete_job(job)
        await jobs_client.long_polling_by_job_id(job_id=job, status="succeeded")

        num_request = 2
        records = []

        url = monitoring_api.generate_top_url(job_id=job)
        async with client.ws_connect(url, headers=jobs_client.headers) as ws:
            # TODO move this ws communication to JobClient
            while True:
                msg = await ws.receive()
                if msg.type == aiohttp.WSMsgType.CLOSE:
                    break
                else:
                    records.append(json.loads(msg.data))

                if len(records) == num_request:
                    # TODO (truskovskiyk 09/12/18) do not use protected prop
                    # https://github.com/aio-libs/aiohttp/issues/3443
                    proto = ws._writer.protocol
                    assert proto.transport is not None
                    proto.transport.close()
                    break

        assert not records

    @pytest.mark.asyncio
    async def test_top_non_existing_job(
        self,
        platform_api: PlatformApiEndpoints,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
    ) -> None:
        headers = jobs_client.headers
        job_id = f"job-{uuid4()}"

        url = platform_api.generate_job_url(job_id)
        async with client.get(url, headers=headers) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.text()
            assert "no such job" in payload

        url = monitoring_api.generate_top_url(job_id=job_id)
        with pytest.raises(WSServerHandshakeError, match="Invalid response status"):
            async with client.ws_connect(url, headers=headers):
                pass

    @pytest.mark.asyncio
    async def test_top_silently_wait_when_job_pending(
        self,
        monitoring_api: MonitoringApiEndpoints,
        platform_api: PlatformApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: Dict[str, Any],
    ) -> None:
        command = 'bash -c "for i in {1..10}; do echo $i; sleep 1; done"'
        job_submit["container"]["command"] = command
        headers = jobs_client.headers

        url = platform_api.jobs_base_url
        async with client.post(url, headers=headers, json=job_submit) as resp:
            assert resp.status == HTTPAccepted.status_code
            payload = await resp.json()
            job_id = payload["id"]
            assert payload["status"] == "pending"

        num_request = 2
        records = []

        job_top_url = monitoring_api.generate_top_url(job_id)
        async with client.ws_connect(job_top_url, headers=headers) as ws:
            job = await jobs_client.get_job_by_id(job_id=job_id)
            assert job["status"] == "pending"

            # silently waiting for a job becomes running
            msg = await ws.receive()
            job = await jobs_client.get_job_by_id(job_id=job_id)
            assert job["status"] == "running"
            assert msg.type == aiohttp.WSMsgType.TEXT

            while True:
                msg = await ws.receive()
                if msg.type == aiohttp.WSMsgType.CLOSE:
                    break
                else:
                    records.append(json.loads(msg.data))

                if len(records) == num_request:
                    # TODO (truskovskiyk 09/12/18) do not use protected prop
                    # https://github.com/aio-libs/aiohttp/issues/3443
                    proto = ws._writer.protocol
                    assert proto.transport is not None
                    proto.transport.close()
                    break

        assert len(records) == num_request
        for message in records:
            assert message == {
                "cpu": mock.ANY,
                "memory": mock.ANY,
                "timestamp": mock.ANY,
            }

        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_top_close_when_job_succeeded(
        self,
        monitoring_api: MonitoringApiEndpoints,
        platform_api: PlatformApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: Dict[str, Any],
    ) -> None:

        command = 'bash -c "for i in {1..2}; do echo $i; sleep 1; done"'
        job_submit["container"]["command"] = command
        headers = jobs_client.headers

        url = platform_api.jobs_base_url
        async with client.post(url, headers=headers, json=job_submit) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")

        job_top_url = monitoring_api.generate_top_url(job_id)
        async with client.ws_connect(job_top_url, headers=headers) as ws:
            msg = await ws.receive()
            job = await jobs_client.get_job_by_id(job_id=job_id)

            assert msg.type == aiohttp.WSMsgType.CLOSE
            assert job["status"] == "succeeded"

        await jobs_client.delete_job(job_id=job_id)


class TestLogApi:
    @pytest.mark.asyncio
    async def test_log_no_permissions_forbidden(
        self,
        monitoring_api: MonitoringApiEndpoints,
        platform_api: PlatformApiEndpoints,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        regular_user_factory: Callable[..., Awaitable[_User]],
        cluster_name: str,
    ) -> None:
        user1 = await regular_user_factory()
        user2 = await regular_user_factory()

        url = platform_api.jobs_base_url
        async with client.post(url, headers=user1.headers, json=job_submit) as resp:
            assert resp.status == HTTPAccepted.status_code
            payload = await resp.json()
            job_id = payload["id"]

        url = monitoring_api.generate_log_url(job_id)
        async with client.get(url, headers=user2.headers) as resp:
            assert resp.status == HTTPForbidden.status_code
            result = await resp.json()
            assert result == {
                "missing": [
                    {
                        "uri": f"job://{cluster_name}/{user1.name}/{job_id}",
                        "action": "read",
                    }
                ]
            }

    @pytest.mark.asyncio
    async def test_log_no_auth_token_provided_unauthorized(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        url = monitoring_api.generate_top_url(job_id=infinite_job)
        async with client.get(url) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    @pytest.mark.asyncio
    async def test_job_log(
        self,
        monitoring_api: MonitoringApiEndpoints,
        platform_api: PlatformApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: Dict[str, Any],
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        request_payload = job_submit
        request_payload["container"]["command"] = command
        headers = jobs_client.headers

        url = platform_api.jobs_base_url
        async with client.post(url, headers=headers, json=request_payload) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            job_id = result["id"]

        await jobs_client.long_polling_by_job_id(job_id, "succeeded")

        url = monitoring_api.generate_log_url(job_id)
        async with client.get(url, headers=headers) as response:
            assert response.status == HTTPOk.status_code
            assert response.content_type == "text/plain"
            assert response.charset == "utf-8"
            assert response.headers["Transfer-Encoding"] == "chunked"
            assert "Content-Encoding" not in response.headers
            actual_payload = await response.read()
            expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
            assert actual_payload == expected_payload.encode()


class TestSaveApi:
    @pytest.mark.asyncio
    async def test_save_no_permissions_forbidden(
        self,
        monitoring_api: MonitoringApiEndpoints,
        platform_api: PlatformApiEndpoints,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        regular_user_factory: Callable[..., Awaitable[_User]],
        cluster_name: str,
    ) -> None:
        user1 = await regular_user_factory()
        user2 = await regular_user_factory()

        url = platform_api.jobs_base_url
        async with client.post(url, headers=user1.headers, json=job_submit) as resp:
            assert resp.status == HTTPAccepted.status_code
            payload = await resp.json()
            job_id = payload["id"]

        url = monitoring_api.generate_save_url(job_id)
        async with client.post(url, headers=user2.headers) as resp:
            assert resp.status == HTTPForbidden.status_code
            result = await resp.json()
            assert result == {
                "missing": [
                    {
                        "uri": f"job://{cluster_name}/{user1.name}/{job_id}",
                        "action": "write",
                    }
                ]
            }

    @pytest.mark.asyncio
    async def test_save_no_auth_token_provided_unauthorized(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        url = monitoring_api.generate_save_url(job_id=infinite_job)
        async with client.post(url) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    @pytest.mark.asyncio
    async def test_save_non_existing_job(
        self,
        platform_api: PlatformApiEndpoints,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
    ) -> None:
        headers = jobs_client.headers
        job_id = f"job-{uuid4()}"

        url = monitoring_api.generate_save_url(job_id=job_id)
        async with client.post(url, headers=headers) as resp:
            assert resp.status == HTTPBadRequest.status_code, str(resp)
            assert "no such job" in await resp.text()

    @pytest.mark.asyncio
    async def test_save_unknown_registry_host(
        self,
        platform_api: PlatformApiEndpoints,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        url = monitoring_api.generate_save_url(job_id=infinite_job)
        headers = jobs_client.headers
        payload = {"container": {"image": "unknown:5000/alpine:latest"}}
        async with client.post(url, headers=headers, json=payload) as resp:
            assert resp.status == HTTPBadRequest.status_code, str(resp)
            resp_payload = await resp.json()
            assert "Unknown registry host" in resp_payload["error"]

    @pytest.mark.asyncio
    async def test_save_not_running_job(
        self,
        platform_api: PlatformApiEndpoints,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
        config: Config,
        kube_client: MyKubeClient,
    ) -> None:
        await jobs_client.delete_job(infinite_job)
        await kube_client.wait_pod_is_terminated(
            pod_name=infinite_job, allow_pod_not_exists=True
        )

        url = monitoring_api.generate_save_url(job_id=infinite_job)
        headers = jobs_client.headers
        payload = {
            "container": {"image": f"{config.registry.host}/alpine:{infinite_job}"}
        }
        async with client.post(url, headers=headers, json=payload) as resp:
            assert resp.status == HTTPOk.status_code, str(resp)
            chunks = [
                json.loads(chunk, encoding="utf-8")
                async for chunk in resp.content
                if chunk
            ]
            debug = f"Received chunks: `{chunks}`"

            assert len(chunks) == 1, debug
            assert "not running" in chunks[0]["error"], debug

    @pytest.mark.asyncio
    async def test_save_push_failed_job_exception_raised(
        self,
        config_factory: Callable[..., Config],
        platform_api: PlatformApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        invalid_docker_config = DockerConfig(docker_engine_api_port=1)
        config = config_factory(docker=invalid_docker_config)

        app = await create_app(config)
        async with create_local_app_server(app, port=8080) as address:
            monitoring_api = MonitoringApiEndpoints(address=address)
            url = monitoring_api.generate_save_url(job_id=infinite_job)

            headers = jobs_client.headers
            image = f"{config.registry.host}/alpine:{infinite_job}"
            payload = {"container": {"image": image}}
            async with client.post(url, headers=headers, json=payload) as resp:
                assert resp.status == HTTPOk.status_code, str(resp)
                chunks = [
                    json.loads(chunk, encoding="utf-8")
                    async for chunk in resp.content
                    if chunk
                ]
                debug = f"Received chunks: `{chunks}`"

                assert len(chunks) == 2, debug

                assert chunks[0]["status"] == "CommitStarted", debug
                assert chunks[0]["details"]["image"] == image, debug
                assert re.match(r"\w{64}", chunks[0]["details"]["container"]), debug

                error = chunks[1]["error"]
                assert (
                    f"Failed to save job '{infinite_job}': DockerError(900" in error
                ), debug
                assert "Connect call failed" in error, debug

    @pytest.mark.asyncio
    async def test_save_ok(
        self,
        platform_api: PlatformApiEndpoints,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
        kube_client: MyKubeClient,
        docker_config: DockerConfig,
        config: Config,
        wait_for_job_docker_client: None,
    ) -> None:
        url = monitoring_api.generate_save_url(job_id=infinite_job)
        headers = jobs_client.headers
        repository = f"{config.registry.host}/alpine"
        image = f"{repository}:{infinite_job}"
        payload = {"container": {"image": image}}

        async with client.post(url, headers=headers, json=payload) as resp:
            assert resp.status == HTTPOk.status_code, str(resp)
            chunks = [
                json.loads(chunk, encoding="utf-8")
                async for chunk in resp.content
                if chunk
            ]
            debug = f"Received chunks: `{chunks}`"
            assert isinstance(chunks, list), debug
            assert all(isinstance(s, dict) for s in chunks), debug
            assert len(chunks) >= 4, debug  # 2 for commit(), >=2 for push()

            # here we rely on chunks to be received in correct order

            assert chunks[0]["status"] == "CommitStarted", debug
            assert chunks[0]["details"]["image"] == image, debug
            assert re.match(r"\w{64}", chunks[0]["details"]["container"]), debug

            assert chunks[1] == {"status": "CommitFinished"}, debug

            msg = f"The push refers to repository [{repository}]"
            assert chunks[2].get("status") == msg, debug

            assert chunks[-1].get("aux", {}).get("Tag") == infinite_job, debug

    @pytest.mark.asyncio
    async def test_attach_nontty(
        self,
        platform_api: PlatformApiEndpoints,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        wait_for_job_docker_client: None,
        job_submit: Dict[str, Any],
    ) -> None:
        command = 'bash -c "for i in {0..9}; do echo $i; sleep 1; done"'
        job_submit["container"]["command"] = command
        headers = jobs_client.headers

        url = platform_api.jobs_base_url
        async with client.post(url, headers=headers, json=job_submit) as response:
            assert response.status == 202
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="running")

        url1 = monitoring_api.generate_attach_url(
            job_id=job_id, stdin=False, stdout=True, stderr=True, logs=True
        )

        content = []
        async with client.ws_connect(url1, method="POST", headers=headers) as ws:
            async for msg in ws:
                content.append(msg.data)

        expected = b"".join(f"\x01{i}\n".encode("ascii") for i in range(10))
        assert b"".join(content) == expected

    @pytest.mark.asyncio
    async def test_attach_tty(
        self,
        platform_api: PlatformApiEndpoints,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        wait_for_job_docker_client: None,
        job_submit: Dict[str, Any],
    ) -> None:
        command = "sh"
        job_submit["container"]["command"] = command
        job_submit["container"]["tty"] = True
        headers = jobs_client.headers

        url = platform_api.jobs_base_url
        async with client.post(url, headers=headers, json=job_submit) as response:
            assert response.status == 202
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="running")

        url1 = monitoring_api.generate_resize_url(job_id=job_id, w=70, h=20)

        async with client.post(url1, headers=headers) as response:
            assert response.status == 200

        url2 = monitoring_api.generate_attach_url(
            job_id=job_id, stdin=True, stdout=True, stderr=True, logs=True
        )

        async with client.ws_connect(url2, method="POST", headers=headers) as ws:
            # We can't be sure if we connect before inital prompt or after
            try:
                await asyncio.wait_for(ws.receive_bytes(), timeout=0.2)
            except asyncio.TimeoutError:
                pass

            await ws.send_bytes(b"\n")
            assert await expect_prompt(ws) == b"\r\n# "
            await ws.send_bytes(b"echo 'abc'\n")
            assert await expect_prompt(ws) == b"echo 'abc'\r\nabc\r\n# "
            await ws.send_bytes(b"exit 1\n")
            assert await expect_prompt(ws) == b"exit 1\r\n"

        result = await jobs_client.long_polling_by_job_id(job_id, status="failed")

        from pprint import pprint

        pprint(result)
        assert result["history"]["exit_code"] == 1, result

    @pytest.mark.asyncio
    async def test_exec_notty_stdout(
        self,
        platform_api: PlatformApiEndpoints,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:

        headers = jobs_client.headers

        url1 = monitoring_api.generate_exec_create_url(infinite_job)
        async with client.post(
            url1,
            headers=headers,
            json={
                "cmd": "sh -c 'sleep 5; echo abc'",
                "stdin": False,
                "stdout": True,
                "stderr": True,
                "tty": False,
            },
        ) as response:
            content = await response.json()
            exec_id = content["exec_id"]

        url2 = monitoring_api.generate_exec_start_url(infinite_job, exec_id)
        async with client.ws_connect(url2, method="POST", headers=headers) as ws:
            data = await ws.receive_bytes()
            assert data == b"\x01abc\n"

    @pytest.mark.asyncio
    async def test_exec_notty_stderr(
        self,
        platform_api: PlatformApiEndpoints,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:

        headers = jobs_client.headers

        url1 = monitoring_api.generate_exec_create_url(infinite_job)
        async with client.post(
            url1,
            headers=headers,
            json={
                "cmd": "sh -c 'sleep 5; echo abc 1>&2'",
                "stdin": False,
                "stdout": True,
                "stderr": True,
                "tty": False,
            },
        ) as response:
            content = await response.json()
            exec_id = content["exec_id"]

        url2 = monitoring_api.generate_exec_start_url(infinite_job, exec_id)
        async with client.ws_connect(url2, method="POST", headers=headers) as ws:
            data = await ws.receive_bytes()
            assert data == b"\x02abc\n"

    @pytest.mark.asyncio
    async def test_exec_tty(
        self,
        platform_api: PlatformApiEndpoints,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:

        headers = jobs_client.headers

        url1 = monitoring_api.generate_exec_create_url(infinite_job)
        async with client.post(
            url1,
            headers=headers,
            json={
                "cmd": "sh",
                "stdin": True,
                "stdout": True,
                "stderr": True,
                "tty": True,
            },
        ) as response:
            content = await response.json()
            exec_id = content["exec_id"]

        url2 = monitoring_api.generate_exec_start_url(infinite_job, exec_id)
        async with client.ws_connect(url2, method="POST", headers=headers) as ws:

            await ws.send_bytes(b"\n")
            assert await expect_prompt(ws) == b"\r\n# "
            await ws.send_bytes(b"echo 'abc'\n")
            assert await expect_prompt(ws) == b"echo 'abc'\r\nabc\r\n# "
            await ws.send_bytes(b"exit 1\n")
            assert await expect_prompt(ws) == b"exit 1\r\n"

    # @pytest.mark.asyncio
    # async def test_exec_tty(
    #     self,
    #     job_factory: JobFactory,
    #     platform_api_client: PlatformApiClient,
    #     jobs_service: JobsService,
    #     user: User,
    #     registry_host: str,
    #     image_tag: str,
    #     wait_for_job_docker_client: Callable[[str], Awaitable[None]],
    # ) -> None:
    #     resources = Resources(
    #         memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
    #     )
    #     job = await job_factory("alpine:latest", "sleep 300", resources,)
    #     await self.wait_for_job_running(job, platform_api_client)

    #     await wait_for_job_docker_client(job.id)

    #     exec_id = await jobs_service.exec_create(job, "sh", tty=True, stdin=True)
    #     async with jobs_service.exec_start(job, exec_id) as stream:
    #         await jobs_service.exec_resize(job, exec_id, w=120, h=15)
    #         assert await expect_prompt(stream) == b"/ # "
    #         await stream.write_in(b"echo 'abc'\n")
    #         assert await expect_prompt(stream) == b"\r/ # "
    #         assert await expect_prompt(stream) == b"echo 'abc'\r\nabc\r\n/ # "
    #         await stream.write_in(b"exit 1\n")
    #         assert await expect_prompt(stream) == b"exit 1\r\n"

    #     ret = await jobs_service.exec_inspect(job, exec_id)
    #     assert ret["ExitCode"] == 1
    #     await platform_api_client.jobs.kill(job.id)
