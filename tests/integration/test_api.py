import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterator
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
from platform_monitoring.api import create_app
from platform_monitoring.config import Config, DockerConfig, PlatformApiConfig
from yarl import URL

from .conftest import ApiAddress, create_local_app_server
from .conftest_auth import _User


@dataclass(frozen=True)
class MonitoringApiEndpoints:
    address: ApiAddress

    @property
    def api_v1_endpoint(self) -> str:
        return f"http://{self.address.host}:{self.address.port}/api/v1"

    @property
    def ping_url(self) -> str:
        return f"{self.api_v1_endpoint}/ping"

    @property
    def secured_ping_url(self) -> str:
        return f"{self.api_v1_endpoint}/secured-ping"

    @property
    def endpoint(self) -> str:
        return f"{self.api_v1_endpoint}/jobs"

    def generate_top_url(self, job_id: str) -> str:
        return f"{self.endpoint}/{job_id}/top"

    def generate_log_url(self, job_id: str) -> str:
        return f"{self.endpoint}/{job_id}/log"

    def generate_save_url(self, job_id: str) -> str:
        return f"{self.endpoint}/{job_id}/save"


@dataclass(frozen=True)
class PlatformApiEndpoints:
    url: URL

    @property
    def endpoint(self) -> str:
        return str(self.url)

    @property
    def platform_config_url(self) -> str:
        return f"{self.endpoint}/config"

    @property
    def jobs_base_url(self) -> str:
        return f"{self.endpoint}/jobs"

    def generate_job_url(self, job_id: str) -> str:
        return f"{self.jobs_base_url}/{job_id}"


@pytest.fixture
async def monitoring_api(config: Config) -> AsyncIterator[MonitoringApiEndpoints]:
    app = await create_app(config)
    async with create_local_app_server(app, port=8080) as address:
        yield MonitoringApiEndpoints(address=address)


@pytest.fixture
async def platform_api(
    platform_api_config: PlatformApiConfig
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
async def infinite_job(
    platform_api: PlatformApiEndpoints,
    client: aiohttp.ClientSession,
    jobs_client: JobsClient,
    job_request_factory: Callable[[], Dict[str, Any]],
) -> AsyncIterator[str]:
    request_payload = job_request_factory()
    request_payload["container"]["command"] = "tail -f /dev/null"
    async with client.post(
        platform_api.jobs_base_url, headers=jobs_client.headers, json=request_payload
    ) as response:
        assert response.status == HTTPAccepted.status_code, await response.text()
        result = await response.json()
        job_id = result["id"]

    yield job_id

    await jobs_client.delete_job(job_id)


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


class TestTopApi:
    @pytest.mark.asyncio
    async def test_top_ok(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        await jobs_client.long_polling_by_job_id(job_id=infinite_job, status="running")
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
        await jobs_client.long_polling_by_job_id(job_id=infinite_job, status="running")

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

    @pytest.mark.asyncio
    async def test_log_no_auth_token_provided_unauthorized(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        await jobs_client.long_polling_by_job_id(job_id=infinite_job, status="running")

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

        await jobs_client.delete_job(job_id)


class TestSaveApi:
    @pytest.mark.asyncio
    async def test_save_no_permissions_forbidden(
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

        url = monitoring_api.generate_save_url(job_id)
        async with client.post(url, headers=user2.headers) as resp:
            assert resp.status == HTTPForbidden.status_code

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
        payload = {"container": {"image": "unknown:5000/ubuntu:latest"}}
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
    ) -> None:
        await jobs_client.delete_job(infinite_job)

        url = monitoring_api.generate_save_url(job_id=infinite_job)
        headers = jobs_client.headers
        payload = {
            "container": {"image": f"{config.registry.host}/ubuntu:{infinite_job}"}
        }
        async with client.post(url, headers=headers, json=payload) as resp:
            assert resp.status == HTTPOk.status_code, str(resp)
            chunks = [
                json.loads(chunk, encoding="utf-8")
                async for chunk in resp.content
                if chunk
            ]
            assert len(chunks) == 1
            assert "not running" in chunks[0]["error"]

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

            await jobs_client.long_polling_by_job_id(infinite_job, status="running")

            headers = jobs_client.headers
            image = f"{config.registry.host}/ubuntu:{infinite_job}"
            payload = {"container": {"image": image}}
            async with client.post(url, headers=headers, json=payload) as resp:
                assert resp.status == HTTPOk.status_code, str(resp)
                chunks = [
                    json.loads(chunk, encoding="utf-8")
                    async for chunk in resp.content
                    if chunk
                ]

                assert len(chunks) == 2

                chunk1 = chunks[0]["status"]
                assert f"Committing image {image}" in chunk1

                chunk2 = chunks[1]["error"]
                assert f"Failed to save job '{infinite_job}': DockerError(503" in chunk2
                assert "getsockopt: connection refused" in chunk2

    @pytest.mark.asyncio
    async def test_save_ok(
        self,
        platform_api: PlatformApiEndpoints,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
        config: Config,
    ) -> None:
        await jobs_client.long_polling_by_job_id(job_id=infinite_job, status="running")

        url = monitoring_api.generate_save_url(job_id=infinite_job)
        headers = jobs_client.headers
        image = f"{config.registry.host}/ubuntu:{infinite_job}"
        payload = {"container": {"image": image}}
        async with client.post(url, headers=headers, json=payload) as resp:
            assert resp.status == HTTPOk.status_code, str(resp)
            chunks = [
                json.loads(chunk, encoding="utf-8")
                async for chunk in resp.content
                if chunk
            ]

            assert isinstance(chunks, list), type(chunks)
            assert all(isinstance(s, dict) for s in chunks)
            assert len(chunks) >= 4  # 2 for commit(), >=2 for push()

            # here we rely on chunks to be received in correct order:
            assert f"Committing image {image}" in chunks[0]["status"], str(chunks)
            assert chunks[1]["status"] == "Committed", str(chunks)
            assert chunks[2]["status"] == (
                f"The push refers to repository [{config.registry.host}/ubuntu]"
            ), str(chunks)
            assert chunks[-1]["aux"]["Tag"] == infinite_job, str(chunks)
