import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterator
from unittest import mock
from uuid import uuid4

import aiohttp
import pytest
from aiohttp.web import HTTPOk
from aiohttp.web_exceptions import HTTPAccepted, HTTPNoContent
from platform_monitoring.api import create_app
from platform_monitoring.config import Config, PlatformApiConfig
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
    def endpoint(self) -> str:
        return f"{self.api_v1_endpoint}/jobs"

    def generate_top_url(self, job_id: str) -> str:
        return f"{self.endpoint}/{job_id}/top"


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
    jobs_client_factory: Callable[[_User], JobsClient],
    regular_user_factory: Callable[..., Awaitable[_User]],
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
        assert isinstance(job_id, str)

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
    async def test_job_top(
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
    async def test_job_top_non_running_job(
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
    async def test_job_top_non_existing_job(
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
        async with client.ws_connect(url, headers=headers) as ws:
            # TODO move this ws communication to JobClient
            msg = await ws.receive()
            assert msg.type == aiohttp.WSMsgType.ERROR

            msg2 = await ws.receive()
            assert msg2.type == aiohttp.WSMsgType.CLOSED
            assert msg2.data is None

    @pytest.mark.asyncio
    async def test_job_top_silently_wait_when_job_pending(
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
    async def test_job_top_close_when_job_succeeded(
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
