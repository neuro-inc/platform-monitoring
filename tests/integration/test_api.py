import asyncio
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Dict, Iterator

import aiohttp
import pytest
from aiohttp.web import HTTPOk
from aiohttp.web_exceptions import HTTPAccepted, HTTPNoContent
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
    async with create_local_app_server(config, port=8080) as address:
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
        headers: Dict[str, str],
    ) -> None:
        self._platform_api = platform_api
        self._client = client
        self._headers = headers

    async def get_job_by_id(self, job_id: str) -> Dict[str, Any]:
        url = self._platform_api.generate_job_url(job_id)
        async with self._client.get(url, headers=self._headers) as response:
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
        async with self._client.delete(url, headers=self._headers) as response:
            if assert_success:
                assert response.status == HTTPNoContent.status_code


@pytest.fixture
def jobs_client_factory(
    platform_api: PlatformApiEndpoints, client: aiohttp.ClientSession
) -> Iterator[Callable[[_User], JobsClient]]:
    def impl(user: _User) -> JobsClient:
        return JobsClient(platform_api, client, headers=user.headers)

    yield impl


@pytest.fixture
def jobs_client(
    jobs_client_factory: Callable[[_User], JobsClient], regular_user: _User
) -> JobsClient:
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
    regular_user: _User,
    jobs_client: JobsClient,
    job_request_factory: Callable[[], Dict[str, Any]],
) -> AsyncIterator[str]:
    request_payload = job_request_factory()
    request_payload["container"]["command"] = "tail -f /dev/null"
    async with client.post(
        platform_api.jobs_base_url, headers=regular_user.headers, json=request_payload
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
    async def test_job_top_silently_wait_when_job_pending(
        self,
        monitoring_api: MonitoringApiEndpoints,
        platform_api: PlatformApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user: _User,
        jobs_client: JobsClient,
        job_submit: Dict[str, Any],
    ) -> None:
        command = 'bash -c "for i in {1..10}; do echo $i; sleep 1; done"'
        job_submit["container"]["command"] = command
        url = platform_api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as resp:
            assert resp.status == HTTPAccepted.status_code
            payload = await resp.json()
            job_id = payload["id"]
            assert payload["status"] == "pending"

        job_top_url = monitoring_api.generate_top_url(job_id)
        async with client.ws_connect(job_top_url, headers=regular_user.headers) as ws:
            while True:
                job = await jobs_client.get_job_by_id(job_id=job_id)
                assert job["status"] == "pending"

                # silently waiting for a job becomes running
                msg = await ws.receive()
                job = await jobs_client.get_job_by_id(job_id=job_id)
                assert job["status"] == "running"
                assert msg.type == aiohttp.WSMsgType.TEXT

                break

        await jobs_client.delete_job(job_id=job_id)
