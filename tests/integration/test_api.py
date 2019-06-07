import asyncio
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterator

import aiohttp
import pytest
from aiohttp.web import HTTPOk
from aiohttp.web_exceptions import HTTPAccepted, HTTPNoContent, HTTPUnauthorized
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
    def secured_ping_url(self) -> str:
        return f"{self.api_v1_endpoint}/secured-ping"

    @property
    def endpoint(self) -> str:
        return f"{self.api_v1_endpoint}/jobs"


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


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(
        self, monitoring_api: MonitoringApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(monitoring_api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == "Pong"
            await asyncio.sleep(10000())

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
    async def test_secured_ping_unauthorized(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
    ) -> None:
        async with client.get(monitoring_api.secured_ping_url) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    @pytest.mark.asyncio
    async def test_platform_create_job(
        self,
        platform_api: PlatformApiEndpoints,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
    ) -> None:
        url = platform_api.jobs_base_url
        async with client.post(
            url, headers=jobs_client.headers, json=job_submit
        ) as resp:
            assert resp.status == HTTPAccepted.status_code
            payload = await resp.json()
            job_id = payload["id"]
            assert payload["status"] == "pending"
            await jobs_client.long_polling_by_job_id(job_id, status="succeeded")
        await jobs_client.delete_job(job_id)
