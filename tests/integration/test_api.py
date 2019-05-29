import asyncio
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Dict, Iterator

import aiohttp
import pytest
from aiohttp.web import HTTPOk
from aiohttp.web_exceptions import HTTPAccepted
from platform_monitoring.config import Config

from .auth import _User
from .conftest import ApiAddress, create_local_app_server


@dataclass(frozen=True)
class MonitoringApiConfig:
    address: ApiAddress

    @property
    def endpoint(self) -> str:
        return f"http://{self.address.host}:{self.address.port}/api/v1"

    @property
    def ping_url(self) -> str:
        return f"{self.endpoint}/ping"


@dataclass(frozen=True)
class PlatformApiConfig:
    endpoint: str

    @property
    def ping_url(self) -> str:
        return f"{self.endpoint}/ping"

    @property
    def platform_config_url(self) -> str:
        return f"{self.endpoint}/config"

    @property
    def jobs_base_url(self) -> str:
        return f"{self.endpoint}/jobs"

    def generate_job_url(self, job_id: str) -> str:
        return f"{self.jobs_base_url}/{job_id}"


@pytest.fixture
async def monitoring_api(config: Config) -> AsyncIterator[MonitoringApiConfig]:
    async with create_local_app_server(config, port=8080) as address:
        yield MonitoringApiConfig(address=address)


@pytest.fixture
async def platform_api(config: Config) -> AsyncIterator[PlatformApiConfig]:
    yield PlatformApiConfig(endpoint=str(config.platform_api.url))


@pytest.fixture
async def client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


class JobsClient:
    def __init__(
        self,
        platform_api_config: PlatformApiConfig,
        client: aiohttp.ClientSession,
        headers: Dict[str, str],
    ) -> None:
        self._platform_api_config = platform_api_config
        self._client = client
        self._headers = headers

    async def get_job_by_id(self, job_id: str) -> Dict[str, Any]:
        url = self._platform_api_config.generate_job_url(job_id)
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


@pytest.fixture
def jobs_client_factory(
    platform_api: PlatformApiConfig, client: aiohttp.ClientSession
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


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(
        self, monitoring_api: MonitoringApiConfig, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(monitoring_api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == "Pong"

    @pytest.mark.asyncio
    async def test_platform_ping(
        self, platform_api: PlatformApiConfig, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(platform_api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == ""

    @pytest.mark.asyncio
    async def test_platform_create_job(
        self,
        platform_api: PlatformApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = platform_api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as resp:
            assert resp.status == HTTPAccepted.status_code
            payload = await resp.json()
            job_id = payload["id"]
            assert payload["status"] == "pending"
            await jobs_client.long_polling_by_job_id(job_id, status="succeeded")
