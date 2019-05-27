from typing import AsyncIterator

import aiohttp
import pytest
from aiohttp.web import HTTPOk
from platform_monitoring.config import Config

from .conftest import ApiAddress, create_local_app_server


class ApiConfigBase:
    def __init__(self, address: ApiAddress) -> None:
        self._address = address

    @property
    def endpoint(self) -> str:
        return f"http://{self._address.host}:{self._address.port}/api/v1"


class MonitoringApiConfig(ApiConfigBase):
    @property
    def ping_url(self) -> str:
        return f"{self.endpoint}/ping"


@pytest.fixture
async def api(config: Config) -> AsyncIterator[MonitoringApiConfig]:
    async with create_local_app_server(config, port=8080) as api_config:
        yield MonitoringApiConfig(api_config)


@pytest.fixture
async def client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(
        self, api: MonitoringApiConfig, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == "Pong"
