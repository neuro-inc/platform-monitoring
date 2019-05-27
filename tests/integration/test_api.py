from dataclasses import dataclass
from typing import AsyncIterator

import aiohttp
import pytest
from aiohttp.web import HTTPOk
from platform_monitoring.config import Config

from .conftest import ApiAddress, create_local_app_server


@dataclass(frozen=True)
class ApiConfig:
    address: ApiAddress

    @property
    def endpoint(self) -> str:
        return f"http://{self.address.host}:{self.address.port}/api/v1"

    @property
    def ping_url(self) -> str:
        return f"{self.endpoint}/ping"


@pytest.fixture
async def api(config: Config) -> AsyncIterator[ApiConfig]:
    async with create_local_app_server(config, port=8080) as api_config:
        yield ApiConfig(api_config)


@pytest.fixture
async def client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(self, api: ApiConfig, client: aiohttp.ClientSession) -> None:
        async with client.get(api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == "Pong"
