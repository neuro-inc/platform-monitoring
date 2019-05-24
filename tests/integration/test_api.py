from typing import AsyncIterator, NamedTuple

import aiohttp
import pytest
from aiohttp.web import HTTPOk
from platform_monitoring.api import create_app
from platform_monitoring.config import Config

from .conftest import ApiAddress, ApiRunner


class ApiConfig(NamedTuple):
    address: ApiAddress

    @property
    def endpoint(self) -> str:
        return f"http://{self.address.host}:{self.address.port}/api/v1"

    @property
    def ping_url(self) -> str:
        return self.endpoint + "/ping"

    @property
    def secured_ping_url(self) -> str:
        return self.endpoint + "/secured-ping"


@pytest.fixture
async def api(config: Config) -> AsyncIterator[ApiConfig]:
    app = await create_app(config)
    runner = ApiRunner(app, port=8080)
    mon_api_address = await runner.run()
    api_config = ApiConfig(address=mon_api_address)
    yield api_config
    await runner.close()


@pytest.fixture
async def client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(self, api: ApiConfig, client: aiohttp.ClientSession) -> None:
        async with client.get(api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
