from dataclasses import dataclass
from typing import AsyncIterator

import aiohttp
import aiohttp.web
import pytest
from async_generator import asynccontextmanager
from platform_monitoring.api import create_app
from platform_monitoring.config import Config, ServerConfig


@pytest.fixture
def config() -> Config:
    return Config(server=ServerConfig(host="0.0.0.0", port=8080))


@dataclass(frozen=True)
class ApiAddress:
    host: str
    port: int


@asynccontextmanager
async def create_local_app_server(
    config: Config, port: int = 8080
) -> AsyncIterator[ApiAddress]:
    app = await create_app(config)
    runner = aiohttp.web.AppRunner(app)
    try:
        await runner.setup()
        api_address = ApiAddress("0.0.0.0", port)
        site = aiohttp.web.TCPSite(runner, api_address.host, api_address.port)
        await site.start()
        yield api_address
    finally:
        await runner.shutdown()
        await runner.cleanup()
