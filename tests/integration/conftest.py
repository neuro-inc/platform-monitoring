import asyncio
import subprocess
import time
from dataclasses import dataclass
from typing import AsyncIterator, Callable, Iterator
from uuid import uuid1

import aiohttp
import aiohttp.web
import pytest
from async_generator import asynccontextmanager
from platform_monitoring.api import create_app
from platform_monitoring.config import (
    Config,
    PlatformApiConfig,
    PlatformAuthConfig,
    ServerConfig,
)
from yarl import URL


pytest_plugins = ["tests.integration.auth"]


@pytest.yield_fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """ This fixture fixes scope mismatch error with implicitly added "event_loop".
    see https://github.com/pytest-dev/pytest-asyncio/issues/68
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


def random_str(length: int = 8) -> str:
    return str(uuid1())[:length]


@pytest.fixture(scope="session")
async def platform_api_config(
    token_factory: Callable[[str], str]
) -> AsyncIterator[PlatformApiConfig]:
    platform_api_base = wait_for_service("platformapi", namespace="default")
    yield PlatformApiConfig(
        url=URL(platform_api_base) / "api/v1",
        token=token_factory("compute"),  # token is hard-coded in the yaml configuration
    )


@pytest.fixture
def config(
    auth_config: PlatformAuthConfig, platform_api_config: PlatformApiConfig
) -> Config:
    return Config(
        server=ServerConfig(host="0.0.0.0", port=8080),
        platform_auth=auth_config,
        platform_api=platform_api_config,
    )


@dataclass(frozen=True)
class ApiAddress:
    host: str
    port: int


@asynccontextmanager
async def create_local_app_server(
    server_config: ServerConfig, port: int = 8080
) -> AsyncIterator[ApiAddress]:
    app = await create_app(server_config)
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


def wait_for_service(  # type: ignore
    service_name: str, namespace: str = "kube-system"
) -> str:
    # ignore type because the linter does not know that `pytest.fail` throws an
    # exception, so it requires to `return None` explicitly, so that the method
    # will return `Optional[List[str]]` which is incorrect
    timeout_s = 60
    interval_s = 10

    while timeout_s:
        process = subprocess.run(
            ("minikube", "service", "-n", namespace, service_name, "--url"),
            stdout=subprocess.PIPE,
        )
        output = process.stdout
        if output:
            return output.decode().strip()
        time.sleep(interval_s)
        timeout_s -= interval_s

    pytest.fail(f"Service {service_name} is unavailable.")
