import asyncio
from typing import AsyncIterator, Callable

import pytest
from neuro_config_client import ConfigClient
from yarl import URL

from platform_monitoring.api import create_platform_api_client
from platform_monitoring.config import PlatformApiConfig, PlatformConfig
from tests.integration.conftest import get_service_url


@pytest.fixture
async def cluster_name(platform_api_config: PlatformApiConfig) -> str:
    await asyncio.wait_for(_wait_for_platform_api_config(platform_api_config), 30)
    return "default"


@pytest.fixture(scope="session")
def cluster_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("cluster")


@pytest.fixture
def platform_config_url(in_minikube: bool) -> URL:
    if in_minikube:
        return URL("http://platformconfig.default:8080")
    return URL(get_service_url("platformconfig", namespace="default"))


@pytest.fixture
def platform_config(
    platform_config_url: URL, token_factory: Callable[[str], str]
) -> PlatformConfig:
    return PlatformConfig(url=platform_config_url, token=token_factory("cluster"))


@pytest.fixture
@pytest.mark.usefixtures("cluster_name")
async def platform_config_client(
    platform_config_url: URL, cluster_token: str
) -> AsyncIterator[ConfigClient]:
    async with ConfigClient(url=platform_config_url, token=cluster_token) as client:
        yield client


async def _wait_for_platform_api_config(
    platform_api_config: PlatformApiConfig, sleep_s: float = 0.1
) -> None:
    while True:
        try:
            async with create_platform_api_client(platform_api_config):
                return
        except Exception:
            pass
        await asyncio.sleep(sleep_s)
