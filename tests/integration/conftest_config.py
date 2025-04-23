import asyncio
from collections.abc import AsyncIterator, Callable

import pytest
from neuro_config_client import ConfigClient
from yarl import URL

from platform_monitoring.api import create_platform_api_client
from platform_monitoring.config import (
    PlatformApiConfig,
    PlatformAppsConfig,
    PlatformConfig,
)
from tests.integration.conftest import get_service_url


@pytest.fixture(scope="session")
async def cluster_name() -> str:
    return "default"


@pytest.fixture(scope="session")
def cluster_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("cluster")


@pytest.fixture(scope="session")
async def platform_config_url(
    platform_api_config: PlatformApiConfig,
    in_minikube: bool,  # noqa: FBT001
) -> URL:
    await asyncio.wait_for(_wait_for_platform_api_config(platform_api_config), 30)
    if in_minikube:
        return URL("http://platformconfig.default:8080")
    return URL(get_service_url("platformconfig", namespace="default"))


@pytest.fixture
def platform_config(
    platform_config_url: URL, token_factory: Callable[[str], str]
) -> PlatformConfig:
    return PlatformConfig(url=platform_config_url, token=token_factory("cluster"))


@pytest.fixture
def platform_apps(
    platform_config_url: URL, token_factory: Callable[[str], str]
) -> PlatformAppsConfig:
    return PlatformAppsConfig(
        url=URL("http://platform-apps"), token=token_factory("cluster")
    )


@pytest.fixture
async def platform_config_client(
    platform_config_url: URL, cluster_token: str
) -> AsyncIterator[ConfigClient]:
    async with ConfigClient(url=platform_config_url, token=cluster_token) as client:
        yield client


async def _wait_for_platform_api_config(
    platform_api_config: PlatformApiConfig, sleep_s: float = 0.1
) -> None:
    while True:
        import warnings

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            try:
                async with create_platform_api_client(
                    platform_api_config.url, platform_api_config.token
                ):
                    return
            except Exception:
                pass
            await asyncio.sleep(sleep_s)
