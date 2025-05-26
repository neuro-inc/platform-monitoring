from collections.abc import AsyncGenerator, Callable

import pytest
from jose import jwt
from neuro_auth_client import AuthClient
from yarl import URL

from platform_monitoring.config import PlatformAuthConfig

from .conftest import get_service_url


@pytest.fixture(scope="session")
def token_factory() -> Callable[[str], str]:
    def _factory(name: str) -> str:
        payload = {"identity": name}
        return jwt.encode(payload, "secret", algorithm="HS256")

    return _factory


@pytest.fixture(scope="session")
def admin_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("admin")


@pytest.fixture(scope="session")
def compute_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("compute")


@pytest.fixture(scope="session")
def auth_config(
    token_factory: Callable[[str], str],
    in_minikube: bool,  # noqa: FBT001
) -> PlatformAuthConfig:
    if in_minikube:
        platform_auth = "http://platformauthapi:8080"
    else:
        platform_auth = get_service_url("platformauthapi", namespace="default")
    return PlatformAuthConfig(
        url=URL(platform_auth),
        token=token_factory("compute"),  # token is hard-coded in the yaml configuration
    )


@pytest.fixture
async def auth_client(
    auth_config: PlatformAuthConfig,
) -> AsyncGenerator[AuthClient]:
    async with AuthClient(auth_config.url, auth_config.token) as client:
        await client.ping()
        yield client
