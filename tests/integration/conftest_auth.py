from dataclasses import dataclass
from typing import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterator,
    Optional,
)

import pytest
from aiohttp.hdrs import AUTHORIZATION
from async_generator import asynccontextmanager
from jose import jwt
from neuro_auth_client import AuthClient, User as AuthClientUser
from platform_monitoring.config import PlatformAuthConfig
from yarl import URL

from tests.integration.conftest import get_service_url, random_str


@pytest.fixture(scope="session")
def token_factory() -> Iterator[Callable[[str], str]]:
    def _factory(name: str) -> str:
        payload = {"identity": name}
        return jwt.encode(payload, "secret", algorithm="HS256")

    yield _factory


@pytest.fixture(scope="session")
def admin_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("admin")


@pytest.fixture(scope="session")
async def auth_config(
    token_factory: Callable[[str], str]
) -> AsyncIterator[PlatformAuthConfig]:
    platform_auth = get_service_url("platformauthapi", namespace="default")
    yield PlatformAuthConfig(
        url=URL(platform_auth),
        token=token_factory("compute"),  # token is hard-coded in the yaml configuration
    )


@asynccontextmanager
async def create_auth_client(
    cfg: PlatformAuthConfig
) -> AsyncGenerator[AuthClient, None]:
    async with AuthClient(url=cfg.url, token=cfg.token) as client:
        yield client


@pytest.fixture
async def auth_client(
    auth_config: PlatformAuthConfig
) -> AsyncGenerator[AuthClient, None]:
    async with create_auth_client(auth_config) as client:
        await client.ping()
        yield client


@dataclass(frozen=True)
class _User(AuthClientUser):
    token: str = ""

    @property
    def headers(self) -> Dict[str, str]:
        return {AUTHORIZATION: f"Bearer {self.token}"}


@pytest.fixture
async def regular_user_factory(
    auth_client: AuthClient, token_factory: Callable[[str], str], admin_token: str
) -> AsyncIterator[Callable[[Optional[str]], Awaitable[_User]]]:
    async def _factory(name: Optional[str] = None) -> _User:
        if not name:
            name = f"user-{random_str(4)}"
        user = AuthClientUser(name=name)
        await auth_client.add_user(user, token=admin_token)
        return _User(name=user.name, token=token_factory(user.name))  # type: ignore

    yield _factory


@pytest.fixture
async def regular_user(regular_user_factory: Callable[[], Awaitable[_User]]) -> _User:
    return await regular_user_factory()
