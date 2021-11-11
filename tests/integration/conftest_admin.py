from typing import AsyncIterator, Awaitable, Callable, Optional

import aiohttp
import pytest
from yarl import URL

from tests.integration.conftest import get_service_url, random_str
from tests.integration.conftest_auth import _User


@pytest.fixture(scope="session")
def admin_url(in_minikube: bool) -> URL:
    if in_minikube:
        platform_admin = "http://platformadmin:8080"
    else:
        platform_admin = get_service_url("platformadmin", namespace="default")
    return URL(platform_admin)


@pytest.fixture
async def admin_client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as client:
        yield client


@pytest.fixture
async def regular_user_factory(
    admin_url: URL,
    admin_client: aiohttp.ClientSession,
    admin_token: str,
    token_factory: Callable[[str], str],
    cluster_name: str,
) -> AsyncIterator[Callable[[Optional[str], Optional[str]], Awaitable[_User]]]:
    default_cluster_name = cluster_name

    async def _factory(
        name: Optional[str] = None, cluster_name: Optional[str] = None
    ) -> _User:
        name = name or f"user-{random_str(8)}"
        cluster_name = cluster_name or default_cluster_name
        async with admin_client.post(
            admin_url / "apis/admin/v1/users",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={"name": name, "email": f"{name}@neu.ro"},
        ) as resp:
            resp.raise_for_status()
        async with admin_client.post(
            admin_url / "apis/admin/v1/clusters" / cluster_name / "users",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={"user_name": name, "role": "user"},
        ) as resp:
            resp.raise_for_status()
        return _User(name=name, token=token_factory(name))

    yield _factory
