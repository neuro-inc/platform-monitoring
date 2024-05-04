from collections.abc import Awaitable, Callable

import aiohttp
import pytest
from yarl import URL

from tests.integration.conftest import get_service_url, random_str
from tests.integration.conftest_auth import _User


@pytest.fixture(scope="session")
def admin_url(in_minikube: bool) -> URL:  # noqa: FBT001
    if in_minikube:
        platform_admin = "http://platformadmin:8080"
    else:
        platform_admin = get_service_url("platformadmin", namespace="default")
    return URL(platform_admin)


@pytest.fixture(scope="session")
def regular_user_factory(
    admin_url: URL,
    admin_token: str,
    token_factory: Callable[[str], str],
    cluster_name: str,
) -> Callable[[str | None, str | None], Awaitable[_User]]:
    default_cluster_name = cluster_name

    async def _factory(
        name: str | None = None, cluster_name: str | None = None
    ) -> _User:
        name = name or f"user-{random_str(8)}"
        cluster_name = cluster_name or default_cluster_name

        async with aiohttp.ClientSession() as client:
            async with client.post(
                admin_url / "apis/admin/v1/users",
                headers={"Authorization": f"Bearer {admin_token}"},
                json={"name": name, "email": f"{name}@neu.ro"},
            ) as resp:
                resp.raise_for_status()
            async with client.post(
                admin_url / "apis/admin/v1/clusters" / cluster_name / "users",
                headers={"Authorization": f"Bearer {admin_token}"},
                json={"user_name": name, "role": "user"},
            ) as resp:
                resp.raise_for_status()
            async with client.post(
                admin_url / "apis/admin/v1/clusters" / cluster_name / "projects",
                headers={"Authorization": f"Bearer {token_factory(name)}"},
                json={"name": name},
            ) as resp:
                resp.raise_for_status()

        return _User(name=name, token=token_factory(name))

    return _factory


@pytest.fixture(scope="session")
async def regular_user1(
    regular_user_factory: Callable[..., Awaitable[_User]],
) -> _User:
    return await regular_user_factory()


@pytest.fixture(scope="session")
async def regular_user2(
    regular_user_factory: Callable[..., Awaitable[_User]],
) -> _User:
    return await regular_user_factory()
