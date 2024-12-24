from collections.abc import Awaitable, Callable
from typing import Protocol

import aiohttp
import pytest
from yarl import URL

from .conftest import ProjectUser, get_service_url, random_str


@pytest.fixture(scope="session")
def admin_url(in_minikube: bool) -> URL:  # noqa: FBT001
    if in_minikube:
        platform_admin = "http://platformadmin:8080"
    else:
        platform_admin = get_service_url("platformadmin", namespace="default")
    return URL(platform_admin)


class UserFactory(Protocol):
    async def __call__(
        self,
        user_name: str | None = None,
        cluster_name: str | None = None,
        org_name: str | None = None,
        project_name: str | None = None,
    ) -> ProjectUser: ...


@pytest.fixture(scope="session")
def regular_user_factory(
    admin_url: URL,
    admin_token: str,
    token_factory: Callable[[str], str],
    cluster_name: str,
) -> UserFactory:
    default_cluster_name = cluster_name

    async def _factory(
        user_name: str | None = None,
        cluster_name: str | None = None,
        org_name: str | None = None,
        project_name: str | None = None,
    ) -> ProjectUser:
        random_suffix = random_str(8)
        cluster_name = cluster_name or default_cluster_name
        org_name = org_name or f"org-{random_suffix}"
        project_name = project_name or f"project-{random_suffix}"

        admin_user_name = f"admin-user-{random_suffix}"
        admin_user_token = token_factory(admin_user_name)

        user_name = user_name or f"user-{random_suffix}"
        user_token = token_factory(user_name)

        cluster_admin_url = admin_url / "apis/admin/v1/clusters" / cluster_name

        async with aiohttp.ClientSession() as client:
            async with client.post(
                admin_url / "apis/admin/v1/users",
                headers={"Authorization": f"Bearer {admin_token}"},
                json={
                    "name": admin_user_name,
                    "email": f"{admin_user_name}@neu.ro",
                },
            ) as resp:
                resp.raise_for_status()
            async with client.post(
                cluster_admin_url / "users",
                headers={"Authorization": f"Bearer {admin_token}"},
                json={"user_name": admin_user_name, "role": "manager"},
            ) as resp:
                resp.raise_for_status()

            async with client.post(
                admin_url / "apis/admin/v1/users",
                headers={"Authorization": f"Bearer {admin_token}"},
                json={"name": user_name, "email": f"{user_name}@neu.ro"},
            ) as resp:
                resp.raise_for_status()

            async with client.post(
                admin_url / "apis/admin/v1/orgs",
                headers={"Authorization": f"Bearer {admin_user_token}"},
                json={"name": org_name},
            ) as resp:
                resp.raise_for_status()
            async with client.patch(
                admin_url / "apis/admin/v1/orgs" / org_name / "balance",
                headers={"Authorization": f"Bearer {admin_token}"},
                json={"credits": "1000"},
            ) as resp:
                resp.raise_for_status()
            async with client.post(
                cluster_admin_url / "orgs",
                headers={"Authorization": f"Bearer {admin_user_token}"},
                json={"org_name": org_name},
            ) as resp:
                resp.raise_for_status()

            async with client.post(
                admin_url / "apis/admin/v1/orgs" / org_name / "users",
                headers={"Authorization": f"Bearer {admin_user_token}"},
                json={"user_name": user_name, "role": "user"},
            ) as resp:
                resp.raise_for_status()

            async with client.post(
                cluster_admin_url / "orgs" / org_name / "projects",
                headers={"Authorization": f"Bearer {user_token}"},
                json={"name": project_name},
            ) as resp:
                resp.raise_for_status()

        return ProjectUser(
            name=user_name,
            token=user_token,
            cluster_name=cluster_name,
            org_name=org_name,
            project_name=project_name,
        )

    return _factory


@pytest.fixture(scope="session")
async def regular_user1(
    regular_user_factory: Callable[..., Awaitable[ProjectUser]],
) -> ProjectUser:
    return await regular_user_factory()


@pytest.fixture(scope="session")
async def regular_user2(
    regular_user_factory: Callable[..., Awaitable[ProjectUser]],
) -> ProjectUser:
    return await regular_user_factory()
