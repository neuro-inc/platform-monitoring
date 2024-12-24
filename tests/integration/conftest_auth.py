from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest
from jose import jwt
from neuro_auth_client import AuthClient, Permission
from yarl import URL

from platform_monitoring.config import PlatformAuthConfig

from .conftest import ProjectUser, get_service_url


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


@pytest.fixture()
async def auth_client(
    auth_config: PlatformAuthConfig,
) -> AsyncGenerator[AuthClient, None]:
    async with AuthClient(auth_config.url, auth_config.token) as client:
        await client.ping()
        yield client


@pytest.fixture()
async def share_job(
    auth_client: AuthClient, cluster_name: str
) -> Callable[[ProjectUser, ProjectUser, str, str], Awaitable[None]]:
    async def _impl(
        owner: ProjectUser, follower: ProjectUser, job_id: str, action: str = "read"
    ) -> None:
        permission = Permission(
            uri=f"job://{cluster_name}/{owner.org_name}/{owner.project_name}/{job_id}",
            action=action,
        )
        await auth_client.grant_user_permissions(
            follower.name, [permission], token=owner.token
        )

    return _impl
