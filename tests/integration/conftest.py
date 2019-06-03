import asyncio
import json
import logging
import subprocess
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterator, Optional
from uuid import uuid1

import aiohttp
import aiohttp.web
import pytest
from async_generator import asynccontextmanager
from async_timeout import timeout
from platform_monitoring.api import create_app
from platform_monitoring.config import (
    Config,
    ElasticsearchConfig,
    KubeConfig,
    PlatformApiConfig,
    PlatformAuthConfig,
    ServerConfig,
)
from yarl import URL


logger = logging.getLogger(__name__)

pytest_plugins = ["tests.integration.auth"]


@pytest.fixture(scope="session")
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
async def client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture(scope="session")
async def wait_for_service(
    client: aiohttp.ClientSession
) -> AsyncIterator[Callable[..., Awaitable[None]]]:
    async def _wait(
        service_name: str,
        service_ping_url: URL,
        timeout_s: float = 30,
        interval_s: float = 1,
    ) -> None:
        async with timeout(timeout_s):
            while True:
                try:
                    async with client.get(service_ping_url) as resp:
                        assert resp.status == aiohttp.web.HTTPOk.status_code
                        break
                except aiohttp.ClientError as e:
                    logging.info(
                        f"Failed to ping service '{service_name}' "
                        f"via url '{service_ping_url}': {e}"
                    )
                    pass
                await asyncio.sleep(interval_s)

    yield _wait


@pytest.fixture(scope="session")
async def platform_api_config(
    token_factory: Callable[[str], str],
    wait_for_service: Callable[..., Awaitable[None]],
) -> AsyncIterator[PlatformApiConfig]:
    base_url = get_service_url("platformapi", namespace="default")
    url = URL(base_url) / "api/v1"
    await wait_for_service("platformapi", url / "ping")
    yield PlatformApiConfig(
        url=url,
        token=token_factory("compute"),  # token is hard-coded in the yaml configuration
    )


@pytest.fixture(scope="session")
async def elasticsearch_config(
    token_factory: Callable[[str], str]
) -> AsyncIterator[ElasticsearchConfig]:
    es_host = get_service_url("elasticsearch-logging", namespace="kube-system")
    yield ElasticsearchConfig(hosts=[es_host])


@pytest.fixture(scope="session")
async def kube_config_payload() -> Dict[str, Any]:
    process = await asyncio.create_subprocess_exec(
        "kubectl", "config", "view", "-o", "json", stdout=asyncio.subprocess.PIPE
    )
    output, _ = await process.communicate()
    payload_str = output.decode().rstrip()
    return json.loads(payload_str)


@pytest.fixture(scope="session")
async def kube_config_cluster_payload(kube_config_payload: Dict[str, Any]) -> Any:
    cluster_name = "minikube"
    clusters = {
        cluster["name"]: cluster["cluster"]
        for cluster in kube_config_payload["clusters"]
    }
    return clusters[cluster_name]


@pytest.fixture(scope="session")
async def kube_config(
    kube_config_cluster_payload: Dict[str, Any],
    kube_config_user_payload: Dict[str, Any],
    cert_authority_data_pem: Optional[str],
) -> KubeConfig:
    cluster = kube_config_cluster_payload
    user = kube_config_user_payload
    kube_config = KubeConfig(
        endpoint_url=cluster["server"],
        cert_authority_data_pem=cert_authority_data_pem,
        auth_cert_path=user["client-certificate"],
        auth_cert_key_path=user["client-key"],
        namespace="platformapi-tests",
    )
    return kube_config


@pytest.fixture
def config(
    auth_config: PlatformAuthConfig,
    platform_api_config: PlatformApiConfig,
    elasticsearch_config: ElasticsearchConfig,
) -> Config:
    return Config(
        server=ServerConfig(host="0.0.0.0", port=8080),
        platform_auth=auth_config,
        platform_api=platform_api_config,
        elasticsearch=elasticsearch_config,
        orchestrator=kube_config,
    )


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


def get_service_url(  # type: ignore
    service_name: str, namespace: str = "default"
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
