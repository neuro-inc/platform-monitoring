import asyncio
import logging
import subprocess
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Iterator
from uuid import uuid1

import aiohttp
import aiohttp.web
import pytest
from aioelasticsearch import Elasticsearch
from async_timeout import timeout
from neuromation.api import Client as PlatformApiClient
from platform_monitoring.api import (
    create_elasticsearch_client,
    create_platform_api_client,
)
from platform_monitoring.config import (
    Config,
    DockerConfig,
    ElasticsearchConfig,
    KubeConfig,
    PlatformApiConfig,
    PlatformAuthConfig,
    RegistryConfig,
    ServerConfig,
)
from yarl import URL


logger = logging.getLogger(__name__)


pytest_plugins = [
    "tests.integration.conftest_auth",
    "tests.integration.conftest_config",
    "tests.integration.conftest_kube",
]


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """ This fixture fixes scope mismatch error with implicitly added "event_loop".
    see https://github.com/pytest-dev/pytest-asyncio/issues/68
    """
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.set_debug(True)

    watcher = asyncio.SafeChildWatcher()  # type: ignore
    watcher.attach_loop(loop)
    asyncio.get_event_loop_policy().set_child_watcher(watcher)

    yield loop
    loop.close()


def random_str(length: int = 8) -> str:
    return str(uuid1())[:length]


@pytest.fixture
async def client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


async def wait_for_service(
    service_name: str,
    service_ping_url: URL,
    timeout_s: float = 30,
    interval_s: float = 1,
) -> None:
    async with timeout(timeout_s):
        while True:
            try:
                async with aiohttp.ClientSession() as client:
                    async with client.get(service_ping_url) as resp:
                        assert resp.status == aiohttp.web.HTTPOk.status_code
                        return
            except aiohttp.ClientError as e:
                logging.info(
                    f"Failed to ping service '{service_name}' "
                    f"via url '{service_ping_url}': {e}"
                )
                pass
            await asyncio.sleep(interval_s)


@pytest.fixture
# TODO (A Yushkovskiy, 05-May-2019) This fixture should have scope="session" in order
#  to be faster, but it causes mysterious errors `RuntimeError: Event loop is closed`
async def platform_api_config(
    token_factory: Callable[[str], str],
) -> AsyncIterator[PlatformApiConfig]:
    base_url = get_service_url("platformapi", namespace="default")
    assert base_url.startswith("http")
    url = URL(base_url) / "api/v1"
    await wait_for_service("platformapi", url / "ping")
    yield PlatformApiConfig(
        url=url,
        token=token_factory("compute"),  # token is hard-coded in the yaml configuration
    )


@pytest.fixture
async def platform_api_client(
    platform_api_config: PlatformApiConfig,
) -> AsyncIterator[PlatformApiClient]:
    async with create_platform_api_client(platform_api_config) as client:
        yield client


@pytest.fixture
# TODO (A Yushkovskiy, 05-May-2019) This fixture should have scope="session" in order
#  to be faster, but it causes mysterious errors `RuntimeError: Event loop is closed`
async def es_config(
    token_factory: Callable[[str], str]
) -> AsyncIterator[ElasticsearchConfig]:
    es_host = get_service_url("elasticsearch-logging", namespace="kube-system")
    yield ElasticsearchConfig(hosts=[es_host])


@pytest.fixture
async def es_client(es_config: ElasticsearchConfig) -> AsyncIterator[Elasticsearch]:
    """ Elasticsearch client that goes directly to elasticsearch-logging service
    without any authentication.
    """
    async with create_elasticsearch_client(es_config) as es_client:
        yield es_client


@pytest.fixture
async def registry_config() -> RegistryConfig:
    url = URL("http://localhost:5000")
    await wait_for_service("docker registry", url / "v2/", timeout_s=120)
    return RegistryConfig(url)


@pytest.fixture
def docker_config() -> DockerConfig:
    return DockerConfig()


@pytest.fixture
def config_factory(
    auth_config: PlatformAuthConfig,
    platform_api_config: PlatformApiConfig,
    es_config: ElasticsearchConfig,
    kube_config: KubeConfig,
    registry_config: RegistryConfig,
    docker_config: DockerConfig,
    cluster_name: str,
) -> Callable[..., Config]:
    def _f(**kwargs: Any) -> Config:
        defaults = dict(
            server=ServerConfig(host="0.0.0.0", port=8080),
            platform_auth=auth_config,
            platform_api=platform_api_config,
            elasticsearch=es_config,
            kube=kube_config,
            registry=registry_config,
            docker=docker_config,
            cluster_name=cluster_name,
        )
        kwargs = {**defaults, **kwargs}
        return Config(**kwargs)

    return _f


@pytest.fixture
def config(config_factory: Callable[..., Config],) -> Config:
    return config_factory()


@dataclass(frozen=True)
class ApiAddress:
    host: str
    port: int


@asynccontextmanager
async def create_local_app_server(
    app: aiohttp.web.Application, port: int = 8080
) -> AsyncIterator[ApiAddress]:
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
            url = output.decode().strip()
            # Sometimes `minikube service ... --url` returns a prefixed
            # string such as: "* https://127.0.0.1:8081/"
            start_idx = url.find("http")
            if start_idx > 0:
                url = url[start_idx:]
            return url
        time.sleep(interval_s)
        timeout_s -= interval_s

    pytest.fail(f"Service {service_name} is unavailable.")
