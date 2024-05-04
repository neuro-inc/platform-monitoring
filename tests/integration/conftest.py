import asyncio
import logging
import subprocess
import time
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid1

import aiohttp
import aiohttp.web
import pytest
from _pytest.fixtures import FixtureRequest
from aiobotocore.client import AioBaseClient
from aioelasticsearch import Elasticsearch
from async_timeout import timeout
from yarl import URL

from platform_monitoring.api import create_elasticsearch_client, create_s3_client
from platform_monitoring.config import (
    Config,
    ContainerRuntimeConfig,
    ElasticsearchConfig,
    KubeConfig,
    LogsConfig,
    LogsStorageType,
    PlatformApiConfig,
    PlatformAuthConfig,
    PlatformConfig,
    RegistryConfig,
    S3Config,
    ServerConfig,
)
from platform_monitoring.container_runtime_client import ContainerRuntimeClientRegistry
from platform_monitoring.logs import s3_client_error


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def in_docker() -> bool:
    return Path("/.dockerenv").is_file()


@pytest.fixture(scope="session")
def in_minikube(in_docker: bool) -> bool:  # noqa: FBT001
    return in_docker


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """This fixture fixes scope mismatch error with implicitly added "event_loop".
    see https://github.com/pytest-dev/pytest-asyncio/issues/68
    """
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.set_debug(True)

    watcher = asyncio.SafeChildWatcher()
    watcher.attach_loop(loop)
    asyncio.get_event_loop_policy().set_child_watcher(watcher)

    yield loop
    loop.close()


def random_str(length: int = 8) -> str:
    return str(uuid1())[:length]


@pytest.fixture(scope="session")
def minikube_ip() -> str:
    return subprocess.check_output(("minikube", "ip"), text=True).strip()


@pytest.fixture()
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
                    async with timeout(1):
                        async with client.get(service_ping_url) as resp:
                            assert resp.status == aiohttp.web.HTTPOk.status_code
                            return
            except Exception as e:
                logging.info(
                    "Failed to ping service '%s' via url '%s': %s",
                    service_name,
                    service_ping_url,
                    e,
                )
            await asyncio.sleep(interval_s)


@pytest.fixture()
# TODO (A Yushkovskiy, 05-May-2019) This fixture should have scope="session" in order
#  to be faster, but it causes mysterious errors `RuntimeError: Event loop is closed`
async def platform_api_config(
    request: FixtureRequest,
    in_minikube: bool,  # noqa: FBT001
    token_factory: Callable[[str], str],
) -> PlatformApiConfig:
    if in_minikube:
        base_url = "http://platformapi:8080"
    else:
        base_url = get_service_url("platformapi", namespace="default")
    assert base_url.startswith("http")
    url = URL(base_url) / "api/v1"
    await wait_for_service("platformapi", url / "ping", timeout_s=120)
    return PlatformApiConfig(
        url=url,
        token=token_factory("compute"),  # token is hard-coded in the yaml configuration
    )


@pytest.fixture()
async def container_runtime_config(in_minikube: bool) -> ContainerRuntimeConfig:  # noqa: FBT001
    if in_minikube:
        url = URL("http://platform-container-runtime:9000")
    else:
        url = URL(get_service_url("platform-container-runtime", namespace="default"))
    assert url
    await wait_for_service(
        "platform-container-runtime", url / "api/v1/ping", timeout_s=120
    )
    assert url.port
    return ContainerRuntimeConfig(port=url.port)


@pytest.fixture()
async def container_runtime_client_registry(
    container_runtime_config: ContainerRuntimeConfig,
) -> AsyncIterator[ContainerRuntimeClientRegistry]:
    async with ContainerRuntimeClientRegistry(
        container_runtime_port=container_runtime_config.port
    ) as registry:
        yield registry


@pytest.fixture()
# TODO (A Yushkovskiy, 05-May-2019) This fixture should have scope="session" in order
#  to be faster, but it causes mysterious errors `RuntimeError: Event loop is closed`
async def es_config(
    request: FixtureRequest,
    in_minikube: bool,  # noqa: FBT001
    token_factory: Callable[[str], str],
) -> ElasticsearchConfig:
    if in_minikube:
        es_host = "http://elasticsearch-logging:9200"
    else:
        es_host = get_service_url("elasticsearch-logging")
    async with Elasticsearch(hosts=[es_host]) as client:
        async with timeout(120):
            while True:
                try:
                    await client.ping()
                    break
                except Exception:
                    await asyncio.sleep(1)
    return ElasticsearchConfig(hosts=[es_host])


@pytest.fixture()
async def es_client(es_config: ElasticsearchConfig) -> AsyncIterator[Elasticsearch]:
    """Elasticsearch client that goes directly to elasticsearch-logging service
    without any authentication.
    """
    async with create_elasticsearch_client(es_config) as es_client:
        yield es_client


@pytest.fixture()
def s3_config() -> S3Config:
    s3_url = get_service_url(service_name="minio")
    return S3Config(
        region="minio",
        access_key_id="access_key",
        secret_access_key="secret_key",
        endpoint_url=URL(s3_url),
        job_logs_bucket_name="logs",
    )


@pytest.fixture()
async def s3_client(s3_config: S3Config) -> AsyncIterator[AioBaseClient]:
    async with create_s3_client(s3_config) as client:
        yield client


@pytest.fixture()
async def s3_logs_bucket(s3_config: S3Config, s3_client: AioBaseClient) -> str:
    try:
        await s3_client.create_bucket(Bucket=s3_config.job_logs_bucket_name)
        logger.info("Bucket %r created", s3_config.job_logs_bucket_name)
    except s3_client_error(409):
        pass
    return s3_config.job_logs_bucket_name


@pytest.fixture()
async def registry_config(request: FixtureRequest, in_minikube: bool) -> RegistryConfig:  # noqa: FBT001
    if in_minikube:
        external_url = URL("http://registry.kube-system")
    else:
        minikube_ip = request.getfixturevalue("minikube_ip")
        external_url = URL(f"http://{minikube_ip}:5000")
    await wait_for_service("docker registry", external_url / "v2/", timeout_s=120)
    # localhost will be insecure by default, so use that
    return RegistryConfig(URL("http://localhost:5000"))


@pytest.fixture()
def config_factory(
    auth_config: PlatformAuthConfig,
    platform_api_config: PlatformApiConfig,
    platform_config: PlatformConfig,
    s3_config: S3Config,
    kube_config: KubeConfig,
    registry_config: RegistryConfig,
    container_runtime_config: ContainerRuntimeConfig,
    cluster_name: str,
) -> Callable[..., Config]:
    def _f(**kwargs: Any) -> Config:
        defaults = {
            "cluster_name": cluster_name,
            "server": ServerConfig(host="0.0.0.0", port=8080),
            "platform_auth": auth_config,
            "platform_api": platform_api_config,
            "platform_config": platform_config,
            "s3": s3_config,
            "logs": LogsConfig(
                storage_type=LogsStorageType.S3, cleanup_interval_sec=0.5
            ),
            "kube": kube_config,
            "registry": registry_config,
            "container_runtime": container_runtime_config,
        }
        kwargs = {**defaults, **kwargs}
        return Config(**kwargs)

    return _f


@pytest.fixture()
def config(config_factory: Callable[..., Config]) -> Config:
    return config_factory()


@pytest.fixture()
def config_s3_storage(
    config_factory: Callable[..., Config], s3_config: S3Config
) -> Config:
    return config_factory(
        logs=LogsConfig(storage_type=LogsStorageType.S3, cleanup_interval_sec=0.5),
        s3=s3_config,
    )


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


def get_service_url(service_name: str, namespace: str = "default") -> str:
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
        url = output.decode().strip()
        if url:
            # Sometimes `minikube service ... --url` returns a prefixed
            # string such as: "* https://127.0.0.1:8081/"
            start_idx = url.find("http")
            if start_idx > 0:
                url = url[start_idx:]
            return url
        time.sleep(interval_s)
        timeout_s -= interval_s

    pytest.fail(f"Service {service_name} is unavailable.")
