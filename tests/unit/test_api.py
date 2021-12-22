from collections.abc import Callable
from unittest import mock

import pytest
from aiobotocore.client import AioBaseClient
from aioelasticsearch import Elasticsearch

from platform_monitoring.api import create_logs_service
from platform_monitoring.config import (
    Config,
    ContainerRuntimeConfig,
    LogsConfig,
    LogsStorageType,
    S3Config,
)
from platform_monitoring.kube_client import KubeClient
from platform_monitoring.logs import ElasticsearchLogsService, S3LogsService


@pytest.fixture
def kube_client() -> mock.Mock:
    return mock.Mock(spec=KubeClient)


@pytest.fixture
def config_factory() -> Callable[[LogsStorageType], Config]:
    def _factory(storage_type: LogsStorageType) -> Config:
        return Config(
            cluster_name="default",
            server=None,  # type: ignore
            platform_api=None,  # type: ignore
            platform_auth=None,  # type: ignore
            platform_config=None,  # type: ignore
            kube=None,  # type: ignore
            container_runtime=ContainerRuntimeConfig(name="docker"),
            registry=None,  # type: ignore
            cors=None,  # type: ignore
            logs=LogsConfig(storage_type=storage_type),
            s3=S3Config(
                region="us-east-1",
                access_key_id="access_key",
                secret_access_key="secret_key",
                job_logs_bucket_name="logs",
                job_logs_key_prefix_format="format",
            ),
        )

    return _factory


def test_create_es_logs_service(
    config_factory: Callable[[LogsStorageType], Config], kube_client: KubeClient
) -> None:
    config = config_factory(LogsStorageType.ELASTICSEARCH)
    result = create_logs_service(
        config, kube_client, es_client=mock.Mock(spec=Elasticsearch)
    )
    assert isinstance(result, ElasticsearchLogsService)


def test_create_s3_logs_service(
    config_factory: Callable[[LogsStorageType], Config], kube_client: KubeClient
) -> None:
    config = config_factory(LogsStorageType.S3)
    result = create_logs_service(
        config, kube_client, s3_client=mock.Mock(spec=AioBaseClient)
    )
    assert isinstance(result, S3LogsService)


def test_create_logs_service_raises(
    config_factory: Callable[[LogsStorageType], Config], kube_client: KubeClient
) -> None:
    config = config_factory(LogsStorageType.S3)
    with pytest.raises(AssertionError):
        create_logs_service(config, kube_client)

    config = config_factory(LogsStorageType.ELASTICSEARCH)
    with pytest.raises(AssertionError):
        create_logs_service(config, kube_client)
