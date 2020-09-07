from typing import Callable
from unittest import mock

import pytest
from aiobotocore.client import AioBaseClient
from aioelasticsearch import Elasticsearch

from platform_monitoring.api import create_log_reader_factory
from platform_monitoring.config import Config, LogsConfig, LogsStorageType, S3Config
from platform_monitoring.kube_client import KubeClient
from platform_monitoring.utils import ElasticsearchLogReaderFactory, S3LogReaderFactory


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
            docker=None,  # type: ignore
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


def test_create_es_log_reader_factory(
    config_factory: Callable[[LogsStorageType], Config], kube_client: KubeClient
) -> None:
    config = config_factory(LogsStorageType.ELASTICSEARCH)
    result = create_log_reader_factory(
        config, kube_client, es_client=mock.Mock(spec=Elasticsearch)
    )
    assert isinstance(result, ElasticsearchLogReaderFactory)


def test_create_s3_log_reader_factory(
    config_factory: Callable[[LogsStorageType], Config], kube_client: KubeClient
) -> None:
    config = config_factory(LogsStorageType.S3)
    result = create_log_reader_factory(
        config,
        kube_client,
        s3_client=mock.Mock(spec=AioBaseClient),
    )
    assert isinstance(result, S3LogReaderFactory)


def test_create_log_reader_factory_raises(
    config_factory: Callable[[LogsStorageType], Config], kube_client: KubeClient
) -> None:
    config = config_factory(LogsStorageType.S3)
    with pytest.raises(AssertionError):
        create_log_reader_factory(config, kube_client)

    config = config_factory(LogsStorageType.ELASTICSEARCH)
    with pytest.raises(AssertionError):
        create_log_reader_factory(config, kube_client)
