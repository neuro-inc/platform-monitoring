import pytest
from aiobotocore.client import AioBaseClient
from apolo_kube_client import KubeClientSelector

from platform_monitoring.config import KubeConfig
from platform_monitoring.logs import (
    S3LogsMetadataService,
    S3LogsMetadataStorage,
    S3LogsService,
)


@pytest.fixture
def s3_logs_metadata_storage(
    s3_client: AioBaseClient, s3_logs_bucket: str
) -> S3LogsMetadataStorage:
    return S3LogsMetadataStorage(s3_client, s3_logs_bucket)


@pytest.fixture
def s3_logs_metadata_service(
    s3_client: AioBaseClient,
    s3_logs_metadata_storage: S3LogsMetadataStorage,
    kube_config: KubeConfig,
) -> S3LogsMetadataService:
    return S3LogsMetadataService(
        s3_client, s3_logs_metadata_storage, kube_config.namespace
    )


@pytest.fixture
def s3_log_service(
    kube_client_selector: KubeClientSelector,
    s3_client: AioBaseClient,
    s3_logs_metadata_service: S3LogsMetadataService,
) -> S3LogsService:
    return S3LogsService(kube_client_selector, s3_client, s3_logs_metadata_service)
