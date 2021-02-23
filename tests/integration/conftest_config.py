import asyncio
from typing import Any, AsyncIterator, Callable, Dict

import aiohttp
import pytest
from aiohttp.web_exceptions import HTTPCreated, HTTPNoContent
from platform_config_client import ConfigClient
from yarl import URL

from platform_monitoring.api import create_platform_api_client
from platform_monitoring.config import PlatformApiConfig, PlatformConfig
from tests.integration.conftest import get_service_url


@pytest.fixture
def cluster_name(_cluster: str) -> str:
    return _cluster


@pytest.fixture(scope="session")
def cluster_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("cluster")


@pytest.fixture
def _cluster_payload() -> Dict[str, Any]:
    return {
        "name": "default",
        "storage": {
            "host": {"mount_path": "/tmp"},
            "url": "http://platformapi/api/v1/storage",
        },
        "blob_storage": {"url": "http://platformapi/api/v1/blob"},
        "registry": {
            "url": "http://localhost:5000",
            "email": "registry@neuromation.io",
        },
        "orchestrator": {
            "kubernetes": {
                "url": "http://localhost:8001",
                "auth_type": "none",
                "namespace": "default",
                "node_label_gpu": "cloud.google.com/gke-accelerator",
                "node_label_preemptible": "cloud.google.com/gke-preemptible",
                "node_label_node_pool": "platform.neuromation.io/nodepool",
                "node_label_job": "platform.neuromation.io/job",
            },
            "is_http_ingress_secure": True,
            "job_hostname_template": "{job_id}.jobs.neu.ro",
            "resource_pool_types": [
                {
                    "name": "minikube",
                    "min_size": 1,
                    "max_size": 1,
                    "cpu": 1.0,
                    "available_cpu": 1.0,
                    "memory_mb": 1024,
                    "available_memory_mb": 1024,
                }
            ],
            "resource_presets": [
                {
                    "name": "cpu-small",
                    "credits_per_hour": "0.0",
                    "cpu": 0.1,
                    "memory_mb": 100,
                }
            ],
        },
        "monitoring": {"url": "http://platformapi/api/v1/jobs"},
        "secrets": {"url": "http://platformapi/api/v1/secrets"},
        "metrics": {"url": "http://platformapi/api/v1/metrics"},
        "disks": {"url": "http://platformapi/api/v1/disk"},
        "ingress": {"acme_environment": "staging"},
    }


@pytest.fixture
def platform_config_url(in_minikube: bool) -> URL:
    if in_minikube:
        return URL("http://platformconfig.default:8080")
    return URL(get_service_url("platformconfig", namespace="default"))


@pytest.fixture
def platform_config(
    platform_config_url: URL, token_factory: Callable[[str], str]
) -> PlatformConfig:
    return PlatformConfig(url=platform_config_url, token=token_factory("cluster"))


@pytest.fixture
async def platform_config_client(
    platform_config_url: URL, cluster_name: str, cluster_token: str
) -> AsyncIterator[ConfigClient]:
    async with ConfigClient(url=platform_config_url, token=cluster_token) as client:
        yield client


@pytest.fixture
async def _cluster(
    client: aiohttp.ClientSession,
    platform_api_config: PlatformApiConfig,
    platform_config_url: URL,
    cluster_token: str,
    _cluster_payload: Dict[str, Any],
) -> AsyncIterator[str]:
    cluster_name = _cluster_payload["name"]
    try:
        response = await client.post(
            platform_config_url / "api/v1/clusters",
            headers={"Authorization": f"Bearer {cluster_token}"},
            json=_cluster_payload,
        )
        assert response.status == HTTPCreated.status_code, await response.text()
        await asyncio.wait_for(_wait_for_platform_api_config(platform_api_config), 30)
        yield cluster_name
    finally:
        response = await client.delete(
            platform_config_url / "api/v1/clusters" / cluster_name,
            headers={"Authorization": f"Bearer {cluster_token}"},
        )
        response_text = await response.text()
        assert response.status == HTTPNoContent.status_code, response_text


async def _wait_for_platform_api_config(
    platform_api_config: PlatformApiConfig, sleep_s: float = 0.1
) -> None:
    while True:
        try:
            async with create_platform_api_client(platform_api_config):
                return
        except Exception:
            pass
        await asyncio.sleep(sleep_s)
