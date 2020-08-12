from typing import Any, AsyncIterator, Callable, Dict

import aiohttp
import pytest
from _pytest.fixtures import FixtureRequest
from aiohttp.web_exceptions import HTTPCreated, HTTPNoContent
from yarl import URL

from platform_monitoring.config_client import ConfigClient
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
        "registry": {
            "url": "http://localhost:5000",
            "email": "registry@neuromation.io",
        },
        "orchestrator": {
            "kubernetes": {
                "url": "http://localhost:8001",
                "ca_data": "certificate",
                "auth_type": "none",
                "token": None,
                "namespace": "default",
                "node_label_gpu": "cloud.google.com/gke-accelerator",
                "node_label_preemptible": "cloud.google.com/gke-preemptible",
            },
            "is_http_ingress_secure": True,
            "job_hostname_template": "{job_id}.jobs.neu.ro",
            "resource_pool_types": [{}],
        },
        "ssh": {"server": "ssh.platform.dev.neuromation.io"},
        "monitoring": {"url": "http://platformapi/api/v1/jobs"},
    }


@pytest.fixture
def _cloud_provider_payload() -> Dict[str, Any]:
    return {
        "type": "on_prem",
        "node_pools": [
            {
                "machine_type": "minikube",
                "min_size": 1,
                "max_size": 1,
                "cpu": 1.0,
                "available_cpu": 1.0,
                "memory_mb": 1024,
                "available_memory_mb": 1024,
            },
        ],
    }


@pytest.fixture
def platform_config_url(in_minikube: bool) -> URL:
    if in_minikube:
        return URL("http://platformconfig.default:8080")
    return URL(get_service_url("platformconfig", namespace="default"))


@pytest.fixture
async def platform_config_client(
    platform_config_url: URL, cluster_name: str, cluster_token: str
) -> AsyncIterator[ConfigClient]:
    async with ConfigClient(
        api_url=platform_config_url / "api/v1", token=cluster_token,
    ) as client:
        yield client


@pytest.fixture
async def _cluster(
    request: FixtureRequest,
    client: aiohttp.ClientSession,
    platform_config_url: URL,
    cluster_token: str,
    _cluster_payload: Dict[str, Any],
    _cloud_provider_payload: Dict[str, Any],
) -> AsyncIterator[str]:
    cluster_name = _cluster_payload["name"]
    try:
        response = await client.post(
            platform_config_url / "api/v1/clusters",
            headers={"Authorization": f"Bearer {cluster_token}"},
            json=_cluster_payload,
        )
        assert response.status == HTTPCreated.status_code, await response.text()
        response = await client.put(
            platform_config_url / "api/v1/clusters" / cluster_name / "cloud_provider",
            headers={"Authorization": f"Bearer {cluster_token}"},
            json=_cloud_provider_payload,
        )
        assert response.status == HTTPNoContent.status_code, await response.text()
        yield cluster_name
    finally:
        response = await client.delete(
            platform_config_url / "api/v1/clusters" / cluster_name,
            headers={"Authorization": f"Bearer {cluster_token}"},
        )
        response_text = await response.text()
        assert response.status == HTTPNoContent.status_code, response_text
