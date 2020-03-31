from typing import Any, AsyncIterator, Callable, Dict

import aiohttp
import pytest
from aiohttp.web_exceptions import HTTPCreated, HTTPNoContent
from yarl import URL

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
            "host": {"mount_path": "/host_mount_path"},
            "url": "http://platformapi/api/v1/storage",
        },
        "registry": {
            "url": "https://platformregistry",
            "email": "registry@neuromation.io",
        },
        "orchestrator": {
            "kubernetes": {
                "url": "http://localhost:8001",
                "ca_data": "certificate",
                "auth_type": "token",
                "token": "token",
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
async def _cluster(
    client: aiohttp.ClientSession, cluster_token: str, _cluster_payload: Dict[str, Any]
) -> AsyncIterator[str]:
    cluster_name = _cluster_payload["name"]
    platform_config_url = URL(get_service_url("platformconfig", namespace="default"))

    try:
        response = await client.post(
            platform_config_url / "api/v1/clusters",
            headers={"Authorization": f"Bearer {cluster_token}"},
            json=_cluster_payload,
        )
        response_text = await response.text()
        assert response.status == HTTPCreated.status_code, response_text
        yield cluster_name
    finally:
        response = await client.delete(
            platform_config_url / "api/v1/clusters" / cluster_name,
            headers={"Authorization": f"Bearer {cluster_token}"},
        )
        response_text = await response.text()
        assert response.status == HTTPNoContent.status_code, response_text
