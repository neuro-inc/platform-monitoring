import asyncio
import json
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

import pytest
from async_timeout import timeout
from platform_monitoring.config import KubeConfig
from platform_monitoring.kube_client import KubeClient, PodStatus


class MyKubeClient(KubeClient):

    # TODO (A Yushkovskiy, 30-May-2019) delete pods automatically

    async def create_pod(self, job_pod_descriptor: Dict[str, Any]) -> PodStatus:
        payload = await self._request(
            method="POST", url=self._pods_url, json=job_pod_descriptor
        )
        self._assert_resource_kind(expected_kind="Pod", payload=payload)
        return self._parse_pod_status(payload)

    async def delete_pod(self, pod_name: str, force: bool = False) -> PodStatus:
        url = self._generate_pod_url(pod_name)
        request_payload = None
        if force:
            request_payload = {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "gracePeriodSeconds": 0,
            }
        payload = await self._request(method="DELETE", url=url, json=request_payload)
        self._assert_resource_kind(expected_kind="Pod", payload=payload)
        return self._parse_pod_status(payload)

    def _parse_pod_status(self, payload: Dict[str, Any]) -> PodStatus:
        if "status" in payload:
            return PodStatus.from_primitive(payload["status"])
        raise ValueError(f"Missing pod status: `{payload}`")

    async def wait_pod_scheduled(
        self,
        pod_name: str,
        node_name: str,
        timeout_s: float = 5.0,
        interval_s: float = 1.0,
    ) -> None:
        try:
            async with timeout(timeout_s):
                while True:
                    raw_pod = await self.get_raw_pod(pod_name)
                    pod_has_node = raw_pod["spec"].get("nodeName") == node_name
                    pod_is_scheduled = "PodScheduled" in [
                        cond["type"]
                        for cond in raw_pod["status"].get("conditions", [])
                        if cond["status"]
                    ]
                    if pod_has_node and pod_is_scheduled:
                        return
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            pytest.fail("Pod unscheduled")


class MyPodDescriptor:
    def __init__(self, job_id: str, **kwargs: Dict[str, Any]) -> None:
        self._payload: Dict[str, Any] = {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": job_id, "labels": {"job": job_id}},
            "spec": {
                "containers": [
                    {
                        "name": job_id,
                        "image": "ubuntu",
                        "env": [],
                        "volumeMounts": [],
                        "terminationMessagePolicy": "FallbackToLogsOnError",
                        "args": ["true"],
                        "resources": {"limits": {"cpu": "10m", "memory": "32Mi"}},
                    }
                ],
                "volumes": [
                    {
                        "name": "storage",
                        "hostPath": {"path": "/tmp", "type": "Directory"},
                    }
                ],
                "restartPolicy": "Never",
                "imagePullSecrets": [],
                "tolerations": [],
            },
            **kwargs,
        }

    @property
    def payload(self) -> Dict[str, Any]:
        return self._payload

    @property
    def name(self) -> str:
        return self._payload["metadata"]["name"]


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
async def kube_config_user_payload(kube_config_payload: Dict[str, Any]) -> Any:
    user_name = "minikube"
    users = {user["name"]: user["user"] for user in kube_config_payload["users"]}
    return users[user_name]


@pytest.fixture(scope="session")
def cert_authority_data_pem(
    kube_config_cluster_payload: Dict[str, Any]
) -> Optional[str]:
    ca_path = kube_config_cluster_payload["certificate-authority"]
    if ca_path:
        return Path(ca_path).read_text()
    return None


@pytest.fixture
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
        namespace="default",
    )
    return kube_config


@pytest.fixture
async def kube_client(kube_config: KubeConfig) -> AsyncIterator[MyKubeClient]:
    # TODO (A Danshyn 06/06/18): create a factory method
    client = MyKubeClient(
        base_url=kube_config.endpoint_url,
        auth_type=kube_config.auth_type,
        cert_authority_data_pem=kube_config.cert_authority_data_pem,
        cert_authority_path=None,  # disabled, see `cert_authority_data_pem`
        auth_cert_path=kube_config.auth_cert_path,
        auth_cert_key_path=kube_config.auth_cert_key_path,
        namespace=kube_config.namespace,
        conn_timeout_s=kube_config.client_conn_timeout_s,
        read_timeout_s=kube_config.client_read_timeout_s,
        conn_pool_size=kube_config.client_conn_pool_size,
    )
    async with client:
        yield client
