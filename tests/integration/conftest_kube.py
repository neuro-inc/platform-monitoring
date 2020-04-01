import asyncio
import json
import shlex
import subprocess
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

import pytest
from async_timeout import timeout
from platform_monitoring.config import KubeConfig
from platform_monitoring.kube_client import JobNotFoundException, KubeClient


class MyKubeClient(KubeClient):

    # TODO (A Yushkovskiy, 30-May-2019) delete pods automatically

    async def create_pod(self, job_pod_descriptor: Dict[str, Any]) -> str:
        payload = await self._request(
            method="POST", url=self._pods_url, json=job_pod_descriptor
        )
        self._assert_resource_kind(expected_kind="Pod", payload=payload)
        return self._parse_pod_status(payload)

    async def delete_pod(self, pod_name: str, force: bool = False) -> str:
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

    def _parse_pod_status(self, payload: Dict[str, Any]) -> str:
        if "status" in payload:
            return payload["status"]
        raise ValueError(f"Missing pod status: `{payload}`")

    async def wait_pod_is_terminated(
        self,
        pod_name: str,
        timeout_s: float = 10.0 * 60,
        interval_s: float = 1.0,
        allow_pod_not_exists: bool = False,
    ) -> None:
        try:
            async with timeout(timeout_s):
                while True:
                    try:
                        state = await self._get_raw_container_state(pod_name)
                    except JobNotFoundException:
                        # job's pod does not exist: maybe it's already garbage-collected
                        if allow_pod_not_exists:
                            return
                        raise
                    is_terminated = bool(state) and "terminated" in state
                    if is_terminated:
                        return
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            pytest.fail("Pod has not terminated yet")

    async def get_node_list(self) -> Dict[str, Any]:
        url = f"{self._api_v1_url}/nodes"
        return await self._request(method="GET", url=url)


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

    def set_image(self, image: str) -> None:
        self._payload["spec"]["containers"][0]["image"] = image

    def set_command(self, command: str) -> None:
        self._payload["spec"]["containers"][0]["args"] = shlex.split(command)

    @property
    def payload(self) -> Dict[str, Any]:
        return self._payload

    @property
    def name(self) -> str:
        return self._payload["metadata"]["name"]


@pytest.fixture(scope="session")
def kube_config_payload() -> Dict[str, Any]:
    result = subprocess.run(
        ["kubectl", "config", "view", "-o", "json"], stdout=subprocess.PIPE
    )
    payload_str = result.stdout.decode().rstrip()
    return json.loads(payload_str)


@pytest.fixture(scope="session")
def kube_config_cluster_payload(kube_config_payload: Dict[str, Any]) -> Any:
    cluster_name = "minikube"
    clusters = {
        cluster["name"]: cluster["cluster"]
        for cluster in kube_config_payload["clusters"]
    }
    return clusters[cluster_name]


@pytest.fixture(scope="session")
def kube_config_user_payload(kube_config_payload: Dict[str, Any]) -> Any:
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
