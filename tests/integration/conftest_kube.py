from __future__ import annotations

import asyncio
import json
import subprocess
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from _pytest.fixtures import FixtureRequest
from apolo_kube_client import (
    KubeClientAuthType as ApoloKubeClientAuthType,
    KubeClientProxy,
    KubeClientSelector,
    KubeConfig as ApoloKubeConfig,
    ResourceNotFound,
    V1Container,
    V1HostPathVolumeSource,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
    V1Volume,
)

from platform_monitoring.config import KubeClientAuthType, KubeConfig
from platform_monitoring.kube_client import (
    Node,
    get_container_status,
)


@pytest.fixture(scope="session")
def in_docker() -> bool:
    return Path("/.dockerenv").is_file()


@pytest.fixture(scope="session")
def in_minikube(in_docker: bool) -> bool:  # noqa: FBT001
    return in_docker


@pytest.fixture(scope="session")
def kube_config_payload() -> dict[str, Any]:
    result = subprocess.run(
        ["kubectl", "config", "view", "-o", "json"], stdout=subprocess.PIPE
    )
    payload_str = result.stdout.decode().rstrip()
    return json.loads(payload_str)


@pytest.fixture(scope="session")
def kube_config_cluster_payload(kube_config_payload: dict[str, Any]) -> Any:
    cluster_name = "minikube"
    clusters = {
        cluster["name"]: cluster["cluster"]
        for cluster in kube_config_payload["clusters"]
    }
    return clusters[cluster_name]


@pytest.fixture(scope="session")
def kube_config_user_payload(kube_config_payload: dict[str, Any]) -> Any:
    user_name = "minikube"
    users = {user["name"]: user["user"] for user in kube_config_payload["users"]}
    return users[user_name]


@pytest.fixture(scope="session")
def cert_authority_data_pem(kube_config_cluster_payload: dict[str, Any]) -> str | None:
    ca_path = kube_config_cluster_payload["certificate-authority"]
    if ca_path:
        return Path(ca_path).read_text()
    return None


@pytest.fixture
async def kube_config(request: FixtureRequest, in_minikube: bool) -> KubeConfig:  # noqa: FBT001
    if in_minikube:
        return KubeConfig(
            endpoint_url="https://kubernetes.default:443",
            cert_authority_data_pem=Path(
                "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
            ).read_text(),
            auth_type=KubeClientAuthType.TOKEN,
            token=Path(
                "/var/run/secrets/kubernetes.io/serviceaccount/token"
            ).read_text(),
            namespace="default",
        )
    cluster = request.getfixturevalue("kube_config_cluster_payload")
    user = request.getfixturevalue("kube_config_user_payload")
    ca_data_pem = request.getfixturevalue("cert_authority_data_pem")
    return KubeConfig(
        endpoint_url=cluster["server"],
        cert_authority_data_pem=ca_data_pem,
        auth_cert_path=user["client-certificate"],
        auth_cert_key_path=user["client-key"],
        namespace="default",
    )


@pytest.fixture
def apolo_kube_config(request: FixtureRequest, *, in_minikube: bool) -> ApoloKubeConfig:
    if in_minikube:
        return ApoloKubeConfig(
            endpoint_url="https://kubernetes.default:443",
            cert_authority_data_pem=Path(
                "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
            ).read_text(),
            auth_type=ApoloKubeClientAuthType.TOKEN,
            token=Path(
                "/var/run/secrets/kubernetes.io/serviceaccount/token"
            ).read_text(),
        )
    cluster = request.getfixturevalue("kube_config_cluster_payload")
    user = request.getfixturevalue("kube_config_user_payload")
    ca_data_pem = request.getfixturevalue("cert_authority_data_pem")
    return ApoloKubeConfig(
        endpoint_url=cluster["server"],
        cert_authority_data_pem=ca_data_pem,
        auth_type=ApoloKubeClientAuthType.CERTIFICATE,
        auth_cert_path=user["client-certificate"],
        auth_cert_key_path=user["client-key"],
    )


@pytest.fixture
async def kube_client_selector(
    apolo_kube_config: ApoloKubeConfig,
) -> AsyncIterator[KubeClientSelector]:
    async with KubeClientSelector(config=apolo_kube_config) as kube_client_selector:
        yield kube_client_selector


@pytest.fixture
async def _kube_node(kube_client_selector: KubeClientSelector) -> Node:
    nodes = await kube_client_selector.host_client.core_v1.node.get_list()
    assert len(nodes.items) == 1, "Should be exactly one minikube node"
    return Node.from_model(nodes.items[0])


@pytest.fixture
async def kube_node_name(_kube_node: Node) -> str:
    assert _kube_node.metadata.name
    return _kube_node.metadata.name


@pytest.fixture
async def kube_container_runtime(_kube_node: Node) -> str:
    version = _kube_node.status.node_info.container_runtime_version
    end = version.find("://")
    return version[0:end]


async def wait_pod_is_deleted(
    kube_client: KubeClientProxy,
    pod_name: str,
    timeout_s: float = 10.0 * 60,
    interval_s: float = 1.0,
) -> None:
    try:
        async with asyncio.timeout(timeout_s):
            while True:
                try:
                    await kube_client.core_v1.pod.get(pod_name)
                except ResourceNotFound:
                    return
                else:
                    await asyncio.sleep(interval_s)
    except TimeoutError:
        pytest.fail(f"Pod {pod_name} has not deleted yet")


async def wait_pod_is_terminated(
    kube_client: KubeClientProxy,
    pod_name: str,
    container_name: str | None = None,
    timeout_s: float = 10.0 * 60,
    interval_s: float = 1.0,
    *,
    allow_pod_not_exists: bool = False,
) -> None:
    try:
        async with asyncio.timeout(timeout_s):
            while True:
                try:
                    state = await get_container_status(
                        kube_client, pod_name, container_name
                    )
                except ResourceNotFound:
                    # job's pod does not exist: maybe it's already garbage-collected
                    if allow_pod_not_exists:
                        return
                    raise
                if state.is_terminated:
                    return
                await asyncio.sleep(interval_s)
    except TimeoutError:
        pytest.fail(f"Pod {pod_name} has not terminated yet")


async def wait_container_is_restarted(
    kube_client: KubeClientProxy,
    name: str,
    count: int = 1,
    *,
    timeout_s: float = 10.0 * 60,
    interval_s: float = 1.0,
) -> None:
    try:
        async with asyncio.timeout(timeout_s):
            while True:
                status = await get_container_status(kube_client, name)
                if status.restart_count >= count:
                    break
                await asyncio.sleep(interval_s)
    except TimeoutError:
        pytest.fail(f"Container {name} has not restarted yet")


@pytest.fixture
def job_pod() -> V1Pod:
    job_id = f"job-{uuid4()}"
    return V1Pod(
        metadata=V1ObjectMeta(
            name=job_id,
            labels={"job": job_id},
        ),
        spec=V1PodSpec(
            tolerations=[],
            image_pull_secrets=[],
            restart_policy="Never",
            volumes=[
                V1Volume(
                    name="storage",
                    host_path=V1HostPathVolumeSource(path="/tmp", type="Directory"),
                )
            ],
            containers=[
                V1Container(
                    name=job_id,
                    image="ubuntu:20.10",
                    env=[],
                    volume_mounts=[],
                    termination_message_policy="FallbackToLogsOnError",
                    args=[
                        "true",
                    ],
                    resources=V1ResourceRequirements(
                        limits={"cpu": "100m", "memory": "128Mi"}
                    ),
                )
            ],
        ),
    )
