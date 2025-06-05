import datetime
import uuid
from collections.abc import Awaitable, Callable, Sequence
from decimal import Decimal
from typing import Any
from unittest import mock

import pytest
from apolo_api_client import ApiClient
from neuro_config_client import (
    Cluster,
    ClusterStatus,
    ConfigClient,
    OrchestratorConfig,
    ResourcePoolType,
    ResourcePreset,
)

from platform_monitoring.container_runtime_client import ContainerRuntimeClientRegistry
from platform_monitoring.jobs_service import JobsService
from platform_monitoring.kube_client import KubeClient, Node, Pod


def create_node(
    node_pool_name: str,
    node_name: str,
    cpu: float,
    memory: int,
    nvidia_gpu: int = 0,
    amd_gpu: int = 0,
) -> Node:
    return Node.from_primitive(
        {
            "metadata": {
                "name": node_name,
                "labels": {
                    "platform.neuromation.io/nodepool": node_pool_name,
                },
            },
            "status": {
                "allocatable": {
                    "cpu": str(cpu),
                    "memory": str(memory),
                    "nvidia.com/gpu": str(nvidia_gpu),
                    "amd.com/gpu": str(amd_gpu),
                },
                "nodeInfo": {"containerRuntimeVersion": "containerd"},
            },
        }
    )


def get_pods_factory(
    *pods: Pod,
) -> Callable[[bool, str, str], Awaitable[Sequence[Pod]]]:
    async def _get_pods(
        all_namespaces: bool = False,  # noqa: FBT001 FBT002
        label_selector: str | None = None,
        field_selector: str | None = None,
    ) -> Sequence[Pod]:
        assert all_namespaces is True
        assert label_selector is None
        assert field_selector == (
            "status.phase!=Failed,status.phase!=Succeeded,status.phase!=Unknown"
        )
        return (*pods,)

    return _get_pods


def create_pod(
    node_name: str | None,
    cpu_m: int,
    memory: int,
    nvidia_gpu: int = 0,
    amd_gpu: int = 0,
) -> Pod:
    job_id = f"job-{uuid.uuid4()}"
    resources = {
        "cpu": f"{cpu_m}m",
        "memory": f"{memory}",
    }
    if nvidia_gpu:
        resources["nvidia.com/gpu"] = str(nvidia_gpu)
    if amd_gpu:
        resources["amd.com/gpu"] = str(amd_gpu)
    payload: dict[str, Any] = {
        "metadata": {
            "name": job_id,
            "labels": {"job": job_id, "platform.neuromation.io/job": job_id},
        },
        "spec": {
            "containers": [
                {"name": "container_name", "resources": {"requests": resources}}
            ]
        },
        "status": {"phase": "Running"},
    }
    if node_name:
        payload["spec"]["nodeName"] = node_name
    return Pod.from_primitive(payload)


@pytest.fixture
def cluster() -> Cluster:
    return Cluster(
        name="default",
        status=ClusterStatus.DEPLOYED,
        created_at=datetime.datetime(2022, 4, 6),
        orchestrator=OrchestratorConfig(
            job_hostname_template="",
            job_internal_hostname_template="",
            job_fallback_hostname="",
            job_schedule_timeout_s=1,
            job_schedule_scale_up_timeout_s=1,
            resource_pool_types=[
                ResourcePoolType(name="minikube-cpu", max_size=2, cpu=1, memory=2**30),
                ResourcePoolType(
                    name="minikube-gpu",
                    max_size=2,
                    cpu=1,
                    memory=2**30,
                    nvidia_gpu=1,
                    amd_gpu=2,
                ),
            ],
            resource_presets=[
                ResourcePreset(
                    name="cpu",
                    credits_per_hour=Decimal(10),
                    cpu=0.2,
                    memory=100 * 2**20,
                    available_resource_pool_names=["minikube-cpu"],
                ),
                ResourcePreset(
                    name="cpu-p",
                    credits_per_hour=Decimal(10),
                    cpu=0.2,
                    memory=100 * 2**20,
                    scheduler_enabled=True,
                    preemptible_node=True,
                ),
                ResourcePreset(
                    name="nvidia-gpu",
                    credits_per_hour=Decimal(10),
                    cpu=0.2,
                    memory=100 * 2**20,
                    nvidia_gpu=1,
                    available_resource_pool_names=["minikube-gpu"],
                ),
                ResourcePreset(
                    name="amd-gpu",
                    credits_per_hour=Decimal(10),
                    cpu=0.2,
                    memory=100 * 2**20,
                    amd_gpu=1,
                    available_resource_pool_names=["minikube-gpu"],
                ),
            ],
        ),
    )


def get_cluster_factory(cluster: Cluster) -> Callable[[str], Awaitable[Cluster]]:
    async def get_cluster(name: str) -> Cluster:
        return cluster

    return get_cluster


@pytest.fixture
def config_client(cluster: Cluster) -> mock.Mock:
    client = mock.Mock(spec=ConfigClient)
    client.get_cluster.side_effect = get_cluster_factory(cluster)
    return client


@pytest.fixture
def jobs_client() -> mock.Mock:
    return mock.Mock(spec=ApiClient)


@pytest.fixture
def kube_client() -> mock.Mock:
    async def get_nodes(label_selector: str = "") -> Sequence[Node]:
        assert label_selector == "platform.neuromation.io/nodepool"
        return [
            create_node("minikube-cpu", "minikube-cpu-1", cpu=1, memory=2**30),
            create_node("minikube-cpu", "minikube-cpu-2", cpu=1, memory=2**30),
            create_node(
                "minikube-gpu",
                "minikube-gpu-1",
                cpu=1,
                memory=2**30,
                nvidia_gpu=1,
                amd_gpu=2,
            ),
            create_node(
                "minikube-gpu",
                "minikube-gpu-2",
                cpu=1,
                memory=2**30,
                nvidia_gpu=1,
                amd_gpu=2,
            ),
        ]

    client = mock.Mock(spec=KubeClient)
    client.get_nodes.side_effect = get_nodes
    client.get_pods.side_effect = get_pods_factory()
    return client


@pytest.fixture
def service(
    config_client: ConfigClient, jobs_client: ApiClient, kube_client: KubeClient
) -> JobsService:
    return JobsService(
        config_client=config_client,
        jobs_client=jobs_client,
        kube_client=kube_client,
        container_runtime_client_registry=ContainerRuntimeClientRegistry(
            container_runtime_port=9000
        ),
        cluster_name="default",
    )


class TestJobsService:
    async def test_get_available_jobs_count(
        self, service: JobsService, kube_client: mock.Mock
    ) -> None:
        kube_client.get_pods.side_effect = get_pods_factory(
            create_pod("minikube-cpu-1", cpu_m=50, memory=128 * 2**20),
            create_pod("minikube-gpu-1", cpu_m=100, memory=256 * 2**20, nvidia_gpu=1),
            create_pod("minikube-cpu-2", cpu_m=50, memory=128 * 2**20),
            create_pod("minikube-gpu-1", cpu_m=100, memory=256 * 2**20, amd_gpu=1),
        )

        result = await service.get_available_jobs_counts()

        assert result == {"cpu": 8, "nvidia-gpu": 1, "amd-gpu": 3, "cpu-p": 0}

    async def test_get_available_jobs_count_free_nodes(
        self, service: JobsService
    ) -> None:
        result = await service.get_available_jobs_counts()

        assert result == {"cpu": 10, "nvidia-gpu": 2, "amd-gpu": 4, "cpu-p": 0}

    async def test_get_available_jobs_count_busy_nodes(
        self, service: JobsService, kube_client: mock.Mock
    ) -> None:
        kube_client.get_pods.side_effect = get_pods_factory(
            create_pod("minikube-cpu-1", cpu_m=1000, memory=2**30),
            create_pod("minikube-cpu-2", cpu_m=1000, memory=2**30),
            create_pod("minikube-gpu-1", cpu_m=1000, memory=2**30, nvidia_gpu=1),
            create_pod("minikube-gpu-2", cpu_m=1000, memory=2**30, nvidia_gpu=1),
        )

        result = await service.get_available_jobs_counts()

        assert result == {"cpu": 0, "nvidia-gpu": 0, "amd-gpu": 0, "cpu-p": 0}

    async def test_get_available_jobs_count_for_pods_without_nodes(
        self, service: JobsService, kube_client: mock.Mock
    ) -> None:
        kube_client.get_pods.side_effect = get_pods_factory(
            create_pod(None, cpu_m=1000, memory=2**30)
        )

        result = await service.get_available_jobs_counts()

        assert result == {"cpu": 10, "nvidia-gpu": 2, "amd-gpu": 4, "cpu-p": 0}
