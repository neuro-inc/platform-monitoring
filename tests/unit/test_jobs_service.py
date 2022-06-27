import datetime
import uuid
from collections.abc import Awaitable, Callable, Sequence
from decimal import Decimal
from typing import Any, Optional
from unittest import mock

import pytest
from neuro_config_client import (
    Cluster,
    ClusterStatus,
    ConfigClient,
    OrchestratorConfig,
    ResourcePoolType,
    ResourcePreset,
)
from neuro_sdk import Jobs as JobsClient

from platform_monitoring.container_runtime_client import ContainerRuntimeClientRegistry
from platform_monitoring.jobs_service import JobsService, NodeNotFoundException
from platform_monitoring.kube_client import KubeClient, Node, Pod


def create_node(node_pool_name: str, node_name: str) -> Node:
    return Node(
        {
            "metadata": {
                "name": node_name,
                "labels": {
                    "platform.neuromation.io/job": "true",
                    "platform.neuromation.io/nodepool": node_pool_name,
                },
            }
        }
    )


def get_pods_factory(
    *pods: Pod,
) -> Callable[[str, str], Awaitable[Sequence[Pod]]]:
    async def _get_pods(
        label_selector: str = "", field_selector: str = ""
    ) -> Sequence[Pod]:
        assert label_selector == "platform.neuromation.io/job"
        assert field_selector == (
            "status.phase!=Failed,status.phase!=Succeeded,status.phase!=Unknown"
        )
        return (*pods,)

    return _get_pods


def create_pod(
    node_name: Optional[str],
    cpu_m: int,
    memory: int,
    gpu: int = 0,
) -> Pod:
    job_id = f"job-{uuid.uuid4()}"
    resources = {
        "cpu": f"{cpu_m}m",
        "memory": f"{memory}",
    }
    if gpu:
        resources["nvidia.com/gpu"] = str(gpu)
    payload: dict[str, Any] = {
        "metadata": {
            "name": job_id,
            "labels": {"job": job_id, "platform.neuromation.io/job": job_id},
        },
        "spec": {"containers": [{"resources": {"requests": resources}}]},
        "status": {"phase": "Running"},
    }
    if node_name:
        payload["spec"]["nodeName"] = node_name
    return Pod(payload)


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
                ResourcePoolType(
                    name="minikube-cpu",
                    max_size=2,
                    available_cpu=1,
                    available_memory=2**30,
                ),
                ResourcePoolType(
                    name="minikube-gpu",
                    max_size=2,
                    available_cpu=1,
                    available_memory=2**30,
                    gpu=1,
                    gpu_model="nvidia-tesla-k80",
                ),
            ],
            resource_presets=[
                ResourcePreset(
                    name="cpu",
                    credits_per_hour=Decimal(10),
                    cpu=0.2,
                    memory=100 * 2**20,
                    resource_affinity=["minikube-cpu"],
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
                    name="gpu",
                    credits_per_hour=Decimal(10),
                    cpu=0.2,
                    memory=100 * 2**20,
                    gpu=1,
                    gpu_model="nvidia-tesla-k80",
                    resource_affinity=["minikube-gpu"],
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
    return mock.Mock(spec=JobsClient)


@pytest.fixture
def kube_client() -> mock.Mock:
    async def get_nodes(label_selector: str = "") -> Sequence[Node]:
        assert label_selector == "platform.neuromation.io/job"
        return [
            create_node("minikube-cpu", "minikube-cpu-1"),
            create_node("minikube-cpu", "minikube-cpu-2"),
            create_node("minikube-gpu", "minikube-gpu-1"),
            create_node("minikube-gpu", "minikube-gpu-2"),
        ]

    client = mock.Mock(spec=KubeClient)
    client.get_nodes.side_effect = get_nodes
    client.get_pods.side_effect = get_pods_factory()
    return client


@pytest.fixture
def service(
    config_client: ConfigClient, jobs_client: JobsClient, kube_client: KubeClient
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
            create_pod("minikube-gpu-1", cpu_m=100, memory=256 * 2**20, gpu=1),
            create_pod("minikube-cpu-2", cpu_m=50, memory=128 * 2**20),
        )

        result = await service.get_available_jobs_counts()

        assert result == {"cpu": 8, "gpu": 1, "cpu-p": 0}

    async def test_get_available_jobs_count_free_nodes(
        self, service: JobsService
    ) -> None:
        result = await service.get_available_jobs_counts()

        assert result == {"cpu": 10, "gpu": 2, "cpu-p": 0}

    async def test_get_available_jobs_count_busy_nodes(
        self, service: JobsService, kube_client: mock.Mock
    ) -> None:
        kube_client.get_pods.side_effect = get_pods_factory(
            create_pod("minikube-cpu-1", cpu_m=1000, memory=2**30),
            create_pod("minikube-cpu-2", cpu_m=1000, memory=2**30),
            create_pod("minikube-gpu-1", cpu_m=1000, memory=2**30, gpu=1),
            create_pod("minikube-gpu-2", cpu_m=1000, memory=2**30, gpu=1),
        )

        result = await service.get_available_jobs_counts()

        assert result == {"cpu": 0, "gpu": 0, "cpu-p": 0}

    async def test_get_available_jobs_count_for_pods_without_nodes(
        self, service: JobsService, kube_client: mock.Mock
    ) -> None:
        kube_client.get_pods.side_effect = get_pods_factory(
            create_pod(None, cpu_m=1000, memory=2**30)
        )

        result = await service.get_available_jobs_counts()

        assert result == {"cpu": 10, "gpu": 2, "cpu-p": 0}

    async def test_get_available_jobs_count_node_not_found(
        self, service: JobsService, kube_client: mock.Mock
    ) -> None:
        kube_client.get_pods.side_effect = get_pods_factory(
            create_pod("unknown", cpu_m=1000, memory=2**30)
        )

        with pytest.raises(NodeNotFoundException, match="Node 'unknown' was not found"):
            await service.get_available_jobs_counts()
