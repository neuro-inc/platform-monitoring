import uuid
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence
from unittest import mock

import pytest
from neuromation.api.jobs import Jobs as JobsClient

from platform_monitoring.config import DockerConfig
from platform_monitoring.config_client import Cluster, ConfigClient
from platform_monitoring.jobs_service import JobsService, NodeNotFoundException, Preset
from platform_monitoring.kube_client import KubeClient, Node, Pod, PodPhase


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
) -> Callable[[str, Sequence[PodPhase]], Awaitable[Sequence[Pod]]]:
    async def _get_pods(
        label_selector: str = "", phases: Sequence[PodPhase] = ()
    ) -> Sequence[Pod]:
        assert label_selector == "platform.neuromation.io/job"
        assert phases == (PodPhase.PENDING, PodPhase.RUNNING)
        return (*pods,)

    return _get_pods


def create_pod(
    node_name: Optional[str], cpu_m: int, memory_mb: int, gpu: int = 0
) -> Pod:
    job_id = f"job-{uuid.uuid4()}"
    resources = {
        "cpu": f"{cpu_m}m",
        "memory": f"{memory_mb}Mi",
    }
    if gpu:
        resources["nvidia.com/gpu"] = str(gpu)
    payload: Dict[str, Any] = {
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
def cluster_payload() -> Dict[str, Any]:
    return {
        "name": "default",
        "cloud_provider": {
            "zones": ["us-east-1a", "us-east-1b"],
            "node_pools": [
                {
                    "name": "minikube-cpu",
                    "max_size": 1,
                    "available_cpu": 1,
                    "available_memory_mb": 1024,
                },
                {
                    "name": "minikube-cpu-p",
                    "max_size": 1,
                    "available_cpu": 1,
                    "available_memory_mb": 1024,
                    "is_preemptible": True,
                },
                {
                    "name": "minikube-gpu",
                    "max_size": 1,
                    "available_cpu": 1,
                    "available_memory_mb": 1024,
                    "gpu": 1,
                    "gpu_model": "nvidia-tesla-k80",
                },
            ],
        },
    }


def get_cluster_factory(
    cluster_payload: Dict[str, Any]
) -> Callable[[str], Awaitable[Cluster]]:
    async def get_cluster(name: str) -> Cluster:
        return Cluster(cluster_payload)

    return get_cluster


@pytest.fixture
def config_client(cluster_payload: Dict[str, Any]) -> mock.Mock:
    client = mock.Mock(spec=ConfigClient)
    client.get_cluster.side_effect = get_cluster_factory(cluster_payload)
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
            create_node("minikube-cpu-p", "minikube-cpu-p-1"),
            create_node("minikube-gpu", "minikube-gpu-1"),
            create_node("minikube-gpu", "minikube-gpu-2"),
        ]

    client = mock.Mock(spec=KubeClient)
    client.get_nodes.side_effect = get_nodes
    client.get_pods.side_effect = get_pods_factory()
    return client


@pytest.fixture
def service(
    config_client: ConfigClient, jobs_client: JobsClient, kube_client: KubeClient,
) -> JobsService:
    return JobsService(
        config_client=config_client,
        jobs_client=jobs_client,
        kube_client=kube_client,
        docker_config=DockerConfig(),
        cluster_name="default",
    )


class TestJobsService:
    @pytest.mark.asyncio
    async def test_get_available_jobs_count(
        self, service: JobsService, kube_client: mock.Mock
    ) -> None:
        kube_client.get_pods.side_effect = get_pods_factory(
            create_pod("minikube-cpu-1", cpu_m=50, memory_mb=128),
            create_pod("minikube-gpu-1", cpu_m=100, memory_mb=256, gpu=1),
            create_pod("minikube-cpu-1", cpu_m=50, memory_mb=128),
        )

        result = await service.get_available_jobs_counts(
            {
                "cpu": Preset(cpu=0.2, memory_mb=100),
                "cpu-p": Preset(cpu=0.2, memory_mb=100, is_preemptible=True),
                "gpu": Preset(
                    cpu=0.2, memory_mb=100, gpu=1, gpu_model="nvidia-tesla-k80"
                ),
                "gpu-unknown": Preset(
                    cpu=0.2, memory_mb=100, gpu=1, gpu_model="nvidia-gtx-1080i"
                ),
            }
        )
        assert result == {"cpu": 9, "cpu-p": 19, "gpu": 1, "gpu-unknown": 0}

    @pytest.mark.asyncio
    async def test_get_available_jobs_count_when_cluster_without_zones(
        self,
        service: JobsService,
        config_client: mock.Mock,
        kube_client: mock.Mock,
        cluster_payload: Dict[str, Any],
    ) -> None:
        del cluster_payload["cloud_provider"]["zones"]
        config_client.get_cluster.side_effect = get_cluster_factory(cluster_payload)

        kube_client.get_pods.side_effect = get_pods_factory(
            create_pod("minikube-cpu-1", cpu_m=100, memory_mb=256),
            create_pod("minikube-gpu-1", cpu_m=100, memory_mb=256, gpu=1),
        )

        result = await service.get_available_jobs_counts(
            {
                "cpu": Preset(cpu=0.2, memory_mb=100),
                "cpu-p": Preset(cpu=0.2, memory_mb=100, is_preemptible=True),
                "gpu": Preset(
                    cpu=0.2, memory_mb=100, gpu=1, gpu_model="nvidia-tesla-k80"
                ),
                "gpu-unknown": Preset(
                    cpu=0.2, memory_mb=100, gpu=1, gpu_model="nvidia-gtx-1080i"
                ),
            }
        )
        assert result == {"cpu": 4, "cpu-p": 9, "gpu": 0, "gpu-unknown": 0}

    @pytest.mark.asyncio
    async def test_get_available_jobs_count_free_nodes(
        self, service: JobsService
    ) -> None:
        result = await service.get_available_jobs_counts(
            {
                "cpu": Preset(cpu=0.2, memory_mb=100),
                "cpu-p": Preset(cpu=0.2, memory_mb=100, is_preemptible=True),
                "gpu": Preset(
                    cpu=0.2, memory_mb=100, gpu=1, gpu_model="nvidia-tesla-k80"
                ),
            }
        )
        assert result == {"cpu": 10, "cpu-p": 20, "gpu": 2}

    @pytest.mark.asyncio
    async def test_get_available_jobs_count_busy_nodes(
        self, service: JobsService, kube_client: mock.Mock
    ) -> None:
        kube_client.get_pods.side_effect = get_pods_factory(
            create_pod("minikube-cpu-1", cpu_m=1000, memory_mb=1024),
            create_pod("minikube-cpu-2", cpu_m=1000, memory_mb=1024),
            create_pod("minikube-gpu-1", cpu_m=1000, memory_mb=1024, gpu=1),
            create_pod("minikube-gpu-2", cpu_m=1000, memory_mb=1024, gpu=1),
        )

        result = await service.get_available_jobs_counts(
            {
                "cpu": Preset(cpu=0.2, memory_mb=100),
                "gpu": Preset(
                    cpu=0.2, memory_mb=100, gpu=1, gpu_model="nvidia-tesla-k80"
                ),
            }
        )
        assert result == {"cpu": 0, "gpu": 0}

    @pytest.mark.asyncio
    async def test_get_available_jobs_count_for_pods_without_nodes(
        self, service: JobsService, kube_client: mock.Mock
    ) -> None:
        kube_client.get_pods.side_effect = get_pods_factory(
            create_pod(None, cpu_m=1000, memory_mb=1024)
        )

        result = await service.get_available_jobs_counts(
            {"cpu": Preset(cpu=0.2, memory_mb=100)}
        )
        assert result == {"cpu": 10}

    @pytest.mark.asyncio
    async def test_get_available_jobs_count_node_not_found(
        self, service: JobsService, kube_client: mock.Mock
    ) -> None:
        kube_client.get_pods.side_effect = get_pods_factory(
            create_pod("unknown", cpu_m=1000, memory_mb=1024)
        )

        with pytest.raises(NodeNotFoundException, match="Node 'unknown' was not found"):
            await service.get_available_jobs_counts(
                {"cpu": Preset(cpu=0.2, memory_mb=100)}
            )
