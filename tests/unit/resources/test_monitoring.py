from kubernetes.client.models import (
    V1Container,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
)
from neuro_config_client import NvidiaGPU, ResourcePoolType

from platform_monitoring.kube_client import ContainerResources, NodeResources
from platform_monitoring.resources.monitoring import (
    ResourcePoolTypeFactory,
    _Node,
    _Pod,
)


class TestResourcePoolTypeFactory:
    def test_create_from_nodes__cpu__no_allocatable__no_pods(self) -> None:
        node = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
            ),
            allocatable=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
            ),
        )
        result = ResourcePoolTypeFactory().create_from_nodes([node], {})

        assert result == ResourcePoolType(
            name="test-node-pool",
            min_size=1,
            max_size=1,
            cpu=4,
            available_cpu=4,
            memory=16 * 2**30,
            available_memory=16 * 2**30,
            disk_size=100 * 2**30,
            available_disk_size=100 * 2**30,
        )

    def test_create_from_nodes__cpu__different_allocatable__no_pods(self) -> None:
        node = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
            ),
            allocatable=NodeResources(
                cpu_m=3000,
                memory=12 * 2**30,
                ephemeral_storage=80 * 2**30,
            ),
        )
        result = ResourcePoolTypeFactory().create_from_nodes([node], {})

        assert result == ResourcePoolType(
            name="test-node-pool",
            min_size=1,
            max_size=1,
            cpu=4,
            available_cpu=3,
            memory=16 * 2**30,
            available_memory=12 * 2**30,
            disk_size=100 * 2**30,
            available_disk_size=80 * 2**30,
        )

    def test_create_from_nodes__cpu__multiple_nodes__no_pods(self) -> None:
        node1 = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
            ),
            allocatable=NodeResources(
                cpu_m=3000,
                memory=12 * 2**30,
                ephemeral_storage=80 * 2**30,
            ),
        )
        node2 = _Node(
            name="test-node-2",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=2000,
                memory=8 * 2**30,
                ephemeral_storage=50 * 2**30,
            ),
            allocatable=NodeResources(
                cpu_m=1000,
                memory=4 * 2**30,
                ephemeral_storage=40 * 2**30,
            ),
        )
        result = ResourcePoolTypeFactory().create_from_nodes([node1, node2], {})

        assert result == ResourcePoolType(
            name="test-node-pool",
            min_size=2,
            max_size=2,
            cpu=2,
            available_cpu=1,
            memory=8 * 2**30,
            available_memory=4 * 2**30,
            disk_size=50 * 2**30,
            available_disk_size=40 * 2**30,
        )

    def test_create_from_nodes__has_pods(self) -> None:
        node = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
            ),
            allocatable=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
            ),
        )
        pod = _Pod(
            name="test-pod-1",
            node_name="test-node-1",
            resource_requests=ContainerResources.from_primitive(
                {"cpu": "1", "memory": "2Gi"}
            ),
        )
        result = ResourcePoolTypeFactory().create_from_nodes(
            [node], {"test-node-1": [pod]}
        )

        assert result == ResourcePoolType(
            name="test-node-pool",
            min_size=1,
            max_size=1,
            cpu=4,
            available_cpu=3,
            memory=16 * 2**30,
            available_memory=14 * 2**30,
            disk_size=100 * 2**30,
            available_disk_size=100 * 2**30,
        )

    def test_create_from_nodes__multiple_nodes__has_pods(self) -> None:
        node1 = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
            ),
            allocatable=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
            ),
        )
        node2 = _Node(
            name="test-node-2",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=2000,
                memory=8 * 2**30,
                ephemeral_storage=50 * 2**30,
            ),
            allocatable=NodeResources(
                cpu_m=2000,
                memory=8 * 2**30,
                ephemeral_storage=50 * 2**30,
            ),
        )
        pod1 = _Pod(
            name="test-pod-1",
            node_name="test-node-1",
            resource_requests=ContainerResources.from_primitive(
                {"cpu": "1", "memory": "2Gi"}
            ),
        )
        pod2 = _Pod(
            name="test-pod-2",
            node_name="test-node-2",
            resource_requests=ContainerResources.from_primitive(
                {"cpu": "0.1", "memory": "100Mi"}
            ),
        )
        result = ResourcePoolTypeFactory().create_from_nodes(
            [node1, node2], {"test-node-1": [pod1], "test-node-2": [pod2]}
        )

        assert result == ResourcePoolType(
            name="test-node-pool",
            min_size=2,
            max_size=2,
            cpu=2,
            available_cpu=1.9,
            memory=8 * 2**30,
            available_memory=8 * 2**30 - 100 * 2**20,
            disk_size=50 * 2**30,
            available_disk_size=50 * 2**30,
        )

    def test_create_from_nodes__gpu(self) -> None:
        node = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=2,
            ),
            allocatable=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=2,
            ),
            nvidia_gpu_model="A100",
            nvidia_gpu_memory=40 * 2**30,
        )
        result = ResourcePoolTypeFactory().create_from_nodes([node], {})

        assert result == ResourcePoolType(
            name="test-node-pool",
            min_size=1,
            max_size=1,
            cpu=4,
            available_cpu=4,
            memory=16 * 2**30,
            available_memory=16 * 2**30,
            disk_size=100 * 2**30,
            available_disk_size=100 * 2**30,
            nvidia_gpu=NvidiaGPU(count=2, model="A100", memory=40 * 2**30),
        )

    def test_create_from_nodes__gpu__multiple_nodes__different_gpus(self) -> None:
        node1 = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=2,
            ),
            allocatable=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=2,
            ),
            nvidia_gpu_model="A100",
            nvidia_gpu_memory=40 * 2**30,
        )
        node2 = _Node(
            name="test-node-2",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=1,
            ),
            allocatable=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=1,
            ),
            nvidia_gpu_model="A100",
            nvidia_gpu_memory=41 * 2**30,
            amd_gpu_device_id=None,
            amd_gpu_vram=None,
        )
        result = ResourcePoolTypeFactory().create_from_nodes([node1, node2], {})

        assert result == ResourcePoolType(
            name="test-node-pool",
            min_size=2,
            max_size=2,
            cpu=4,
            available_cpu=4,
            memory=16 * 2**30,
            available_memory=16 * 2**30,
            disk_size=100 * 2**30,
            available_disk_size=100 * 2**30,
            nvidia_gpu=NvidiaGPU(count=1, model="A100", memory=40 * 2**30),
        )

    def test_create_from_nodes__gpu__multiple_nodes__different_gpu_models(self) -> None:
        node1 = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=2,
            ),
            allocatable=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=2,
            ),
            nvidia_gpu_model="A100",
            nvidia_gpu_memory=40 * 2**30,
        )
        node2 = _Node(
            name="test-node-2",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=2000,
                memory=8 * 2**30,
                ephemeral_storage=50 * 2**30,
                nvidia_gpu=1,
            ),
            allocatable=NodeResources(
                cpu_m=2000,
                memory=8 * 2**30,
                ephemeral_storage=50 * 2**30,
                nvidia_gpu=1,
            ),
            nvidia_gpu_model="V100",
            nvidia_gpu_memory=16 * 2**30,
            amd_gpu_device_id=None,
            amd_gpu_vram=None,
        )
        result = ResourcePoolTypeFactory().create_from_nodes([node1, node2], {})

        assert result == ResourcePoolType(
            name="test-node-pool",
            min_size=2,
            max_size=2,
            cpu=2,
            available_cpu=2,
            memory=8 * 2**30,
            available_memory=8 * 2**30,
            disk_size=50 * 2**30,
            available_disk_size=50 * 2**30,
            nvidia_gpu=NvidiaGPU(count=2, model="A100", memory=40 * 2**30),
        )


class TestPod:
    def test_from_v1_pod__with_init_containers(self) -> None:
        v1_pod = V1Pod(
            metadata=V1ObjectMeta(name="test-pod-1"),
            spec=V1PodSpec(
                node_name="test-node-1",
                init_containers=[
                    V1Container(
                        name="test-init-container-1",
                        resources=V1ResourceRequirements(
                            requests={"cpu": "200m", "memory": "512Mi"}
                        ),
                    ),
                    V1Container(
                        name="test-init-container-2",
                        resources=V1ResourceRequirements(
                            requests={"cpu": "300m", "memory": "512Mi"}
                        ),
                    ),
                ],
                containers=[
                    V1Container(
                        name="test-container-1",
                        resources=V1ResourceRequirements(
                            requests={"cpu": "100m", "memory": "256Mi"}
                        ),
                    ),
                ],
            ),
        )

        pod = _Pod.from_v1_pod(v1_pod)

        assert pod == _Pod(
            name="test-pod-1",
            node_name="test-node-1",
            resource_requests=ContainerResources(cpu_m=300, memory=512 * 2**20),
        )
