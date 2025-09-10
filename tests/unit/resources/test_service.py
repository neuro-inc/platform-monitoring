from kubernetes.client import (
    V1Container,
    V1Node,
    V1NodeStatus,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
)
from neuro_config_client import NvidiaGPU, ResourcePoolType

from platform_monitoring.resources.monitoring import (
    ResourcePoolTypeFactory,
)


class TestResourcePoolTypeFactory:
    def test_create_from_nodes__cpu__no_allocatable__no_pods(self) -> None:
        node = V1Node(
            metadata=V1ObjectMeta(
                name="test-node-1",
                labels={
                    "platform.apolo.us/node-pool": "test-node-pool",
                },
            ),
            status=V1NodeStatus(
                capacity={"cpu": "4", "memory": "16Gi", "ephemeral-storage": "100Gi"},
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
        node = V1Node(
            metadata=V1ObjectMeta(
                name="test-node-1",
                labels={
                    "platform.apolo.us/node-pool": "test-node-pool",
                },
            ),
            status=V1NodeStatus(
                capacity={"cpu": "4", "memory": "16Gi", "ephemeral-storage": "100Gi"},
                allocatable={"cpu": "3", "memory": "12Gi", "ephemeral-storage": "80Gi"},
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
        node1 = V1Node(
            metadata=V1ObjectMeta(
                name="test-node-1",
                labels={
                    "platform.apolo.us/node-pool": "test-node-pool",
                },
            ),
            status=V1NodeStatus(
                capacity={"cpu": "4", "memory": "16Gi", "ephemeral-storage": "100Gi"},
                allocatable={"cpu": "3", "memory": "12Gi", "ephemeral-storage": "80Gi"},
            ),
        )
        node2 = V1Node(
            metadata=V1ObjectMeta(
                name="test-node-2",
                labels={
                    "platform.apolo.us/node-pool": "test-node-pool",
                },
            ),
            status=V1NodeStatus(
                capacity={"cpu": "2", "memory": "8Gi", "ephemeral-storage": "50Gi"},
                allocatable={"cpu": "1", "memory": "4Gi", "ephemeral-storage": "40Gi"},
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
        node = V1Node(
            metadata=V1ObjectMeta(
                name="test-node-1",
                labels={
                    "platform.apolo.us/node-pool": "test-node-pool",
                },
            ),
            status=V1NodeStatus(
                capacity={"cpu": "4", "memory": "16Gi", "ephemeral-storage": "100Gi"},
            ),
        )
        pod = V1Pod(
            metadata=V1ObjectMeta(
                name="test-pod-1",
            ),
            spec=V1PodSpec(
                node_name="test-node-1",
                containers=[
                    V1Container(
                        name="test-container",
                        resources=V1ResourceRequirements(
                            requests={"cpu": "1", "memory": "2Gi"}
                        ),
                    )
                ],
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
        node1 = V1Node(
            metadata=V1ObjectMeta(
                name="test-node-1",
                labels={
                    "platform.apolo.us/node-pool": "test-node-pool",
                },
            ),
            status=V1NodeStatus(
                capacity={"cpu": "4", "memory": "16Gi", "ephemeral-storage": "100Gi"},
            ),
        )
        node2 = V1Node(
            metadata=V1ObjectMeta(
                name="test-node-2",
                labels={
                    "platform.apolo.us/node-pool": "test-node-pool",
                },
            ),
            status=V1NodeStatus(
                capacity={"cpu": "2", "memory": "8Gi", "ephemeral-storage": "50Gi"},
            ),
        )
        pod1 = V1Pod(
            metadata=V1ObjectMeta(
                name="test-pod-1",
            ),
            spec=V1PodSpec(
                node_name="test-node-1",
                containers=[
                    V1Container(
                        name="test-container",
                        resources=V1ResourceRequirements(
                            requests={"cpu": "1", "memory": "2Gi"}
                        ),
                    )
                ],
            ),
        )
        pod2 = V1Pod(
            metadata=V1ObjectMeta(
                name="test-pod-2",
            ),
            spec=V1PodSpec(
                node_name="test-node-2",
                containers=[
                    V1Container(
                        name="test-container",
                        resources=V1ResourceRequirements(
                            requests={"cpu": "0.1", "memory": "100Mi"}
                        ),
                    )
                ],
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
        node = V1Node(
            metadata=V1ObjectMeta(
                name="test-node-1",
                labels={
                    "platform.apolo.us/node-pool": "test-node-pool",
                    "nvidia.com/gpu.product": "A100",
                    "nvidia.com/gpu.memory": str(40 * 2**10),
                },
            ),
            status=V1NodeStatus(
                capacity={
                    "cpu": "4",
                    "memory": "16Gi",
                    "ephemeral-storage": "100Gi",
                    "nvidia.com/gpu": "2",
                },
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
            nvidia_gpu=NvidiaGPU(count=2, model="A100", memory=40 * 2**30),
        )

    def test_create_from_nodes__gpu__multiple_nodes__different_gpus(self) -> None:
        node1 = V1Node(
            metadata=V1ObjectMeta(
                name="test-node-1",
                labels={
                    "platform.apolo.us/node-pool": "test-node-pool",
                    "nvidia.com/gpu.product": "A100",
                    "nvidia.com/gpu.memory": str(40 * 2**10),
                },
            ),
            status=V1NodeStatus(
                capacity={
                    "cpu": "4",
                    "memory": "16Gi",
                    "ephemeral-storage": "100Gi",
                    "nvidia.com/gpu": "2",
                },
            ),
        )
        node2 = V1Node(
            metadata=V1ObjectMeta(
                name="test-node-2",
                labels={
                    "platform.apolo.us/node-pool": "test-node-pool",
                    "nvidia.com/gpu.product": "A100",
                    "nvidia.com/gpu.memory": str(41 * 2**10),
                },
            ),
            status=V1NodeStatus(
                capacity={
                    "cpu": "4",
                    "memory": "16Gi",
                    "ephemeral-storage": "100Gi",
                    "nvidia.com/gpu": "1",
                },
            ),
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
        node1 = V1Node(
            metadata=V1ObjectMeta(
                name="test-node-1",
                labels={
                    "platform.apolo.us/node-pool": "test-node-pool",
                    "nvidia.com/gpu.product": "A100",
                    "nvidia.com/gpu.memory": str(40 * 2**10),
                },
            ),
            status=V1NodeStatus(
                capacity={
                    "cpu": "4",
                    "memory": "16Gi",
                    "ephemeral-storage": "100Gi",
                    "nvidia.com/gpu": "2",
                },
            ),
        )
        node2 = V1Node(
            metadata=V1ObjectMeta(
                name="test-node-2",
                labels={
                    "platform.apolo.us/node-pool": "test-node-pool",
                    "nvidia.com/gpu.product": "V100",
                    "nvidia.com/gpu.memory": str(16 * 2**10),
                },
            ),
            status=V1NodeStatus(
                capacity={
                    "cpu": "2",
                    "memory": "8Gi",
                    "ephemeral-storage": "50Gi",
                    "nvidia.com/gpu": "1",
                },
            ),
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
