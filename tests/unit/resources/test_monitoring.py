from kubernetes.client.models import (
    V1Container,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
)
from neuro_config_client import AMDGPU, NvidiaGPU, ResourcePoolType

from platform_monitoring.kube_client import ContainerResources, NodeResources
from platform_monitoring.resources.monitoring import (
    ResourcePoolTypeFactory,
    _Node,
    _Pod,
)


class TestResourcePoolTypeFactory:
    def test_create_from_nodes__no_pods(self) -> None:
        node = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=2,
                nvidia_migs={"1g.5gb": 7},
                amd_gpu=3,
            ),
            allocatable=NodeResources(
                cpu_m=3000,
                memory=12 * 2**30,
                ephemeral_storage=80 * 2**30,
                nvidia_gpu=1,
                nvidia_migs={"1g.5gb": 3},
                amd_gpu=2,
            ),
            nvidia_gpu_model="A100",
            nvidia_gpu_memory=40 * 2**30,
            nvidia_mig_models={"1g.5gb": "A100-1g.5gb"},
            nvidia_mig_memories={"1g.5gb": 5 * 2**30},
            amd_gpu_device_id="RX-6800",
            amd_gpu_vram=80 * 2**30,
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
            nvidia_gpu=NvidiaGPU(count=1, model="A100", memory=40 * 2**30),
            nvidia_migs={
                "1g.5gb": NvidiaGPU(count=3, model="A100-1g.5gb", memory=5 * 2**30)
            },
            amd_gpu=AMDGPU(count=2, model="RX-6800", memory=80 * 2**30),
        )

    def test_create_from_nodes__multiple_nodes__no_pods(self) -> None:
        node1 = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=4,
                nvidia_migs={"1g.5gb": 14},
                amd_gpu=8,
            ),
            allocatable=NodeResources(
                cpu_m=3000,
                memory=12 * 2**30,
                ephemeral_storage=80 * 2**30,
                nvidia_gpu=3,
                nvidia_migs={"1g.5gb": 13},
                amd_gpu=7,
            ),
            nvidia_gpu_model="A100",
            nvidia_gpu_memory=40 * 2**30,
            nvidia_mig_models={"1g.5gb": "A100-1g.5gb"},
            nvidia_mig_memories={"1g.5gb": 5 * 2**30},
            amd_gpu_device_id="RX-6800",
            amd_gpu_vram=80 * 2**30,
        )
        node2 = _Node(
            name="test-node-2",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=2000,
                memory=8 * 2**30,
                ephemeral_storage=50 * 2**30,
                nvidia_gpu=2,
                nvidia_migs={"1g.5gb": 7},
                amd_gpu=4,
            ),
            allocatable=NodeResources(
                cpu_m=1000,
                memory=4 * 2**30,
                ephemeral_storage=40 * 2**30,
                nvidia_gpu=1,
                nvidia_migs={"1g.5gb": 6},
                amd_gpu=3,
            ),
            nvidia_gpu_model="A100",
            nvidia_gpu_memory=40 * 2**30,
            nvidia_mig_models={"1g.5gb": "A100-1g.5gb"},
            nvidia_mig_memories={"1g.5gb": 5 * 2**30},
            amd_gpu_device_id="RX-6800",
            amd_gpu_vram=80 * 2**30,
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
            nvidia_gpu=NvidiaGPU(count=1, model="A100", memory=40 * 2**30),
            nvidia_migs={
                "1g.5gb": NvidiaGPU(count=6, model="A100-1g.5gb", memory=5 * 2**30)
            },
            amd_gpu=AMDGPU(count=3, model="RX-6800", memory=80 * 2**30),
        )

    def test_create_from_nodes__gpu__multiple_nodes__different_models__no_pods(
        self,
    ) -> None:
        node1 = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=2,
                nvidia_migs={"1g.5gb": 14},
                amd_gpu=3,
            ),
            allocatable=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=2,
                nvidia_migs={"1g.5gb": 14},
                amd_gpu=3,
            ),
            nvidia_gpu_model="A100",
            nvidia_gpu_memory=40 * 2**30,
            nvidia_mig_models={"1g.5gb": "A100-1g.5gb"},
            nvidia_mig_memories={"1g.5gb": 5 * 2**30 - 1},
            amd_gpu_device_id="RX-6800",
            amd_gpu_vram=80 * 2**30,
        )
        node2 = _Node(
            name="test-node-2",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=2000,
                memory=8 * 2**30,
                ephemeral_storage=50 * 2**30,
                nvidia_gpu=1,
                nvidia_migs={"1g.5gb": 7},
                amd_gpu=2,
            ),
            allocatable=NodeResources(
                cpu_m=2000,
                memory=8 * 2**30,
                ephemeral_storage=50 * 2**30,
                nvidia_gpu=1,
                nvidia_migs={"1g.5gb": 7},
                amd_gpu=2,
            ),
            nvidia_gpu_model="V100",
            nvidia_gpu_memory=16 * 2**30,
            nvidia_mig_models={"1g.5gb": "V100-1g.5gb"},
            nvidia_mig_memories={"1g.5gb": 5 * 2**30},
            amd_gpu_device_id="RX-7800",
            amd_gpu_vram=40 * 2**30,
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
            nvidia_migs={
                "1g.5gb": NvidiaGPU(
                    count=14, model="A100-1g.5gb", memory=5 * 2**30 - 1
                ),
            },
            amd_gpu=AMDGPU(count=3, model="RX-6800", memory=80 * 2**30),
        )

    def test_create_from_nodes__has_pods(self) -> None:
        node = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=1,
                nvidia_migs={"1g.5gb": 7},
                amd_gpu=3,
            ),
            allocatable=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=2,
                nvidia_migs={"1g.5gb": 7},
                amd_gpu=3,
            ),
            nvidia_gpu_model="A100",
            nvidia_gpu_memory=40 * 2**30,
            nvidia_mig_models={"1g.5gb": "A100-1g.5gb"},
            nvidia_mig_memories={"1g.5gb": 5 * 2**30},
            amd_gpu_device_id="RX-6800",
            amd_gpu_vram=40 * 2**30,
        )
        pod = _Pod(
            name="test-pod-1",
            node_name="test-node-1",
            resource_requests=ContainerResources.from_primitive(
                {
                    "cpu": "1",
                    "memory": "2Gi",
                    "nvidia.com/gpu": "1",
                    "nvidia.com/mig-1g.5gb": "1",
                    "amd.com/gpu": "1",
                }
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
            nvidia_gpu=NvidiaGPU(count=1, model="A100", memory=40 * 2**30),
            nvidia_migs={
                "1g.5gb": NvidiaGPU(count=6, model="A100-1g.5gb", memory=5 * 2**30)
            },
            amd_gpu=AMDGPU(count=2, model="RX-6800", memory=40 * 2**30),
        )

    def test_create_from_nodes__multiple_nodes__has_pods(self) -> None:
        node1 = _Node(
            name="test-node-1",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=2,
                nvidia_migs={"1g.5gb": 14},
                amd_gpu=3,
            ),
            allocatable=NodeResources(
                cpu_m=4000,
                memory=16 * 2**30,
                ephemeral_storage=100 * 2**30,
                nvidia_gpu=2,
                nvidia_migs={"1g.5gb": 14},
                amd_gpu=3,
            ),
            nvidia_gpu_model="A100",
            nvidia_gpu_memory=40 * 2**30,
            nvidia_mig_models={"1g.5gb": "A100-1g.5gb"},
            nvidia_mig_memories={"1g.5gb": 5 * 2**30},
            amd_gpu_device_id="RX-6800",
            amd_gpu_vram=40 * 2**30,
        )
        node2 = _Node(
            name="test-node-2",
            node_pool_name="test-node-pool",
            capacity=NodeResources(
                cpu_m=2000,
                memory=8 * 2**30,
                ephemeral_storage=50 * 2**30,
                nvidia_gpu=1,
                nvidia_migs={"1g.5gb": 7},
                amd_gpu=2,
            ),
            allocatable=NodeResources(
                cpu_m=2000,
                memory=8 * 2**30,
                ephemeral_storage=50 * 2**30,
                nvidia_gpu=1,
                nvidia_migs={"1g.5gb": 7},
                amd_gpu=2,
            ),
            nvidia_gpu_model="A100",
            nvidia_gpu_memory=40 * 2**30,
            nvidia_mig_memories={"1g.5gb": 5 * 2**30},
            amd_gpu_device_id="RX-6800",
            amd_gpu_vram=40 * 2**30,
        )
        pod1 = _Pod(
            name="test-pod-1",
            node_name="test-node-1",
            resource_requests=ContainerResources.from_primitive(
                {
                    "cpu": "1",
                    "memory": "2Gi",
                    "nvidia.com/gpu": "1",
                    "nvidia.com/mig-1g.5gb": "10",
                    "amd.com/gpu": "2",
                }
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
            nvidia_gpu=NvidiaGPU(count=1, model="A100", memory=40 * 2**30),
            nvidia_migs={
                "1g.5gb": NvidiaGPU(count=4, model="A100-1g.5gb", memory=5 * 2**30)
            },
            amd_gpu=AMDGPU(count=1, model="RX-6800", memory=40 * 2**30),
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
                            requests={
                                "cpu": "200m",
                                "memory": "512Mi",
                                "nvidia.com/gpu": "1",
                                "nvidia.com/mig-1g.5gb": "1",
                                "nvidia.com/mig-2g.10gb": "1",
                                "amd.com/gpu": "1",
                            }
                        ),
                    ),
                    V1Container(
                        name="test-init-container-2",
                        resources=V1ResourceRequirements(
                            requests={
                                "cpu": "300m",
                                "memory": "512Mi",
                                "nvidia.com/gpu": "2",
                                "nvidia.com/mig-1g.5gb": "2",
                                "nvidia.com/mig-3g.20gb": "1",
                                "amd.com/gpu": "2",
                            }
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
            resource_requests=ContainerResources(
                cpu_m=300,
                memory=512 * 2**20,
                nvidia_gpu=2,
                nvidia_migs={
                    "1g.5gb": 2,
                    "2g.10gb": 1,
                    "3g.20gb": 1,
                },
                amd_gpu=2,
            ),
        )
