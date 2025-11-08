import asyncio
import itertools
import logging
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from contextlib import aclosing, suppress
from dataclasses import dataclass, field, replace
from typing import Protocol, Self, TypeVar

import tenacity
from apolo_kube_client import KubeClient, V1Container, V1Node, V1Pod
from neuro_config_client import (
    AMDGPU,
    GPU,
    ConfigClient,
    NvidiaGPU,
    PatchClusterRequest,
    PatchOrchestratorConfigRequest,
    ResourcePoolType,
)

from ..kube_client import ContainerResources, NodeResources, parse_memory


LOGGER = logging.getLogger(__name__)


APOLO_PLATFORM_NODE_POOL_LABEL_KEY = "platform.apolo.us/node-pool"
APOLO_PLATFORM_ROLE_LABEL_KEY = "platform.apolo.us/role"
APOLO_PLATFORM_JOB_LABEL_KEY = "platform.apolo.us/job"
APOLO_PLATFORM_APP_INSTANCE_NAME_LABEL_KEY = "platform.apolo.us/app-instance-name"

NEURO_PLATFORM_NODE_POOL_LABEL_KEY = "platform.neuromation.io/nodepool"

NVIDIA_GPU_PRODUCT = "nvidia.com/gpu.product"
NVIDIA_GPU_MEMORY = "nvidia.com/gpu.memory"
NVIDIA_MIG_MEMORY_PATTERN = re.compile(r"nvidia\.com/mig-(?P<profile_name>.+)\.memory")
NVIDIA_MIG_MODEL_PATTERN = re.compile(r"nvidia\.com/mig-(?P<profile_name>.+)\.product")

AMD_GPU_DEVICE_ID = "amd.com/gpu.device-id"
AMD_GPU_VRAM = "amd.com/gpu.vram"

PODS_LABEL_SELECTOR = (
    f"!{APOLO_PLATFORM_JOB_LABEL_KEY},!{APOLO_PLATFORM_APP_INSTANCE_NAME_LABEL_KEY}"
)

MiB = 2**20


@dataclass(frozen=True, kw_only=True)
class _Node:
    name: str
    node_pool_name: str
    capacity: NodeResources
    allocatable: NodeResources
    nvidia_gpu_model: str | None = None
    nvidia_gpu_memory: int | None = None
    nvidia_mig_models: Mapping[str, str] = field(default_factory=dict)
    nvidia_mig_memories: Mapping[str, int] = field(default_factory=dict)
    amd_gpu_device_id: str | None = None
    amd_gpu_vram: int | None = None

    @classmethod
    def from_v1_node(cls, node: V1Node) -> Self:
        assert node.metadata.name, "node name is required"
        labels = node.metadata.labels or {}
        node_pool_name = _get_node_pool_name(node)
        assert node_pool_name, "node pool name is required"
        return cls(
            name=node.metadata.name,
            node_pool_name=node_pool_name,
            capacity=NodeResources.from_primitive(node.status.capacity or {}),
            allocatable=NodeResources.from_primitive(
                node.status.allocatable or node.status.capacity or {}
            ),
            nvidia_gpu_model=labels.get(NVIDIA_GPU_PRODUCT),
            nvidia_gpu_memory=int(labels.get(NVIDIA_GPU_MEMORY, "0")) * MiB or None,
            nvidia_mig_models={
                match.group("profile_name"): value
                for key, value in labels.items()
                if (match := NVIDIA_MIG_MODEL_PATTERN.fullmatch(key))
            },
            nvidia_mig_memories={
                match.group("profile_name"): int(value) * MiB
                for key, value in labels.items()
                if (match := NVIDIA_MIG_MEMORY_PATTERN.fullmatch(key))
            },
            amd_gpu_device_id=labels.get(AMD_GPU_DEVICE_ID),
            amd_gpu_vram=parse_memory(labels.get(AMD_GPU_VRAM, "0")) or None,
        )

    def allocate_resources(self, resources: ContainerResources) -> Self:
        return replace(self, allocatable=self.allocatable - resources)


@dataclass(frozen=True)
class _Pod:
    name: str
    node_name: str
    resource_requests: ContainerResources

    @classmethod
    def from_v1_pod(cls, pod: V1Pod) -> Self:
        assert pod.metadata.name, "pod name is required"
        assert pod.spec, "pod must have a spec"
        assert pod.spec.node_name, "pod must be scheduled on a node"
        init_resource_requests = cls._max_container_resource_requests(
            pod.spec.init_containers or ()
        )
        resource_requests = cls._sum_container_resource_requests(
            pod.spec.containers or ()
        )
        return cls(
            name=pod.metadata.name,
            node_name=pod.spec.node_name,
            resource_requests=cls._max_container_resources(
                init_resource_requests, resource_requests
            ),
        )

    @classmethod
    def _max_container_resources(
        cls, r1: ContainerResources, r2: ContainerResources
    ) -> ContainerResources:
        return ContainerResources(
            cpu_m=max(r1.cpu_m, r2.cpu_m),
            memory=max(r1.memory, r2.memory),
            nvidia_gpu=max(r1.nvidia_gpu, r2.nvidia_gpu),
            nvidia_migs={
                profile_name: max(
                    r1.nvidia_migs.get(profile_name, 0),
                    r2.nvidia_migs.get(profile_name, 0),
                )
                for profile_name in set(r1.nvidia_migs) | set(r2.nvidia_migs)
            },
            amd_gpu=max(r1.amd_gpu, r2.amd_gpu),
        )

    @classmethod
    def _max_container_resource_requests(
        cls, containers: Sequence[V1Container]
    ) -> ContainerResources:
        resource_requests = ContainerResources()
        for container in containers:
            if container.resources and container.resources.requests:
                resource_requests = cls._max_container_resources(
                    resource_requests,
                    ContainerResources.from_primitive(container.resources.requests),
                )
        return resource_requests

    @classmethod
    def _sum_container_resource_requests(
        cls, containers: Sequence[V1Container]
    ) -> ContainerResources:
        resource_requests = ContainerResources()
        for container in containers:
            if container.resources and container.resources.requests:
                resource_requests += ContainerResources.from_primitive(
                    container.resources.requests
                )
        return resource_requests


class _KubeState(Protocol):
    @property
    def nodes(self) -> Mapping[str, _Node]: ...

    @property
    def pods(self) -> Mapping[str, _Pod]: ...


class MonitoringService(_KubeState):
    def __init__(
        self,
        *,
        kube_client: KubeClient,
        config_client: ConfigClient,
        cluster_name: str,
    ) -> None:
        self._kube_client = kube_client
        self._nodes: dict[str, _Node] = {}
        self._pods: dict[str, _Pod] = {}
        self._nodes_watch_task: asyncio.Task[None] | None = None
        self._pods_watch_task: asyncio.Task[None] | None = None

        self._cluster_syncer = ClusterSyncer(
            kube_state=self,
            config_client=config_client,
            cluster_name=cluster_name,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await _cancel_task(self._nodes_watch_task)
        await _cancel_task(self._pods_watch_task)
        await self._cluster_syncer.stop()

    @property
    def nodes(self) -> Mapping[str, _Node]:
        return self._nodes

    @property
    def pods(self) -> Mapping[str, _Pod]:
        return self._pods

    async def start(self) -> None:
        nodes_resource_version = await self._reset_nodes()
        pods_resource_version = await self._reset_pods()

        self._cluster_syncer.start()
        self._cluster_syncer.notify()  # Initial sync during startup

        self._nodes_watch_task = asyncio.create_task(
            self._run_nodes_watcher(resource_version=nodes_resource_version)
        )
        self._pods_watch_task = asyncio.create_task(
            self._run_pods_watcher(resource_version=pods_resource_version)
        )

    async def _reset_nodes(self) -> str | None:
        node_list = await self._kube_client.core_v1.node.get_list()
        resource_version = node_list.metadata.resource_version
        LOGGER.info("Nodes resource version: %s", resource_version)
        self._init_nodes(node_list.items)
        return resource_version

    def _init_nodes(self, nodes: Sequence[V1Node]) -> None:
        LOGGER.debug("Initializing nodes")
        new_nodes = {}
        for node in nodes:
            if self._is_workload_node(node):
                LOGGER.debug("Adding workload node %s", node.metadata.name)
                assert node.metadata.name, "node name is required"
                new_nodes[node.metadata.name] = _Node.from_v1_node(node)
            else:
                LOGGER.debug("Ignoring non-workload node %s", node.metadata.name)
        self._nodes = new_nodes

    async def _run_nodes_watcher(self, *, resource_version: str | None) -> None:
        LOGGER.info("Starting nodes watcher")

        while True:
            try:
                if resource_version is None:
                    resource_version = await self._reset_nodes()
                    self._cluster_syncer.notify()

                watch = self._kube_client.core_v1.node.watch(
                    resource_version=resource_version, allow_watch_bookmarks=True
                )
                async with aclosing(watch.stream()) as event_stream:
                    async for event in event_stream:
                        notify = False
                        match event.type:
                            case "ADDED" | "MODIFIED":
                                notify = self._handle_node_update(event.object)
                            case "DELETED":
                                notify = self._handle_node_deletion(event.object)
                        if notify:
                            self._cluster_syncer.notify()
                        else:
                            LOGGER.debug("Nodes state unchanged")
            except Exception:
                LOGGER.exception("Nodes watcher failed")

            resource_version = None

    def _is_workload_node(self, node: V1Node) -> bool:
        labels = node.metadata.labels
        LOGGER.debug("Node %s labels: %r", node.metadata.name, labels)
        if not labels:
            return False
        if value := labels.get(APOLO_PLATFORM_ROLE_LABEL_KEY):
            if value.lower() != "workload":
                return False
        return bool(_get_node_pool_name(node))

    def _handle_node_update(self, node: V1Node) -> bool:
        assert node.metadata.name, "node name is required"
        if self._is_workload_node(node):
            LOGGER.debug("Updating workload node %s", node.metadata.name)
            old = self._nodes.get(node.metadata.name)
            new = _Node.from_v1_node(node)
            if old == new:
                return False
            self._nodes[node.metadata.name] = _Node.from_v1_node(node)
            return True
        LOGGER.debug("Ignoring non-workload node %s", node.metadata.name)
        old = self._nodes.pop(node.metadata.name, None)
        return old is not None

    def _handle_node_deletion(self, node: V1Node) -> bool:
        assert node.metadata.name, "node name is required"
        old = self._nodes.pop(node.metadata.name, None)
        if old is not None:
            LOGGER.debug("Removing node %s", node.metadata.name)
            return True
        return False

    async def _reset_pods(self) -> str | None:
        pod_list = await self._kube_client.core_v1.pod.get_list(
            label_selector=PODS_LABEL_SELECTOR, all_namespaces=True
        )
        resource_version = pod_list.metadata.resource_version
        LOGGER.info("Pods resource version: %s", resource_version)
        self._init_pods(pod_list.items)
        return resource_version

    def _init_pods(self, pods: Sequence[V1Pod]) -> None:
        LOGGER.debug("Initializing pods")
        new_pods = {}
        for pod in pods:
            assert pod.metadata.name, "pod name is required"
            if self._is_pod_running(pod):
                LOGGER.debug("Adding running pod %s", pod.metadata.name)
                new_pods[pod.metadata.name] = _Pod.from_v1_pod(pod)
            else:
                LOGGER.debug("Ignoring non-running pod %s", pod.metadata.name)
        self._pods = new_pods

    async def _run_pods_watcher(self, *, resource_version: str | None) -> None:
        LOGGER.info("Starting pods watcher")

        while True:
            try:
                if resource_version is None:
                    resource_version = await self._reset_pods()
                    self._cluster_syncer.notify()

                watch = self._kube_client.core_v1.pod.watch(
                    label_selector=PODS_LABEL_SELECTOR,
                    all_namespaces=True,
                    resource_version=resource_version,
                    allow_watch_bookmarks=True,
                )
                async with aclosing(watch.stream()) as event_stream:
                    async for event in event_stream:
                        notify = False
                        match event.type:
                            case "ADDED" | "MODIFIED":
                                notify = self._handle_pod_update(event.object)
                            case "DELETED":
                                notify = self._handle_pod_deletion(event.object)
                        if notify:
                            self._cluster_syncer.notify()
                        else:
                            LOGGER.debug("Pods state unchanged")
            except Exception:
                LOGGER.exception("Pods watcher failed")

            resource_version = None

    @staticmethod
    def _is_pod_running(pod: V1Pod) -> bool:
        assert pod.metadata, "pod metadata is required"
        assert pod.spec, "pod spec is required"
        LOGGER.debug("Pod %s node name: %r", pod.metadata.name, pod.spec.node_name)
        LOGGER.debug("Pod %s status: %r", pod.metadata.name, pod.status)
        return bool(
            pod.spec
            and pod.spec.node_name
            and pod.status
            and pod.status.phase == "Running"
        )

    def _handle_pod_update(self, pod: V1Pod) -> bool:
        assert pod.metadata.name, "pod name is required"
        if self._is_pod_running(pod):
            LOGGER.debug("Updating running pod %s", pod.metadata.name)
            old = self._pods.get(pod.metadata.name)
            new = _Pod.from_v1_pod(pod)
            if old == new:
                return False
            self._pods[pod.metadata.name] = _Pod.from_v1_pod(pod)
            return True
        LOGGER.debug("Ignoring non-running pod %s", pod.metadata.name)
        old = self._pods.pop(pod.metadata.name, None)
        return old is not None

    def _handle_pod_deletion(self, pod: V1Pod) -> bool:
        assert pod.metadata.name, "pod name is required"
        old = self._pods.pop(pod.metadata.name, None)
        if old is not None:
            LOGGER.debug("Removing pod %s", pod.metadata.name)
            return True
        return False


async def _cancel_task(task: asyncio.Task[None] | None) -> None:
    if task:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def _get_node_pool_name(node: V1Node) -> str | None:
    labels = node.metadata.labels
    if not labels:
        return None
    return labels.get(APOLO_PLATFORM_NODE_POOL_LABEL_KEY) or labels.get(
        NEURO_PLATFORM_NODE_POOL_LABEL_KEY
    )


class ClusterSyncer:
    def __init__(
        self, *, kube_state: _KubeState, config_client: ConfigClient, cluster_name: str
    ) -> None:
        self._config_client = config_client
        self._kube_state = kube_state
        self._cluster_name = cluster_name
        self._event = asyncio.Event()
        self._resource_pool_type_factory = ResourcePoolTypeFactory()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._start_cluster_sync())

    async def stop(self) -> None:
        await _cancel_task(self._task)

    def notify(self) -> None:
        self._event.set()

    async def _start_cluster_sync(self) -> None:
        LOGGER.info("Starting cluster sync")

        while True:
            await self._event.wait()
            self._event.clear()

            LOGGER.info("Syncing cluster resource pools")
            await self._sync_cluster()

    # Retry forever
    @tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=0.1, max=15),
        before_sleep=tenacity.before_sleep_log(LOGGER, logging.WARNING),
        after=tenacity.after_log(LOGGER, logging.INFO),
        retry=tenacity.retry_if_not_exception_type(asyncio.CancelledError),
    )
    async def _sync_cluster(self) -> None:
        # TODO: temporary solution to avoid overriding sizes of node pools
        # which support autoscaling. It's better to implement optimistic locking.
        cluster = await self._config_client.get_cluster(self._cluster_name)
        current_pool_types = cluster.orchestrator.resource_pool_types
        pool_types = self._create_resource_pool_types()
        pool_types = self._update_resource_pool_sizes(
            pool_types=pool_types, current_pool_types=current_pool_types
        )
        pool_types.extend(
            self._get_downscaled_resource_pools(
                pool_types=pool_types, current_pool_types=current_pool_types
            )
        )
        pool_types.sort(key=lambda t: t.name)

        if pool_types == current_pool_types:
            LOGGER.info("Cluster resource pools are up-to-date")
            return

        LOGGER.debug("Current resource pools: %r", current_pool_types)
        LOGGER.debug("New resource pools: %r", pool_types)
        LOGGER.info("Updating cluster resource pools")

        await self._config_client.patch_cluster(
            self._cluster_name,
            PatchClusterRequest(
                orchestrator=PatchOrchestratorConfigRequest(
                    resource_pool_types=pool_types
                )
            ),
        )

    def _create_resource_pool_types(self) -> list[ResourcePoolType]:
        result = []
        pods = self._group_pods_by_node()
        for nodes in self._group_nodes_by_node_pool().values():
            pool_type = self._resource_pool_type_factory.create_from_nodes(nodes, pods)
            result.append(pool_type)
            LOGGER.debug("Detected resource pool type: %r", pool_type)
        result.sort(key=lambda t: t.name)
        return result

    def _group_nodes_by_node_pool(self) -> dict[str, list[_Node]]:
        result = defaultdict(list)
        for node in self._kube_state.nodes.values():
            node_pool_name = node.node_pool_name
            if node_pool_name:
                result[node_pool_name].append(node)
        return result

    def _group_pods_by_node(self) -> dict[str, list[_Pod]]:
        result = defaultdict(list)
        for pod in self._kube_state.pods.values():
            if pod.node_name:
                result[pod.node_name].append(pod)
        return result

    def _update_resource_pool_sizes(
        self,
        *,
        pool_types: Sequence[ResourcePoolType],
        current_pool_types: Sequence[ResourcePoolType],
    ) -> list[ResourcePoolType]:
        current_pool_types_by_name = {pt.name: pt for pt in current_pool_types}
        updated_pool_types = []
        for pool_type in pool_types:
            current_pool_type = current_pool_types_by_name.get(pool_type.name)
            if not current_pool_type:
                updated_pool_types.append(pool_type)
                continue
            if current_pool_type.min_size == current_pool_type.max_size:
                # Allow updating sizes of non-autoscaling pools only
                updated_pool_types.append(pool_type)
                continue
            LOGGER.debug(
                "Preserving sizes of resource pool %s: min_size=%d, max_size=%d",
                pool_type.name,
                current_pool_type.min_size,
                current_pool_type.max_size,
            )
            updated_pool_types.append(
                replace(
                    pool_type,
                    min_size=current_pool_type.min_size,
                    max_size=current_pool_type.max_size,
                )
            )
        return updated_pool_types

    def _get_downscaled_resource_pools(
        self,
        *,
        pool_types: Sequence[ResourcePoolType],
        current_pool_types: Sequence[ResourcePoolType],
    ) -> list[ResourcePoolType]:
        pool_types_by_name = {pt.name: pt for pt in pool_types}
        downscaled_pool_types = []
        for pool_type in current_pool_types:
            if pool_type.name in pool_types_by_name:
                continue
            if pool_type.min_size != pool_type.max_size:
                downscaled_pool_types.append(pool_type)
        return downscaled_pool_types


_T_GPU = TypeVar("_T_GPU", bound=GPU)


class ResourcePoolTypeFactory:
    def create_from_nodes(
        self, nodes: Sequence[_Node], pods: Mapping[str, Sequence[_Pod]]
    ) -> ResourcePoolType:
        assert nodes, "at least one node is required"

        cpu = []
        available_cpu = []
        memory = []
        available_memory = []
        disk_size = []
        available_disk_size = []
        nvidia_gpu = []
        nvidia_migs = []
        amd_gpu = []
        # TODO: support Intel GPU

        for node in nodes:
            LOGGER.debug("Node %s capacity: %r", node.name, node.capacity)
            LOGGER.debug("Node %s allocatable: %r", node.name, node.allocatable)
            node_pods = pods.get(node.name, ())
            allocated = sum(
                (pod.resource_requests for pod in node_pods), start=ContainerResources()
            )
            node = node.allocate_resources(allocated)
            LOGGER.debug("Node %s pods: %r", node.name, node_pods)
            LOGGER.debug("Node %s allocated: %r", node.name, allocated)
            LOGGER.debug("Node %s available: %r", node.name, node.allocatable)

            cpu.append(node.capacity.cpu)
            available_cpu.append(node.allocatable.cpu)
            memory.append(node.capacity.memory)
            available_memory.append(node.allocatable.memory)
            disk_size.append(node.capacity.ephemeral_storage)
            available_disk_size.append(node.allocatable.ephemeral_storage)
            if ng := self._create_nvidia_gpu(node):
                nvidia_gpu.append(ng)
            if migs := self._create_nvidia_migs(node):
                nvidia_migs.append(migs)
            if ag := self._create_amd_gpu(node):
                amd_gpu.append(ag)

        return ResourcePoolType(
            name=nodes[0].node_pool_name,
            min_size=len(nodes),
            max_size=len(nodes),
            cpu=min(cpu),
            available_cpu=min(available_cpu),
            memory=min(memory),
            available_memory=min(available_memory),
            disk_size=min(disk_size),
            available_disk_size=min(available_disk_size),
            nvidia_gpu=self._min_gpu(nvidia_gpu, NvidiaGPU),
            nvidia_migs=self._min_nvidia_migs(nvidia_migs),
            amd_gpu=self._min_gpu(amd_gpu, AMDGPU),
        )

    def _create_nvidia_gpu(self, node: _Node) -> NvidiaGPU | None:
        if not node.allocatable.has_nvidia_gpu:
            return None
        if not node.nvidia_gpu_model:
            return None
        return NvidiaGPU(
            count=node.allocatable.nvidia_gpu,
            model=node.nvidia_gpu_model,
            memory=node.nvidia_gpu_memory,
        )

    def _create_nvidia_migs(self, node: _Node) -> dict[str, NvidiaGPU] | None:
        if not node.allocatable.has_nvidia_migs:
            return None
        result = {
            key: NvidiaGPU(
                count=count,
                model=model,
                memory=node.nvidia_mig_memories.get(key),
            )
            for key, count in node.allocatable.nvidia_migs.items()
            if (model := node.nvidia_mig_models.get(key))
        }
        return result or None

    def _create_amd_gpu(self, node: _Node) -> AMDGPU | None:
        if not node.allocatable.has_amd_gpu:
            return None
        if not node.amd_gpu_device_id:
            return None
        return AMDGPU(
            count=node.allocatable.amd_gpu,
            model=node.amd_gpu_device_id,
            memory=node.amd_gpu_vram,
        )

    def _min_gpu(self, gpus: list[_T_GPU], cls: type[_T_GPU]) -> _T_GPU | None:
        if not gpus:
            return None
        model = next(gpu.model for gpu in gpus)
        count = min(gpu.count for gpu in gpus if gpu.model == model)
        memory = min(
            (gpu.memory for gpu in gpus if gpu.memory and gpu.model == model),
            default=None,
        )
        return cls(count=count, model=model, memory=memory)

    def _min_nvidia_migs(
        self, nvidia_migs: list[dict[str, NvidiaGPU]]
    ) -> dict[str, NvidiaGPU] | None:
        if not nvidia_migs:
            return None
        grouped_migs = defaultdict(list)
        for profile_name, mig in itertools.chain.from_iterable(
            migs.items() for migs in nvidia_migs
        ):
            grouped_migs[profile_name].append(mig)
        result = {}
        for profile_name, migs in grouped_migs.items():
            model = next(mig.model for mig in migs)
            result[profile_name] = NvidiaGPU(
                count=min(mig.count for mig in migs if mig.model == model),
                model=model,
                memory=min(
                    (mig.memory for mig in migs if mig.memory and mig.model == model),
                    default=None,
                ),
            )
        return result
