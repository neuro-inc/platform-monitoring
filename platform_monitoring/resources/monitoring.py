import asyncio
import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence
from contextlib import suppress
from typing import Protocol, Self, TypeVar

import tenacity
from apolo_kube_client import KubeClient
from kubernetes.client.models import V1Node, V1Pod
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

AMD_GPU_DEVICE_ID = "amd.com/gpu.device-id"
AMD_GPU_VRAM = "amd.com/gpu.vram"

PODS_LABEL_SELECTOR = (
    f"!{APOLO_PLATFORM_JOB_LABEL_KEY},!{APOLO_PLATFORM_APP_INSTANCE_NAME_LABEL_KEY}"
)


class _KubeState(Protocol):
    @property
    def nodes(self) -> Mapping[str, V1Node]: ...

    @property
    def pods(self) -> Mapping[str, V1Pod]: ...


class MonitoringService(_KubeState):
    def __init__(
        self,
        *,
        kube_client: KubeClient,
        config_client: ConfigClient,
        cluster_name: str,
    ) -> None:
        self._kube_client = kube_client
        self._nodes: dict[str, V1Node] = {}
        self._pods: dict[str, V1Pod] = {}
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
    def nodes(self) -> Mapping[str, V1Node]:
        return self._nodes

    @property
    def pods(self) -> Mapping[str, V1Pod]:
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

    async def _reset_nodes(self) -> str:
        node_list = await self._kube_client.core_v1.node.get_list()
        resource_version = node_list.metadata.resource_version
        LOGGER.info("Nodes resource version: %s", resource_version)
        self._init_nodes(node_list.items)
        return resource_version

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
                async for event in watch.stream():
                    match event.type:
                        case "ADDED" | "MODIFIED":
                            self._handle_node_update(event.object)
                        case "DELETED":
                            self._handle_node_deletion(event.object)
                    self._cluster_syncer.notify()
            except Exception:
                LOGGER.exception("Nodes watcher failed")

            resource_version = None

    def _init_nodes(self, nodes: Sequence[V1Node]) -> None:
        LOGGER.debug("Initializing nodes")
        new_nodes = {}
        for node in nodes:
            if self._is_workload_node(node):
                LOGGER.debug("Adding workload node %s", node.metadata.name)
                new_nodes[node.metadata.name] = node
            else:
                LOGGER.debug("Ignoring non-workload node %s", node.metadata.name)
        self._nodes = new_nodes

    def _is_workload_node(self, node: V1Node) -> bool:
        labels = node.metadata.labels
        LOGGER.debug("Node %s labels: %r", node.metadata.name, labels)
        if not labels:
            return False
        if value := labels.get(APOLO_PLATFORM_ROLE_LABEL_KEY):
            if value.lower() != "workload":
                return False
        return bool(_get_node_pool_name(node))

    def _handle_node_update(self, node: V1Node) -> None:
        if self._is_workload_node(node):
            LOGGER.debug("Updating workload node %s", node.metadata.name)
            self._nodes[node.metadata.name] = node
        else:
            LOGGER.debug("Ignoring non-workload node %s", node.metadata.name)
            self._nodes.pop(node.metadata.name, None)

    def _handle_node_deletion(self, node: V1Node) -> None:
        result = self._nodes.pop(node.metadata.name, None)
        if result is not None:
            LOGGER.debug("Removing node %s", node.metadata.name)

    async def _reset_pods(self) -> str:
        pod_list = await self._kube_client.core_v1.pod.get_list(
            label_selector=PODS_LABEL_SELECTOR, all_namespaces=True
        )
        resource_version = pod_list.metadata.resource_version
        LOGGER.info("Pods resource version: %s", resource_version)
        self._init_pods(pod_list.items)
        return resource_version

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
                async for event in watch.stream():
                    match event.type:
                        case "ADDED" | "MODIFIED":
                            self._handle_pod_update(event.object)
                        case "DELETED":
                            self._handle_pod_deletion(event.object)
                    self._cluster_syncer.notify()
            except Exception:
                LOGGER.exception("Pods watcher failed")

            resource_version = None

    def _init_pods(self, pods: Sequence[V1Pod]) -> None:
        LOGGER.debug("Initializing pods")
        new_pods = {}
        for pod in pods:
            if self._is_pod_running(pod):
                LOGGER.debug("Adding running pod %s", pod.metadata.name)
                new_pods[pod.metadata.name] = pod
            else:
                LOGGER.debug("Ignoring non-running pod %s", pod.metadata.name)
        self._pods = new_pods

    def _is_pod_running(self, pod: V1Pod) -> bool:
        LOGGER.debug("Pod %s node name: %r", pod.metadata.name, pod.spec.node_name)
        LOGGER.debug("Pod %s status: %r", pod.metadata.name, pod.status)
        return pod.spec.node_name and pod.status and pod.status.phase == "Running"

    def _handle_pod_update(self, pod: V1Pod) -> None:
        if self._is_pod_running(pod):
            LOGGER.debug("Updating running pod %s", pod.metadata.name)
            self._pods[pod.metadata.name] = pod
        else:
            LOGGER.debug("Ignoring non-running pod %s", pod.metadata.name)
            self._pods.pop(pod.metadata.name, None)

    def _handle_pod_deletion(self, pod: V1Pod) -> None:
        result = self._pods.pop(pod.metadata.name, None)
        if result is not None:
            LOGGER.debug("Removing pod %s", pod.metadata.name)


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

        last_synced_pool_types: list[ResourcePoolType] = []

        while True:
            await self._event.wait()
            self._event.clear()

            LOGGER.info("Syncing cluster resource pools")
            last_synced_pool_types = await self._sync_cluster(last_synced_pool_types)

    # Retry forever
    @tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=0.1, max=15),
        before_sleep=tenacity.before_sleep_log(LOGGER, logging.WARNING),
        after=tenacity.after_log(LOGGER, logging.INFO),
        retry=tenacity.retry_if_not_exception_type(asyncio.CancelledError),
    )
    async def _sync_cluster(
        self, last_synced_pool_types: Sequence[ResourcePoolType]
    ) -> list[ResourcePoolType]:
        pool_types = self._create_resource_pool_types()
        if pool_types == last_synced_pool_types:
            LOGGER.info("Cluster resource pools are up-to-date")
            return pool_types

        LOGGER.info("Updating cluster resource pools")

        await self._config_client.patch_cluster(
            self._cluster_name,
            PatchClusterRequest(
                orchestrator=PatchOrchestratorConfigRequest(
                    resource_pool_types=pool_types
                )
            ),
        )
        return pool_types

    def _create_resource_pool_types(self) -> list[ResourcePoolType]:
        result = []
        pods = self._group_pods_by_node()
        for nodes in self._group_nodes_by_node_pool().values():
            pool_type = self._resource_pool_type_factory.create_from_nodes(nodes, pods)
            result.append(pool_type)
            LOGGER.debug("Detected resource pool type: %r", pool_type)
        result.sort(key=lambda t: t.name)
        return result

    def _group_nodes_by_node_pool(self) -> dict[str, list[V1Node]]:
        result = defaultdict(list)
        for node in self._kube_state.nodes.values():
            node_pool_name = _get_node_pool_name(node)
            if node_pool_name:
                result[node_pool_name].append(node)
        return result

    def _group_pods_by_node(self) -> dict[str, list[V1Pod]]:
        result = defaultdict(list)
        for pod in self._kube_state.pods.values():
            if pod.spec.node_name:
                result[pod.spec.node_name].append(pod)
        return result


_T_GPU = TypeVar("_T_GPU", bound=GPU)


class ResourcePoolTypeFactory:
    def create_from_nodes(
        self, nodes: Sequence[V1Node], pods: Mapping[str, Sequence[V1Pod]]
    ) -> ResourcePoolType:
        assert nodes, "at least one node is required"

        name = _get_node_pool_name(nodes[0])
        assert name is not None, "node pool name must be set"

        cpu = []
        available_cpu = []
        memory = []
        available_memory = []
        disk_size = []
        available_disk_size = []
        nvidia_gpu = []
        amd_gpu = []
        # TODO: support Intel GPU

        for node in nodes:
            capacity = NodeResources.from_primitive(node.status.capacity or {})
            LOGGER.debug("Node %s capacity: %r", node.metadata.name, capacity)
            allocatable = NodeResources.from_primitive(
                node.status.allocatable or node.status.capacity or {}
            )
            LOGGER.debug("Node %s allocatable: %r", node.metadata.name, allocatable)
            allocated = self._get_node_allocated_resources(
                pods.get(node.metadata.name, ())
            )
            LOGGER.debug("Node %s allocated: %r", node.metadata.name, allocated)

            # Subtract resources that are allocated to platform services on each node
            cpu.append(capacity.cpu)
            available_cpu.append(allocatable.cpu - allocated.cpu)
            memory.append(capacity.memory)
            available_memory.append(allocatable.memory - allocated.memory)
            disk_size.append(capacity.ephemeral_storage)
            available_disk_size.append(allocatable.ephemeral_storage)
            if ng := self._create_nvidia_gpu(node, capacity):
                nvidia_gpu.append(ng)
            if ag := self._create_amd_gpu(node, capacity):
                amd_gpu.append(ag)

        return ResourcePoolType(
            name=name,
            min_size=len(nodes),
            max_size=len(nodes),
            cpu=min(cpu),
            available_cpu=min(available_cpu),
            memory=min(memory),
            available_memory=min(available_memory),
            disk_size=min(disk_size),
            available_disk_size=min(available_disk_size),
            nvidia_gpu=self._min_gpu(nvidia_gpu, NvidiaGPU),
            amd_gpu=self._min_gpu(amd_gpu, AMDGPU),
        )

    def _get_node_allocated_resources(
        self, pods: Sequence[V1Pod]
    ) -> ContainerResources:
        resources = ContainerResources()
        for pod in pods:
            for container in pod.spec.containers:
                if container.resources and container.resources.requests:
                    resources += ContainerResources.from_primitive(
                        container.resources.requests
                    )
        return resources

    def _create_nvidia_gpu(
        self, node: V1Node, capacity: NodeResources
    ) -> NvidiaGPU | None:
        if not capacity.has_nvidia_gpu:
            return None
        labels = node.metadata.labels
        if not labels:
            return None
        if model := labels.get(NVIDIA_GPU_PRODUCT):
            return NvidiaGPU(
                count=capacity.nvidia_gpu,
                model=model,
                memory=parse_memory(labels.get(NVIDIA_GPU_MEMORY, "0")),
            )
        return None

    def _create_amd_gpu(self, node: V1Node, capacity: NodeResources) -> AMDGPU | None:
        if not capacity.has_amd_gpu:
            return None
        labels = node.metadata.labels
        if not labels:
            return None
        if model := labels.get(AMD_GPU_DEVICE_ID):
            return AMDGPU(
                count=capacity.amd_gpu,
                model=model,
                memory=parse_memory(labels.get(AMD_GPU_VRAM, "0")),
            )
        return None

    def _min_gpu(self, gpus: list[_T_GPU], cls: type[_T_GPU]) -> _T_GPU | None:
        if not gpus:
            return None
        model = next(gpu.model for gpu in gpus if gpu.model)
        count = min(gpu.count for gpu in gpus if gpu.model == model)
        memory = min(gpu.memory for gpu in gpus if gpu.memory and gpu.model == model)
        return cls(count=count, model=model, memory=memory)
