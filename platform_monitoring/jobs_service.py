import asyncio
from collections import defaultdict
from collections.abc import AsyncGenerator, AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass

import aiohttp
from neuro_config_client import ConfigClient, ResourcePoolType

from .config import KubeConfig
from .container_runtime_client import (
    ContainerNotFoundError,
    ContainerRuntimeClientRegistry,
    ContainerRuntimeError,
)
from .kube_client import (
    DEFAULT_MAX_PODS_PER_NODE,
    ContainerResources,
    JobNotFoundException,
    KubeClient,
    NodeResources,
    Pod,
)
from .platform_api_client import ApiClient, Job
from .user import User
from .utils import KubeHelper, asyncgeneratorcontextmanager


@dataclass(frozen=True)
class ExecCreate:
    cmd: str
    stdin: bool
    stdout: bool
    stderr: bool
    tty: bool


class JobException(Exception):
    pass


class JobNotRunningException(JobException):
    pass


class NodeNotFoundException(Exception):
    def __init__(self, name: str) -> None:
        super().__init__(f"Node {name!r} was not found")


class JobsService:
    def __init__(
        self,
        *,
        config_client: ConfigClient,
        jobs_client: ApiClient,
        kube_client: KubeClient,
        container_runtime_client_registry: ContainerRuntimeClientRegistry,
        cluster_name: str,
        kube_node_pool_label: str = KubeConfig.node_pool_label,
    ) -> None:
        self._config_client = config_client
        self._jobs_client = jobs_client
        self._kube_client = kube_client
        self._kube_helper = KubeHelper()
        self._container_runtime_client_registry = container_runtime_client_registry
        self._cluster_name = cluster_name
        self._kube_node_pool_label = kube_node_pool_label

    async def get(self, job_id: str) -> Job:
        return await self._jobs_client.get_job(job_id)

    def get_jobs_for_log_removal(
        self,
    ) -> AbstractAsyncContextManager[AsyncGenerator[Job, None]]:
        return self._jobs_client.get_jobs(
            cluster_name=self._cluster_name,
            being_dropped=True,
            logs_removed=False,
        )

    async def mark_logs_dropped(self, job_id: str) -> None:
        await self._jobs_client.mark_job_logs_dropped(job_id)

    @asyncgeneratorcontextmanager
    async def save(
        self, job: Job, user: User, image: str
    ) -> AsyncGenerator[bytes, None]:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)
        assert cont_id

        assert pod.status.host_ip
        runtime_client = await self._container_runtime_client_registry.get(
            pod.status.host_ip
        )
        # print(555555, runtime_client, pod.status.host_ip)

        try:
            async with runtime_client.commit(
                container_id=cont_id,
                image=image,
                username=user.name,
                password=user.token,
            ) as commit:
                async for chunk in commit:
                    # print(66666, chunk)
                    yield chunk
        except ContainerNotFoundError:
            raise
        except ContainerRuntimeError as ex:
            msg = f"Failed to save job '{job.id}': {ex}"
            raise JobException(msg) from ex

    @asynccontextmanager
    async def attach(
        self,
        job: Job,
        *,
        tty: bool = False,
        stdin: bool = False,
        stdout: bool = True,
        stderr: bool = True,
    ) -> AsyncIterator[aiohttp.ClientWebSocketResponse]:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)
        assert cont_id

        if pod.stdin and not pod.stdin_once:
            # If stdin remains open we need to pass stdin = True.
            # Otherwise connection closes immediately.
            stdin = True

        if not pod.stdin:
            stdin = False

        if pod.tty and stdin and not tty:
            # Otherwise connection closes immediately.
            tty = True

        if not pod.tty:
            tty = False

        assert pod.status.host_ip
        runtime_client = await self._container_runtime_client_registry.get(
            pod.status.host_ip
        )

        async with runtime_client.attach(
            cont_id, tty=tty, stdin=stdin, stdout=stdout, stderr=stderr
        ) as ws:
            yield ws

    @asynccontextmanager
    async def exec(
        self,
        job: Job,
        *,
        cmd: str,
        tty: bool = False,
        stdin: bool = False,
        stdout: bool = True,
        stderr: bool = True,
    ) -> AsyncIterator[aiohttp.ClientWebSocketResponse]:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)
        assert cont_id

        assert pod.status.host_ip
        runtime_client = await self._container_runtime_client_registry.get(
            pod.status.host_ip
        )

        async with runtime_client.exec(
            cont_id, cmd, tty=tty, stdin=stdin, stdout=stdout, stderr=stderr
        ) as ws:
            yield ws

    async def kill(self, job: Job) -> None:
        pod_name = self._kube_helper.get_job_pod_name(job)

        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)
        assert cont_id

        assert pod.status.host_ip
        runtime_client = await self._container_runtime_client_registry.get(
            pod.status.host_ip
        )

        await runtime_client.kill(cont_id)

    async def port_forward(
        self, job: Job, port: int
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._get_running_jobs_pod(pod_name)
        # print(44444444, pod.status.pod_ip, port)
        reader, writer = await asyncio.open_connection(pod.status.pod_ip, port)
        return reader, writer

    async def _get_running_jobs_pod(self, job_id: str) -> Pod:
        pod: Pod | None
        try:
            pod = await self._kube_client.get_pod(job_id)
            if not pod.status.is_running:
                pod = None
        except JobNotFoundException:
            # job's pod does not exist: it might be already garbage-collected
            pod = None

        if not pod:
            msg = f"Job '{job_id}' is not running."
            raise JobNotRunningException(msg)

        return pod

    async def get_available_jobs_counts(self) -> Mapping[str, int]:
        result: dict[str, int] = {}
        cluster = await self._config_client.get_cluster(self._cluster_name)
        assert cluster.orchestrator is not None
        available_resources = await self._get_available_resources_by_node_pool()
        pool_types = {p.name: p for p in cluster.orchestrator.resource_pool_types}
        for preset in cluster.orchestrator.resource_presets:
            available_jobs_count = 0
            preset_resources = ContainerResources(
                cpu_m=int(preset.cpu * 1000),
                memory=preset.memory,
                nvidia_gpu=preset.nvidia_gpu or 0,
                amd_gpu=preset.amd_gpu or 0,
            )
            node_pools = [pool_types[r] for r in preset.available_resource_pool_names]
            for node_pool in node_pools:
                node_resource_limit = self._get_node_resource_limit(node_pool)
                node_pool_available_resources = available_resources.get(
                    node_pool.name, []
                )
                running_nodes_count = len(node_pool_available_resources)
                free_nodes_count = node_pool.max_size - running_nodes_count
                # get number of jobs that can be scheduled on running nodes
                # in the current node pool
                for node_available_resources in node_pool_available_resources:
                    available_jobs_count += min(
                        node_available_resources // preset_resources,
                        node_available_resources.pods,
                    )
                # get number of jobs that can be scheduled on free nodes
                # in the current node pool
                if free_nodes_count > 0:
                    available_jobs_count += free_nodes_count * min(
                        DEFAULT_MAX_PODS_PER_NODE,
                        node_resource_limit // preset_resources,
                    )
            result[preset.name] = available_jobs_count
        return result

    async def _get_available_resources_by_node_pool(
        self,
    ) -> dict[str, list[NodeResources]]:
        result: dict[str, list[NodeResources]] = defaultdict(list)
        pods = await self._kube_client.get_pods(
            all_namespaces=True,
            field_selector=",".join(
                (
                    "status.phase!=Failed",
                    "status.phase!=Succeeded",
                    "status.phase!=Unknown",
                ),
            ),
        )
        nodes = await self._kube_client.get_nodes(
            label_selector=self._kube_node_pool_label
        )
        nodes_by_name = {node.metadata.name: node for node in nodes}
        for node_name, node_pods in self._get_pods_by_node(pods).items():
            if not (node := nodes_by_name.get(node_name)):
                continue
            node_pool_name = node.metadata.labels[self._kube_node_pool_label]
            resource_requests = sum(
                (pod.resource_requests for pod in node_pods), ContainerResources()
            )
            available_resources = node.status.allocatable - resource_requests
            available_resources = available_resources.with_pods(
                available_resources.pods - len(node_pods)
            )
            result[node_pool_name].append(available_resources)
        return result

    def _get_pods_by_node(self, pods: Sequence[Pod]) -> dict[str, list[Pod]]:
        result: dict[str, list[Pod]] = defaultdict(list)
        for pod in pods:
            if pod.spec.node_name:
                result[pod.spec.node_name].append(pod)
        return result

    def _get_node_resource_limit(
        self, node_pool: ResourcePoolType
    ) -> ContainerResources:
        return ContainerResources(
            cpu_m=int(node_pool.available_cpu * 1000),
            memory=node_pool.available_memory,
            nvidia_gpu=node_pool.nvidia_gpu or 0,
            amd_gpu=node_pool.amd_gpu or 0,
        )
