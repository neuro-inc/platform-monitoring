import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from functools import reduce
from typing import Optional

import aiohttp
from neuro_config_client import ConfigClient
from neuro_config_client.models import ResourcePoolType
from neuro_sdk import JobDescription as Job, Jobs as JobsClient

from .config import KubeConfig
from .container_runtime_client import (
    ContainerNotFoundError,
    ContainerRuntimeClientRegistry,
    ContainerRuntimeError,
)
from .kube_client import JobNotFoundException, KubeClient, Pod, Resources
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
        config_client: ConfigClient,
        jobs_client: JobsClient,
        kube_client: KubeClient,
        container_runtime_client_registry: ContainerRuntimeClientRegistry,
        cluster_name: str,
        kube_job_label: str = KubeConfig.job_label,
        kube_node_pool_label: str = KubeConfig.node_pool_label,
    ) -> None:
        self._config_client = config_client
        self._jobs_client = jobs_client
        self._kube_client = kube_client
        self._kube_helper = KubeHelper()
        self._container_runtime_client_registry = container_runtime_client_registry
        self._cluster_name = cluster_name
        self._kube_job_label = kube_job_label
        self._kube_node_pool_label = kube_node_pool_label

    async def get(self, job_id: str) -> Job:
        return await self._jobs_client.status(job_id)

    def get_jobs_for_log_removal(
        self,
    ) -> AbstractAsyncContextManager[AsyncIterator[Job]]:
        return self._jobs_client.list(
            cluster_name=self._cluster_name,
            _being_dropped=True,
            _logs_removed=False,
        )

    async def mark_logs_dropped(self, job_id: str) -> None:
        url = self._jobs_client._config.api_url / "jobs" / job_id / "drop_progress"
        payload = {"logs_removed": True}
        auth = await self._jobs_client._config._api_auth()
        async with self._jobs_client._core.request(
            "POST", url, auth=auth, json=payload
        ):
            pass

    @asyncgeneratorcontextmanager
    async def save(self, job: Job, user: User, image: str) -> AsyncIterator[bytes]:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)
        assert cont_id

        runtime_client = await self._container_runtime_client_registry.get(pod.host_ip)

        try:
            async with runtime_client.commit(
                container_id=cont_id,
                image=image,
                username=user.name,
                password=user.token,
            ) as commit:
                async for chunk in commit:
                    yield chunk
        except ContainerNotFoundError:
            raise
        except ContainerRuntimeError as ex:
            raise JobException(f"Failed to save job '{job.id}': {ex}")

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

        runtime_client = await self._container_runtime_client_registry.get(pod.host_ip)

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

        runtime_client = await self._container_runtime_client_registry.get(pod.host_ip)

        async with runtime_client.exec(
            cont_id, cmd, tty=tty, stdin=stdin, stdout=stdout, stderr=stderr
        ) as ws:
            yield ws

    async def kill(self, job: Job) -> None:
        pod_name = self._kube_helper.get_job_pod_name(job)

        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)
        assert cont_id

        runtime_client = await self._container_runtime_client_registry.get(pod.host_ip)

        await runtime_client.kill(cont_id)

    async def port_forward(
        self, job: Job, port: int
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._get_running_jobs_pod(pod_name)
        reader, writer = await asyncio.open_connection(pod.pod_ip, port)
        return reader, writer

    async def _get_running_jobs_pod(self, job_id: str) -> Pod:
        pod: Optional[Pod]
        try:
            pod = await self._kube_client.get_pod(job_id)
            if not pod.is_phase_running:
                pod = None
        except JobNotFoundException:
            # job's pod does not exist: it might be already garbage-collected
            pod = None

        if not pod:
            raise JobNotRunningException(f"Job '{job_id}' is not running.")

        return pod

    async def get_available_jobs_counts(self) -> Mapping[str, int]:
        result: dict[str, int] = {}
        cluster = await self._config_client.get_cluster(self._cluster_name)
        assert cluster.orchestrator is not None
        resource_requests = await self._get_resource_requests_by_node_pool()
        pool_types = {p.name: p for p in cluster.orchestrator.resource_pool_types}
        for preset in cluster.orchestrator.resource_presets:
            available_jobs_count = 0
            preset_resources = Resources(
                cpu_m=int(preset.cpu * 1000),
                memory_mb=preset.memory_mb,
                gpu=preset.gpu or 0,
            )
            preset_pool_types = [pool_types[r] for r in preset.resource_affinity]
            for node_pool in preset_pool_types:
                node_resource_limit = self._get_node_resource_limit(node_pool)
                node_resource_requests = resource_requests.get(node_pool.name, [])
                running_nodes_count = len(node_resource_requests)
                free_nodes_count = node_pool.max_size - running_nodes_count
                # get number of jobs that can be scheduled on running nodes
                # in the current node pool
                for request in node_resource_requests:
                    available_resources = node_resource_limit.available(request)
                    available_jobs_count += available_resources.count(preset_resources)
                # get number of jobs that can be scheduled on free nodes
                # in the current node pool
                if free_nodes_count > 0:
                    available_jobs_count += (
                        free_nodes_count * node_resource_limit.count(preset_resources)
                    )
            result[preset.name] = available_jobs_count
        return result

    async def _get_resource_requests_by_node_pool(self) -> dict[str, list[Resources]]:
        result: dict[str, list[Resources]] = {}
        pods = await self._kube_client.get_pods(
            label_selector=self._kube_job_label,
            field_selector=",".join(
                (
                    "status.phase!=Failed",
                    "status.phase!=Succeeded",
                    "status.phase!=Unknown",
                ),
            ),
        )
        nodes = await self._kube_client.get_nodes(label_selector=self._kube_job_label)
        for node_name, node_pods in self._group_pods_by_node(pods).items():
            if not node_name:
                continue
            for node in nodes:
                if node.name == node_name:
                    break
            else:
                raise NodeNotFoundException(node_name)
            node_pool_name = node.get_label(self._kube_node_pool_label)
            if not node_pool_name:  # pragma: no coverage
                continue
            pod_resources = [p.resource_requests for p in node_pods]
            node_resources = reduce(Resources.add, pod_resources, Resources())
            result.setdefault(node_pool_name, []).append(node_resources)
        return result

    def _group_pods_by_node(
        self, pods: Sequence[Pod]
    ) -> dict[Optional[str], list[Pod]]:
        result: dict[Optional[str], list[Pod]] = {}
        for pod in pods:
            group = result.get(pod.node_name)
            if not group:
                group = []
                result[pod.node_name] = group
            group.append(pod)
        return result

    def _get_node_resource_limit(self, node_pool: ResourcePoolType) -> Resources:
        return Resources(
            cpu_m=int(node_pool.available_cpu * 1000),
            memory_mb=node_pool.available_memory_mb,
            gpu=node_pool.gpu or 0,
        )
