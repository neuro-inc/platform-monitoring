import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import reduce
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import aiohttp
from aiodocker.exceptions import DockerError
from aiodocker.stream import Stream
from elasticsearch.client import logger
from neuro_sdk import JobDescription as Job, Jobs as JobsClient
from platform_config_client import ConfigClient
from platform_config_client.models import ResourcePoolType

from platform_monitoring.config import DockerConfig

from .config import DOCKER_API_VERSION, KubeConfig
from .container_runtime_client import ContainerRuntimeClientRegistry
from .docker_client import Docker, ImageReference
from .kube_client import JobNotFoundException, KubeClient, Pod, Resources
from .user import User
from .utils import KubeHelper, asyncgeneratorcontextmanager


@dataclass(frozen=True)
class Container:
    image: ImageReference


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
        docker_config: DockerConfig,
        cluster_name: str,
        kube_job_label: str = KubeConfig.job_label,
        kube_node_pool_label: str = KubeConfig.node_pool_label,
    ) -> None:
        self._config_client = config_client
        self._jobs_client = jobs_client
        self._kube_client = kube_client
        self._kube_helper = KubeHelper()
        self._docker_config = docker_config
        self._container_runtime_client_registry = container_runtime_client_registry
        self._cluster_name = cluster_name
        self._kube_job_label = kube_job_label
        self._kube_node_pool_label = kube_node_pool_label

    async def get(self, job_id: str) -> Job:
        return await self._jobs_client.status(job_id)

    def get_jobs_for_log_removal(self) -> AsyncContextManager[AsyncIterator[Job]]:
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
    async def save(
        self, job: Job, user: User, container: Container
    ) -> AsyncIterator[Dict[str, Any]]:
        pod_name = self._kube_helper.get_job_pod_name(job)

        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)
        assert cont_id
        async with self._get_docker_client(pod) as docker:
            try:
                repo = container.image.repository
                tag = container.image.tag

                yield {
                    "status": "CommitStarted",
                    "details": {"container": cont_id, "image": f"{repo}:{tag}"},
                }
                await docker.images.commit(container=cont_id, repo=repo, tag=tag)  # type: ignore  # noqa
                # TODO (A.Yushkovskiy) check result of commit() and break if failed
                yield {"status": "CommitFinished"}

                push_auth = dict(username=user.name, password=user.token)
                async for chunk in docker.images.push(
                    name=repo, tag=tag, auth=push_auth, stream=True
                ):
                    yield chunk

            except DockerError as error:
                raise JobException(f"Failed to save job '{job.id}': {error}")

    @asynccontextmanager
    async def attach(
        self,
        job: Job,
        *,
        stdout: bool = False,
        stderr: bool = False,
        stdin: bool = False,
        logs: bool = False,
    ) -> AsyncIterator[Stream]:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)

        async with self._get_docker_client(pod) as docker:
            container = docker.containers.container(cont_id)
            # If container is dead, we should close the connection
            # Unfortunately, if it died recently, the docker API
            # will not update immediately, so we have to do some re-checks
            # here. This adds 1 second delay (configurable below):
            checks = 2
            while checks > 0:
                data = await container.show()
                if not data["State"]["Running"]:
                    raise JobNotRunningException(f"Job '{job.id}' is not running.")
                checks -= 1
                if checks > 0:
                    await asyncio.sleep(0.5)

            async with container.attach(
                stdin=stdin, stdout=stdout, stderr=stderr, logs=logs
            ) as stream:
                yield stream

    async def resize(self, job: Job, *, w: int, h: int) -> None:
        pod_name = self._kube_helper.get_job_pod_name(job)

        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)
        assert cont_id

        async with self._get_docker_client(pod) as docker:
            container = docker.containers.container(cont_id)
            await container.resize(w=w, h=h)

    async def kill(self, job: Job, signal: Union[str, int]) -> None:
        pod_name = self._kube_helper.get_job_pod_name(job)

        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)
        assert cont_id

        async with self._get_docker_client(pod) as docker:
            container = docker.containers.container(cont_id)
            await container.kill(signal=signal)

    async def exec_create(
        self,
        job: Job,
        cmd: str,
        stdout: bool = True,
        stderr: bool = True,
        stdin: bool = False,
        tty: bool = False,
    ) -> str:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)
        async with self._get_docker_client(pod) as docker:
            container = docker.containers.container(cont_id)
            exe = await container.exec(
                cmd=cmd, stdin=stdin, stdout=stdout, stderr=stderr, tty=tty
            )
            return exe.id

    async def exec_resize(self, job: Job, exec_id: str, *, w: int, h: int) -> None:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._get_running_jobs_pod(pod_name)
        async with self._get_docker_client(pod) as docker:
            exe = docker.containers.exec(exec_id)
            await exe.resize(w=w, h=h)

    async def exec_inspect(self, job: Job, exec_id: str) -> Dict[str, Any]:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._get_running_jobs_pod(pod_name)
        async with self._get_docker_client(pod) as docker:
            exe = docker.containers.exec(exec_id)
            return await exe.inspect()

    @asynccontextmanager
    async def exec_start(self, job: Job, exec_id: str) -> AsyncIterator[Stream]:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._get_running_jobs_pod(pod_name)
        async with self._get_docker_client(pod) as docker:
            exe = docker.containers.exec(exec_id)
            async with exe.start(detach=False) as stream:
                yield stream

    @asynccontextmanager
    async def attach_v2(
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

        logger.info(pod.host_ip)

        runtime_client = await self._container_runtime_client_registry.get(pod.host_ip)

        async with runtime_client.attach(
            cont_id, tty=tty, stdin=stdin, stdout=stdout, stderr=stderr
        ) as ws:
            yield ws

    @asynccontextmanager
    async def exec_v2(
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

    async def kill_v2(self, job: Job) -> None:
        pod_name = self._kube_helper.get_job_pod_name(job)

        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)
        assert cont_id

        runtime_client = await self._container_runtime_client_registry.get(pod.host_ip)

        await runtime_client.kill(cont_id)

    async def port_forward(
        self, job: Job, port: int
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
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

    @asynccontextmanager
    async def _get_docker_client(
        self, pod: Pod, force_close: bool = True
    ) -> AsyncIterator[Docker]:
        # NOTE: because Docker socket is wrapped in a proxy it may delay socket
        # disconnects, making the keep-alive socket taken from pool not reliable (it
        # may have already disconnected). We did see some examples of such behaviour
        # with Nginx. Thus we end up with "force-close" to recycle the socket after
        # each connection to be sure we can execute the command safely.

        url = f"http://{pod.host_ip}:{self._docker_config.docker_engine_api_port}"
        session = await self._kube_client.create_http_client(force_close=force_close)
        async with Docker(
            url=url,
            session=session,
            connector=session.connector,
            api_version=DOCKER_API_VERSION,
        ) as docker:
            # from "aiodocker.docker.Docker" to ".docker_client.Docker"
            yield cast(Docker, docker)

    async def get_available_jobs_counts(self) -> Mapping[str, int]:
        result: Dict[str, int] = {}
        cluster = await self._config_client.get_cluster(self._cluster_name)
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

    async def _get_resource_requests_by_node_pool(self) -> Dict[str, List[Resources]]:
        result: Dict[str, List[Resources]] = {}
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
    ) -> Dict[Optional[str], List[Pod]]:
        result: Dict[Optional[str], List[Pod]] = {}
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
