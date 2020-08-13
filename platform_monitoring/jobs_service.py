import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import reduce
from itertools import groupby
from typing import Any, AsyncIterator, Dict, List, Mapping, Optional, Tuple, Union, cast

from aiodocker.exceptions import DockerError
from aiodocker.stream import Stream
from neuromation.api import JobDescription as Job
from neuromation.api.jobs import Jobs as JobsClient

from platform_monitoring.config import DockerConfig

from .config import DOCKER_API_VERSION
from .config_client import Cluster, ConfigClient, NodePool
from .docker_client import Docker, ImageReference
from .kube_client import JobNotFoundException, KubeClient, Pod, PodPhase, Resources
from .user import User
from .utils import KubeHelper


NODE_POOL_LABEL = "platform.neuromation.io/nodepool"
JOB_LABEL = "platform.neuromation.io/job"


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


class NodeNotFoundException(Exception):
    def __init__(self, name: str) -> None:
        super().__init__(f"Node {name!r} was not found")


@dataclass(frozen=True)
class Preset:
    cpu: float
    memory_mb: int
    gpu: int = 0
    gpu_model: str = ""
    is_preemptible: bool = False

    @property
    def resources(self) -> Resources:
        return Resources(
            cpu_m=int(self.cpu * 1000), memory_mb=self.memory_mb, gpu=self.gpu,
        )

    def can_be_scheduled(self, node_pool: NodePool) -> bool:
        return self.gpu_model == node_pool.gpu_model and (
            self.is_preemptible or not node_pool.is_preemptible
        )


class JobsService:
    def __init__(
        self,
        config_client: ConfigClient,
        jobs_client: JobsClient,
        kube_client: KubeClient,
        docker_config: DockerConfig,
        cluster_name: str,
    ) -> None:
        self._config_client = config_client
        self._jobs_client = jobs_client
        self._kube_client = kube_client
        self._kube_helper = KubeHelper()
        self._docker_config = docker_config
        self._cluster_name = cluster_name

    async def get(self, job_id: str) -> Job:
        return await self._jobs_client.status(job_id)

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
            raise JobException(f"Job '{job_id}' is not running.")

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

    async def get_available_jobs_counts(
        self, presets: Mapping[str, Preset]
    ) -> Mapping[str, int]:
        result: Dict[str, int] = {}
        cluster = await self._config_client.get_cluster(self._cluster_name)
        resource_requests = await self._get_resource_requests_by_node_pool()
        for preset_name, preset in presets.items():
            available_jobs_count = 0
            for node_pool in cluster.node_pools:
                if not preset.can_be_scheduled(node_pool):
                    continue
                node_resource_limit = self._get_node_resource_limit(node_pool)
                node_resource_requests = resource_requests.get(node_pool.name, [])
                running_nodes_count = len(node_resource_requests)
                free_nodes_count = (
                    self._get_max_nodes_count(cluster, node_pool) - running_nodes_count
                )
                # get number of jobs that can be scheduled on running nodes
                # in the current node pool
                for request in node_resource_requests:
                    available_resources = node_resource_limit.available(request)
                    available_jobs_count += available_resources.count(preset.resources)
                # get number of jobs that can be scheduled on free nodes
                # in the current node pool
                if free_nodes_count > 0:
                    available_jobs_count += (
                        free_nodes_count * node_resource_limit.count(preset.resources)
                    )
            result[preset_name] = available_jobs_count
        return result

    async def _get_resource_requests_by_node_pool(self) -> Dict[str, List[Resources]]:
        result: Dict[str, List[Resources]] = {}
        pods = await self._kube_client.get_pods(
            label_selector=JOB_LABEL, phases=(PodPhase.PENDING, PodPhase.RUNNING)
        )
        nodes = await self._kube_client.get_nodes(label_selector=JOB_LABEL)
        for node_name, node_pods in groupby(pods, lambda p: p.node_name):
            if not node_name:  # pragma: no coverage
                continue
            for node in nodes:
                if node.name == node_name:
                    break
            else:
                raise NodeNotFoundException(node_name)
            node_pool_name = node.get_label(NODE_POOL_LABEL)
            if not node_pool_name:  # pragma: no coverage
                continue
            pod_resources = [p.resource_requests for p in node_pods]
            node_resources = reduce(Resources.add, pod_resources, Resources())
            result.setdefault(node_pool_name, []).append(node_resources)
        return result

    def _get_max_nodes_count(self, cluster: Cluster, node_pool: NodePool) -> int:
        return max(1, cluster.zones_count) * node_pool.max_size

    def _get_node_resource_limit(self, node_pool: NodePool) -> Resources:
        return Resources(
            cpu_m=node_pool.available_cpu_m,
            memory_mb=node_pool.available_memory_mb,
            gpu=node_pool.gpu,
        )
