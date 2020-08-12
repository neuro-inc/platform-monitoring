import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Optional, Tuple, Union, cast

from aiodocker.exceptions import DockerError
from aiodocker.stream import Stream
from neuromation.api import JobDescription as Job
from neuromation.api.jobs import Jobs as JobsClient

from platform_monitoring.config import DockerConfig

from .config import DOCKER_API_VERSION
from .docker_client import Docker, ImageReference
from .kube_client import JobNotFoundException, KubeClient, Pod
from .user import User
from .utils import KubeHelper


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


class JobsService:
    def __init__(
        self,
        jobs_client: JobsClient,
        kube_client: KubeClient,
        docker_config: DockerConfig,
    ) -> None:
        self._jobs_client = jobs_client
        self._kube_client = kube_client
        self._kube_helper = KubeHelper()
        self._docker_config = docker_config

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
