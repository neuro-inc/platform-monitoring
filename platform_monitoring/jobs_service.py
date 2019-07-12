from dataclasses import dataclass

from neuromation.api import JobDescription as Job
from neuromation.api.jobs import Jobs as JobsClient

from .docker_client import Docker, DockerError, check_docker_push_result
from .kube_client import KubeClient
from .user import User
from .utils import KubeHelper


@dataclass(frozen=True)
class ContainerImage:
    repo: str
    tag: str = "latest"

    # TODO: belongs

    @classmethod
    def create(cls, image: str) -> "ContainerImage":
        repo, tag = image.rsplit(":", 1)
        # TODO: validate
        return ContainerImage(repo, tag or cls.tag)

    def __str__(self) -> str:
        return f"{self.repo}:{self.tag}"


@dataclass(frozen=True)
class Container:
    image: ContainerImage


class JobException(Exception):
    pass


class JobsService:
    def __init__(self, jobs_client: JobsClient, kube_client: KubeClient) -> None:
        self._jobs_client = jobs_client
        self._kube_client = kube_client

        self._kube_helper = KubeHelper()

        self._docker_engine_api_port = 2375

    async def get(self, job_id: str) -> Job:
        return await self._jobs_client.status(job_id)

    async def save(self, job: Job, user: User, container: Container) -> None:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._kube_client.get_pod(pod_name)
        if not pod.is_phase_running:
            raise JobException(f"Job '{job.id}' is not running.")

        container_id = pod.get_container_id(pod_name)
        assert container_id
        async with self._kube_client.get_node_proxy_client(
            pod.node_name, self._docker_engine_api_port
        ) as proxy_client:
            docker = Docker(
                url=str(proxy_client.url),
                session=proxy_client.session,
                connector=proxy_client.session.connector,
            )
            try:
                await docker.images.commit(
                    container=container_id,
                    repo=container.image.repo,
                    tag=container.image.tag,
                )
                push_result = await docker.images.push(
                    name=container.image.repo,
                    tag=container.image.tag,
                    auth=dict(username=user.name, password=user.token),
                )
                check_docker_push_result(
                    container.image.repo, container.image.tag, push_result
                )
            except DockerError as error:
                raise JobException(f"Failed to save job '{job.id}': {error}")
