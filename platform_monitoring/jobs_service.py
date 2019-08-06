from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict

from aiodocker.exceptions import DockerError
from neuromation.api import JobDescription as Job
from neuromation.api.jobs import Jobs as JobsClient
from platform_monitoring.config import DockerConfig

from .docker_client import Docker, ImageReference
from .kube_client import KubeClient
from .user import User
from .utils import KubeHelper


@dataclass(frozen=True)
class Container:
    image: ImageReference


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
        pod = await self._kube_client.get_pod(pod_name)
        if not pod.is_phase_running:
            raise JobException(f"Job '{job.id}' is not running.")

        cont_id = pod.get_container_id(pod_name)
        assert cont_id
        async with self._kube_client.get_node_proxy_client(
            pod.node_name, self._docker_config.docker_engine_api_port
        ) as proxy_client:
            docker = Docker(
                url=str(proxy_client.url),
                session=proxy_client.session,
                connector=proxy_client.session.connector,
            )
            try:
                repo = container.image.repository
                name = container.image.path
                tag = container.image.tag

                yield {
                    "status": f"Committing container {cont_id} as image {name}:{tag}"
                }
                await docker.images.commit(container=cont_id, repo=repo, tag=tag)
                # TODO (A.Yushkovskiy) check result of commit() and break if failed
                yield {"status": "Committed"}

                push_auth = dict(username=user.name, password=user.token)
                async for chunk in await docker.images.push(
                    name=repo, tag=tag, auth=push_auth, stream=True
                ):
                    yield chunk

            except DockerError as error:
                raise JobException(f"Failed to save job '{job.id}': {error}")
