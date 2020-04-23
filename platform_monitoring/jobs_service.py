from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Optional, Sequence

from aiodocker.exceptions import DockerError
from aiodocker.stream import Stream
from neuromation.api import JobDescription as Job
from neuromation.api.jobs import Jobs as JobsClient
from platform_monitoring.config import DockerConfig

from .docker_client import Docker, ImageReference
from .kube_client import JobNotFoundException, KubeClient, Pod
from .user import User
from .utils import KubeHelper


def _monkey_patch() -> None:
    from aiohttp.client_reqrep import ClientResponse

    ClientResponse._response_eof = lambda self: None  # type: ignore


_monkey_patch()
del _monkey_patch


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

        pod = await self._get_running_jobs_pod(pod_name)
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
                tag = container.image.tag

                yield {
                    "status": "CommitStarted",
                    "details": {"container": cont_id, "image": f"{repo}:{tag}"},
                }
                await docker.images.commit(container=cont_id, repo=repo, tag=tag)
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

        async with self._kube_client.get_node_proxy_client(
            pod.node_name, self._docker_config.docker_engine_api_port
        ) as proxy_client:
            session = await self._kube_client.create_http_client()
            docker = Docker(
                url=str(proxy_client.url), session=session, connector=session.connector,
            )
            container = docker.containers.container(cont_id)

            from aiohttp.client_proto import ResponseHandler
            from aiohttp.client_reqrep import ClientResponse
            import sys

            old_close = ResponseHandler.close
            old_response_eof = ClientResponse._response_eof
            old_resp_close = ClientResponse.close
            old_release = ClientResponse.release

            def close(self: ResponseHandler) -> None:
                print("CLOSE")
                import traceback

                traceback.print_stack(file=sys.stdout)
                old_close(self)

            #ResponseHandler.close = close  # type: ignore

            def _response_eof(self: ClientResponse) -> None:
                print("RESPONSE_EOF", self.url)
                import traceback

                traceback.print_stack(file=sys.stdout)
                old_response_eof(self)
                print("CONN", repr(self._connection))

            ClientResponse._response_eof = _response_eof  # type: ignore

            def resp_close(self: ClientResponse) -> None:
                print("RESPONSE_CLOSE", self.url)
                import traceback

                traceback.print_stack(file=sys.stdout)
                old_resp_close(self)
                print("CONN", repr(self._connection))

            #ClientResponse.close = resp_close  # type: ignore

            def resp_release(self: ClientResponse) -> None:
                print("RESPONSE_RELEASE", self.url)
                import traceback

                traceback.print_stack(file=sys.stdout)
                old_release(self)
                print("CONN", repr(self._connection))

            #ClientResponse.release = resp_release  # type: ignore

            try:
                async with self._kube_client.get_node_proxy_client(
                    pod.node_name, self._docker_config.docker_engine_api_port
                ) as proxy_client:
                    session = await self._kube_client.create_http_client()
                    docker = Docker(
                        url=str(proxy_client.url),
                        session=session,
                        connector=session.connector,
                    )
                    container = docker.containers.container(cont_id)
                    await container.resize(w=80, h=25)
                    async with container.attach(
                        stdin=stdin, stdout=stdout, stderr=stderr, logs=logs
                    ) as stream:
                        print("BEFORE_YIELD")
                        try:
                            yield stream
                            print("AFTER_YIELD")
                        except Exception as exc:
                            print("AFTER_YIELD_EXC", exc)
                            raise
            finally:
                ResponseHandler.close = old_close  # type: ignore
                ClientResponse.close = old_resp_close  # type: ignore
                ClientResponse.release = old_release  # type: ignore
                ClientResponse._response_eof = old_response_eof  # type: ignore
            # async with container.attach(
            #     stdin=stdin, stdout=stdout, stderr=stderr, logs=logs
            # ) as stream:
            #     yield stream

    async def resize(self, job: Job, *, w: int, h: int) -> None:
        pod_name = self._kube_helper.get_job_pod_name(job)

        pod = await self._get_running_jobs_pod(pod_name)
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
            container = docker.containers.container(cont_id)
            await container.resize(w=w, h=h)

    @asynccontextmanager
    async def exec(
        self,
        job: Job,
        cmd: Sequence[str],
        stdout: bool = True,
        stderr: bool = True,
        stdin: bool = False,
        tty: bool = False,
    ) -> AsyncIterator[Stream]:
        pod_name = self._kube_helper.get_job_pod_name(job)
        pod = await self._get_running_jobs_pod(pod_name)
        cont_id = pod.get_container_id(pod_name)
        async with self._kube_client.get_node_proxy_client(
            pod.node_name, self._docker_config.docker_engine_api_port
        ) as proxy_client:
            session = await self._kube_client.create_http_client()
            docker = Docker(
                url=str(proxy_client.url), session=session, connector=session.connector,
            )
            execute = await docker.containers.container(cont_id).exec(
                cmd=cmd, stdin=stdin, stdout=stdout, stderr=stderr, tty=tty
            )
            async with execute.start(detach=False) as stream:
                await execute.resize(w=80, h=25)
                yield stream

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
