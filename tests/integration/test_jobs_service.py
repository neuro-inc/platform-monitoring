import asyncio
import re
import uuid
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

import aiohttp
import pytest
from aiodocker import Docker
from aiodocker.stream import Stream
from async_timeout import timeout
from neuromation.api import (
    Client as PlatformApiClient,
    Container as JobContainer,
    JobDescription as Job,
    JobStatus,
    Resources,
)
from platform_config_client import ConfigClient

from platform_monitoring.config import DOCKER_API_VERSION, DockerConfig
from platform_monitoring.jobs_service import (
    Container,
    ImageReference,
    JobException,
    JobsService,
)
from platform_monitoring.user import User

from .conftest_kube import MyKubeClient


# arguments: (image, command, resources)
JobFactory = Callable[..., Awaitable[Job]]


@pytest.fixture
async def job_factory(
    platform_api_client: PlatformApiClient,
) -> AsyncIterator[JobFactory]:
    jobs = []

    async def _factory(
        image: str, command: Optional[str], resources: Resources, tty: bool = False
    ) -> Job:
        container = JobContainer(
            image=platform_api_client.parse.remote_image(image),
            command=command,
            resources=resources,
            tty=tty,
        )
        job = await platform_api_client.jobs.run(container)
        jobs.append(job)
        return job

    yield _factory

    for job in jobs:
        await platform_api_client.jobs.kill(job.id)


@pytest.fixture
async def wait_for_job_docker_client(
    kube_client: MyKubeClient, docker_config: DockerConfig
) -> Callable[[str], Awaitable[None]]:
    async def go(job_id: str) -> None:
        timeout_s: float = 60
        interval_s: float = 1

        pod_name = job_id
        async with timeout(timeout_s):
            pod = await kube_client.get_pod(pod_name)
            async with kube_client.get_node_proxy_client(
                pod.node_name, docker_config.docker_engine_api_port
            ) as proxy_client:
                docker = Docker(
                    url=str(proxy_client.url),
                    session=proxy_client.session,
                    connector=proxy_client.session.connector,
                    api_version=DOCKER_API_VERSION,
                )
                while True:
                    try:
                        await docker.version()
                        return
                    except aiohttp.ClientError as e:
                        print(f"Failed to ping docker client: {proxy_client.url}: {e}")
                        await asyncio.sleep(interval_s)

    return go


async def expect_prompt(stream: Stream) -> bytes:
    _ansi_re = re.compile(br"\033\[[;?0-9]*[a-zA-Z]")
    try:
        ret: bytes = b""
        async with timeout(3):
            while not ret.strip().endswith(b"/ #"):
                msg = await stream.read_out()
                if msg is None:
                    break
                assert msg.stream == 1
                ret += _ansi_re.sub(b"", msg.data)
            return ret
    except asyncio.TimeoutError:
        raise AssertionError(f"[Timeout] {ret!r}")


@pytest.mark.usefixtures("cluster_name")
class TestJobsService:
    @pytest.fixture
    async def jobs_service(
        self,
        platform_config_client: ConfigClient,
        platform_api_client: PlatformApiClient,
        kube_client: MyKubeClient,
        docker_config: DockerConfig,
        cluster_name: str,
    ) -> JobsService:
        return JobsService(
            config_client=platform_config_client,
            jobs_client=platform_api_client.jobs,
            kube_client=kube_client,
            docker_config=docker_config,
            cluster_name=cluster_name,
        )

    async def wait_for_job(
        self,
        job: Job,
        platform_api_client: PlatformApiClient,
        condition: Callable[[Job], bool],
        timeout_s: float = 300.0,
        interval_s: float = 1.0,
    ) -> None:
        try:
            async with timeout(timeout_s):
                while True:
                    job = await platform_api_client.jobs.status(job.id)
                    if condition(job):
                        return
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            pytest.fail(f"Job '{job.id}' has not reached the condition")

    async def wait_for_job_running(
        self,
        job: Job,
        platform_api_client: PlatformApiClient,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        def _condition(job: Job) -> bool:
            if job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
                pytest.fail(f"Job '{job.id} has completed'")
            if job.status == JobStatus.CANCELLED:
                pytest.fail(f"Job '{job.id} has been cancelled'")
            return job.status == JobStatus.RUNNING

        await self.wait_for_job(job, platform_api_client, _condition, *args, **kwargs)

    async def wait_for_job_succeeded(
        self,
        job: Job,
        platform_api_client: PlatformApiClient,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        def _condition(job: Job) -> bool:
            if job.status == JobStatus.FAILED:
                pytest.fail(f"Job '{job.id} has failed'")
            if job.status == JobStatus.CANCELLED:
                pytest.fail(f"Job '{job.id} has been cancelled'")
            return job.status == JobStatus.SUCCEEDED

        await self.wait_for_job(job, platform_api_client, _condition, *args, **kwargs)

    @pytest.fixture
    def user(self) -> User:
        return User(name="compute", token="testtoken")

    @pytest.fixture
    def registry_host(self) -> str:
        return "localhost:5000"

    @pytest.fixture
    def image_tag(self) -> str:
        return str(uuid.uuid4())[:8]

    @pytest.mark.asyncio
    async def test_save_ok(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
        user: User,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16,
            cpu=0.1,
            gpu=None,
            shm=False,
            gpu_model=None,
            tpu_type=None,
            tpu_software_version=None,
        )
        job = await job_factory(
            "alpine:latest", "sh -c 'echo -n 123 > /test; sleep 300'", resources
        )
        await self.wait_for_job_running(job, platform_api_client)

        container = Container(
            image=ImageReference(
                domain=registry_host, path=f"{user.name}/alpine", tag=image_tag
            )
        )

        async for chunk in jobs_service.save(job, user, container):
            pass

        new_job = await job_factory(
            str(container.image), 'sh -c \'[ "$(cat /test)" = "123" ]\'', resources
        )
        await self.wait_for_job_succeeded(new_job, platform_api_client)

    @pytest.mark.asyncio
    async def test_save_no_tag(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
        user: User,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16,
            cpu=0.1,
            gpu=None,
            shm=False,
            gpu_model=None,
            tpu_type=None,
            tpu_software_version=None,
        )
        job = await job_factory(
            "alpine:latest",
            f"sh -c 'echo -n {image_tag} > /test; sleep 300'",
            resources,
        )
        await self.wait_for_job_running(job, platform_api_client)

        container = Container(
            image=ImageReference(domain=registry_host, path=f"{user.name}/alpine")
        )

        async for chunk in jobs_service.save(job, user, container):
            pass

        new_job = await job_factory(
            str(container.image),
            f'sh -c \'[ "$(cat /test)" = "{image_tag}" ]\'',
            resources,
        )
        await self.wait_for_job_succeeded(new_job, platform_api_client)

    @pytest.mark.asyncio
    async def test_save_pending_job(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
        user: User,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16 ** 10,
            cpu=0.1,
            gpu=None,
            shm=False,
            gpu_model=None,
            tpu_type=None,
            tpu_software_version=None,
        )
        job = await job_factory("alpine:latest", None, resources)

        container = Container(
            image=ImageReference(
                domain=registry_host, path=f"{user.name}/alpine", tag=image_tag
            )
        )

        with pytest.raises(JobException, match="is not running"):
            async for chunk in jobs_service.save(job, user, container):
                pass

    @pytest.mark.asyncio
    async def test_save_push_failure(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
        user: User,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16,
            cpu=0.1,
            gpu=None,
            shm=False,
            gpu_model=None,
            tpu_type=None,
            tpu_software_version=None,
        )
        job = await job_factory("alpine:latest", "sh -c 'sleep 300'", resources)
        await self.wait_for_job_running(job, platform_api_client)

        registry_host = "unknown:5000"
        container = Container(
            image=ImageReference(
                domain=registry_host, path=f"{user.name}/alpine", tag=image_tag
            )
        )
        repository = f"{registry_host}/{user.name}/alpine"

        data = [chunk async for chunk in jobs_service.save(job, user, container)]
        assert len(data) == 4, str(data)

        assert data[0]["status"] == "CommitStarted"
        assert data[0]["details"]["image"] == f"{repository}:{image_tag}"
        assert re.match(r"\w{64}", data[0]["details"]["container"])

        assert data[1] == {"status": "CommitFinished"}

        msg = f"The push refers to repository [{repository}]"
        assert data[2]["status"] == msg

        assert "status" not in data[3]
        msg = f"Get https://{registry_host}/v2/: dial tcp: lookup unknown"
        assert msg in data[3]["error"]

    @pytest.mark.asyncio
    async def test_save_commit_fails_with_exception(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
        user: User,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16,
            cpu=0.1,
            gpu=None,
            shm=False,
            gpu_model=None,
            tpu_type=None,
            tpu_software_version=None,
        )
        job = await job_factory("alpine:latest", "sh -c 'sleep 300'", resources)
        await self.wait_for_job_running(job, platform_api_client)

        registry_host = "localhost:5000"
        container = Container(
            image=ImageReference(
                domain=registry_host, path="InvalidImageName", tag=image_tag
            )
        )

        with pytest.raises(
            JobException,
            match="invalid reference format: repository name must be lowercase",
        ):
            async for chunk in jobs_service.save(job, user, container):
                pass

    @pytest.mark.asyncio
    async def test_attach_nontty(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
    ) -> None:
        resources = Resources(
            memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory(
            "alpine:latest",
            "sh -c 'while true; do echo abc; sleep 1; done'",
            resources,
            tty=False,
        )
        await self.wait_for_job_running(job, platform_api_client)

        job = await platform_api_client.jobs.status(job.id)

        async with jobs_service.attach(
            job, stdin=False, stdout=True, stderr=True, logs=True
        ) as stream:
            data = await stream.read_out()
            assert data is not None
            assert data.stream == 1
            assert data.data == b"abc\n"

        await platform_api_client.jobs.kill(job.id)

    @pytest.mark.asyncio
    async def test_attach_tty(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
    ) -> None:
        resources = Resources(
            memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory("alpine:latest", "sh", resources, tty=True)
        await self.wait_for_job_running(job, platform_api_client)

        job = await platform_api_client.jobs.status(job.id)
        await jobs_service.resize(job, w=80, h=25)

        async with jobs_service.attach(
            job, stdin=True, stdout=True, stderr=True, logs=False
        ) as stream:
            # We can't be sure if we connect before inital prompt or after
            try:
                await asyncio.wait_for(stream.read_out(), timeout=0.2)
            except asyncio.TimeoutError:
                pass

            await stream.write_in(b"\n")
            assert await expect_prompt(stream) == b"\r\n/ # "
            await stream.write_in(b"echo 'abc'\n")
            assert await expect_prompt(stream) == b"echo 'abc'\r\nabc\r\n/ # "
            await stream.write_in(b"exit 1\n")
            assert await expect_prompt(stream) == b"exit 1\r\n"

        for r in range(10):
            job = await platform_api_client.jobs.status(job.id)
            if job.status != JobStatus.RUNNING:
                break
            await asyncio.sleep(1)
        assert job.history.exit_code == 1

    @pytest.mark.asyncio
    async def test_exec_no_tty_stdout(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
    ) -> None:
        resources = Resources(
            memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory("alpine:latest", "sleep 300", resources)
        await self.wait_for_job_running(job, platform_api_client)

        exec_id = await jobs_service.exec_create(job, "sh -c 'sleep 5; echo abc'")
        async with jobs_service.exec_start(job, exec_id) as stream:
            data = await stream.read_out()
            assert data is not None
            assert data.data == b"abc\n"
            assert data.stream == 1

        await platform_api_client.jobs.kill(job.id)

    @pytest.mark.asyncio
    async def test_exec_no_tty_stderr(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
    ) -> None:
        resources = Resources(
            memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory("alpine:latest", "sleep 300", resources)
        await self.wait_for_job_running(job, platform_api_client)

        exec_id = await jobs_service.exec_create(job, "sh -c 'sleep 5; echo abc 1>&2'")
        async with jobs_service.exec_start(job, exec_id) as stream:
            data = await stream.read_out()
            assert data is not None
            assert data.data == b"abc\n"
            assert data.stream == 2

        await platform_api_client.jobs.kill(job.id)

    @pytest.mark.asyncio
    async def test_exec_tty(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
        wait_for_job_docker_client: Callable[[str], Awaitable[None]],
    ) -> None:
        resources = Resources(
            memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory("alpine:latest", "sleep 300", resources)
        await self.wait_for_job_running(job, platform_api_client)

        await wait_for_job_docker_client(job.id)

        exec_id = await jobs_service.exec_create(job, "sh", tty=True, stdin=True)
        async with jobs_service.exec_start(job, exec_id) as stream:
            await jobs_service.exec_resize(job, exec_id, w=120, h=15)
            assert await expect_prompt(stream) == b"/ # "
            await stream.write_in(b"echo 'abc'\n")
            val = await expect_prompt(stream)
            # this trick is required to make tests stable
            # for some reason the prompt is missed sometimes
            if val == b"\r/ # ":
                val = await expect_prompt(stream)
            val = val.strip()
            if val.startswith(b"/ # "):
                val = val[4:]
            assert val == b"echo 'abc'\r\nabc\r\n/ #"
            await stream.write_in(b"exit 1\n")
            assert await expect_prompt(stream) == b"exit 1\r\n"

        ret = await jobs_service.exec_inspect(job, exec_id)
        assert ret["ExitCode"] == 1
        await platform_api_client.jobs.kill(job.id)

    @pytest.mark.asyncio
    async def test_get_available_jobs_count(self, jobs_service: JobsService) -> None:
        result = await jobs_service.get_available_jobs_counts()
        assert result and "cpu-small" in result
