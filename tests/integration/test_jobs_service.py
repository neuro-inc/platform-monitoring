import asyncio
import re
import sys
import uuid
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

from aiodocker.stream import Stream
import pytest
from aiohttp.client_proto import ResponseHandler
from aiohttp.client_reqrep import ClientResponse
from async_timeout import timeout
from neuromation.api import (
    Client as PlatformApiClient,
    Container as JobContainer,
    JobDescription as Job,
    JobStatus,
    Resources,
)
from platform_monitoring.config import DockerConfig
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


async def expect_prompt(stream: Stream) -> bytes:
    try:
        ret: bytes = b""
        async with timeout(3):
            while b"/ #" not in ret:
                msg = await stream.read_out()
                if msg is None:
                    break
                assert msg.stream == 1
                ret += msg.data
            return ret.replace(b"\x1b[6n", b"")
    except asyncio.TimeoutError:
        raise AssertionError(f"[Timeout] {ret}")


@pytest.mark.usefixtures("cluster_name")
class TestJobsService:
    @pytest.fixture
    async def jobs_service(
        self,
        platform_api_client: PlatformApiClient,
        kube_client: MyKubeClient,
        docker_config: DockerConfig,
    ) -> JobsService:
        return JobsService(platform_api_client.jobs, kube_client, docker_config)

    @pytest.fixture
    async def job_factory(
        self, platform_api_client: PlatformApiClient
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
    async def test_attach_ok(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
        user: User,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory(
            "alpine:latest",
            "sh -c 'sleep 60; echo abc; echo def; sleep 300'",
            resources,
            tty=False,
        )
        await self.wait_for_job_running(job, platform_api_client)

        job = await platform_api_client.jobs.status(job.id)

        await jobs_service.resize(job, w=80, h=25)

        from aiodocker.stream import Stream

        old_aenter = Stream.__aenter__
        old_aexit = Stream.__aexit__

        async def aenter(self):
            print("AENTER")
            return await old_aenter(self)

        Stream.__aenter__ = aenter

        async def aexit(self, *args):
            print("AEXIT", args)
            return await old_aexit(self, *args)

        Stream.__aexit__ = aexit

        async with jobs_service.attach(
            job, stdin=False, stdout=True, stderr=True, logs=False
        ) as stream:
            print("enter")
            conn = stream._resp.connection
            proto = conn.protocol
            parser = proto._payload_parser
            proto._payload_parser = _Parser(parser)
            print(parser, parser.tty, parser.queue, parser.queue._buffer)
            delay = 0.01
            for i in range(1000):
                data = await stream.read_out()
                if data is None:
                    print("sleep")
                    delay *= 2
                    await asyncio.sleep(delay)
                else:
                    break
            else:
                assert False, "Timeout"
            assert data.stream == 1
            assert data.data == b"abc\n"

        await platform_api_client.jobs.kill(job.id)

    @pytest.mark.asyncio
    async def test_exec_no_tty_stdout(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
        user: User,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory("alpine:latest", "sleep 300", resources,)
        await self.wait_for_job_running(job, platform_api_client)

        exec_id = await jobs_service.exec_create(job, "sh -c 'sleep 5; echo abc'")
        async with jobs_service.exec_start(job, exec_id) as stream:
            data = await stream.read_out()
            assert data.data == b"abc\n"
            assert data.stream == 1

        await platform_api_client.jobs.kill(job.id)

    @pytest.mark.asyncio
    async def test_exec_no_tty_stderr(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
        user: User,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory("alpine:latest", "sleep 300", resources,)
        await self.wait_for_job_running(job, platform_api_client)

        exec_id = await jobs_service.exec_create(job, "sh -c 'sleep 5; echo abc 1>&2'")
        async with jobs_service.exec_start(job, exec_id) as stream:
            data = await stream.read_out()
            assert data.data == b"abc\n"
            assert data.stream == 2

        await platform_api_client.jobs.kill(job.id)

    @pytest.mark.asyncio
    async def test_exec_tty(
        self,
        job_factory: JobFactory,
        platform_api_client: PlatformApiClient,
        jobs_service: JobsService,
        user: User,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory("alpine:latest", "sleep 300", resources,)
        await self.wait_for_job_running(job, platform_api_client)

        exec_id = await jobs_service.exec_create(job, "sh", tty=True, stdin=True)
        # await jobs_service.exec_resize(job, exec_id, w=120, h=15)
        async with jobs_service.exec_start(job, exec_id) as stream:
            assert await expect_prompt(stream) == b"/ # "
            await stream.write_in(b"echo 'abc'\n")
            assert await expect_prompt(stream) == b"echo 'abc'\r\nabc\r\n/ # "
            await stream.write_in(b"exit 1\n")
            assert await expect_prompt(stream) == b"exit 1\r\n"

        ret = await jobs_service.exec_inspect(job, exec_id)
        assert ret == {}
        await platform_api_client.jobs.kill(job.id)


class _Parser:
    def __init__(self, orig):
        print("INIT")
        self._orig = orig

    def feed_eof(self):
        print("EOF")
        return
        import traceback
        import sys

        traceback.print_stack(file=sys.stdout)
        self._orig.feed_eof()

    def feed_data(self, data):
        print("DATA", data)
        self._orig.feed_data(data)

    def set_exception(self, exc):
        print("EXC")
        self._orig.set_exception(exc)


old_close = ResponseHandler.close
old_data_received = ResponseHandler.data_received
old_response_eof = ClientResponse._response_eof
old_resp_close = ClientResponse.close
old_release = ClientResponse.release


def data_received(self: ResponseHandler, data: bytes) -> None:
    print("DATA_RECEIVED")
    print(data)
    old_data_received(self, data)


ResponseHandler.data_received = data_received  # type: ignore


def close(self: ResponseHandler) -> None:
    print("CLOSE")
    import traceback

    traceback.print_stack(file=sys.stdout)
    old_close(self)


# ResponseHandler.close = close  # type: ignore


def _response_eof(self: ClientResponse) -> None:
    if "attach" not in str(self.url):
        return
    print("RESPONSE_EOF", self.url)
    return
    import traceback

    traceback.print_stack(file=sys.stdout)
    old_response_eof(self)
    print("CONN", repr(self._connection))


# ClientResponse._response_eof = _response_eof  # type: ignore


def resp_close(self: ClientResponse) -> None:
    print("RESPONSE_CLOSE", self.url)
    import traceback

    traceback.print_stack(file=sys.stdout)
    old_resp_close(self)
    print("CONN", repr(self._connection))


# ClientResponse.close = resp_close  # type: ignore


def resp_release(self: ClientResponse) -> None:
    print("RESPONSE_RELEASE", self.url)
    import traceback

    traceback.print_stack(file=sys.stdout)
    old_release(self)
    print("CONN", repr(self._connection))


# ClientResponse.release = resp_release  # type: ignore
