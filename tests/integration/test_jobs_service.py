import asyncio
import contextlib
import json
import re
import uuid
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

import pytest
from async_timeout import timeout
from neuro_sdk import (
    Client as PlatformApiClient,
    Container as JobContainer,
    IllegalArgumentError,
    JobDescription as Job,
    JobStatus,
    Resources,
)
from platform_config_client import ConfigClient

from platform_monitoring.container_runtime_client import ContainerRuntimeClientRegistry
from platform_monitoring.jobs_service import JobException, JobsService
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
        with contextlib.suppress(IllegalArgumentError):  # Ignore if job was dropped
            await platform_api_client.jobs.kill(job.id)


@pytest.mark.usefixtures("cluster_name")
class TestJobsService:
    @pytest.fixture
    async def jobs_service(
        self,
        platform_config_client: ConfigClient,
        platform_api_client: PlatformApiClient,
        kube_client: MyKubeClient,
        container_runtime_client_registry: ContainerRuntimeClientRegistry,
        cluster_name: str,
    ) -> JobsService:
        return JobsService(
            config_client=platform_config_client,
            jobs_client=platform_api_client.jobs,
            kube_client=kube_client,
            container_runtime_client_registry=container_runtime_client_registry,
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
            memory_mb=32,
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

        image = f"{registry_host}/{user.name}/alpine:{image_tag}"

        async with jobs_service.save(job, user, image) as it:
            async for chunk in it:
                pass

        new_job = await job_factory(
            image, 'sh -c \'[ "$(cat /test)" = "123" ]\'', resources
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
            memory_mb=32,
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

        image = f"{registry_host}/{user.name}/alpine"

        async with jobs_service.save(job, user, image) as it:
            async for chunk in it:
                pass

        new_job = await job_factory(
            image,
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

        image = f"{registry_host}/{user.name}/alpine:{image_tag}"

        with pytest.raises(JobException, match="is not running"):
            async with jobs_service.save(job, user, image) as it:
                async for chunk in it:
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
            memory_mb=32,
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
        image = f"{registry_host}/{user.name}/alpine:{image_tag}"
        repository = f"{registry_host}/{user.name}/alpine"

        async with jobs_service.save(job, user, image) as it:
            data = [json.loads(chunk) async for chunk in it]
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
            memory_mb=32,
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
        image = f"{registry_host}/InvalidImageName:{image_tag}"

        with pytest.raises(
            JobException, match="error: repository name must be lowercase"
        ):
            async with jobs_service.save(job, user, image) as it:
                async for chunk in it:
                    pass

    @pytest.mark.asyncio
    async def test_get_available_jobs_count(self, jobs_service: JobsService) -> None:
        result = await jobs_service.get_available_jobs_counts()
        assert result and "cpu-small" in result

    @pytest.mark.asyncio
    async def test_mark_logs_dropped(
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
            "sh -c 'exit 0'",
            resources,
            tty=False,
        )
        await self.wait_for_job_succeeded(job, platform_api_client)

        await platform_api_client.jobs.kill(job.id)

        # Drop request
        # TODO: replace with sdk call when available
        url = platform_api_client._config.api_url / "jobs" / job.id / "drop"
        auth = await platform_api_client._config._api_auth()
        async with platform_api_client._core.request("POST", url, auth=auth):
            pass

        # Job should be still there
        assert await platform_api_client.jobs.status(job.id)

        await jobs_service.mark_logs_dropped(job.id)

        with pytest.raises(IllegalArgumentError):
            await platform_api_client.jobs.status(job.id)
