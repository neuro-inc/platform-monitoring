import asyncio
import contextlib
import json
import os
import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from tempfile import mktemp
from typing import Any

import pytest
from apolo_sdk import (
    Client as JobsClient,
    Container as JobContainer,
    Factory as ClientFactory,
    IllegalArgumentError,
    JobDescription as Job,
    JobStatus,
    Resources,
)
from neuro_config_client import ConfigClient
from yarl import URL

from platform_monitoring.api import create_platform_api_client
from platform_monitoring.config import PlatformApiConfig
from platform_monitoring.container_runtime_client import ContainerRuntimeClientRegistry
from platform_monitoring.jobs_service import JobException, JobsService
from platform_monitoring.platform_api_client import ApiClient

from .conftest_admin import ProjectUser
from .conftest_kube import MyKubeClient


# arguments: (image, command, resources)
JobFactory = Callable[..., Awaitable[Job]]


@contextlib.asynccontextmanager
async def create_apolo_client(
    url: URL, *, user: ProjectUser
) -> AsyncIterator[JobsClient]:
    config_path = Path(mktemp())
    client_factory = ClientFactory(config_path)
    await client_factory.login_with_token(url=url / "api/v1", token=user.token)
    client = None
    try:
        client = await client_factory.get()
        await client.config.switch_org(user.org_name)
        await client.config.switch_project(user.project_name)

        yield client
    finally:
        if client:
            await client.close()


@pytest.fixture
async def job_factory(
    apolo_client: JobsClient,
) -> AsyncIterator[JobFactory]:
    jobs = []

    async def _factory(
        image: str, command: str | None, resources: Resources, *, tty: bool = False
    ) -> Job:
        container = JobContainer(
            image=apolo_client.parse.remote_image(image),
            command=command,
            resources=resources,
            tty=tty,
        )
        job = await apolo_client.jobs.run(container)
        jobs.append(job)
        return job

    yield _factory

    for job in jobs:
        with contextlib.suppress(IllegalArgumentError):  # Ignore if job was dropped
            await apolo_client.jobs.kill(job.id)


@pytest.mark.usefixtures("cluster_name")
class TestJobsService:
    @pytest.fixture
    async def user(self, regular_user1: ProjectUser) -> ProjectUser:
        return regular_user1

    @pytest.fixture
    def registry_host(self) -> str:
        return os.environ.get("REGISTRY_HOST", "localhost:5000")

    @pytest.fixture
    def image_tag(self) -> str:
        return str(uuid.uuid4())[:8]

    @pytest.fixture
    async def apolo_client(
        self, platform_api_config: PlatformApiConfig, regular_user1: ProjectUser
    ) -> AsyncIterator[JobsClient]:
        async with create_apolo_client(
            platform_api_config.url, user=regular_user1
        ) as client:
            yield client

    @pytest.fixture
    async def platform_api_client(
        self, platform_api_config: PlatformApiConfig, user: ProjectUser
    ) -> AsyncIterator[ApiClient]:
        async with create_platform_api_client(
            platform_api_config.url, user.token
        ) as client:
            yield client

    @pytest.fixture
    async def jobs_service(
        self,
        platform_config_client: ConfigClient,
        platform_api_client: ApiClient,
        kube_client: MyKubeClient,
        container_runtime_client_registry: ContainerRuntimeClientRegistry,
        cluster_name: str,
    ) -> JobsService:
        return JobsService(
            config_client=platform_config_client,
            jobs_client=platform_api_client,
            kube_client=kube_client,
            container_runtime_client_registry=container_runtime_client_registry,
            cluster_name=cluster_name,
        )

    async def wait_for_job(
        self,
        job: Job,
        apolo_client: JobsClient,
        condition: Callable[[Job], bool],
        timeout_s: float = 300.0,
        interval_s: float = 1.0,
    ) -> None:
        try:
            async with asyncio.timeout(timeout_s):
                while True:
                    job = await apolo_client.jobs.status(job.id)
                    if condition(job):
                        return
                    await asyncio.sleep(interval_s)
        except TimeoutError:
            pytest.fail(f"Job '{job.id}' has not reached the condition")

    async def wait_for_job_running(
        self,
        job: Job,
        apolo_client: JobsClient,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        def _condition(job: Job) -> bool:
            if job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
                pytest.fail(f"Job '{job.id} has completed'")
            if job.status == JobStatus.CANCELLED:
                pytest.fail(f"Job '{job.id} has been cancelled'")
            return job.status == JobStatus.RUNNING

        await self.wait_for_job(job, apolo_client, _condition, *args, **kwargs)

    async def wait_for_job_succeeded(
        self,
        job: Job,
        apolo_client: JobsClient,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        def _condition(job: Job) -> bool:
            if job.status == JobStatus.FAILED:
                pytest.fail(f"Job '{job.id} has failed'")
            if job.status == JobStatus.CANCELLED:
                pytest.fail(f"Job '{job.id} has been cancelled'")
            return job.status == JobStatus.SUCCEEDED

        await self.wait_for_job(job, apolo_client, _condition, *args, **kwargs)

    async def test_save_ok(
        self,
        job_factory: JobFactory,
        apolo_client: JobsClient,
        jobs_service: JobsService,
        user: ProjectUser,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory=32 * 1024**2,
            cpu=0.1,
            shm=False,
            nvidia_gpu=None,
            nvidia_gpu_model=None,
            tpu_type=None,
            tpu_software_version=None,
        )
        job = await job_factory(
            "alpine:latest", "sh -c 'echo -n 123 > /test; sleep 15'", resources
        )
        await self.wait_for_job_running(job, apolo_client)

        image = (
            f"{registry_host}/{user.org_name}/{user.project_name}/alpine:{image_tag}"
        )

        async with jobs_service.save(job, user, image) as it:
            async for _ in it:
                pass

        new_job = await job_factory(
            image, 'sh -c \'[ "$(cat /test)" = "123" ]\'', resources
        )
        await self.wait_for_job_succeeded(new_job, apolo_client, timeout_s=60.0)

    async def test_save_no_tag(
        self,
        job_factory: JobFactory,
        apolo_client: JobsClient,
        jobs_service: JobsService,
        user: ProjectUser,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory=32 * 1024**2,
            cpu=0.1,
            shm=False,
            nvidia_gpu=None,
            nvidia_gpu_model=None,
            tpu_type=None,
            tpu_software_version=None,
        )
        job = await job_factory(
            "alpine:latest",
            f"sh -c 'echo -n {image_tag} > /test; sleep 15'",
            resources,
        )
        await self.wait_for_job_running(job, apolo_client)

        image = f"{registry_host}/{user.org_name}/{user.project_name}/alpine"

        async with jobs_service.save(job, user, image) as it:
            data = [json.loads(chunk) async for chunk in it]

        assert data[0]["status"] == "CommitStarted"
        assert data[0]["details"]["image"] == image

        assert data[1] == {"status": "CommitFinished"}

        assert "status" not in data[2]
        assert all(
            check_str in data[2]["error"] for check_str in ["Image", image, "not found"]
        )

    async def test_save_pending_job(
        self,
        job_factory: JobFactory,
        apolo_client: JobsClient,
        jobs_service: JobsService,
        user: ProjectUser,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory=2**60,
            cpu=0.1,
            shm=False,
            nvidia_gpu=None,
            nvidia_gpu_model=None,
            tpu_type=None,
            tpu_software_version=None,
        )
        job = await job_factory("alpine:latest", None, resources)

        image = (
            f"{registry_host}/{user.org_name}/{user.project_name}/alpine:{image_tag}"
        )

        with pytest.raises(JobException, match="is not running"):  # noqa: PT012
            async with jobs_service.save(job, user, image) as it:
                async for _ in it:
                    pass

    async def test_save_push_failure(
        self,
        job_factory: JobFactory,
        apolo_client: JobsClient,
        jobs_service: JobsService,
        user: ProjectUser,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory=32 * 1024**2,
            cpu=0.1,
            shm=False,
            nvidia_gpu=None,
            nvidia_gpu_model=None,
            tpu_type=None,
            tpu_software_version=None,
        )
        job = await job_factory("alpine:latest", "sh -c 'sleep 300'", resources)
        await self.wait_for_job_running(job, apolo_client)

        registry_host = "unknown:5000"
        image = (
            f"{registry_host}/{user.org_name}/{user.project_name}/alpine:{image_tag}"
        )
        repository = f"{registry_host}/{user.org_name}/{user.project_name}/alpine"

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
        assert "Cannot connect to host unknown:5000" in data[3]["error"]

    async def test_save_commit_fails_with_exception(
        self,
        job_factory: JobFactory,
        apolo_client: JobsClient,
        jobs_service: JobsService,
        user: ProjectUser,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory=32 * 1024**2,
            cpu=0.1,
            shm=False,
            nvidia_gpu=None,
            nvidia_gpu_model=None,
            tpu_type=None,
            tpu_software_version=None,
        )
        job = await job_factory("alpine:latest", "sh -c 'sleep 300'", resources)
        await self.wait_for_job_running(job, apolo_client)

        registry_host = "localhost:5000"
        image = (
            f"{registry_host}/{user.org_name}/{user.project_name}/"
            f"InvalidImageName:{image_tag}"
        )

        with pytest.raises(JobException, match="repository name must be lowercase"):  # noqa: PT012
            async with jobs_service.save(job, user, image) as it:
                async for _ in it:
                    pass

    async def test_get_available_jobs_count(self, jobs_service: JobsService) -> None:
        result = await jobs_service.get_available_jobs_counts()
        assert result
        assert "cpu-small" in result

    async def test_mark_logs_dropped(
        self,
        job_factory: JobFactory,
        apolo_client: JobsClient,
        jobs_service: JobsService,
    ) -> None:
        resources = Resources(
            memory=16 * 1024**2,
            cpu=0.1,
            shm=False,
            nvidia_gpu=None,
            nvidia_gpu_model=None,
        )
        job = await job_factory(
            "alpine:latest",
            "sh -c 'exit 0'",
            resources,
            tty=False,
        )
        await self.wait_for_job_succeeded(job, apolo_client)

        await apolo_client.jobs.kill(job.id)

        # Drop request
        # TODO: replace with sdk call when available
        url = apolo_client._config.api_url / "jobs" / job.id / "drop"
        auth = await apolo_client._config._api_auth()
        async with apolo_client._core.request("POST", url, auth=auth):
            pass

        # Job should be still there
        assert await apolo_client.jobs.status(job.id)

        await jobs_service.mark_logs_dropped(job.id)

        with pytest.raises(IllegalArgumentError):
            await apolo_client.jobs.status(job.id)
