import asyncio
import uuid
from typing import Any, AsyncIterator, Awaitable, Callable

import pytest
from async_timeout import timeout
from neuromation.api import (
    Client as PlatformApiClient,
    Image,
    JobDescription as Job,
    JobStatus,
    Resources,
)
from neuromation.api.jobs import Jobs as JobsClient
from platform_monitoring.jobs_service import (
    Container,
    ImageReference,
    JobException,
    JobsService,
)
from platform_monitoring.user import User

from .conftest_kube import MyKubeClient


JobFactory = Callable[[Image, Resources], Awaitable[Job]]


class TestJobsService:
    @pytest.fixture
    async def jobs_client(self, platform_api_client: PlatformApiClient) -> JobsClient:
        return platform_api_client.jobs

    @pytest.fixture
    async def job_factory(self, jobs_client: JobsClient) -> AsyncIterator[JobFactory]:
        jobs = []

        async def _factory(image: Image, resources: Resources) -> Job:
            job = await jobs_client.submit(image=image, resources=resources)
            jobs.append(job)
            return job

        yield _factory

        for job in jobs:
            await jobs_client.kill(job.id)

    async def wait_for_job(
        self,
        job: Job,
        jobs_client: JobsClient,
        condition: Callable[[Job], bool],
        timeout_s: float = 300.0,
        interval_s: float = 1.0,
    ) -> None:
        try:
            async with timeout(timeout_s):
                while True:
                    job = await jobs_client.status(job.id)
                    if condition(job):
                        return
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            pytest.fail(f"Job '{job.id}' has not reached the condition")

    async def wait_for_job_running(
        self, job: Job, jobs_client: JobsClient, *args: Any, **kwargs: Any
    ) -> None:
        def _condition(job: Job) -> bool:
            if job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
                pytest.fail(f"Job '{job.id} has completed'")
            return job.status == JobStatus.RUNNING

        await self.wait_for_job(job, jobs_client, _condition, *args, **kwargs)

    async def wait_for_job_succeeded(
        self, job: Job, jobs_client: JobsClient, *args: Any, **kwargs: Any
    ) -> None:
        def _condition(job: Job) -> bool:
            if job.status == JobStatus.FAILED:
                pytest.fail(f"Job '{job.id} has failed'")
            return job.status == JobStatus.SUCCEEDED

        await self.wait_for_job(job, jobs_client, _condition, *args, **kwargs)

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
    async def test_save(
        self,
        job_factory: JobFactory,
        jobs_client: JobsClient,
        kube_client: MyKubeClient,
        user: User,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory(
            Image(
                image="alpine:latest", command="sh -c 'echo -n 123 > /test; sleep 300'"
            ),
            resources,
        )
        await self.wait_for_job_running(job, jobs_client)

        container = Container(
            image=ImageReference(
                domain=registry_host, path=f"{user.name}/alpine", tag=image_tag
            )
        )

        jobs_service = JobsService(jobs_client, kube_client)
        await jobs_service.save(job, user, container)

        new_job = await job_factory(
            Image(
                image=str(container.image),
                command='sh -c \'[ "$(cat /test)" = "123" ]\'',
            ),
            resources,
        )
        await self.wait_for_job_succeeded(new_job, jobs_client)

    @pytest.mark.asyncio
    async def test_save_no_tag(
        self,
        job_factory: JobFactory,
        jobs_client: JobsClient,
        kube_client: MyKubeClient,
        user: User,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory(
            Image(
                image="alpine:latest",
                command=f"sh -c 'echo -n {image_tag} > /test; sleep 300'",
            ),
            resources,
        )
        await self.wait_for_job_running(job, jobs_client)

        container = Container(
            image=ImageReference(domain=registry_host, path=f"{user.name}/alpine")
        )

        jobs_service = JobsService(jobs_client, kube_client)
        await jobs_service.save(job, user, container)

        new_job = await job_factory(
            Image(
                image=str(container.image),
                command=f'sh -c \'[ "$(cat /test)" = "{image_tag}" ]\'',
            ),
            resources,
        )
        await self.wait_for_job_succeeded(new_job, jobs_client)

    @pytest.mark.asyncio
    async def test_save_pending_job(
        self,
        job_factory: JobFactory,
        jobs_client: JobsClient,
        kube_client: MyKubeClient,
        user: User,
        registry_host: str,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16 ** 10, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory(Image(image="alpine:latest", command=None), resources)

        container = Container(
            image=ImageReference(
                domain=registry_host, path=f"{user.name}/alpine", tag=image_tag
            )
        )

        jobs_service = JobsService(jobs_client, kube_client)
        with pytest.raises(JobException, match="is not running"):
            await jobs_service.save(job, user, container)

    @pytest.mark.asyncio
    async def test_save_push_failure(
        self,
        job_factory: JobFactory,
        jobs_client: JobsClient,
        kube_client: MyKubeClient,
        user: User,
        image_tag: str,
    ) -> None:
        resources = Resources(
            memory_mb=16, cpu=0.1, gpu=None, shm=False, gpu_model=None
        )
        job = await job_factory(
            Image(image="alpine:latest", command="sh -c 'sleep 300'"), resources
        )
        await self.wait_for_job_running(job, jobs_client)

        registry_host = "unknown:5000"
        container = Container(
            image=ImageReference(
                domain=registry_host, path=f"{user.name}/alpine", tag=image_tag
            )
        )

        jobs_service = JobsService(jobs_client, kube_client)
        with pytest.raises(JobException, match="Failed to push image"):
            await jobs_service.save(job, user, container)
