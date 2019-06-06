import asyncio
from uuid import uuid4

import pytest
from platform_monitoring.config import KubeConfig
from platform_monitoring.kube_client import JobNotFoundException, KubeClient

from .conftest_kube import MyKubeClient, MyPodDescriptor


@pytest.fixture
def job_pod() -> MyPodDescriptor:
    return MyPodDescriptor(f"job-{uuid4()}")


# class TestKubeClient:
#     @pytest.mark.asyncio
#     async def test_wait_pod_is_running_not_found(self, kube_client: KubeClient) -> None:
#         with pytest.raises(JobNotFoundException):
#             await kube_client.wait_pod_is_running(pod_name="unknown")
#
#     @pytest.mark.asyncio
#     async def test_wait_pod_is_running_timed_out(
#         self,
#         kube_config: KubeConfig,
#         kube_client: MyKubeClient,
#         job_pod: MyPodDescriptor,
#     ) -> None:
#         # TODO (A Yushkovskiy, 31-May-2019) check returned pod statuses
#         await kube_client.create_pod(job_pod.payload)
#         with pytest.raises(asyncio.TimeoutError):
#             await kube_client.wait_pod_is_running(pod_name=job_pod.name, timeout_s=0.1)
#         await kube_client.delete_pod(job_pod.name)
#
#     @pytest.mark.asyncio
#     async def test_wait_pod_is_running(
#         self,
#         kube_config: KubeConfig,
#         kube_client: MyKubeClient,
#         job_pod: MyPodDescriptor,
#     ) -> None:
#         # TODO (A Yushkovskiy, 31-May-2019) check returned pod statuses
#         await kube_client.create_pod(job_pod.payload)
#         is_waiting = await kube_client.is_container_waiting(job_pod.name)
#         assert is_waiting
#         await kube_client.wait_pod_is_running(pod_name=job_pod.name, timeout_s=60.0)
#         is_waiting = await kube_client.is_container_waiting(job_pod.name)
#         assert not is_waiting
#         await kube_client.delete_pod(job_pod.name)
