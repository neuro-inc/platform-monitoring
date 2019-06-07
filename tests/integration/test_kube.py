import asyncio
import uuid
from typing import Any, AsyncIterator, Dict
from unittest import mock
from uuid import uuid4

import pytest
from aiohttp import web
from platform_monitoring.config import KubeConfig
from platform_monitoring.kube_client import (
    JobNotFoundException,
    KubeClient,
    PodContainerStats,
)

from tests.integration.conftest import ApiAddress, create_local_app_server

from .conftest_kube import MyKubeClient, MyPodDescriptor


@pytest.fixture
def job_pod() -> MyPodDescriptor:
    return MyPodDescriptor(f"job-{uuid4()}")


@pytest.fixture
async def mock_kubernetes_server() -> AsyncIterator[ApiAddress]:
    async def _get_pod(request: web.Request) -> web.Response:
        payload: Dict[str, Any] = {
            "kind": "Pod",
            "metadata": {"name": "testname"},
            "spec": {
                "containers": [{"name": "testname", "image": "testimage"}],
                "nodeName": "whatever",
            },
            "status": {"phase": "Running"},
        }

        return web.json_response(payload)

    async def _stats_summary(request: web.Request) -> web.Response:
        # Explicitly return plain text to trigger ContentTypeError
        return web.Response(content_type="text/plain")

    def _create_app() -> web.Application:
        app = web.Application()
        app.add_routes(
            [
                web.get("/api/v1/namespaces/mock/pods/whatever", _get_pod),
                web.get(
                    "/api/v1/nodes/whatever:10255/proxy/stats/summary", _stats_summary
                ),
            ]
        )
        return app

    app = _create_app()
    async with create_local_app_server(app) as address:
        yield address


class TestKubeClient:
    @pytest.mark.asyncio
    async def test_wait_pod_is_running_not_found(
        self, kube_client: MyKubeClient
    ) -> None:
        with pytest.raises(JobNotFoundException):
            await kube_client.wait_pod_is_running(pod_name="unknown")

    @pytest.mark.asyncio
    async def test_wait_pod_is_running_timed_out(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        # TODO (A Yushkovskiy, 31-May-2019) check returned job_pod statuses
        await kube_client.create_pod(job_pod.payload)
        with pytest.raises(asyncio.TimeoutError):
            await kube_client.wait_pod_is_running(pod_name=job_pod.name, timeout_s=0.1)
        await kube_client.delete_pod(job_pod.name)

    @pytest.mark.asyncio
    async def test_wait_pod_is_running(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        # TODO (A Yushkovskiy, 31-May-2019) check returned job_pod statuses
        await kube_client.create_pod(job_pod.payload)
        is_waiting = await kube_client.is_container_waiting(job_pod.name)
        assert is_waiting
        await kube_client.wait_pod_is_running(pod_name=job_pod.name, timeout_s=60.0)
        is_waiting = await kube_client.is_container_waiting(job_pod.name)
        assert not is_waiting
        await kube_client.delete_pod(job_pod.name)

    @pytest.mark.asyncio
    async def test_get_pod_container_stats_error_json_response_parsing(
        self, mock_kubernetes_server: ApiAddress
    ) -> None:
        srv = mock_kubernetes_server
        async with KubeClient(
            base_url=str(f"http://{srv.host}:{srv.port}"), namespace="mock"
        ) as client:
            stats = await client.get_pod_container_stats("whatever", "whenever")
            assert stats is None

    @pytest.mark.asyncio
    async def test_get_pod_container_stats(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        await kube_client.wait_pod_is_running(pod_name=job_pod.name, timeout_s=60.0)

        pod_metrics = []
        while True:
            stats = await kube_client.get_pod_container_stats(
                job_pod.name, job_pod.name
            )
            if stats:
                pod_metrics.append(stats)
            else:
                break
            await asyncio.sleep(1)

        assert pod_metrics
        assert pod_metrics[0] == PodContainerStats(cpu=mock.ANY, memory=mock.ANY)
        assert pod_metrics[0].cpu >= 0.0
        assert pod_metrics[0].memory > 0.0

    @pytest.mark.asyncio
    async def test_get_pod_container_stats_no_pod(
        self, kube_config: KubeConfig, kube_client: MyKubeClient
    ) -> None:
        pod_name = str(uuid.uuid4())
        with pytest.raises(JobNotFoundException):
            await kube_client.get_pod_container_stats(pod_name, pod_name)

    @pytest.mark.asyncio
    async def test_get_pod_container_stats_not_scheduled_yet(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        await kube_client.create_pod(job_pod.payload)

        stats = await kube_client.get_pod_container_stats(job_pod.name, job_pod.name)
        assert stats is None
