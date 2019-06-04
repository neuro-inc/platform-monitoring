import asyncio
from typing import Any, AsyncIterator, Dict
from uuid import uuid4

import pytest
from aiohttp import web
from platform_monitoring.config import KubeConfig
from platform_monitoring.kube_client import JobNotFoundException, KubeClient

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
    async def test_wait_pod_is_running_not_found(self, kube_client: KubeClient) -> None:
        with pytest.raises(JobNotFoundException):
            await kube_client.wait_pod_is_running(pod_name="unknown")

    @pytest.mark.asyncio
    async def test_wait_pod_is_running_timed_out(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        # TODO (A Yushkovskiy, 31-May-2019) check returned pod statuses
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
        # TODO (A Yushkovskiy, 31-May-2019) check returned pod statuses
        await kube_client.create_pod(job_pod.payload)
        is_waiting = await kube_client.is_container_waiting(job_pod.name)
        assert is_waiting
        await kube_client.wait_pod_is_running(pod_name=job_pod.name, timeout_s=60.0)
        is_waiting = await kube_client.is_container_waiting(job_pod.name)
        assert not is_waiting
        await kube_client.delete_pod(job_pod.name)

    @pytest.mark.asyncio
    async def test_check_pod_exists_true(
        self, kube_client: MyKubeClient, job_pod: MyPodDescriptor
    ) -> None:
        await kube_client.create_pod(job_pod.payload)
        does_exist = await kube_client.check_pod_exists(pod_name=job_pod.name)
        assert does_exist is True
        await kube_client.delete_pod(job_pod.name)

    @pytest.mark.asyncio
    async def test_check_pod_exists_false(
        self, kube_client: MyKubeClient, job_pod: MyPodDescriptor
    ) -> None:
        does_exist = await kube_client.check_pod_exists(pod_name="unknown")
        assert does_exist is False

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
