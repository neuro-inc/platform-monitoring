import asyncio
import logging
import re
import uuid
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta, timezone
from typing import Any, Union
from unittest import mock
from uuid import uuid4

import pytest
from aiobotocore.client import AioBaseClient
from aioelasticsearch import Elasticsearch
from aiohttp import web
from async_timeout import timeout
from yarl import URL

from platform_monitoring.config import KubeConfig
from platform_monitoring.kube_client import (
    JobNotFoundException,
    KubeClient,
    KubeClientException,
    PodContainerStats,
    PodPhase,
)
from platform_monitoring.logs import (
    ElasticsearchLogReader,
    ElasticsearchLogsService,
    LogsService,
    PodContainerLogReader,
    S3LogReader,
    S3LogsService,
    get_first_log_entry_time,
)
from platform_monitoring.utils import parse_date

from .conftest_kube import MyKubeClient, MyPodDescriptor
from tests.integration.conftest import ApiAddress, create_local_app_server

logger = logging.getLogger(__name__)


@pytest.fixture
def job_pod() -> MyPodDescriptor:
    return MyPodDescriptor(f"job-{uuid4()}")


@pytest.fixture
async def mock_kubernetes_server() -> AsyncIterator[ApiAddress]:
    async def _get_pod(request: web.Request) -> web.Response:
        payload: dict[str, Any] = {
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

    async def _gpu_metrics(request: web.Request) -> web.Response:
        return web.Response(content_type="text/plain")

    def _create_app() -> web.Application:
        app = web.Application()
        app.add_routes(
            [
                web.get("/api/v1/namespaces/mock/pods/whatever", _get_pod),
                web.get(
                    "/api/v1/nodes/whatever:10255/proxy/stats/summary", _stats_summary
                ),
                web.get("/api/v1/nodes/whatever:9400/proxy/metrics", _gpu_metrics),
            ]
        )
        return app

    app = _create_app()
    async with create_local_app_server(app) as address:
        yield address


@pytest.fixture
def elasticsearch_log_service(
    kube_client: MyKubeClient, es_client: Elasticsearch, kube_container_runtime: str
) -> ElasticsearchLogsService:
    return ElasticsearchLogsService(
        kube_client, es_client, container_runtime=kube_container_runtime
    )


@pytest.fixture
def s3_log_service(
    kube_client: MyKubeClient,
    kube_container_runtime: str,
    s3_client: AioBaseClient,
    s3_logs_bucket: str,
    s3_logs_key_prefix_format: str,
) -> S3LogsService:
    return S3LogsService(
        kube_client,
        s3_client,
        container_runtime=kube_container_runtime,
        bucket_name=s3_logs_bucket,
        key_prefix_format=s3_logs_key_prefix_format,
    )


class TestKubeClient:
    async def test_wait_pod_is_running_not_found(
        self, kube_client: MyKubeClient
    ) -> None:
        with pytest.raises(JobNotFoundException):
            await kube_client.wait_pod_is_running(pod_name="unknown")

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

    async def test_status_restart_never(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        # TODO (A Yushkovskiy, 31-May-2019) check returned job_pod statuses
        job_pod.set_command("sleep 5")
        await kube_client.create_pod(job_pod.payload)
        status = await kube_client.get_container_status(job_pod.name)
        assert not status.can_restart
        assert status.is_waiting
        assert not status.is_running
        assert not status.is_terminated
        assert status.restart_count == 0
        assert status.started_at is None
        assert status.finished_at is None

        status = await kube_client.wait_pod_is_running(job_pod.name, timeout_s=60.0)
        assert not status.can_restart
        assert not status.is_waiting
        assert status.is_running
        assert not status.is_terminated
        assert status.restart_count == 0
        assert status.started_at is not None
        assert status.finished_at is None

        await kube_client.wait_pod_is_terminated(job_pod.name, timeout_s=60.0)
        status = await kube_client.get_container_status(job_pod.name)
        assert not status.can_restart
        assert not status.is_waiting
        assert not status.is_running
        assert status.is_terminated
        assert status.is_pod_terminated
        assert status.restart_count == 0
        assert status.started_at is not None
        assert status.finished_at is not None

        await kube_client.delete_pod(job_pod.name)
        await kube_client.wait_pod_is_deleted(job_pod.name, timeout_s=60.0)
        with pytest.raises(JobNotFoundException):
            await kube_client.get_container_status(job_pod.name)

    async def test_status_restart_always(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        # TODO (A Yushkovskiy, 31-May-2019) check returned job_pod statuses
        job_pod.set_command("sleep 5")
        job_pod.set_restart_policy("Always")
        try:
            await kube_client.create_pod(job_pod.payload)
            status = await kube_client.get_container_status(job_pod.name)
            assert status.can_restart
            assert status.is_waiting
            assert not status.is_running
            assert not status.is_terminated
            assert status.restart_count == 0
            assert status.started_at is None
            assert status.finished_at is None

            status = await kube_client.wait_pod_is_running(
                pod_name=job_pod.name, timeout_s=60.0
            )
            assert status.can_restart
            assert not status.is_waiting
            assert status.is_running
            assert not status.is_terminated
            assert status.restart_count == 0
            assert status.started_at is not None
            assert status.finished_at is None
            first_started_at = status.started_at

            await kube_client.wait_pod_is_terminated(job_pod.name)
            status = await kube_client.get_container_status(job_pod.name)
            assert status.can_restart
            assert not status.is_waiting
            assert not status.is_running
            assert status.is_terminated
            assert not status.is_pod_terminated
            assert status.restart_count == 0
            assert status.started_at is not None
            assert status.started_at == first_started_at
            assert status.finished_at is not None
            first_finished_at = status.finished_at

            await kube_client.wait_container_is_restarted(job_pod.name)
            status = await kube_client.get_container_status(job_pod.name)
            assert status.can_restart
            assert not status.is_waiting
            assert status.is_running
            assert not status.is_terminated
            assert status.restart_count == 1
            assert status.started_at is not None
            assert status.finished_at is not None
            assert status.started_at != first_started_at
            assert status.finished_at == first_finished_at
        finally:
            await kube_client.delete_pod(job_pod.name)

    async def test_status_restart_on_failure_success(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        job_pod.set_command("sleep 5")
        job_pod.set_restart_policy("OnFailure")
        await kube_client.create_pod(job_pod.payload)
        status = await kube_client.get_container_status(job_pod.name)
        assert status.can_restart
        assert status.is_waiting
        assert not status.is_running
        assert not status.is_terminated
        assert status.restart_count == 0
        assert status.started_at is None
        assert status.finished_at is None

        status = await kube_client.wait_pod_is_running(job_pod.name, timeout_s=60.0)
        assert status.can_restart
        assert not status.is_waiting
        assert status.is_running
        assert not status.is_terminated
        assert status.restart_count == 0
        assert status.started_at is not None
        assert status.finished_at is None

        await kube_client.wait_pod_is_terminated(job_pod.name, timeout_s=60.0)
        status = await kube_client.get_container_status(job_pod.name)
        assert not status.can_restart
        assert not status.is_waiting
        assert not status.is_running
        assert status.is_terminated
        assert status.is_pod_terminated
        assert status.restart_count == 0
        assert status.started_at is not None
        assert status.finished_at is not None

        await kube_client.delete_pod(job_pod.name)
        await kube_client.wait_pod_is_deleted(job_pod.name, timeout_s=60.0)
        with pytest.raises(JobNotFoundException):
            await kube_client.get_container_status(job_pod.name)

    async def test_status_restart_on_failure_failure(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        job_pod.set_command("bash -c 'sleep 5; false'")
        job_pod.set_restart_policy("OnFailure")
        try:
            await kube_client.create_pod(job_pod.payload)
            status = await kube_client.get_container_status(job_pod.name)
            assert status.can_restart
            assert status.is_waiting
            assert not status.is_running
            assert not status.is_terminated
            assert status.restart_count == 0
            assert status.started_at is None
            assert status.finished_at is None

            status = await kube_client.wait_pod_is_running(
                pod_name=job_pod.name, timeout_s=60.0
            )
            assert status.can_restart
            assert not status.is_waiting
            assert status.is_running
            assert not status.is_terminated
            assert status.restart_count == 0
            assert status.started_at is not None
            assert status.finished_at is None
            first_started_at = status.started_at

            await kube_client.wait_pod_is_terminated(job_pod.name)
            status = await kube_client.get_container_status(job_pod.name)
            assert status.can_restart
            assert not status.is_waiting
            assert not status.is_running
            assert status.is_terminated
            assert not status.is_pod_terminated
            assert status.restart_count == 0
            assert status.started_at is not None
            assert status.started_at == first_started_at
            assert status.finished_at is not None
            first_finished_at = status.finished_at

            await kube_client.wait_container_is_restarted(job_pod.name)
            status = await kube_client.get_container_status(job_pod.name)
            assert status.can_restart
            assert not status.is_waiting
            assert status.is_running
            assert not status.is_terminated
            assert status.restart_count == 1
            assert status.started_at is not None
            assert status.finished_at is not None
            assert status.started_at != first_started_at
            assert status.finished_at == first_finished_at
        finally:
            await kube_client.delete_pod(job_pod.name)

    async def test_get_pod_container_stats_error_json_response_parsing(
        self, mock_kubernetes_server: ApiAddress
    ) -> None:
        srv = mock_kubernetes_server
        async with KubeClient(
            base_url=str(f"http://{srv.host}:{srv.port}"), namespace="mock"
        ) as client:
            stats = await client.get_pod_container_stats(
                "whatever", "whatever", "whenever"
            )
            assert stats is None

    async def test_get_pod_container_gpu_stats(
        self, mock_kubernetes_server: ApiAddress
    ) -> None:
        srv = mock_kubernetes_server
        async with KubeClient(
            base_url=str(f"http://{srv.host}:{srv.port}"),
            namespace="mock",
            nvidia_dcgm_node_port=9400,
        ) as client:
            stats = await client.get_pod_container_gpu_stats(
                "whatever", "whatever", "whenever"
            )
            assert stats is not None

    async def test_get_pod_container_gpu_stats_no_nvidia_dcgm_port(
        self, mock_kubernetes_server: ApiAddress
    ) -> None:
        srv = mock_kubernetes_server
        async with KubeClient(
            base_url=str(f"http://{srv.host}:{srv.port}"), namespace="mock"
        ) as client:
            stats = await client.get_pod_container_gpu_stats(
                "whatever", "whatever", "whenever"
            )
            assert stats is None

    async def test_get_pod_container_stats(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        kube_node_name: str,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        await kube_client.wait_pod_is_not_waiting(pod_name=job_pod.name, timeout_s=60.0)

        pod_metrics = []
        while True:
            stats = await kube_client.get_pod_container_stats(
                kube_node_name, job_pod.name, job_pod.name
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

    async def test_check_pod_exists_true(
        self, kube_client: MyKubeClient, job_pod: MyPodDescriptor
    ) -> None:
        await kube_client.create_pod(job_pod.payload)
        does_exist = await kube_client.check_pod_exists(pod_name=job_pod.name)
        assert does_exist is True
        await kube_client.delete_pod(job_pod.name)

    async def test_check_pod_exists_false(
        self, kube_client: MyKubeClient, job_pod: MyPodDescriptor
    ) -> None:
        does_exist = await kube_client.check_pod_exists(pod_name="unknown")
        assert does_exist is False

    async def test_create_log_stream_not_found(self, kube_client: KubeClient) -> None:
        with pytest.raises(JobNotFoundException):
            async with kube_client.create_pod_container_logs_stream(
                pod_name="unknown", container_name="unknown"
            ):
                pass

    @pytest.mark.xfail
    async def test_create_log_stream_creating(
        self, kube_client: MyKubeClient, job_pod: MyPodDescriptor
    ) -> None:
        await kube_client.create_pod(job_pod.payload)

        async with timeout(5.0):
            while True:
                stream_cm = kube_client.create_pod_container_logs_stream(
                    pod_name=job_pod.name, container_name=job_pod.name
                )
                with pytest.raises(JobNotFoundException) as cm:
                    async with stream_cm:
                        pass
                if "has not created" in str(cm.value):
                    break
                await asyncio.sleep(0.1)

    async def test_create_log_stream(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        await kube_client.create_pod(job_pod.payload)
        await kube_client.wait_pod_is_not_waiting(pod_name=job_pod.name, timeout_s=60.0)
        stream_cm = kube_client.create_pod_container_logs_stream(
            pod_name=job_pod.name, container_name=job_pod.name
        )
        async with stream_cm as stream:
            payload = await stream.read()
            assert payload == b""

    async def test_get_node_proxy_client(
        self, kube_config: KubeConfig, kube_client: MyKubeClient
    ) -> None:
        node_list = await kube_client.get_node_list()
        node_name = node_list["items"][0]["metadata"]["name"]
        async with kube_client.get_node_proxy_client(
            node_name, KubeConfig.kubelet_node_port
        ) as client:
            assert client.url == URL(
                kube_config.endpoint_url
                + f"/api/v1/nodes/{node_name}:{KubeConfig.kubelet_node_port}/proxy"
            )

            async with client.session.get(URL(f"{client.url}/stats/summary")) as resp:
                assert resp.status == 200, await resp.text()
                payload = await resp.json()
                assert "node" in payload

    async def test_get_nodes(self, kube_client: MyKubeClient) -> None:
        nodes = await kube_client.get_nodes()
        assert nodes

        nodes = await kube_client.get_nodes(label_selector="kubernetes.io/os=linux")
        assert nodes
        assert all(node.get_label("kubernetes.io/os") == "linux" for node in nodes)

    async def test_get_pods(
        self, kube_client: MyKubeClient, job_pod: MyPodDescriptor
    ) -> None:
        try:
            await kube_client.create_pod(job_pod.payload)

            pods = await kube_client.get_pods()
            assert pods
            assert any(pod.name == job_pod.name for pod in pods)

            pods = await kube_client.get_pods(label_selector=f"job={job_pod.name}")
            assert len(pods) == 1
            assert pods[0].name == job_pod.name

            pods = await kube_client.get_pods(
                field_selector=",".join(
                    (
                        "status.phase!=Failed",
                        "status.phase!=Succeeded",
                        "status.phase!=Unknown",
                    ),
                ),
            )
            assert pods
            assert all(
                pod.phase in (PodPhase.PENDING, PodPhase.RUNNING) for pod in pods
            )
        finally:
            await kube_client.delete_pod(job_pod.name)


class TestLogReader:
    async def _consume_log_reader(
        self,
        log_reader: AbstractAsyncContextManager[AsyncIterator[bytes]],
        delay: float = 0,
    ) -> bytes:
        buffer = bytearray()
        try:
            async with log_reader as it:
                async for chunk in it:
                    buffer += chunk
                    if delay:
                        await asyncio.sleep(delay)
        except asyncio.CancelledError:
            logger.exception("Cancelled logs reading")
            buffer += b"CANCELLED"
        return bytes(buffer)

    def _remove_timestamps(self, data: bytes) -> bytes:
        result = []
        for line in data.splitlines(True):
            timestamp, rest = line.split(b" ", 1)
            assert re.fullmatch(
                rb"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+Z",
                timestamp,
            )
            result.append(rest)
        return b"".join(result)

    async def test_read_instantly_succeeded(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        await kube_client.create_pod(job_pod.payload)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=job_pod.name, container_name=job_pod.name
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b""

    async def test_read_instantly_failed(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        command = 'bash -c "echo -n Failure!; false"'
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=job_pod.name, container_name=job_pod.name
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b"Failure!"

    async def test_read_timed_out(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        command = 'bash -c "sleep 5; echo -n Success!"'
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        log_reader = PodContainerLogReader(
            client=kube_client,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            client_read_timeout_s=1,
        )
        with pytest.raises(asyncio.TimeoutError):
            await self._consume_log_reader(log_reader)

    async def test_read_succeeded(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=job_pod.name, container_name=job_pod.name
        )
        payload = await self._consume_log_reader(log_reader)
        expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
        assert payload == expected_payload.encode()

    async def test_read_with_timestamps(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        log_reader = PodContainerLogReader(
            client=kube_client,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            timestamps=True,
        )
        payload = await self._consume_log_reader(log_reader)
        payload = self._remove_timestamps(payload)
        expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
        assert payload == expected_payload.encode()

    async def test_read_since(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        command = "bash -c 'echo first; sleep 3; echo second; sleep 3; echo third'"
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        log_reader = PodContainerLogReader(
            client=kube_client,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            timestamps=True,
        )
        payload = await self._consume_log_reader(log_reader)
        assert self._remove_timestamps(payload) == b"first\nsecond\nthird\n"
        lines = payload.splitlines()
        second_ts = parse_date(lines[1].split()[0].decode())

        log_reader = PodContainerLogReader(
            client=kube_client,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            since=second_ts,
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b"second\nthird\n"

        log_reader = PodContainerLogReader(
            client=kube_client,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            since=second_ts + timedelta(seconds=1),
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b"third\n"

    async def test_read_cancelled(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        command = 'bash -c "for i in {1..60}; do echo $i; sleep 1; done"'
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        await kube_client.wait_pod_is_running(pod_name=job_pod.name, timeout_s=60.0)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=job_pod.name, container_name=job_pod.name
        )
        task = asyncio.ensure_future(self._consume_log_reader(log_reader))
        await asyncio.sleep(10)
        task.cancel()
        payload = await task
        expected_payload = "\n".join(str(i) for i in range(1, 6))
        assert payload.startswith(expected_payload.encode())

    async def test_read_restarted(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        command = """bash -c 'echo "`date +%s` Restart"; sleep 1; false'"""
        job_pod.set_command(command)
        job_pod.set_restart_policy("Always")
        try:
            await kube_client.create_pod(job_pod.payload)
            log_reader = PodContainerLogReader(
                client=kube_client,
                pod_name=job_pod.name,
                container_name=job_pod.name,
                previous=True,
            )
            with pytest.raises(KubeClientException):
                await self._consume_log_reader(log_reader)

            log_reader = PodContainerLogReader(
                client=kube_client, pod_name=job_pod.name, container_name=job_pod.name
            )
            payload = await self._consume_log_reader(log_reader)
            assert b" Restart\n" in payload
            orig_timestamp = int(payload.split()[0])

            await kube_client.wait_container_is_restarted(job_pod.name)

            for i in range(3)[::-1]:
                try:
                    log_reader = PodContainerLogReader(
                        client=kube_client,
                        pod_name=job_pod.name,
                        container_name=job_pod.name,
                        previous=True,
                    )
                    payload = await self._consume_log_reader(log_reader)
                    break
                except KubeClientException:
                    if not i:
                        raise
                    await asyncio.sleep(1)
                    continue

            assert b" Restart\n" in payload
            assert payload.count(b"Restart") == 1
            timestamp = int(payload.split()[0])
            assert timestamp >= orig_timestamp
        finally:
            await kube_client.delete_pod(job_pod.name)

    @pytest.skip
    async def test_elasticsearch_log_reader(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        kube_container_runtime: str,
        job_pod: MyPodDescriptor,
        es_client: Elasticsearch,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        await kube_client.wait_pod_is_terminated(job_pod.name)

        await self._check_kube_logs(
            kube_client,
            namespace_name=kube_config.namespace,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            expected_payload=expected_payload,
        )
        await kube_client.delete_pod(job_pod.name)
        for timestamps in [False, True]:
            await self._check_es_logs(
                es_client,
                container_runtime=kube_container_runtime,
                namespace_name=kube_config.namespace,
                pod_name=job_pod.name,
                container_name=job_pod.name,
                expected_payload=expected_payload,
                timestamps=timestamps,
            )

    @pytest.skip
    async def test_elasticsearch_log_reader_restarted(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        kube_container_runtime: str,
        job_pod: MyPodDescriptor,
        es_client: Elasticsearch,
    ) -> None:
        command = 'bash -c "sleep 5; for i in {1..5}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
        job_pod.set_command(command)
        job_pod.set_restart_policy("Always")
        try:
            await kube_client.create_pod(job_pod.payload)
            await kube_client.wait_container_is_restarted(job_pod.name, 2)
        finally:
            await kube_client.delete_pod(job_pod.name)

        await self._check_es_logs(
            es_client,
            container_runtime=kube_container_runtime,
            namespace_name=kube_config.namespace,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            expected_payload=expected_payload * 2,
        )

    async def test_s3_log_reader(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        kube_container_runtime: str,
        job_pod: MyPodDescriptor,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_logs_key_prefix_format: str,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        await kube_client.wait_pod_is_terminated(job_pod.name)

        await self._check_kube_logs(
            kube_client,
            namespace_name=kube_config.namespace,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            expected_payload=expected_payload,
        )
        await kube_client.delete_pod(job_pod.name)
        for timestamps in [False, True]:
            await self._check_s3_logs(
                s3_client,
                container_runtime=kube_container_runtime,
                bucket_name=s3_logs_bucket,
                prefix_format=s3_logs_key_prefix_format,
                namespace_name=kube_config.namespace,
                pod_name=job_pod.name,
                container_name=job_pod.name,
                expected_payload=expected_payload,
                timestamps=timestamps,
            )

    async def test_s3_log_reader_restarted(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        kube_container_runtime: str,
        job_pod: MyPodDescriptor,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_logs_key_prefix_format: str,
    ) -> None:
        command = 'bash -c "sleep 5; for i in {1..5}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
        job_pod.set_command(command)
        job_pod.set_restart_policy("Always")
        try:
            await kube_client.create_pod(job_pod.payload)
            await kube_client.wait_container_is_restarted(job_pod.name, 2)
        finally:
            await kube_client.delete_pod(job_pod.name)

        await self._check_s3_logs(
            s3_client,
            container_runtime=kube_container_runtime,
            bucket_name=s3_logs_bucket,
            prefix_format=s3_logs_key_prefix_format,
            namespace_name=kube_config.namespace,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            expected_payload=expected_payload * 2,
        )

    async def test_s3_logs_cleanup(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        kube_container_runtime: str,
        job_pod: MyPodDescriptor,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_logs_key_prefix_format: str,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        await kube_client.wait_pod_is_terminated(job_pod.name)

        await self._check_s3_logs(
            s3_client,
            container_runtime=kube_container_runtime,
            bucket_name=s3_logs_bucket,
            prefix_format=s3_logs_key_prefix_format,
            namespace_name=kube_config.namespace,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            expected_payload=expected_payload,
        )

        service = S3LogsService(
            kube_client,
            s3_client,
            container_runtime=kube_container_runtime,
            bucket_name=s3_logs_bucket,
            key_prefix_format=s3_logs_key_prefix_format,
        )

        await service.drop_logs(job_pod.name)

        await self._check_s3_logs(
            s3_client,
            container_runtime=kube_container_runtime,
            bucket_name=s3_logs_bucket,
            prefix_format=s3_logs_key_prefix_format,
            namespace_name=kube_config.namespace,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            expected_payload=b"",
            timeout_s=1,
        )

    async def _check_kube_logs(
        self,
        kube_client: KubeClient,
        namespace_name: str,
        pod_name: str,
        container_name: str,
        expected_payload: Any,
    ) -> None:
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod_name, container_name=container_name
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == expected_payload, "Pod logs did not match."

    async def _check_es_logs(
        self,
        es_client: Elasticsearch,
        container_runtime: str,
        namespace_name: str,
        pod_name: str,
        container_name: str,
        expected_payload: Any,
        timeout_s: float = 120.0,
        interval_s: float = 1.0,
        timestamps: bool = False,
    ) -> None:
        payload = b""
        try:
            async with timeout(timeout_s):
                while True:
                    log_reader = ElasticsearchLogReader(
                        es_client,
                        container_runtime=container_runtime,
                        namespace_name=namespace_name,
                        pod_name=pod_name,
                        container_name=container_name,
                        timestamps=timestamps,
                    )
                    payload = await self._consume_log_reader(log_reader)
                    if timestamps:
                        payload = self._remove_timestamps(payload)
                    if payload == expected_payload:
                        return
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            pytest.fail(f"Pod logs did not match. Last payload: {payload!r}")

    async def _check_s3_logs(
        self,
        s3_client: AioBaseClient,
        container_runtime: str,
        bucket_name: str,
        prefix_format: str,
        namespace_name: str,
        pod_name: str,
        container_name: str,
        expected_payload: Any,
        timeout_s: float = 120.0,
        interval_s: float = 1.0,
        timestamps: bool = False,
    ) -> None:
        payload = b""
        try:
            async with timeout(timeout_s):
                while True:
                    log_reader = S3LogReader(
                        s3_client,
                        container_runtime=container_runtime,
                        bucket_name=bucket_name,
                        prefix_format=prefix_format,
                        namespace_name=namespace_name,
                        pod_name=pod_name,
                        container_name=container_name,
                        timestamps=timestamps,
                    )
                    payload = await self._consume_log_reader(log_reader)
                    if timestamps:
                        payload = self._remove_timestamps(payload)
                    if payload == expected_payload:
                        return
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            pytest.fail(f"Pod logs did not match. Last payload: {payload!r}")

    @pytest.skip
    async def test_elasticsearch_log_reader_empty(
        self, es_client: Elasticsearch, kube_container_runtime: str
    ) -> None:
        namespace_name = pod_name = container_name = str(uuid.uuid4())
        log_reader = ElasticsearchLogReader(
            es_client,
            container_runtime=kube_container_runtime,
            namespace_name=namespace_name,
            pod_name=pod_name,
            container_name=container_name,
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b""

    async def test_s3_log_reader_empty(
        self,
        kube_container_runtime: str,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_logs_key_prefix_format: str,
    ) -> None:
        namespace_name = pod_name = container_name = str(uuid.uuid4())
        log_reader = S3LogReader(
            s3_client,
            container_runtime=kube_container_runtime,
            bucket_name=s3_logs_bucket,
            prefix_format=s3_logs_key_prefix_format,
            namespace_name=namespace_name,
            pod_name=pod_name,
            container_name=container_name,
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b""

    async def _test_get_job_log_reader(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        factory: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        command = 'bash -c "echo hello"'
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)

        pod_name = job_pod.name

        await kube_client.wait_pod_is_terminated(pod_name, timeout_s=120)

        log_reader = factory.get_pod_log_reader(pod_name, archive_delay_s=10.0)
        payload = await self._consume_log_reader(log_reader)
        assert payload == b"hello\n"

        await kube_client.delete_pod(job_pod.name)

        log_reader = factory.get_pod_log_reader(pod_name, archive_delay_s=10.0)
        payload = await self._consume_log_reader(log_reader)
        assert payload == b"hello\n"

    @pytest.skip
    async def test_get_job_elasticsearch_log_reader(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        elasticsearch_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_get_job_log_reader(
            kube_config, kube_client, elasticsearch_log_service, job_pod
        )

    async def test_get_job_s3_log_reader(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        s3_log_service: S3LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_get_job_log_reader(
            kube_config, kube_client, s3_log_service, job_pod
        )

    async def _test_empty_log_reader(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        factory: LogsService,
    ) -> None:
        command = "sleep 5"
        job_pod.set_command(command)
        names = []
        tasks = []
        done = False

        async def stop_func() -> bool:
            if done:
                return True
            await asyncio.sleep(1)
            return False

        def run_log_reader(
            name: str, delay: float = 0, timeout_s: float = 60.0
        ) -> None:
            async def coro() -> Union[bytes, Exception]:
                await asyncio.sleep(delay)
                try:
                    async with timeout(timeout_s):
                        log_reader = factory.get_pod_log_reader(
                            job_pod.name,
                            separator=b"===",
                            archive_delay_s=600.0,
                            stop_func=stop_func,
                        )
                        return await self._consume_log_reader(log_reader)
                except Exception as e:
                    logger.exception("Error in logs reading for %r", name)
                    return e

            names.append(name)
            task = asyncio.ensure_future(coro())
            tasks.append(task)

        try:
            await kube_client.create_pod(job_pod.payload)
            run_log_reader("created", timeout_s=120)
            await kube_client.wait_pod_is_running(pod_name=job_pod.name, timeout_s=120)
            for i in range(4):
                run_log_reader(f"started [{i}]", delay=i * 2)
            await kube_client.wait_pod_is_terminated(job_pod.name)
        finally:
            done = True
            await kube_client.delete_pod(job_pod.name)
        run_log_reader("deleting")
        await kube_client.wait_pod_is_deleted(job_pod.name)
        run_log_reader("deleted")

        payloads = await asyncio.gather(*tasks)

        # Output for debugging
        for i, (name, payload) in enumerate(zip(names, payloads)):
            print(f"{i}. {name}: {payload!r}")

        # All logs are completely either live or archive, no separator.
        for name, payload in zip(names, payloads):
            assert payload == b"", name

    @pytest.skip
    async def test_elasticsearch_empty_log_reader(
        self,
        kube_client: MyKubeClient,
        elasticsearch_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_empty_log_reader(
            kube_client, job_pod, elasticsearch_log_service
        )

    async def test_s3_empty_log_reader(
        self,
        kube_client: MyKubeClient,
        s3_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_empty_log_reader(kube_client, job_pod, s3_log_service)

    async def _test_merged_log_reader(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        factory: LogsService,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do sleep 1; echo $i; done"'
        job_pod.set_command(command)
        names = []
        tasks = []
        done = False

        async def stop_func() -> bool:
            if done:
                return True
            await asyncio.sleep(1)
            return False

        def run_log_reader(
            name: str, delay: float = 0, timeout_s: float = 60.0
        ) -> None:
            async def coro() -> Union[bytes, Exception]:
                await asyncio.sleep(delay)
                try:
                    async with timeout(timeout_s):
                        log_reader = factory.get_pod_log_reader(
                            job_pod.name,
                            separator=b"===",
                            archive_delay_s=600.0,
                            stop_func=stop_func,
                        )
                        return await self._consume_log_reader(log_reader)
                except Exception as e:
                    logger.exception("Error in logs reading for %r", name)
                    return e

            names.append(name)
            task = asyncio.ensure_future(coro())
            tasks.append(task)

        try:
            await kube_client.create_pod(job_pod.payload)
            run_log_reader("created", timeout_s=120)
            await kube_client.wait_pod_is_running(pod_name=job_pod.name, timeout_s=120)
            for i in range(4):
                run_log_reader(f"started [{i}]", delay=i * 2)
            await kube_client.wait_pod_is_terminated(job_pod.name)
            await asyncio.sleep(10)
        finally:
            done = True
            await kube_client.delete_pod(job_pod.name)
        run_log_reader("deleting")
        await kube_client.wait_pod_is_deleted(job_pod.name)
        run_log_reader("deleted")

        payloads = await asyncio.gather(*tasks)

        # Output for debugging
        for i, (name, payload) in enumerate(zip(names, payloads)):
            print(f"{i}. {name}: {payload!r}")

        expected_payload = "".join(f"{i}\n" for i in range(1, 6)).encode()
        # All logs are completely either live or archive, no separator.
        for name, payload in zip(names, payloads):
            assert payload == expected_payload, name

    @pytest.skip
    async def test_elasticsearch_merged_log_reader(
        self,
        kube_client: MyKubeClient,
        elasticsearch_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_merged_log_reader(
            kube_client, job_pod, elasticsearch_log_service
        )

    async def test_s3_merged_log_reader(
        self,
        kube_client: MyKubeClient,
        s3_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_merged_log_reader(kube_client, job_pod, s3_log_service)

    async def _test_merged_log_reader_restarted(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        factory: LogsService,
    ) -> None:
        command = (
            'bash -c "sleep 5; date +[%T]; for i in {1..5}; do sleep 1; echo $i; done"'
        )
        job_pod.set_command(command)
        job_pod.set_restart_policy("Always")
        names = []
        tasks = []
        done = False

        async def stop_func() -> bool:
            if done:
                return True
            await asyncio.sleep(1)
            return False

        def run_log_reader(
            name: str, delay: float = 0, timeout_s: float = 60.0
        ) -> None:
            async def coro() -> Union[bytes, Exception]:
                await asyncio.sleep(delay)
                try:
                    async with timeout(timeout_s):
                        log_reader = factory.get_pod_log_reader(
                            job_pod.name,
                            separator=b"===",
                            archive_delay_s=600.0,
                            stop_func=stop_func,
                        )
                        return await self._consume_log_reader(log_reader)
                except Exception as e:
                    logger.exception("Error in logs reading for %r", name)
                    return e

            names.append(name)
            task = asyncio.ensure_future(coro())
            tasks.append(task)

        try:
            await kube_client.create_pod(job_pod.payload)
            run_log_reader("created", timeout_s=180)
            await kube_client.wait_pod_is_running(pod_name=job_pod.name, timeout_s=120)
            for i in range(4):
                run_log_reader(f"started [{i}]", delay=i * 2, timeout_s=90)
            await kube_client.wait_container_is_restarted(job_pod.name, 1)
            for i in range(4):
                run_log_reader(f"restarted 1 [{i}]", delay=i * 2)
            await kube_client.wait_container_is_restarted(job_pod.name, 2)
            for i in range(4):
                run_log_reader(f"restarted 2 [{i}]", delay=i * 2)
            await kube_client.wait_pod_is_terminated(job_pod.name)
            await asyncio.sleep(10)
        finally:
            done = True
            await kube_client.delete_pod(job_pod.name)
        run_log_reader("deleting")
        await kube_client.wait_pod_is_deleted(job_pod.name)
        run_log_reader("deleted")

        payloads = await asyncio.gather(*tasks)

        # Output for debugging
        for i, (name, payload) in enumerate(zip(names, payloads)):
            print(f"{i}. {name}: {payload!r}")

        expected_payload = "".join(f"{i}\n" for i in range(1, 6)).encode()
        payload0 = payloads[0]
        assert re.sub(rb"\[.*?\]\n", b"", payload0) == expected_payload * 3
        for i, (name, payload) in enumerate(zip(names, payloads)):
            if i < 2 or i >= len(names) - 1:
                # No separator in earlest (live only) and
                # latest (archive only) logs.
                assert b"===" not in payload, name
            elif "restarted 1 " in name:
                # There should be parts of live and archive logs,
                # and a separator between them.
                assert payload.count(b"===\n") == 1, name
            # The last line in the archive can be duplicated in live logs.
            payload = re.sub(rb"(?m)^(.*\n)(?====\n\1)", b"", payload)
            # Remove separator between archive and live logs.
            payload = payload.replace(b"===\n", b"")
            assert payload == payload0, name

    @pytest.skip
    async def test_elasticsearch_merged_log_reader_restarted(
        self,
        kube_client: MyKubeClient,
        elasticsearch_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_merged_log_reader_restarted(
            kube_client,
            job_pod,
            elasticsearch_log_service,
        )

    async def test_s3_merged_log_reader_restarted(
        self,
        kube_client: MyKubeClient,
        s3_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_merged_log_reader_restarted(
            kube_client, job_pod, s3_log_service
        )

    async def _test_merged_log_reader_restarted_since(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        factory: LogsService,
    ) -> None:
        command = "bash -c 'echo begin; sleep 5; echo end; false'"
        job_pod.set_command(command)
        job_pod.set_restart_policy("OnFailure")
        starts = []
        tasks = []

        def run_log_reader(since: datetime) -> None:
            async def coro() -> bytes:
                log_reader = factory.get_pod_log_reader(
                    job_pod.name, since=since, separator=b"===", archive_delay_s=20.0
                )
                return await self._consume_log_reader(log_reader)

            starts.append(since)
            task = asyncio.ensure_future(coro())
            tasks.append(task)

        try:
            await kube_client.create_pod(job_pod.payload)
            await kube_client.wait_pod_is_running(pod_name=job_pod.name, timeout_s=120)
            await kube_client.wait_pod_is_terminated(job_pod.name)
            status = await kube_client.get_container_status(job_pod.name)
            finished1 = status.finished_at
            assert finished1

            await kube_client.wait_container_is_restarted(job_pod.name, 1)
            status = await kube_client.get_container_status(job_pod.name)
            started2 = status.started_at
            assert started2
            run_log_reader(since=started2)
            run_log_reader(since=datetime.now(tz=timezone.utc))

            await kube_client.wait_pod_is_terminated(job_pod.name)
            status = await kube_client.get_container_status(job_pod.name)
            assert status.started_at == started2
            finished2 = status.finished_at
            assert finished2

            await kube_client.wait_container_is_restarted(job_pod.name, 2)
            run_log_reader(since=finished1 - timedelta(seconds=2))
            run_log_reader(since=started2)
            run_log_reader(since=finished2 - timedelta(seconds=2))
            run_log_reader(since=finished2 + timedelta(seconds=2))
            await kube_client.wait_pod_is_terminated(job_pod.name)
        finally:
            await kube_client.delete_pod(job_pod.name)
        await kube_client.wait_pod_is_deleted(job_pod.name)
        run_log_reader(since=finished1 - timedelta(seconds=2))
        run_log_reader(since=started2)
        run_log_reader(since=finished2 - timedelta(seconds=2))
        run_log_reader(since=finished2 + timedelta(seconds=2))

        payloads = await asyncio.gather(*tasks)

        # Output for debugging
        for i, (since, payload) in enumerate(zip(starts, payloads)):
            print(f"{i}. [{since:%T}] {payload!r}")
        assert payloads == [
            b"begin\nend\nbegin\nend\n",
            b"end\nbegin\nend\n",
            b"end\nbegin\nend\n===\nbegin\nend\n",
            b"begin\nend\n===\nbegin\nend\n",
            b"end\n===\nbegin\nend\n",
            b"begin\nend\n",
            b"end\nbegin\nend\nbegin\nend\n",
            b"begin\nend\nbegin\nend\n",
            b"end\nbegin\nend\n",
            b"begin\nend\n",
        ]

    @pytest.skip
    async def test_elasticsearch_merged_log_reader_restarted_since(
        self,
        kube_client: MyKubeClient,
        elasticsearch_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_merged_log_reader_restarted_since(
            kube_client,
            job_pod,
            elasticsearch_log_service,
        )

    async def test_s3_merged_log_reader_restarted_since(
        self,
        kube_client: MyKubeClient,
        s3_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_merged_log_reader_restarted_since(
            kube_client, job_pod, s3_log_service
        )

    async def _test_large_log_reader(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        factory: LogsService,
    ) -> None:
        num = 10000
        command = f"seq {num}"
        job_pod.set_command(command)

        try:
            await kube_client.create_pod(job_pod.payload)
            await kube_client.wait_pod_is_terminated(job_pod.name)
            log_reader = factory.get_pod_log_reader(
                job_pod.name, separator=b"===", archive_delay_s=30.0
            )
            payload = (await self._consume_log_reader(log_reader, delay=0.001)).decode()
        finally:
            await kube_client.delete_pod(job_pod.name)

        expected_payload = "".join(f"{i + 1}\n" for i in range(num))
        assert payload == expected_payload

    async def test_large_log_reader(
        self,
        kube_client: MyKubeClient,
        s3_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_large_log_reader(kube_client, job_pod, s3_log_service)

    async def test_get_first_log_entry_time(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
    ) -> None:
        pod_name = job_pod.name
        command = "bash -c 'sleep 5; echo first; sleep 5; echo second'"
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        first_ts = await get_first_log_entry_time(kube_client, pod_name, timeout_s=1)
        assert first_ts is None
        await kube_client.wait_pod_is_running(pod_name)
        first_ts = await get_first_log_entry_time(kube_client, pod_name, timeout_s=1)
        assert first_ts is None
        first_ts = await get_first_log_entry_time(kube_client, pod_name, timeout_s=5)
        assert first_ts is not None
        await kube_client.wait_pod_is_terminated(pod_name)
        status = await kube_client.get_container_status(pod_name)
        assert status.started_at is not None
        assert status.finished_at is not None
        assert first_ts > status.started_at
        assert first_ts < status.finished_at
        first_ts2 = await get_first_log_entry_time(kube_client, pod_name, timeout_s=1)
        assert first_ts2 == first_ts
