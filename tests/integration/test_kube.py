from __future__ import annotations

import asyncio
import json
import logging
import re
import tempfile
import uuid
import zlib
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    Sequence,
)
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest import mock
from uuid import uuid4

import aiohttp
import aiohttp.web
import botocore.exceptions
import pytest
from aiobotocore.client import AioBaseClient
from aiohttp import web
from elasticsearch import AsyncElasticsearch

from platform_monitoring.api import create_s3_logs_bucket
from platform_monitoring.config import KubeConfig, S3Config
from platform_monitoring.kube_client import (
    JobNotFoundException,
    KubeClient,
    KubeClientAuthType,
    KubeClientException,
    PodContainerStats,
    PodPhase,
)
from platform_monitoring.logs import (
    ZLIB_WBITS,
    ElasticsearchLogReader,
    ElasticsearchLogsService,
    LogsService,
    LokiLogsService,
    PodContainerLogReader,
    S3FileReader,
    S3LogFile,
    S3LogReader,
    S3LogRecord,
    S3LogRecordsReader,
    S3LogRecordsWriter,
    S3LogsMetadata,
    S3LogsMetadataService,
    S3LogsMetadataStorage,
    S3LogsService,
    get_first_log_entry_time,
)
from platform_monitoring.loki_client import LokiClient
from platform_monitoring.utils import parse_date

from .conftest import ApiAddress, create_local_app_server
from .conftest_kube import MyKubeClient, MyPodDescriptor


logger = logging.getLogger(__name__)


@pytest.fixture()
def job_pod() -> MyPodDescriptor:
    return MyPodDescriptor(f"job-{uuid4()}")


@pytest.fixture()
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

    def _unauthorized_gpu_metrics() -> Callable[
        [web.Request], Coroutine[Any, Any, web.Response]
    ]:
        async def _inner(request: web.Request) -> web.Response:
            auth_header = request.headers.get("Authorization", "")
            if auth_header.split(" ")[1] == "authorized":
                return web.Response(content_type="text/plain")
            return web.Response(status=401)

        return _inner

    def _create_app() -> web.Application:
        app = web.Application()
        app.add_routes(
            [
                web.get("/api/v1/namespaces/mock/pods/whatever", _get_pod),
                web.get(
                    "/api/v1/nodes/whatever:10255/proxy/stats/summary", _stats_summary
                ),
                web.get("/api/v1/nodes/whatever:9400/proxy/metrics", _gpu_metrics),
                web.get(
                    "/api/v1/nodes/unauthorized:9400/proxy/metrics",
                    _unauthorized_gpu_metrics(),
                ),
            ]
        )
        return app

    app = _create_app()
    async with create_local_app_server(app) as address:
        yield address


@pytest.fixture()
def elasticsearch_log_service(
    kube_client: MyKubeClient, es_client: AsyncElasticsearch
) -> ElasticsearchLogsService:
    return ElasticsearchLogsService(kube_client, es_client)


@pytest.fixture()
def s3_logs_metadata_storage(
    s3_client: AioBaseClient, s3_logs_bucket: str
) -> S3LogsMetadataStorage:
    return S3LogsMetadataStorage(s3_client, s3_logs_bucket)


@pytest.fixture()
def s3_logs_metadata_service(
    s3_client: AioBaseClient,
    s3_logs_metadata_storage: S3LogsMetadataStorage,
    kube_config: KubeConfig,
) -> S3LogsMetadataService:
    return S3LogsMetadataService(
        s3_client, s3_logs_metadata_storage, kube_config.namespace
    )


@pytest.fixture()
def s3_log_service(
    kube_client: MyKubeClient,
    s3_client: AioBaseClient,
    s3_logs_metadata_service: S3LogsMetadataService,
) -> S3LogsService:
    return S3LogsService(kube_client, s3_client, s3_logs_metadata_service)


@pytest.fixture()
def loki_log_service(
    kube_client: MyKubeClient, loki_client: LokiClient
) -> LokiLogsService:
    return LokiLogsService(kube_client, loki_client, 60 * 60 * 24)


TOKEN_KEY = aiohttp.web.AppKey("token", dict[str, str])


class TestKubeClientTokenUpdater:
    @pytest.fixture()
    async def kube_app(self) -> aiohttp.web.Application:
        async def _get_nodes(request: aiohttp.web.Request) -> aiohttp.web.Response:
            auth = request.headers["Authorization"]
            token = auth.split()[-1]
            app[TOKEN_KEY]["value"] = token
            return aiohttp.web.json_response({"kind": "NodeList", "items": []})

        app = aiohttp.web.Application()
        app[TOKEN_KEY] = {"value": ""}
        app.router.add_routes([aiohttp.web.get("/api/v1/nodes", _get_nodes)])
        return app

    @pytest.fixture()
    async def kube_server(
        self, kube_app: aiohttp.web.Application, unused_tcp_port_factory: Any
    ) -> AsyncIterator[str]:
        async with create_local_app_server(
            kube_app, port=unused_tcp_port_factory()
        ) as address:
            yield f"http://{address.host}:{address.port}"

    @pytest.fixture()
    def kube_token_path(self) -> Iterator[str]:
        _, path = tempfile.mkstemp()
        Path(path).write_text("token-1")
        yield path
        Path(path).unlink()

    @pytest.fixture()
    async def kube_client(
        self, kube_server: str, kube_token_path: str
    ) -> AsyncIterator[KubeClient]:
        async with KubeClient(
            base_url=kube_server,
            namespace="default",
            auth_type=KubeClientAuthType.TOKEN,
            token_path=kube_token_path,
            token_update_interval_s=1,
        ) as client:
            yield client

    async def test_token_periodically_updated(
        self,
        kube_app: aiohttp.web.Application,
        kube_client: KubeClient,
        kube_token_path: str,
    ) -> None:
        await kube_client.get_nodes()
        assert kube_app[TOKEN_KEY]["value"] == "token-1"

        Path(kube_token_path).write_text("token-2")
        await asyncio.sleep(2)

        await kube_client.get_nodes()
        assert kube_app[TOKEN_KEY]["value"] == "token-2"


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

    @pytest.mark.xfail()
    async def test_create_log_stream_creating(
        self, kube_client: MyKubeClient, job_pod: MyPodDescriptor
    ) -> None:
        await kube_client.create_pod(job_pod.payload)

        async with asyncio.timeout(5.0):
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

    async def test_get_nodes(self, kube_client: MyKubeClient) -> None:
        nodes = await kube_client.get_nodes()
        assert nodes

        nodes = await kube_client.get_nodes(label_selector="kubernetes.io/os=linux")
        assert nodes
        assert all(
            node.metadata.labels.get("kubernetes.io/os") == "linux" for node in nodes
        )

    async def test_get_pods(
        self, kube_client: MyKubeClient, job_pod: MyPodDescriptor
    ) -> None:
        try:
            await kube_client.create_pod(job_pod.payload)

            pods = await kube_client.get_pods()
            assert pods
            assert any(pod.metadata.name == job_pod.name for pod in pods)

            pods = await kube_client.get_pods(label_selector=f"job={job_pod.name}")
            assert len(pods) == 1
            assert pods[0].metadata.name == job_pod.name

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
                pod.status.phase in (PodPhase.PENDING, PodPhase.RUNNING) for pod in pods
            )
        finally:
            await kube_client.delete_pod(job_pod.name)

    @pytest.mark.parametrize(
        ("token", "is_valid"),
        [
            ("authorized", True),
            ("badtoken", False),
        ],
    )
    async def test_get_pod_container_gpu_stats_handles_unauthorized(
        self,
        mock_kubernetes_server: ApiAddress,
        tmp_path: Path,
        token: str,
        is_valid: bool,  # noqa: FBT001
    ) -> None:
        srv = mock_kubernetes_server
        token_path = tmp_path / "token"
        token_path.write_text(token)
        async with KubeClient(
            base_url=f"http://{srv.host}:{srv.port}",
            namespace="mock",
            nvidia_dcgm_node_port=9400,
            auth_type=KubeClientAuthType.TOKEN,
            token="bad",
            token_path=str(token_path),
        ) as client:
            stats = await client.get_pod_container_gpu_stats("unauthorized", "p", "c")
            if is_valid:
                assert stats
            else:
                assert not stats


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
        for line in data.splitlines(keepends=True):
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

            if b"unable to retrieve container logs" in payload:
                pytest.skip("unable to retrieve container logs")
            assert b" Restart\n" in payload
            assert payload.count(b"Restart") == 1
            timestamp = int(payload.split()[0])
            assert timestamp >= orig_timestamp
        finally:
            await kube_client.delete_pod(job_pod.name)

    async def test_elasticsearch_log_reader(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        es_client: AsyncElasticsearch,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        await kube_client.wait_pod_is_terminated(job_pod.name)

        await self._check_kube_logs(
            kube_client=kube_client,
            namespace_name=kube_config.namespace,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            expected_payload=expected_payload,
        )
        await asyncio.sleep(10)
        await kube_client.delete_pod(job_pod.name)
        for timestamps in [False, True]:
            await self._check_es_logs(
                es_client=es_client,
                namespace_name=kube_config.namespace,
                pod_name=job_pod.name,
                container_name=job_pod.name,
                expected_payload=expected_payload,
                timestamps=timestamps,
            )

    async def test_elasticsearch_log_reader_restarted(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        es_client: AsyncElasticsearch,
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
            es_client=es_client,
            namespace_name=kube_config.namespace,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            expected_payload=expected_payload * 2,
        )

    async def test_s3_log_reader(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        s3_client: AioBaseClient,
        s3_logs_metadata_service: S3LogsMetadataService,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        await kube_client.wait_pod_is_terminated(job_pod.name)

        await self._check_kube_logs(
            kube_client=kube_client,
            namespace_name=kube_config.namespace,
            pod_name=job_pod.name,
            container_name=job_pod.name,
            expected_payload=expected_payload,
        )
        await kube_client.delete_pod(job_pod.name)
        for timestamps in [False, True]:
            await self._check_s3_logs(
                s3_client=s3_client,
                metadata_service=s3_logs_metadata_service,
                pod_name=job_pod.name,
                expected_payload=expected_payload,
                timestamps=timestamps,
            )

    async def test_s3_log_reader_restarted(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        s3_client: AioBaseClient,
        s3_logs_metadata_service: S3LogsMetadataService,
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
            s3_client=s3_client,
            metadata_service=s3_logs_metadata_service,
            pod_name=job_pod.name,
            expected_payload=expected_payload * 2,
        )

    async def test_s3_logs_cleanup(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        s3_client: AioBaseClient,
        s3_logs_metadata_service: S3LogsMetadataService,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        await kube_client.wait_pod_is_terminated(job_pod.name)

        await self._check_s3_logs(
            s3_client=s3_client,
            metadata_service=s3_logs_metadata_service,
            pod_name=job_pod.name,
            expected_payload=expected_payload,
        )

        service = S3LogsService(kube_client, s3_client, s3_logs_metadata_service)

        await service.drop_logs(job_pod.name)

        await self._check_s3_logs(
            s3_client=s3_client,
            metadata_service=s3_logs_metadata_service,
            pod_name=job_pod.name,
            expected_payload=b"",
            timeout_s=1,
        )

    async def _check_kube_logs(
        self,
        *,
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
        *,
        es_client: AsyncElasticsearch,
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
            async with asyncio.timeout(timeout_s):
                while True:
                    log_reader = ElasticsearchLogReader(
                        es_client,
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
        except TimeoutError:
            pytest.fail(f"Pod logs did not match. Last payload: {payload!r}")

    async def _check_s3_logs(
        self,
        *,
        s3_client: AioBaseClient,
        metadata_service: S3LogsMetadataService,
        pod_name: str,
        expected_payload: Any,
        timeout_s: float = 120.0,
        interval_s: float = 1.0,
        timestamps: bool = False,
    ) -> None:
        payload = b""
        try:
            async with asyncio.timeout(timeout_s):
                while True:
                    log_reader = S3LogReader(
                        s3_client,
                        metadata_service,
                        pod_name=pod_name,
                        timestamps=timestamps,
                    )
                    payload = await self._consume_log_reader(log_reader)
                    if timestamps:
                        payload = self._remove_timestamps(payload)
                    if payload == expected_payload:
                        return
                    await asyncio.sleep(interval_s)
        except TimeoutError:
            pytest.fail(f"Pod logs did not match. Last payload: {payload!r}")

    async def test_elasticsearch_log_reader_empty(
        self, es_client: AsyncElasticsearch
    ) -> None:
        namespace_name = pod_name = container_name = str(uuid.uuid4())
        log_reader = ElasticsearchLogReader(
            es_client,
            namespace_name=namespace_name,
            pod_name=pod_name,
            container_name=container_name,
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b""

    async def test_s3_log_reader_empty(
        self,
        s3_client: AioBaseClient,
        s3_logs_metadata_service: S3LogsMetadataService,
    ) -> None:
        pod_name = str(uuid.uuid4())
        log_reader = S3LogReader(s3_client, s3_logs_metadata_service, pod_name=pod_name)
        payload = await self._consume_log_reader(log_reader)
        assert payload == b""

    async def _test_get_job_log_reader(
        self,
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

        await asyncio.sleep(10)
        await kube_client.delete_pod(job_pod.name)

        log_reader = factory.get_pod_log_reader(pod_name, archive_delay_s=10.0)
        payload = await self._consume_log_reader(log_reader)
        assert payload == b"hello\n"

    async def test_get_job_elasticsearch_log_reader(
        self,
        kube_client: MyKubeClient,
        elasticsearch_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_get_job_log_reader(
            kube_client, elasticsearch_log_service, job_pod
        )

    async def test_get_job_s3_log_reader(
        self,
        kube_client: MyKubeClient,
        s3_log_service: S3LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_get_job_log_reader(kube_client, s3_log_service, job_pod)

    async def test_get_job_loki_log_reader(
        self,
        kube_client: MyKubeClient,
        loki_log_service: LokiLogsService,
        job_pod: MyPodDescriptor,
        s3_client: AioBaseClient,
        s3_config: S3Config,
    ) -> None:
        await create_s3_logs_bucket(s3_client, s3_config)
        await self._test_get_job_log_reader(kube_client, loki_log_service, job_pod)

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
            async def coro() -> bytes | Exception:
                await asyncio.sleep(delay)
                try:
                    async with asyncio.timeout(timeout_s):
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
        for i, (name, payload) in enumerate(zip(names, payloads, strict=False)):
            print(f"{i}. {name}: {payload!r}")  # noqa: T201

        # All logs are completely either live or archive, no separator.
        for name, payload in zip(names, payloads, strict=False):
            assert payload == b"", name

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

    async def test_loki_empty_log_reader(
        self,
        kube_client: MyKubeClient,
        loki_log_service: LokiLogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_empty_log_reader(kube_client, job_pod, loki_log_service)

    async def _test_merged_log_reader(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        factory: LogsService,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do sleep 1; echo $i; done; sleep 2"'
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
            async def coro() -> bytes | Exception:
                await asyncio.sleep(delay)
                try:
                    async with asyncio.timeout(timeout_s):
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
        for i, (name, payload) in enumerate(zip(names, payloads, strict=False)):
            print(f"111111111 {i}. {name}: {payload!r}")  # noqa: T201

        expected_payload = "".join(f"{i}\n" for i in range(1, 6)).encode()
        # All logs are completely either live or archive, no separator.
        for name, payload in zip(names, payloads, strict=False):
            assert payload == expected_payload, name

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

    async def test_loki_merged_log_reader(
        self,
        kube_client: MyKubeClient,
        loki_log_service: LokiLogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_merged_log_reader(kube_client, job_pod, loki_log_service)

    async def _test_merged_log_reader_restarted(  # noqa: C901
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        factory: LogsService,
    ) -> None:
        command = (
            'bash -c "date +[%T]; for i in {1..5}; do sleep 1; echo $i; done; sleep 4"'
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
            async def coro() -> bytes | Exception:
                await asyncio.sleep(delay)
                try:
                    async with asyncio.timeout(timeout_s):
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
            await kube_client.wait_pod_is_running(job_pod.name)
            for i in range(4):
                run_log_reader(f"restarted 1 [{i}]", delay=i * 2)
            await kube_client.wait_container_is_restarted(job_pod.name, 2)
            await kube_client.wait_pod_is_running(job_pod.name)
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

        payloads: list[bytes] = await asyncio.gather(*tasks)  # type: ignore

        # Output for debugging
        for i, (name, payload) in enumerate(zip(names, payloads, strict=False)):
            print(f"{i}. {name}: {payload!r}")  # noqa: T201
            logger.info("44444444 %d %s %r", i, name, payload)

        expected_payload = "".join(f"{i}\n" for i in range(1, 6)).encode()
        payload0 = payloads[0]

        assert re.sub(rb"\[.*?\]\n", b"", payload0) == expected_payload * 3
        for i, (name, payload) in enumerate(zip(names, payloads, strict=False)):
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

    @pytest.mark.xfail()
    async def test_s3_merged_log_reader_restarted(
        self,
        kube_client: MyKubeClient,
        s3_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_merged_log_reader_restarted(
            kube_client, job_pod, s3_log_service
        )

    async def test_loki_merged_log_reader_restarted(
        self,
        kube_client: MyKubeClient,
        loki_log_service: LokiLogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_merged_log_reader_restarted(
            kube_client, job_pod, loki_log_service
        )

    async def _test_merged_log_reader_restarted_since(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        factory: LogsService,
    ) -> None:
        command = "bash -c 'echo begin; sleep 5; echo end; sleep 2; false'"
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
            logger.info(f"status 1: {status}")  # noqa: G004
            finished1 = status.finished_at
            assert finished1

            await kube_client.wait_container_is_restarted(job_pod.name, 1)
            await kube_client.wait_pod_is_running(job_pod.name)
            status = await kube_client.get_container_status(job_pod.name)
            logger.info(f"status 2: {status}")  # noqa: G004
            started2 = status.started_at
            assert started2
            run_log_reader(since=started2)
            run_log_reader(since=started2 + timedelta(seconds=2))

            await kube_client.wait_pod_is_terminated(job_pod.name)
            status = await kube_client.get_container_status(job_pod.name)
            logger.info(f"status 3: {status}")  # noqa: G004
            assert status.started_at == started2
            finished2 = status.finished_at
            assert finished2

            await kube_client.wait_container_is_restarted(job_pod.name, 2)
            await kube_client.wait_pod_is_running(job_pod.name)
            run_log_reader(since=finished1 - timedelta(seconds=4))
            run_log_reader(since=started2)
            run_log_reader(since=finished2 - timedelta(seconds=4))
            run_log_reader(since=finished2 + timedelta(seconds=2))
            await kube_client.wait_pod_is_terminated(job_pod.name)
            await asyncio.sleep(10)
        finally:
            await kube_client.delete_pod(job_pod.name)
        await kube_client.wait_pod_is_deleted(job_pod.name)
        run_log_reader(since=finished1 - timedelta(seconds=4))
        run_log_reader(since=started2)
        run_log_reader(since=finished2 - timedelta(seconds=4))
        run_log_reader(since=finished2 + timedelta(seconds=2))

        payloads = await asyncio.gather(*tasks)

        # Output for debugging
        for i, (since, payload) in enumerate(zip(starts, payloads, strict=False)):
            print(f"{i}. [{since:%T}] {payload!r}")  # noqa: T201
            logger.info("333333333333 %d [%s] %r", i, since, payload)
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

    async def test_loki_merged_log_reader_restarted_since(
        self,
        kube_client: MyKubeClient,
        loki_log_service: LokiLogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_merged_log_reader_restarted_since(
            kube_client, job_pod, loki_log_service
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

    async def test_s3_large_log_reader(
        self,
        kube_client: MyKubeClient,
        s3_log_service: LogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_large_log_reader(kube_client, job_pod, s3_log_service)

    async def test_loki_large_log_reader(
        self,
        kube_client: MyKubeClient,
        loki_log_service: LokiLogsService,
        job_pod: MyPodDescriptor,
    ) -> None:
        await self._test_large_log_reader(kube_client, job_pod, loki_log_service)

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

    async def test_s3_log_reader_running_pod_compacted(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        s3_client: AioBaseClient,
        s3_logs_metadata_service: S3LogsMetadataService,
        s3_log_service: S3LogsService,
    ) -> None:
        command = 'bash -c "for i in {1..10}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 11)) + "\n").encode()
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        await kube_client.wait_pod_is_running(job_pod.name)
        await asyncio.sleep(5)

        await s3_log_service.compact_one(job_pod.name)

        await kube_client.wait_pod_is_terminated(job_pod.name)
        await asyncio.sleep(5)
        await kube_client.delete_pod(job_pod.name)

        for timestamps in [False, True]:
            await self._check_s3_logs(
                s3_client=s3_client,
                metadata_service=s3_logs_metadata_service,
                pod_name=job_pod.name,
                expected_payload=expected_payload,
                timestamps=timestamps,
            )

    async def test_s3_log_reader_terminated_pod_compacted(
        self,
        kube_client: MyKubeClient,
        job_pod: MyPodDescriptor,
        s3_client: AioBaseClient,
        s3_logs_metadata_service: S3LogsMetadataService,
        s3_log_service: S3LogsService,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
        job_pod.set_command(command)
        await kube_client.create_pod(job_pod.payload)
        await kube_client.wait_pod_is_terminated(job_pod.name)
        await asyncio.sleep(5)
        await kube_client.delete_pod(job_pod.name)

        await s3_log_service.compact_one(job_pod.name)

        for timestamps in [False, True]:
            await self._check_s3_logs(
                s3_client=s3_client,
                metadata_service=s3_logs_metadata_service,
                pod_name=job_pod.name,
                expected_payload=expected_payload,
                timestamps=timestamps,
            )


@pytest.fixture()
async def write_lines_to_s3(
    s3_client: AioBaseClient, s3_logs_bucket: str
) -> Callable[..., Awaitable[None]]:
    async def _put(key: str, *lines: str, compress: bool = False) -> None:
        body = "\n".join(lines).encode()
        if compress:
            compress_obj = zlib.compressobj(wbits=ZLIB_WBITS)
            body = compress_obj.compress(body) + compress_obj.flush()
        kwargs = {
            "Bucket": s3_logs_bucket,
            "Key": key,
            "Body": body,
        }
        if compress:
            kwargs["ContentType"] = "application/x-gzip"
        await s3_client.put_object(**kwargs)

    return _put


class TestS3FileReader:
    async def test_iter_lines__without_compression(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        key = f"tests/{uuid.uuid4()}"
        reader = S3FileReader(s3_client, s3_logs_bucket, key)

        await write_lines_to_s3(key, "1", "2", "3")

        result = [line async for line in reader.iter_lines()]

        assert result == [b"1", b"2", b"3"]

    async def test_iter_lines__with_compression(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        key = f"tests/{uuid.uuid4()}"
        # chunk_size = 1 will for DecompressObj to periodically
        # keep decompressed data in internal buffer and not return
        # it to the caller.
        reader = S3FileReader(s3_client, s3_logs_bucket, key, chunk_size=1)

        await write_lines_to_s3(key, "1", "2", "3", compress=True)

        result = [line async for line in reader.iter_lines()]

        assert result == [b"1", b"2", b"3"]


class TestS3LogsMetadataStorage:
    @pytest.mark.parametrize("cache_metadata", [(True,), (False,)])
    async def test_get(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        cache_metadata: bool,  # noqa: FBT001
    ) -> None:
        pod_name = f"test-{uuid.uuid4()}"
        metadata = S3LogsMetadata(
            last_compaction_time=datetime(2023, 1, 2), last_merged_key="key"
        )
        storage = S3LogsMetadataStorage(
            s3_client, s3_logs_bucket, cache_metadata=cache_metadata
        )

        await storage.put(pod_name, metadata)
        result = await storage.get(pod_name)

        assert result == metadata

    @pytest.mark.parametrize("cache_metadata", [(True,), (False,)])
    async def test_get__no_key(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        cache_metadata: bool,  # noqa: FBT001
    ) -> None:
        pod_name = f"test-{uuid.uuid4()}"
        storage = S3LogsMetadataStorage(
            s3_client, s3_logs_bucket, cache_metadata=cache_metadata
        )

        result = await storage.get(pod_name)

        assert result == S3LogsMetadata()

    @pytest.mark.parametrize("cache_metadata", [(True,), (False,)])
    async def test_put(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        cache_metadata: bool,  # noqa: FBT001
    ) -> None:
        pod_name = f"test-{uuid.uuid4()}"
        metadata = S3LogsMetadata(
            last_compaction_time=datetime(2023, 1, 2), last_merged_key="key"
        )
        storage = S3LogsMetadataStorage(
            s3_client, s3_logs_bucket, cache_metadata=cache_metadata
        )

        await storage.put(pod_name, metadata)
        result = await storage.get(pod_name)

        assert result == metadata

        resp = await s3_client.get_object(
            Bucket=s3_logs_bucket, Key=storage.get_metadata_key(pod_name)
        )
        resp_body = await resp["Body"].read()

        assert json.loads(resp_body) == metadata.to_primitive()


def _create_raw_log_key_prefix(pod_name: str) -> str:
    return S3LogsMetadataService.RAW_LOG_KEY_PREFIX_FORMAT.format(
        namespace_name="default", pod_name=pod_name, container_name=pod_name
    )


def _create_merged_key_prefix(pod_name: str) -> str:
    return f"{S3LogRecordsWriter.LOGS_KEY_PREFIX}/{pod_name}"


def _create_log_file(key: str) -> S3LogFile:
    return S3LogFile(
        key=key,
        records_count=1,
        size=2,
        first_record_time=datetime(2023, 1, 1, 0, 0),
        last_record_time=datetime(2023, 1, 1, 0, 0, 30),
    )


class TestS3LogsMetadataService:
    async def test_get_log_keys__raw_keys_not_deleted_after_merge(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        merged_log_key_prefix = _create_merged_key_prefix(pod_name)
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        metadata = S3LogsMetadata(
            log_files=[_create_log_file(f"{merged_log_key_prefix}/202301010000_0.gz")],
            last_merged_key=f"{raw_log_key_prefix}/202301010000_0.gz",
        )
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_1.gz", '{"log":"4"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_0.gz", '{"log":"3"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_1.gz", '{"log":"2"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_0.gz", '{"log":"1"}'
        )

        keys = await s3_logs_metadata_service.get_log_keys(pod_name)

        assert keys == [
            f"{merged_log_key_prefix}/202301010000_0.gz",
            f"{raw_log_key_prefix}/202301010000_1.gz",
            f"{raw_log_key_prefix}/202301010001_0.gz",
            f"{raw_log_key_prefix}/202301010001_1.gz",
        ]

    async def test_get_log_keys__raw_keys_deleted_after_merge(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        merged_log_key_prefix = _create_merged_key_prefix(pod_name)
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        metadata = S3LogsMetadata(
            log_files=[_create_log_file(f"{merged_log_key_prefix}/202301010000_0.gz")],
            last_merged_key=f"{raw_log_key_prefix}/202301010000_0.gz",
        )
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_1.gz", '{"log":"4"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_0.gz", '{"log":"3"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_1.gz", '{"log":"2"}'
        )

        keys = await s3_logs_metadata_service.get_log_keys(pod_name)

        assert keys == [
            f"{merged_log_key_prefix}/202301010000_0.gz",
            f"{raw_log_key_prefix}/202301010000_1.gz",
            f"{raw_log_key_prefix}/202301010001_0.gz",
            f"{raw_log_key_prefix}/202301010001_1.gz",
        ]

    async def test_get_log_keys__no_raw_keys(
        self, s3_logs_metadata_service: S3LogsMetadataService
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_merged_key_prefix(pod_name)
        metadata = S3LogsMetadata(
            log_files=[_create_log_file(f"{raw_log_key_prefix}/202301010000_0.gz")],
            last_merged_key=f"{raw_log_key_prefix}/202301010000_0.gz",
        )
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)

        keys = await s3_logs_metadata_service.get_log_keys(pod_name)

        assert keys == [f"{raw_log_key_prefix}/202301010000_0.gz"]

    async def test_get_log_keys_since(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        merged_log_key_prefix = _create_merged_key_prefix(pod_name)
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        metadata = S3LogsMetadata(
            log_files=[_create_log_file(f"{merged_log_key_prefix}/202301010000_0.gz")],
            last_merged_key=f"{raw_log_key_prefix}/202301010000_0.gz",
        )
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_1.gz", '{"log":"4"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_0.gz", '{"log":"3"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_1.gz", '{"log":"2"}'
        )

        all_keys = [
            f"{merged_log_key_prefix}/202301010000_0.gz",
            f"{raw_log_key_prefix}/202301010000_1.gz",
            f"{raw_log_key_prefix}/202301010001_0.gz",
            f"{raw_log_key_prefix}/202301010001_1.gz",
        ]

        keys = await s3_logs_metadata_service.get_log_keys(
            pod_name, since=datetime(2023, 1, 1, 0, 0, 30)
        )
        assert keys == all_keys

        keys = await s3_logs_metadata_service.get_log_keys(
            pod_name, since=datetime(2023, 1, 1, 0, 0, 31)
        )
        assert keys == all_keys[1:]

        keys = await s3_logs_metadata_service.get_log_keys(
            pod_name, since=datetime(2023, 1, 1, 0, 1)
        )
        assert keys == all_keys[2:]

    async def test_get_raw_log_keys(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_1.gz", '{"log":"3"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_0.gz", '{"log":"2"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_0.gz", '{"log":"1"}'
        )

        keys = await s3_logs_metadata_service.get_raw_log_keys(pod_name)

        assert keys == [
            f"{raw_log_key_prefix}/202301010000_0.gz",
            f"{raw_log_key_prefix}/202301010001_0.gz",
            f"{raw_log_key_prefix}/202301010001_1.gz",
        ]

    async def test_get_raw_log_keys_since(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_1.gz", '{"log":"3"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_0.gz", '{"log":"2"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_0.gz", '{"log":"1"}'
        )

        all_keys = [
            f"{raw_log_key_prefix}/202301010000_0.gz",
            f"{raw_log_key_prefix}/202301010001_0.gz",
            f"{raw_log_key_prefix}/202301010001_1.gz",
        ]

        keys = await s3_logs_metadata_service.get_raw_log_keys(
            pod_name, since=datetime(2023, 1, 1, 0, 0)
        )
        assert keys == all_keys

        keys = await s3_logs_metadata_service.get_raw_log_keys(
            pod_name, since=datetime(2023, 1, 1, 0, 1)
        )
        assert keys == all_keys[1:]

        keys = await s3_logs_metadata_service.get_raw_log_keys(
            pod_name, since=datetime(2023, 1, 1, 0, 2)
        )
        assert keys == []

    async def test_get_pods_compact_queue__old_logs(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        metadata = S3LogsMetadata(
            last_compaction_time=datetime.now(UTC) - timedelta(hours=1)
        )
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_0.gz", '{"log":"1"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_1.gz", '{"log":"2"}'
        )

        queue = await s3_logs_metadata_service.get_pods_compact_queue(
            compact_interval=60
        )

        assert pod_name in queue

    async def test_get_pods_compact_queue__recent_logs(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        metadata = S3LogsMetadata(last_compaction_time=datetime.now(UTC))
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_0.gz", '{"log":"1"}'
        )

        queue = await s3_logs_metadata_service.get_pods_compact_queue(
            compact_interval=60
        )

        # Recent logs should wait compaction_interval
        assert pod_name not in queue

    async def test_add_pod_to_cleanup_queue(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_logs_metadata_service: S3LogsMetadataService,
    ) -> None:
        pod_name = str(uuid.uuid4())
        await s3_logs_metadata_service.add_pod_to_cleanup_queue(pod_name)

        await s3_client.get_object(
            Bucket=s3_logs_bucket,
            Key=f"{S3LogsMetadataService.CLEANUP_KEY_PREFIX}/{pod_name}",
        )

    async def test_remove_pod_from_cleanup_queue(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_logs_metadata_service: S3LogsMetadataService,
    ) -> None:
        pod_name = str(uuid.uuid4())
        await s3_logs_metadata_service.add_pod_to_cleanup_queue(pod_name)
        await s3_client.get_object(
            Bucket=s3_logs_bucket,
            Key=f"{S3LogsMetadataService.CLEANUP_KEY_PREFIX}/{pod_name}",
        )
        await s3_logs_metadata_service.remove_pod_from_cleanup_queue(pod_name)

        with pytest.raises(botocore.exceptions.ClientError):
            await s3_client.get_object(
                Bucket=s3_logs_bucket,
                Key=f"{S3LogsMetadataService.CLEANUP_KEY_PREFIX}/{pod_name}",
            )

    async def test_add_pod_to_cleanup_queue__old_logs(
        self, s3_logs_metadata_service: S3LogsMetadataService
    ) -> None:
        pod_name = str(uuid.uuid4())
        metadata = S3LogsMetadata(
            last_compaction_time=datetime.now(UTC) - timedelta(hours=1)
        )
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await s3_logs_metadata_service.add_pod_to_cleanup_queue(pod_name)

        queue = await s3_logs_metadata_service.get_pods_cleanup_queue(
            cleanup_interval=60
        )

        assert pod_name in queue

    async def test_add_pod_to_cleanup_queue__recent_logs(
        self, s3_logs_metadata_service: S3LogsMetadataService
    ) -> None:
        pod_name = str(uuid.uuid4())
        metadata = S3LogsMetadata(last_compaction_time=datetime.now(UTC))
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await s3_logs_metadata_service.add_pod_to_cleanup_queue(pod_name)

        queue = await s3_logs_metadata_service.get_pods_cleanup_queue()

        assert pod_name not in queue


class TestS3LogRecordReader:
    @pytest.fixture()
    def reader(
        self, s3_client: AioBaseClient, s3_logs_bucket: str
    ) -> S3LogRecordsReader:
        return S3LogRecordsReader(s3_client, s3_logs_bucket)

    async def test_iter_records__raw_keys(
        self,
        reader: S3LogRecordsReader,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
        ]
        await write_lines_to_s3(
            pod_keys[0], '{"time":"2023-01-01T12:34:56.123456","log":"1"}'
        )
        await write_lines_to_s3(
            pod_keys[1], '{"time":"2023-01-01T12:34:57.123456","log":"2"}'
        )

        result = [r async for r in reader.iter_records(pod_keys)]

        assert result == [
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 56, 123456, UTC),
                time_str="2023-01-01T12:34:56.123456",
                message="1",
                container_id="c1",
            ),
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 57, 123456, UTC),
                time_str="2023-01-01T12:34:57.123456",
                message="2",
                container_id="c1",
            ),
        ]

    async def test_iter_records__merged_keys(
        self,
        reader: S3LogRecordsReader,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_merged_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}/c1/202301011234_0.gz",
            f"{raw_log_key_prefix}/c1/202301011234_1.gz",
        ]
        await write_lines_to_s3(
            pod_keys[0], '{"time":"2023-01-01T12:34:56.123456","log":"1"}'
        )
        await write_lines_to_s3(
            pod_keys[1], '{"time":"2023-01-01T12:34:57.123456","log":"2"}'
        )

        result = [r async for r in reader.iter_records(pod_keys)]

        assert result == [
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 56, 123456, UTC),
                time_str="2023-01-01T12:34:56.123456",
                message="1",
                container_id="c1",
            ),
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 57, 123456, UTC),
                time_str="2023-01-01T12:34:57.123456",
                message="2",
                container_id="c1",
            ),
        ]

    async def test_iter_records__fallback_to_key_time(
        self,
        reader: S3LogRecordsReader,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [f"{raw_log_key_prefix}-c1.log/202301011234_0.gz"]
        await write_lines_to_s3(pod_keys[0], '{"log":"1"}')

        result = [r async for r in reader.iter_records(pod_keys)]

        assert result == [
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, tzinfo=UTC),
                time_str="2023-01-01T12:34:00.000000",
                message="1",
                container_id="c1",
            )
        ]

    async def test_iter_records__fallback_to_last_record_time(
        self,
        reader: S3LogRecordsReader,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
        ]
        await write_lines_to_s3(
            pod_keys[0],
            '{"time":"2023-01-01T12:34:56.123456","log":"1"}',
            '{"log":"2"}',
        )
        await write_lines_to_s3(pod_keys[1], '{"log":"3"}')

        result = [r async for r in reader.iter_records(pod_keys)]

        assert result == [
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 56, 123456, UTC),
                time_str="2023-01-01T12:34:56.123456",
                message="1",
                container_id="c1",
            ),
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 56, 123456, UTC),
                time_str="2023-01-01T12:34:56.123456",
                message="2",
                container_id="c1",
            ),
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 56, 123456, UTC),
                time_str="2023-01-01T12:34:56.123456",
                message="3",
                container_id="c1",
            ),
        ]


class TestS3LogRecordWriter:
    @pytest.fixture()
    def records(self) -> list[S3LogRecord]:
        return [
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 56, 123456, UTC),
                time_str="2023-01-01T12:34:56.123456",
                message="1",
                container_id="c1",
            ),
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 57, 123456, UTC),
                time_str="2023-01-01T12:34:57.123456",
                message="2",
                container_id="c1",
            ),
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 58, 123456, UTC),
                time_str="2023-01-01T12:34:58.123456",
                message="2",
                container_id="c1",
                stream="stderr",
            ),
        ]

    async def _assert_records_written(
        self, writer: S3LogRecordsWriter, expected_records: Sequence[S3LogRecord]
    ) -> None:
        reader = S3LogRecordsReader(writer._s3_client, writer._bucket_name)
        keys = [f.key for f in writer.get_output_files()]
        result = [r async for r in reader.iter_records(keys)]
        assert result == expected_records

    async def test_write__single_file(
        self, s3_client: AioBaseClient, s3_logs_bucket: str, records: list[S3LogRecord]
    ) -> None:
        async with S3LogRecordsWriter(
            s3_client, s3_logs_bucket, str(uuid.uuid4())
        ) as writer:
            for record in records:
                await writer.write(record)

        assert writer.get_output_files() == [
            S3LogFile(
                key=mock.ANY,
                records_count=3,
                size=162,
                first_record_time=records[0].time,
                last_record_time=records[2].time,
            )
        ]

        await self._assert_records_written(writer, records)

    async def test_write__multiple_files(
        self, s3_client: AioBaseClient, s3_logs_bucket: str, records: list[S3LogRecord]
    ) -> None:
        async with S3LogRecordsWriter(
            s3_client, s3_logs_bucket, str(uuid.uuid4()), size_limit=110
        ) as writer:
            for record in records:
                await writer.write(record)

        assert writer.get_output_files() == [
            S3LogFile(
                key=mock.ANY,
                records_count=2,
                size=96,
                first_record_time=records[0].time,
                last_record_time=records[1].time,
            ),
            S3LogFile(
                key=mock.ANY,
                records_count=1,
                size=66,
                first_record_time=records[2].time,
                last_record_time=records[2].time,
            ),
        ]

        await self._assert_records_written(writer, records)

    async def test_write__long_record(
        self, s3_client: AioBaseClient, s3_logs_bucket: str, records: list[S3LogRecord]
    ) -> None:
        async with S3LogRecordsWriter(
            s3_client, s3_logs_bucket, str(uuid.uuid4()), size_limit=1
        ) as writer:
            for record in records:
                await writer.write(record)

        assert writer.get_output_files() == [
            S3LogFile(
                key=mock.ANY,
                records_count=1,
                size=48,
                first_record_time=records[0].time,
                last_record_time=records[0].time,
            ),
            S3LogFile(
                key=mock.ANY,
                records_count=1,
                size=48,
                first_record_time=records[1].time,
                last_record_time=records[1].time,
            ),
            S3LogFile(
                key=mock.ANY,
                records_count=1,
                size=66,
                first_record_time=records[2].time,
                last_record_time=records[2].time,
            ),
        ]

        await self._assert_records_written(writer, records)

    async def test_write__shared_buffer(
        self, s3_client: AioBaseClient, s3_logs_bucket: str, records: list[S3LogRecord]
    ) -> None:
        buffer = BytesIO()

        async with S3LogRecordsWriter(
            s3_client, s3_logs_bucket, str(uuid.uuid4()), buffer=buffer
        ) as writer:
            for record in records:
                await writer.write(record)

        async with S3LogRecordsWriter(
            s3_client, s3_logs_bucket, str(uuid.uuid4()), buffer=buffer
        ) as writer:
            for record in records:
                await writer.write(record)

        assert writer.get_output_files() == [
            S3LogFile(
                key=mock.ANY,
                records_count=3,
                size=162,
                first_record_time=records[0].time,
                last_record_time=records[2].time,
            ),
        ]

        await self._assert_records_written(writer, records)


class TestS3LogsService:
    @pytest.fixture()
    def assert_records_written(
        self, s3_client: AioBaseClient, s3_logs_bucket: str
    ) -> Callable[..., Awaitable[None]]:
        async def _assert(keys: Iterable[str], expected_records: Sequence[str]) -> None:
            result = []
            for key in keys:
                reader = S3FileReader(s3_client, s3_logs_bucket, key)
                result.extend([line.decode() async for line in reader.iter_lines()])
            assert result == expected_records

        return _assert

    async def test_compact_one(
        self,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
        assert_records_written: Callable[..., Awaitable[None]],
    ) -> None:
        now = datetime.now(UTC)
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
        ]
        records = [
            '{"time":"2023-01-01T12:34:56.123456","log":"1"}',
            '{"time":"2023-01-01T12:34:57.123456","log":"2"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await write_lines_to_s3(pod_keys[1], records[1])
        await s3_log_service.compact_one(pod_name)

        metadata = await s3_logs_metadata_service.get_metadata(pod_name)
        assert metadata.last_merged_key == pod_keys[1]
        assert metadata.last_compaction_time
        assert metadata.last_compaction_time >= now
        await assert_records_written(metadata.get_log_keys(), records)

        # merged keys should not be deleted after merge
        raw_keys = await s3_logs_metadata_service.get_raw_log_keys(pod_name)
        assert raw_keys == pod_keys

        # pod should be added to cleanup queue every time logs are merged
        cleanup_queue = await s3_logs_metadata_service.get_pods_cleanup_queue(
            cleanup_interval=0
        )
        assert pod_name in cleanup_queue

    async def test_compact_one__resume_write_to_last_file(
        self,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
        assert_records_written: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
        ]
        records = [
            '{"time":"2023-01-01T12:34:56.123456","log":"1"}',
            '{"time":"2023-01-01T12:34:57.123456","log":"2"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await s3_log_service.compact_one(pod_name)

        metadata = await s3_logs_metadata_service.get_metadata(pod_name)
        last_merged_key = metadata.get_log_keys()[0]
        assert len(metadata.get_log_keys()) == 1
        await assert_records_written(metadata.get_log_keys(), [records[0]])

        await write_lines_to_s3(pod_keys[1], records[1])
        await s3_log_service.compact_one(pod_name)

        metadata = await s3_logs_metadata_service.get_metadata(pod_name)
        assert len(metadata.get_log_keys()) == 1
        assert last_merged_key != metadata.get_log_keys()[0]
        await assert_records_written(metadata.get_log_keys(), records)

    async def test_compact_one__merged_keys_deleted(
        self,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
        ]
        records = [
            '{"time": "2023-01-01T12:34:56.123456", "log": "1"}',
            '{"time": "2023-01-01T12:34:57.123456", "log": "2"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await s3_log_service.compact_one(pod_name)

        await write_lines_to_s3(pod_keys[1], records[1])
        await s3_log_service.compact_one(pod_name)

        raw_keys = await s3_logs_metadata_service.get_raw_log_keys(pod_name)
        assert raw_keys == pod_keys[1:]

        await s3_log_service.compact_one(pod_name)

        raw_keys = await s3_logs_metadata_service.get_raw_log_keys(pod_name)
        assert raw_keys == []

    async def test_compact_one__orphaned_keys_deleted(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_2.gz",
        ]
        records = [
            '{"time": "2023-01-01T12:34:56.123456", "log": "1"}',
            '{"time": "2023-01-01T12:34:57.123456", "log": "2"}',
            '{"time": "2023-01-01T12:34:58.123456", "log": "3"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await s3_log_service.compact_one(pod_name)

        metadata = await s3_logs_metadata_service.get_metadata(pod_name)
        orphaned_key = metadata.get_log_keys()[0]

        # Current log file has space for more log records.
        # After merge its content will be copied to a new log file
        # and it will become orphaned.
        await write_lines_to_s3(pod_keys[1], records[1])
        await s3_log_service.compact_one(pod_name)

        await write_lines_to_s3(pod_keys[2], records[2])
        await s3_log_service.compact_one(pod_name)

        with pytest.raises(botocore.exceptions.ClientError):
            await s3_client.get_object(Bucket=s3_logs_bucket, Key=orphaned_key)

    async def test_cleanup_one__merged_keys_deleted(
        self,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
        ]
        records = [
            '{"time": "2023-01-01T12:34:56.123456", "log": "1"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await s3_log_service.compact_one(pod_name)

        raw_keys = await s3_logs_metadata_service.get_raw_log_keys(pod_name)
        assert raw_keys

        await s3_log_service.cleanup_one(pod_name)

        raw_keys = await s3_logs_metadata_service.get_raw_log_keys(pod_name)
        assert raw_keys == []

        queue = await s3_logs_metadata_service.get_pods_cleanup_queue(
            cleanup_interval=0
        )
        assert pod_name not in queue

    async def test_cleanup_one__orphaned_keys_deleted(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
        ]
        records = [
            '{"time": "2023-01-01T12:34:56.123456", "log": "1"}',
            '{"time": "2023-01-01T12:34:57.123456", "log": "2"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await s3_log_service.compact_one(pod_name)

        metadata = await s3_logs_metadata_service.get_metadata(pod_name)
        orphaned_key = metadata.get_log_keys()[0]

        # Create orphaned file
        await write_lines_to_s3(pod_keys[1], records[1])
        await s3_log_service.compact_one(pod_name)

        # Orphaned should still exist
        await s3_client.get_object(Bucket=s3_logs_bucket, Key=orphaned_key)

        await s3_log_service.cleanup_one(pod_name)

        with pytest.raises(botocore.exceptions.ClientError):
            await s3_client.get_object(Bucket=s3_logs_bucket, Key=orphaned_key)

    async def test_compact_all(
        self,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
        ]
        records = [
            '{"time": "2023-01-01T12:34:56.123456", "log": "1"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await s3_log_service.compact_all(pod_names=[pod_name])

        queue = await s3_logs_metadata_service.get_pods_cleanup_queue(
            cleanup_interval=0
        )
        assert pod_name in queue

        await s3_log_service.compact_all(cleanup_interval=0, pod_names=[pod_name])

        queue = await s3_logs_metadata_service.get_pods_cleanup_queue(
            cleanup_interval=0
        )
        assert pod_name not in queue
