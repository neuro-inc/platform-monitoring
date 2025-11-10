from __future__ import annotations

import asyncio
import logging
import re
import shlex
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from aiobotocore.client import AioBaseClient
from apolo_kube_client import (
    KubeClientException,
    KubeClientProxy,
    KubeClientSelector,
    V1Container,
    V1HostPathVolumeSource,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
    V1Volume,
)
from elasticsearch import AsyncElasticsearch

from platform_monitoring.api import create_s3_logs_bucket
from platform_monitoring.config import S3Config
from platform_monitoring.kube_client import (
    get_container_status,
    wait_pod_is_running,
)
from platform_monitoring.logs import (
    ElasticsearchLogReader,
    ElasticsearchLogsService,
    LogsService,
    LokiLogsService,
    PodContainerLogReader,
    S3LogReader,
    S3LogsMetadataService,
    S3LogsMetadataStorage,
    S3LogsService,
    get_first_log_entry_time as kube_first_log_time,
)
from platform_monitoring.loki_client import LokiClient
from platform_monitoring.utils import parse_date
from tests.integration.conftest_kube import (
    wait_container_is_restarted,
    wait_pod_is_deleted,
    wait_pod_is_terminated,
)


logger = logging.getLogger(__name__)


@pytest.fixture
def job_pod() -> V1Pod:
    job_id = f"job-{uuid4()}"
    return V1Pod(
        metadata=V1ObjectMeta(
            name=job_id,
            labels={"job": job_id},
        ),
        spec=V1PodSpec(
            tolerations=[],
            image_pull_secrets=[],
            restart_policy="Never",
            volumes=[
                V1Volume(
                    name="storage",
                    host_path=V1HostPathVolumeSource(path="/tmp", type="Directory"),
                )
            ],
            containers=[
                V1Container(
                    name=job_id,
                    image="ubuntu:20.10",
                    env=[],
                    volume_mounts=[],
                    termination_message_policy="FallbackToLogsOnError",
                    args=[
                        "true",
                    ],
                    resources=V1ResourceRequirements(
                        limits={"cpu": "100m", "memory": "128Mi"}
                    ),
                )
            ],
        ),
    )


@pytest.fixture
def org_name() -> str:
    return uuid4().hex


@pytest.fixture
def project_name() -> str:
    return uuid4().hex


@pytest.fixture
async def elasticsearch_log_service(
    kube_client_selector: KubeClientSelector,
    es_client: AsyncElasticsearch,
) -> ElasticsearchLogsService:
    return ElasticsearchLogsService(kube_client_selector, es_client)


@pytest.fixture
def loki_log_service(
    kube_client_selector: KubeClientSelector, loki_client: LokiClient
) -> LokiLogsService:
    return LokiLogsService(kube_client_selector, loki_client, 60 * 60 * 24)


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

    @pytest.fixture
    async def kube_client_with_reader(
        self,
        kube_client_selector: KubeClientSelector,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> AsyncIterator[tuple[KubeClientProxy, PodContainerLogReader]]:
        pod_name = job_pod.metadata.name
        assert job_pod.spec
        assert pod_name
        async with kube_client_selector.get_client(
            org_name=org_name,
            project_name=project_name,
        ) as kube_client:
            log_reader = PodContainerLogReader(
                kube_client_selector=kube_client_selector,
                pod_name=pod_name,
                container_name=job_pod.spec.containers[0].name,
                org_name=org_name,
                project_name=project_name,
            )
            yield kube_client, log_reader

    async def test_read_instantly_succeeded(
        self,
        kube_client_with_reader: tuple[KubeClientProxy, PodContainerLogReader],
        job_pod: V1Pod,
        org_name: str,
        project_name: str,
    ) -> None:
        assert job_pod.spec
        assert job_pod.metadata.name
        kube_client, log_reader = kube_client_with_reader
        await kube_client.core_v1.pod.create(job_pod)
        payload = await self._consume_log_reader(log_reader)
        assert payload == b""
        await kube_client.core_v1.pod.delete(job_pod.metadata.name)

    async def test_read_instantly_failed(
        self,
        kube_client_with_reader: tuple[KubeClientProxy, PodContainerLogReader],
        job_pod: V1Pod,
    ) -> None:
        assert job_pod.spec
        assert job_pod.metadata.name
        kube_client, log_reader = kube_client_with_reader
        try:
            # Run a shell snippet that prints and exits non-zero.
            # Ensure we override default args (['true']) so they don't interfere.
            job_pod.spec.containers[0].command = [
                "/bin/bash",
                "-c",
                "echo -n Failure!; false",
            ]
            job_pod.spec.containers[0].args = []
            await kube_client.core_v1.pod.create(job_pod)
            payload = await self._consume_log_reader(log_reader)
            assert payload == b"Failure!"
        finally:
            await kube_client.core_v1.pod.delete(job_pod.metadata.name)

    async def test_read_succeeded(
        self,
        kube_client_with_reader: tuple[KubeClientProxy, PodContainerLogReader],
        job_pod: V1Pod,
    ) -> None:
        assert job_pod.spec
        assert job_pod.metadata.name
        kube_client, log_reader = kube_client_with_reader
        try:
            job_pod.spec.containers[0].command = [
                "/bin/bash",
                "-c",
                "for i in {1..5}; do echo $i; sleep 1; done",
            ]
            job_pod.spec.containers[0].args = []
            await kube_client.core_v1.pod.create(job_pod)
            payload = await self._consume_log_reader(log_reader)
            expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
            assert payload == expected_payload.encode()
        finally:
            await kube_client.core_v1.pod.delete(job_pod.metadata.name)

    async def test_read_with_timestamps(
        self,
        kube_client_selector: KubeClientSelector,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name,
            project_name=project_name,
        ) as kube_client:
            try:
                job_pod.spec.containers[0].command = [
                    "/bin/bash",
                    "-c",
                    "for i in {1..5}; do echo $i; sleep 1; done",
                ]
                job_pod.spec.containers[0].args = []
                await kube_client.core_v1.pod.create(job_pod)
                log_reader = PodContainerLogReader(
                    kube_client_selector=kube_client_selector,
                    pod_name=pod_name,
                    container_name=job_pod.spec.containers[0].name,
                    org_name=org_name,
                    project_name=project_name,
                    timestamps=True,
                )
                payload = await self._consume_log_reader(log_reader)
                payload = self._remove_timestamps(payload)
                expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
                assert payload == expected_payload.encode()
            finally:
                await kube_client.core_v1.pod.delete(pod_name)

    async def test_read_since(
        self,
        kube_client_selector: KubeClientSelector,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name,
            project_name=project_name,
        ) as kube_client:
            try:
                job_pod.spec.containers[0].command = [
                    "/bin/bash",
                    "-c",
                    "echo first; sleep 3; echo second; sleep 3; echo third",
                ]
                job_pod.spec.containers[0].args = []
                await kube_client.core_v1.pod.create(job_pod)

                log_reader = PodContainerLogReader(
                    kube_client_selector=kube_client_selector,
                    pod_name=pod_name,
                    container_name=job_pod.spec.containers[0].name,
                    org_name=org_name,
                    project_name=project_name,
                    timestamps=True,
                )
                payload = await self._consume_log_reader(log_reader)
                assert self._remove_timestamps(payload) == b"first\nsecond\nthird\n"
                lines = payload.splitlines()
                second_ts = parse_date(lines[1].split()[0].decode())

                log_reader = PodContainerLogReader(
                    kube_client_selector=kube_client_selector,
                    pod_name=pod_name,
                    container_name=job_pod.spec.containers[0].name,
                    org_name=org_name,
                    project_name=project_name,
                    since=second_ts,
                )
                payload = await self._consume_log_reader(log_reader)
                assert payload == b"second\nthird\n"

                log_reader = PodContainerLogReader(
                    kube_client_selector=kube_client_selector,
                    pod_name=pod_name,
                    container_name=job_pod.spec.containers[0].name,
                    org_name=org_name,
                    project_name=project_name,
                    since=second_ts + timedelta(seconds=1),
                )
                payload = await self._consume_log_reader(log_reader)
                assert payload == b"third\n"
            finally:
                await kube_client.core_v1.pod.delete(pod_name)

    async def test_read_cancelled(
        self,
        kube_client_selector: KubeClientSelector,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name,
            project_name=project_name,
        ) as kube_client:
            job_pod.spec.containers[0].command = [
                "/bin/bash",
                "-c",
                "for i in {1..60}; do echo $i; sleep 1; done",
            ]
            job_pod.spec.containers[0].args = []
            await kube_client.core_v1.pod.create(job_pod)
            try:
                await wait_pod_is_running(kube_client, pod_name, timeout_s=60.0)
                log_reader = PodContainerLogReader(
                    kube_client_selector=kube_client_selector,
                    pod_name=pod_name,
                    container_name=job_pod.spec.containers[0].name,
                    org_name=org_name,
                    project_name=project_name,
                )
                task = asyncio.ensure_future(self._consume_log_reader(log_reader))
                await asyncio.sleep(10)
                task.cancel()
                payload = await task
                expected_prefix = "\n".join(str(i) for i in range(1, 6))
                assert payload.startswith(expected_prefix.encode())
            finally:
                await kube_client.core_v1.pod.delete(pod_name)

    async def test_read_restarted(
        self,
        kube_client_selector: KubeClientSelector,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec

        async with kube_client_selector.get_client(
            org_name=org_name,
            project_name=project_name,
        ) as kube_client:
            job_pod.spec.containers[0].command = [
                "/bin/bash",
                "-c",
                'echo "$(date +%s) Restart"; sleep 1; false',
            ]
            job_pod.spec.containers[0].args = []
            job_pod.spec.restart_policy = "Always"
            try:
                await kube_client.core_v1.pod.create(job_pod)

                # previous logs should not exist yet
                log_reader_prev = PodContainerLogReader(
                    kube_client_selector=kube_client_selector,
                    pod_name=pod_name,
                    container_name=job_pod.spec.containers[0].name,
                    org_name=org_name,
                    project_name=project_name,
                    previous=True,
                )
                with pytest.raises(KubeClientException):
                    await self._consume_log_reader(log_reader_prev)

                # read current logs
                log_reader = PodContainerLogReader(
                    kube_client_selector=kube_client_selector,
                    pod_name=pod_name,
                    container_name=job_pod.spec.containers[0].name,
                    org_name=org_name,
                    project_name=project_name,
                )
                payload = await self._consume_log_reader(log_reader)
                assert b" Restart\n" in payload
                orig_timestamp = int(payload.split()[0])

                # wait until restart_count becomes >= 1
                async with asyncio.timeout(120):
                    while True:
                        status = await get_container_status(kube_client, pod_name)
                        if status.restart_count >= 1:
                            break
                        await asyncio.sleep(1)

                # try reading previous logs with retries
                payload_prev = b""
                for i in range(3)[::-1]:
                    try:
                        log_reader_prev = PodContainerLogReader(
                            kube_client_selector=kube_client_selector,
                            pod_name=pod_name,
                            container_name=job_pod.spec.containers[0].name,
                            org_name=org_name,
                            project_name=project_name,
                            previous=True,
                        )
                        payload_prev = await self._consume_log_reader(log_reader_prev)
                        break
                    except KubeClientException:
                        if not i:
                            raise
                        await asyncio.sleep(1)
                        continue

                if b"unable to retrieve container logs" in payload_prev:
                    pytest.skip("unable to retrieve container logs")
                assert b" Restart\n" in payload_prev
                assert payload_prev.count(b"Restart") == 1
                timestamp = int(payload_prev.split()[0])
                assert timestamp >= orig_timestamp
            finally:
                await kube_client.core_v1.pod.delete(pod_name)

    async def test_elasticsearch_log_reader(
        self,
        kube_client_selector: KubeClientSelector,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
        es_client: AsyncElasticsearch,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
            expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
            job_pod.spec.containers[0].args = shlex.split(command)
            await kube_client.core_v1.pod.create(job_pod)
            await wait_pod_is_terminated(kube_client, pod_name)

            await self._check_kube_logs(
                kube_client_selector,
                org_name=org_name,
                project_name=project_name,
                pod_name=pod_name,
                container_name=pod_name,
                expected_payload=expected_payload,
            )
            await asyncio.sleep(10)
            await kube_client.core_v1.pod.delete(pod_name)
            for timestamps in [False, True]:
                await self._check_es_logs(
                    es_client=es_client,
                    namespace_name=kube_client._namespace,
                    pod_name=pod_name,
                    container_name=pod_name,
                    expected_payload=expected_payload,
                    timestamps=timestamps,
                )

    async def test_elasticsearch_log_reader_restarted(
        self,
        kube_client_selector: KubeClientSelector,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
        es_client: AsyncElasticsearch,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            command = 'bash -c "sleep 5; for i in {1..5}; do echo $i; sleep 1; done"'
            expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
            job_pod.spec.containers[0].args = shlex.split(command)
            job_pod.spec.restart_policy = "Always"
            try:
                await kube_client.core_v1.pod.create(job_pod)
                await wait_container_is_restarted(kube_client, pod_name, 2)
            finally:
                await kube_client.core_v1.pod.delete(pod_name)

            await self._check_es_logs(
                es_client=es_client,
                namespace_name=kube_client._namespace,
                pod_name=pod_name,
                container_name=pod_name,
                expected_payload=expected_payload * 2,
            )

    async def test_s3_log_reader(
        self,
        kube_client_selector: KubeClientSelector,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
            expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
            job_pod.spec.containers[0].args = shlex.split(command)
            await kube_client.core_v1.pod.create(job_pod)
            await wait_pod_is_terminated(kube_client, pod_name)

            await self._check_kube_logs(
                kube_client_selector,
                org_name=org_name,
                project_name=project_name,
                pod_name=pod_name,
                container_name=pod_name,
                expected_payload=expected_payload,
            )
            await kube_client.core_v1.pod.delete(pod_name)
            for timestamps in [False, True]:
                await self._check_s3_logs(
                    s3_client=s3_client,
                    s3_bucket=s3_logs_bucket,
                    kube_namespace=kube_client._namespace,
                    pod_name=pod_name,
                    expected_payload=expected_payload,
                    timestamps=timestamps,
                )

    async def test_s3_log_reader_restarted(
        self,
        kube_client_selector: KubeClientSelector,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            command = 'bash -c "sleep 5; for i in {1..5}; do echo $i; sleep 1; done"'
            expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
            job_pod.spec.containers[0].args = shlex.split(command)
            job_pod.spec.restart_policy = "Always"
            try:
                await kube_client.core_v1.pod.create(job_pod)
                await wait_container_is_restarted(kube_client, pod_name, 2)
            finally:
                await kube_client.core_v1.pod.delete(pod_name)

            await self._check_s3_logs(
                s3_client=s3_client,
                s3_bucket=s3_logs_bucket,
                kube_namespace=kube_client._namespace,
                pod_name=pod_name,
                expected_payload=expected_payload * 2,
            )

    async def _check_kube_logs(
        self,
        kube_client_selector: KubeClientSelector,
        *,
        org_name: str,
        project_name: str,
        pod_name: str,
        container_name: str,
        expected_payload: bytes,
        timestamps: bool = False,
    ) -> None:
        log_reader = PodContainerLogReader(
            kube_client_selector=kube_client_selector,
            pod_name=pod_name,
            container_name=container_name,
            org_name=org_name,
            project_name=project_name,
            timestamps=timestamps,
        )
        payload = await self._consume_log_reader(log_reader)
        if timestamps:
            payload = self._remove_timestamps(payload)
        assert payload == expected_payload, "Pod logs did not match."

    #
    async def _check_es_logs(
        self,
        *,
        es_client: AsyncElasticsearch,
        namespace_name: str,
        pod_name: str,
        container_name: str,
        expected_payload: bytes,
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
        s3_bucket: str,
        kube_namespace: str,
        pod_name: str,
        expected_payload: bytes,
        timeout_s: float = 120.0,
        interval_s: float = 1.0,
        timestamps: bool = False,
    ) -> None:
        payload = b""
        metadata_storage = S3LogsMetadataStorage(s3_client, s3_bucket)
        metadata_service = S3LogsMetadataService(
            s3_client, metadata_storage, kube_namespace
        )
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

    #
    async def test_elasticsearch_log_reader_empty(
        self, es_client: AsyncElasticsearch
    ) -> None:
        namespace_name = pod_name = container_name = str(uuid4())
        log_reader = ElasticsearchLogReader(
            es_client,
            namespace_name=namespace_name,
            pod_name=pod_name,
            container_name=container_name,
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b""

    #
    async def test_s3_log_reader_empty(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
    ) -> None:
        pod_name = str(uuid4())
        metadata_storage = S3LogsMetadataStorage(s3_client, s3_logs_bucket)
        # Namespace is irrelevant for empty reader; choose default
        metadata_service = S3LogsMetadataService(s3_client, metadata_storage, "default")
        log_reader = S3LogReader(s3_client, metadata_service, pod_name=pod_name)
        payload = await self._consume_log_reader(log_reader)
        assert payload == b""

    async def test_get_first_log_entry_time(
        self,
        kube_client_selector: KubeClientSelector,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name,
            project_name=project_name,
        ) as kube_client:
            job_pod.spec.containers[0].command = [
                "/bin/bash",
                "-c",
                "sleep 5; echo first; sleep 5; echo second",
            ]
            job_pod.spec.containers[0].args = []
            await kube_client.core_v1.pod.create(job_pod)
            first_ts = await kube_first_log_time(kube_client, pod_name, timeout_s=1)
            assert first_ts is None
            await wait_pod_is_running(kube_client, pod_name)
            first_ts = await kube_first_log_time(kube_client, pod_name, timeout_s=1)
            assert first_ts is None
            first_ts = await kube_first_log_time(kube_client, pod_name, timeout_s=5)
            assert first_ts is not None
            # Wait for termination and check boundaries
            async with asyncio.timeout(120):
                while True:
                    status = await get_container_status(kube_client, pod_name)
                    if status.is_terminated:
                        break
                    await asyncio.sleep(1)
            status = await get_container_status(kube_client, pod_name)
            assert status.started_at is not None
            assert status.finished_at is not None
            assert first_ts > status.started_at
            assert first_ts < status.finished_at
            first_ts2 = await kube_first_log_time(kube_client, pod_name, timeout_s=1)
            assert first_ts2 == first_ts

    async def _test_get_job_log_reader(
        self,
        kube_client: KubeClientProxy,
        factory: LogsService,
        job_pod: V1Pod,
        org_name: str,
        project_name: str,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec

        job_pod.spec.containers[0].command = ["bash", "-c", "echo hello"]
        await kube_client.core_v1.pod.create(job_pod)
        await wait_pod_is_terminated(kube_client, pod_name, timeout_s=60)

        log_reader = factory.get_pod_log_reader(
            pod_name,
            namespace=kube_client._namespace,
            org_name=org_name,
            project_name=project_name,
            archive_delay_s=120.0,
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b"hello\n"

        await asyncio.sleep(10)
        await kube_client.core_v1.pod.delete(pod_name)

        log_reader = factory.get_pod_log_reader(
            pod_name,
            namespace=kube_client._namespace,
            org_name=org_name,
            project_name=project_name,
            archive_delay_s=10.0,
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b"hello\n"

    async def test_get_job_elasticsearch_log_reader(
        self,
        kube_client_with_reader: tuple[KubeClientProxy, PodContainerLogReader],
        elasticsearch_log_service: LogsService,
        job_pod: V1Pod,
        org_name: str,
        project_name: str,
    ) -> None:
        kube_client, _ = kube_client_with_reader
        await self._test_get_job_log_reader(
            kube_client, elasticsearch_log_service, job_pod, org_name, project_name
        )

    @pytest.mark.skip("temp skip until vcluster migration will be done")
    async def test_get_job_s3_log_reader(
        self,
        kube_client_with_reader: tuple[KubeClientProxy, PodContainerLogReader],
        s3_log_service: S3LogsService,
        job_pod: V1Pod,
        org_name: str,
        project_name: str,
    ) -> None:
        kube_client, _ = kube_client_with_reader
        await self._test_get_job_log_reader(
            kube_client, s3_log_service, job_pod, org_name, project_name
        )

    async def test_get_job_loki_log_reader(
        self,
        kube_client_with_reader: tuple[KubeClientProxy, PodContainerLogReader],
        loki_log_service: LokiLogsService,
        job_pod: V1Pod,
        s3_client: AioBaseClient,
        s3_config: S3Config,
        org_name: str,
        project_name: str,
    ) -> None:
        kube_client, _ = kube_client_with_reader
        await create_s3_logs_bucket(s3_client, s3_config)
        await self._test_get_job_log_reader(
            kube_client, loki_log_service, job_pod, org_name, project_name
        )

    async def _test_empty_log_reader(
        self,
        kube_client_selector: KubeClientSelector,
        job_pod: V1Pod,
        org_name: str,
        project_name: str,
        factory: LogsService,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            command = "sleep 5"
            job_pod.spec.containers[0].args = shlex.split(command)
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
                                pod_name,
                                namespace=kube_client._namespace,
                                org_name=org_name,
                                project_name=project_name,
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
                await kube_client.core_v1.pod.create(job_pod)
                run_log_reader("created", timeout_s=120)
                await wait_pod_is_running(kube_client, pod_name)
                for i in range(4):
                    run_log_reader(f"started [{i}]", delay=i * 2)
                await wait_pod_is_terminated(kube_client, pod_name)
            finally:
                done = True
                await kube_client.core_v1.pod.delete(pod_name)
            run_log_reader("deleting")
            await wait_pod_is_deleted(kube_client, pod_name)
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
        kube_client_selector: KubeClientSelector,
        elasticsearch_log_service: LogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_empty_log_reader(
            kube_client_selector,
            job_pod,
            org_name,
            project_name,
            elasticsearch_log_service,
        )

    async def test_s3_empty_log_reader(
        self,
        kube_client_selector: KubeClientSelector,
        s3_log_service: LogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_empty_log_reader(
            kube_client_selector, job_pod, org_name, project_name, s3_log_service
        )

    async def test_loki_empty_log_reader(
        self,
        kube_client_selector: KubeClientSelector,
        loki_log_service: LokiLogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_empty_log_reader(
            kube_client_selector, job_pod, org_name, project_name, loki_log_service
        )

    async def _test_merged_log_reader(
        self,
        kube_client_selector: KubeClientSelector,
        job_pod: V1Pod,
        org_name: str,
        project_name: str,
        factory: LogsService,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            command = 'bash -c "for i in {1..5}; do sleep 1; echo $i; done; sleep 2"'
            job_pod.spec.containers[0].args = shlex.split(command)
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
                                pod_name,
                                namespace=kube_client._namespace,
                                org_name=org_name,
                                project_name=project_name,
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
                await kube_client.core_v1.pod.create(job_pod)
                run_log_reader("created", timeout_s=120)
                await wait_pod_is_running(kube_client, pod_name)
                for i in range(4):
                    run_log_reader(f"started [{i}]", delay=i * 2)
                await wait_pod_is_terminated(kube_client, pod_name)
                await asyncio.sleep(10)
            finally:
                done = True
                await kube_client.core_v1.pod.delete(pod_name)
            run_log_reader("deleting")
            await wait_pod_is_deleted(kube_client, pod_name)
            run_log_reader("deleted")

            payloads = await asyncio.gather(*tasks)

            # Output for debugging
            for i, (name, payload) in enumerate(zip(names, payloads, strict=False)):
                print(f"{i}. {name}: {payload!r}")  # noqa: T201

            expected_payload = "".join(f"{i}\n" for i in range(1, 6)).encode()
            # All logs are completely either live or archive, no separator.
            for name, payload in zip(names, payloads, strict=False):
                assert payload == expected_payload, name

    async def test_elasticsearch_merged_log_reader(
        self,
        kube_client_selector: KubeClientSelector,
        elasticsearch_log_service: LogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_merged_log_reader(
            kube_client_selector,
            job_pod,
            org_name,
            project_name,
            elasticsearch_log_service,
        )

    @pytest.mark.skip("temp skip until vcluster migration will be done")
    async def test_s3_merged_log_reader(
        self,
        kube_client_selector: KubeClientSelector,
        s3_log_service: LogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_merged_log_reader(
            kube_client_selector, job_pod, org_name, project_name, s3_log_service
        )

    async def test_loki_merged_log_reader(
        self,
        kube_client_selector: KubeClientSelector,
        loki_log_service: LokiLogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_merged_log_reader(
            kube_client_selector, job_pod, org_name, project_name, loki_log_service
        )

    async def _test_merged_log_reader_restarted(  # noqa: C901
        self,
        kube_client_selector: KubeClientSelector,
        job_pod: V1Pod,
        org_name: str,
        project_name: str,
        factory: LogsService,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            command = 'bash -c "date +[%T]; for i in {1..5}; do sleep 1; echo $i; done; sleep 4"'  # noqa: E501
            job_pod.spec.containers[0].args = shlex.split(command)
            job_pod.spec.restart_policy = "Always"

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
                                pod_name,
                                namespace=kube_client._namespace,
                                org_name=org_name,
                                project_name=project_name,
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
                await kube_client.core_v1.pod.create(job_pod)
                run_log_reader("created", timeout_s=180)
                await wait_pod_is_running(kube_client, pod_name)
                for i in range(4):
                    run_log_reader(f"started [{i}]", delay=i * 2, timeout_s=90)
                await wait_container_is_restarted(kube_client, pod_name, 1)
                await wait_pod_is_running(kube_client, pod_name)
                for i in range(4):
                    run_log_reader(f"restarted 1 [{i}]", delay=i * 2)
                    await wait_container_is_restarted(kube_client, pod_name, 2)
                await wait_pod_is_running(kube_client, pod_name)
                for i in range(4):
                    run_log_reader(f"restarted 2 [{i}]", delay=i * 2)
                await wait_pod_is_terminated(kube_client, pod_name)
                await asyncio.sleep(10)
            finally:
                done = True
                await kube_client.core_v1.pod.delete(pod_name)
            run_log_reader("deleting")
            await wait_pod_is_deleted(kube_client, pod_name)
            run_log_reader("deleted")

            payloads: list[bytes] = await asyncio.gather(*tasks)  # type: ignore

            # Output for debugging
            for i, (name, payload) in enumerate(zip(names, payloads, strict=False)):
                print(f"{i}. {name}: {payload!r}")  # noqa: T201

            expected_payload = "".join(f"{i}\n" for i in range(1, 6)).encode()
            payload0 = payloads[0]

            assert re.sub(rb"\[.*?\]\n", b"", payload0) == expected_payload * 3
            for i, (name, payload) in enumerate(zip(names, payloads, strict=False)):
                if i < 2 or i >= len(names) - 1:
                    # No separator in the earliest (live only) and
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
        kube_client_selector: KubeClientSelector,
        elasticsearch_log_service: LogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_merged_log_reader_restarted(
            kube_client_selector,
            job_pod,
            org_name,
            project_name,
            elasticsearch_log_service,
        )

    @pytest.mark.xfail
    async def test_s3_merged_log_reader_restarted(
        self,
        kube_client_selector: KubeClientSelector,
        s3_log_service: LogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_merged_log_reader_restarted(
            kube_client_selector, job_pod, org_name, project_name, s3_log_service
        )

    async def test_loki_merged_log_reader_restarted(
        self,
        kube_client_selector: KubeClientSelector,
        loki_log_service: LokiLogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_merged_log_reader_restarted(
            kube_client_selector, job_pod, org_name, project_name, loki_log_service
        )

    async def _test_merged_log_reader_restarted_since(
        self,
        kube_client_selector: KubeClientSelector,
        job_pod: V1Pod,
        org_name: str,
        project_name: str,
        factory: LogsService,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            command = "bash -c 'echo begin; sleep 5; echo end; sleep 2; false'"
            job_pod.spec.containers[0].args = shlex.split(command)
            job_pod.spec.restart_policy = "OnFailure"
            starts = []
            tasks = []

            def run_log_reader(since: datetime) -> None:
                async def coro() -> bytes:
                    log_reader = factory.get_pod_log_reader(
                        pod_name,
                        namespace=kube_client._namespace,
                        org_name=org_name,
                        project_name=project_name,
                        since=since,
                        separator=b"===",
                        archive_delay_s=20.0,
                    )
                    return await self._consume_log_reader(log_reader)

                starts.append(since)
                task = asyncio.ensure_future(coro())
                tasks.append(task)

            try:
                await kube_client.core_v1.pod.create(job_pod)
                await wait_pod_is_running(kube_client, pod_name)
                await wait_pod_is_terminated(kube_client, pod_name)
                status = await get_container_status(kube_client, pod_name)
                logger.info(f"status 1: {status}")  # noqa: G004
                finished1 = status.finished_at
                assert finished1

                await wait_container_is_restarted(kube_client, pod_name, 1)
                await wait_pod_is_running(kube_client, pod_name)
                status = await get_container_status(kube_client, pod_name)
                logger.info(f"status 2: {status}")  # noqa: G004
                started2 = status.started_at
                assert started2
                run_log_reader(since=started2)
                run_log_reader(since=started2 + timedelta(seconds=2))

                await wait_pod_is_terminated(kube_client, pod_name)
                status = await get_container_status(kube_client, pod_name)
                logger.info(f"status 3: {status}")  # noqa: G004
                assert status.started_at == started2
                finished2 = status.finished_at
                assert finished2

                await wait_container_is_restarted(kube_client, pod_name, 2)
                await wait_pod_is_running(kube_client, pod_name)
                run_log_reader(since=finished1 - timedelta(seconds=4))
                run_log_reader(since=started2)
                run_log_reader(since=finished2 - timedelta(seconds=4))
                run_log_reader(since=finished2 + timedelta(seconds=2))
                await wait_pod_is_terminated(kube_client, pod_name)
                await asyncio.sleep(10)
            finally:
                await kube_client.core_v1.pod.delete(pod_name)
            await wait_pod_is_deleted(kube_client, pod_name)
            run_log_reader(since=finished1 - timedelta(seconds=4))
            run_log_reader(since=started2)
            run_log_reader(since=finished2 - timedelta(seconds=4))
            run_log_reader(since=finished2 + timedelta(seconds=2))

            payloads = await asyncio.gather(*tasks)

            # Output for debugging
            for i, (since, payload) in enumerate(zip(starts, payloads, strict=False)):
                print(f"{i}. [{since:%T}] {payload!r}")  # noqa: T201

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
        kube_client_selector: KubeClientSelector,
        elasticsearch_log_service: LogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_merged_log_reader_restarted_since(
            kube_client_selector,
            job_pod,
            org_name,
            project_name,
            elasticsearch_log_service,
        )

    async def test_s3_merged_log_reader_restarted_since(
        self,
        kube_client_selector: KubeClientSelector,
        s3_log_service: LogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_merged_log_reader_restarted_since(
            kube_client_selector, job_pod, org_name, project_name, s3_log_service
        )

    async def test_loki_merged_log_reader_restarted_since(
        self,
        kube_client_selector: KubeClientSelector,
        loki_log_service: LokiLogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_merged_log_reader_restarted_since(
            kube_client_selector, job_pod, org_name, project_name, loki_log_service
        )

    async def _test_large_log_reader(
        self,
        kube_client_selector: KubeClientSelector,
        job_pod: V1Pod,
        org_name: str,
        project_name: str,
        factory: LogsService,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            num = 10000
            command = f"seq {num}"
            job_pod.spec.containers[0].args = shlex.split(command)

            try:
                await kube_client.core_v1.pod.create(job_pod)
                await wait_pod_is_terminated(kube_client, pod_name)
                log_reader = factory.get_pod_log_reader(
                    pod_name,
                    namespace=kube_client._namespace,
                    org_name=org_name,
                    project_name=project_name,
                    separator=b"===",
                    archive_delay_s=30.0,
                )
                payload = (
                    await self._consume_log_reader(log_reader, delay=0.001)
                ).decode()
            finally:
                await kube_client.core_v1.pod.delete(pod_name)

            expected_payload = "".join(f"{i + 1}\n" for i in range(num))
            assert payload == expected_payload

    async def test_s3_large_log_reader(
        self,
        kube_client_selector: KubeClientSelector,
        s3_log_service: LogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_large_log_reader(
            kube_client_selector, job_pod, org_name, project_name, s3_log_service
        )

    async def test_loki_large_log_reader(
        self,
        kube_client_selector: KubeClientSelector,
        loki_log_service: LokiLogsService,
        org_name: str,
        project_name: str,
        job_pod: V1Pod,
    ) -> None:
        await self._test_large_log_reader(
            kube_client_selector, job_pod, org_name, project_name, loki_log_service
        )

    async def test_s3_log_reader_running_pod_compacted(
        self,
        kube_client_selector: KubeClientSelector,
        job_pod: V1Pod,
        org_name: str,
        project_name: str,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_log_service: S3LogsService,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            command = 'bash -c "for i in {1..10}; do echo $i; sleep 1; done"'
            expected_payload = ("\n".join(str(i) for i in range(1, 11)) + "\n").encode()
            job_pod.spec.containers[0].args = shlex.split(command)
            await kube_client.core_v1.pod.create(job_pod)
            await wait_pod_is_running(kube_client, pod_name)
            await asyncio.sleep(5)

            await s3_log_service.compact_one(pod_name)

            await wait_pod_is_terminated(kube_client, pod_name)
            await asyncio.sleep(5)
            await kube_client.core_v1.pod.delete(pod_name)

            for timestamps in [False, True]:
                await self._check_s3_logs(
                    s3_client=s3_client,
                    s3_bucket=s3_logs_bucket,
                    kube_namespace=kube_client._namespace,
                    pod_name=pod_name,
                    expected_payload=expected_payload,
                    timestamps=timestamps,
                )

    async def test_s3_log_reader_terminated_pod_compacted(
        self,
        kube_client_selector: KubeClientSelector,
        job_pod: V1Pod,
        org_name: str,
        project_name: str,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_log_service: S3LogsService,
    ) -> None:
        pod_name = job_pod.metadata.name
        assert pod_name
        assert job_pod.spec
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
            expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
            job_pod.spec.containers[0].args = shlex.split(command)
            await kube_client.core_v1.pod.create(job_pod)
            await wait_pod_is_terminated(kube_client, pod_name)
            await asyncio.sleep(5)
            await kube_client.core_v1.pod.delete(pod_name)

            await s3_log_service.compact_one(pod_name)

            for timestamps in [False, True]:
                await self._check_s3_logs(
                    s3_client=s3_client,
                    s3_bucket=s3_logs_bucket,
                    kube_namespace=kube_client._namespace,
                    pod_name=pod_name,
                    expected_payload=expected_payload,
                    timestamps=timestamps,
                )
