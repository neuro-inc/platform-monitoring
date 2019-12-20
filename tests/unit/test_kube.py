from typing import Any, Dict
from unittest import mock

import aiohttp
import pytest
from platform_monitoring.kube_client import (
    JobError,
    Pod,
    PodContainerStats,
    StatsSummary,
)
from platform_monitoring.logs import FilteredStreamWrapper


class TestPod:
    def test_no_node_name(self) -> None:
        pod = Pod({"spec": {}})
        assert pod.node_name is None

    def test_node_name(self) -> None:
        pod = Pod({"spec": {"nodeName": "testnode"}})
        assert pod.node_name == "testnode"

    def test_no_status(self) -> None:
        pod = Pod({"spec": {}})
        with pytest.raises(ValueError, match="Missing pod status"):
            pod.get_container_status("testcontainer")

    def test_no_container_status(self) -> None:
        pod = Pod({"spec": {}, "status": {"containerStatuses": []}})
        container_status = pod.get_container_status("testcontainer")
        assert container_status == {}

    def test_container_status(self) -> None:
        pod = Pod(
            {
                "spec": {},
                "status": {
                    "containerStatuses": [{"name": ""}, {"name": "testcontainer"}]
                },
            }
        )
        container_status = pod.get_container_status("testcontainer")
        assert container_status == {"name": "testcontainer"}

    def test_no_container_id(self) -> None:
        pod = Pod(
            {"spec": {}, "status": {"containerStatuses": [{"name": "testcontainer"}]}}
        )
        container_id = pod.get_container_id("testcontainer")
        assert container_id is None

    def test_container_id(self) -> None:
        pod = Pod(
            {
                "spec": {},
                "status": {
                    "containerStatuses": [
                        {
                            "name": "testcontainer",
                            "containerID": "docker://testcontainerid",
                        }
                    ]
                },
            }
        )
        container_id = pod.get_container_id("testcontainer")
        assert container_id == "testcontainerid"

    def test_is_phase_running_false(self) -> None:
        pod = Pod({"spec": {}, "status": {"phase": "Pending"}})
        assert not pod.is_phase_running

    def test_is_phase_running(self) -> None:
        pod = Pod({"spec": {}, "status": {"phase": "Running"}})
        assert pod.is_phase_running


class TestPodContainerStats:
    def test_from_primitive_no_keys(self) -> None:
        payload: Dict[str, Any] = {"memory": {}}
        stats = PodContainerStats.from_primitive(payload)
        empty_stats = PodContainerStats(cpu=0.0, memory=0.0)
        assert stats == empty_stats
        payload = {"cpu": {}}
        stats = PodContainerStats.from_primitive(payload)
        assert stats == empty_stats
        payload = {}
        stats = PodContainerStats.from_primitive(payload)
        assert stats == empty_stats

    def test_from_primitive_empty(self) -> None:
        payload: Dict[str, Any] = {"cpu": {}, "memory": {}}
        stats = PodContainerStats.from_primitive(payload)
        assert stats == PodContainerStats(cpu=0.0, memory=0.0)

    def test_from_primitive(self) -> None:
        payload = {
            "cpu": {"usageNanoCores": 1000},
            "memory": {"workingSetBytes": 1024 * 1024},
            "accelerators": [
                {"dutyCycle": 20, "memoryUsed": 2 * 1024 * 1024},
                {"dutyCycle": 30, "memoryUsed": 4 * 1024 * 1024},
            ],
        }
        stats = PodContainerStats.from_primitive(payload)
        assert stats == PodContainerStats(
            cpu=0.000001, memory=1.0, gpu_duty_cycle=25, gpu_memory=6.0
        )


class TestStatsSummary:
    def test_get_pod_container_stats_error_response(self) -> None:
        payload: Dict[str, Any] = {
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "message": "message",
            "reason": "Forbidden",
            "details": {"name": "default-pool", "kind": "nodes"},
            "code": 403,
        }
        with pytest.raises(JobError, match="Invalid stats summary response"):
            StatsSummary(payload)

    def test_get_pod_container_stats_no_pod(self) -> None:
        payload: Dict[str, Any] = {"pods": []}
        stats = StatsSummary(payload).get_pod_container_stats(
            "namespace", "pod", "container"
        )
        assert stats is None

    def test_get_pod_container_stats_no_containers(self) -> None:
        payload = {"pods": [{"podRef": {"namespace": "namespace", "name": "pod"}}]}
        stats = StatsSummary(payload).get_pod_container_stats(
            "namespace", "pod", "container"
        )
        assert stats is None

    def test_get_pod_container_stats(self) -> None:
        payload = {
            "pods": [
                {
                    "podRef": {"namespace": "namespace", "name": "pod"},
                    "containers": [{"name": "container", "cpu": {}, "memory": {}}],
                }
            ]
        }
        stats = StatsSummary(payload).get_pod_container_stats(
            "namespace", "pod", "container"
        )
        assert stats


class TestFilteredStreamWrapper:
    @pytest.mark.asyncio
    async def test_read_eof(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False))
        reader.feed_eof()
        stream = FilteredStreamWrapper(reader)
        chunk = await stream.read()
        assert not chunk

    @pytest.mark.asyncio
    async def test_read_two_lines_eof(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False))
        reader.feed_data(b"line1\n")
        reader.feed_data(b"line2")
        reader.feed_eof()
        stream = FilteredStreamWrapper(reader)
        chunk = await stream.read()
        assert chunk == b"line1\n"
        chunk = await stream.read()
        assert chunk == b"line2"

    @pytest.mark.asyncio
    async def test_half_line(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False))
        reader.feed_data(b"line1\n")
        reader.feed_data(b"line2\n")
        stream = FilteredStreamWrapper(reader)
        chunk = await stream.read(size=2)
        assert chunk == b"li"
        chunk = await stream.read(size=2)
        assert chunk == b"ne"

        reader.feed_data(b"line3")
        reader.feed_eof()

        chunk = await stream.read(size=2)
        assert chunk == b"1\n"
        chunk = await stream.read()
        assert chunk == b"line2\n"
        chunk = await stream.read()
        assert chunk == b"line3"

    @pytest.mark.asyncio
    async def test_filtered_single_rpc_error(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False))
        reader.feed_data(b"line1\n")
        reader.feed_data(b"rpc error: code = whatever")
        reader.feed_eof()
        stream = FilteredStreamWrapper(reader)
        chunk = await stream.read()
        assert chunk == b"line1\n"
        chunk = await stream.read()
        assert not chunk

    @pytest.mark.asyncio
    async def test_filtered_two_rpc_errors(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False))
        reader.feed_data(b"line1\n")
        reader.feed_data(b"rpc error: code = whatever\n")
        reader.feed_data(b"rpc error: code = again\n")
        reader.feed_eof()
        stream = FilteredStreamWrapper(reader)
        chunk = await stream.read()
        assert chunk == b"line1\n"
        chunk = await stream.read()
        assert chunk == b"rpc error: code = whatever\n"
        chunk = await stream.read()
        assert not chunk

    @pytest.mark.asyncio
    async def test_not_filtered_single_rpc_not_eof(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False))
        reader.feed_data(b"line1\n")
        reader.feed_data(b"rpc error: code = whatever\n")
        reader.feed_data(b"line2\n")
        reader.feed_eof()
        stream = FilteredStreamWrapper(reader)
        chunk = await stream.read()
        assert chunk == b"line1\n"
        chunk = await stream.read()
        assert chunk == b"rpc error: code = whatever\n"
        chunk = await stream.read()
        assert chunk == b"line2\n"
        chunk = await stream.read()
        assert not chunk

    @pytest.mark.asyncio
    async def test_min_line_chunk(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False))
        stream = FilteredStreamWrapper(reader)

        async def _read_all() -> List[bytes]:
            chunks: List[bytes] = []
            while True:
                c = await stream.read()
                chunks.append(c)
                if not c:
                    break
            return chunks

        async def _feed_raw_chunk(data: bytes) -> None:
            reader.feed_data(data)
            await asyncio.sleep(0.0)

        task = asyncio.create_task(_read_all())
        await _feed_raw_chunk(b"chunk01\r")
        await _feed_raw_chunk(b"chunk02\r")
        await _feed_raw_chunk(b"chunk03\r")
        await _feed_raw_chunk(b"chunk04\r")
        await _feed_raw_chunk(b"chunk05\r\n")
        await _feed_raw_chunk(b"chunk06\r\n")
        await _feed_raw_chunk(b"chunk07\r")
        await _feed_raw_chunk(b"chunk08\r\n")
        await _feed_raw_chunk(b"rpc error: ")
        await _feed_raw_chunk(b"code =")
        reader.feed_eof()
        chunks = await task
        assert chunks == [
            b"chunk01\rchunk02\rchunk03\r",
            b"chunk04\r",
            b"chunk05\r\n",
            b"chunk06\r\n",
            b"chunk07\rchunk08\r\n",
            b"",
        ]
