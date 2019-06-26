from typing import Any, Dict
from unittest import mock

import aiohttp
import pytest
from platform_monitoring.kube_client import PodContainerStats, StatsSummary
from platform_monitoring.logs import FilteredStreamWrapper


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
