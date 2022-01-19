import asyncio
from typing import Any
from unittest import mock

import aiohttp
import pytest

from platform_monitoring.kube_client import (
    JobError,
    Node,
    Pod,
    PodContainerStats,
    PodPhase,
    Resources,
    StatsSummary,
)
from platform_monitoring.logs import filter_out_rpc_error


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

    def test_phase(self) -> None:
        pod = Pod({"spec": {}, "status": {"phase": "Running"}})
        assert pod.phase == PodPhase.RUNNING

    def test_is_phase_running_false(self) -> None:
        pod = Pod({"spec": {}, "status": {"phase": "Pending"}})
        assert not pod.is_phase_running

    def test_is_phase_running(self) -> None:
        pod = Pod({"spec": {}, "status": {"phase": "Running"}})
        assert pod.is_phase_running

    def test_no_resource_requests(self) -> None:
        pod = Pod({"spec": {"containers": [{"resources": {}}]}})
        assert pod.resource_requests == Resources()

    def test_resource_requests_cpu_milicores(self) -> None:
        pod = Pod(
            {"spec": {"containers": [{"resources": {"requests": {"cpu": "100m"}}}]}}
        )
        assert pod.resource_requests == Resources(cpu_m=100)

    def test_resource_requests_cpu_cores(self) -> None:
        pod = Pod({"spec": {"containers": [{"resources": {"requests": {"cpu": "1"}}}]}})
        assert pod.resource_requests == Resources(cpu_m=1000)

    def test_resource_requests_memory_mebibytes(self) -> None:
        pod = Pod(
            {
                "spec": {
                    "containers": [{"resources": {"requests": {"memory": "1000Mi"}}}]
                }
            }
        )
        assert pod.resource_requests == Resources(memory_mb=1000)

    def test_resource_requests_memory_gibibytes(self) -> None:
        pod = Pod(
            {"spec": {"containers": [{"resources": {"requests": {"memory": "1Gi"}}}]}}
        )
        assert pod.resource_requests == Resources(memory_mb=1024)

    def test_resource_requests_gpu(self) -> None:
        pod = Pod(
            {
                "spec": {
                    "containers": [{"resources": {"requests": {"nvidia.com/gpu": "1"}}}]
                }
            }
        )
        assert pod.resource_requests == Resources(gpu=1)

    def test_resource_requests_for_multiple_containers(self) -> None:
        pod = Pod(
            {
                "spec": {
                    "containers": [
                        {"resources": {"requests": {"cpu": "0.5", "memory": "512Mi"}}},
                        {
                            "resources": {
                                "requests": {
                                    "cpu": "1",
                                    "memory": "1Gi",
                                    "nvidia.com/gpu": "1",
                                }
                            }
                        },
                    ]
                }
            }
        )
        assert pod.resource_requests == Resources(cpu_m=1500, memory_mb=1536, gpu=1)


class TestPodContainerStats:
    def test_from_primitive_no_keys(self) -> None:
        payload: dict[str, Any] = {"memory": {}}
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
        payload: dict[str, Any] = {"cpu": {}, "memory": {}}
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
        payload: dict[str, Any] = {
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
        payload: dict[str, Any] = {"pods": []}
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


class TestFilterOutRPCError:
    async def test_iter_eof(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False), 1024)
        reader.feed_eof()
        it = filter_out_rpc_error(reader)
        chunks = [chunk async for chunk in it]
        assert chunks == []

    async def test_read_two_lines_eof(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False), 1024)
        reader.feed_data(b"line1\n")
        reader.feed_data(b"line2")
        reader.feed_eof()
        it = filter_out_rpc_error(reader)
        chunks = [chunk async for chunk in it]
        assert chunks == [b"line1\n", b"line2"]

    async def test_filtered_single_rpc_error(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False), 1024)
        reader.feed_data(b"line1\n")
        reader.feed_data(b"rpc error: code = whatever")
        reader.feed_eof()
        it = filter_out_rpc_error(reader)
        chunks = [chunk async for chunk in it]
        assert chunks == [b"line1\n"]

    async def test_filtered_single_rpc_error2(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False), 1024)
        reader.feed_data(b"line1\n")
        reader.feed_data(
            b"Unable to retrieve container logs for docker://0123456789abcdef"
        )
        reader.feed_eof()
        it = filter_out_rpc_error(reader)
        chunks = [chunk async for chunk in it]
        assert chunks == [b"line1\n"]

    async def test_filtered_single_rpc_error3(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False), 1024)
        reader.feed_data(b"line1\n")
        reader.feed_data(
            b'failed to try resolving symlinks in path "/var/log/pods/xxx.log": '
            b"lstat /var/log/pods/xxx.log: no such file or directory"
        )
        reader.feed_eof()
        it = filter_out_rpc_error(reader)
        chunks = [chunk async for chunk in it]
        assert chunks == [b"line1\n"]

    async def test_filtered_two_rpc_errors(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False), 1024)
        reader.feed_data(b"line1\n")
        reader.feed_data(b"rpc error: code = whatever\n")
        reader.feed_data(b"rpc error: code = again\n")
        reader.feed_eof()
        it = filter_out_rpc_error(reader)
        chunks = [chunk async for chunk in it]
        assert chunks == [b"line1\n", b"rpc error: code = whatever\n"]

    async def test_not_filtered_single_rpc_not_eof(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False), 1024)
        reader.feed_data(b"line1\n")
        reader.feed_data(b"rpc error: code = whatever\n")
        reader.feed_data(b"line2\n")
        reader.feed_eof()
        it = filter_out_rpc_error(reader)
        chunks = [chunk async for chunk in it]
        assert chunks == [b"line1\n", b"rpc error: code = whatever\n", b"line2\n"]

    async def test_min_line_chunk(self) -> None:
        reader = aiohttp.StreamReader(mock.Mock(_reading_paused=False), 1024)
        it = filter_out_rpc_error(reader)

        async def _read_all() -> list[bytes]:
            return [chunk async for chunk in it]

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
        ]


class TestNode:
    def test_name(self) -> None:
        node = Node({"metadata": {"name": "default"}})
        assert node.name == "default"

    def test_get_label(self) -> None:
        node = Node({"metadata": {"labels": {"hello": "world"}}})
        assert node.get_label("hello") == "world"

    def test_get_label_is_none(self) -> None:
        node = Node({"metadata": {}})
        assert node.get_label("hello") is None


class TestResources:
    def test_add(self) -> None:
        resources1 = Resources(cpu_m=1, memory_mb=2, gpu=3)
        resources2 = Resources(cpu_m=4, memory_mb=5, gpu=6)
        assert resources1.add(resources2) == Resources(cpu_m=5, memory_mb=7, gpu=9)

    def test_available(self) -> None:
        total = Resources(cpu_m=1000, memory_mb=1024, gpu=2)
        used = Resources(cpu_m=100, memory_mb=256, gpu=1)
        assert total.available(used) == Resources(cpu_m=900, memory_mb=768, gpu=1)

    def test_count(self) -> None:
        total = Resources(cpu_m=1000, memory_mb=1024, gpu=2)

        assert total.count(Resources(cpu_m=100, memory_mb=128, gpu=1)) == 2
        assert total.count(Resources(cpu_m=100, memory_mb=128)) == 8
        assert total.count(Resources(cpu_m=100)) == 10
        assert total.count(Resources(cpu_m=1100)) == 0
        assert total.count(Resources()) == 110
        assert Resources().count(Resources()) == 0
