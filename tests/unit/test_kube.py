import asyncio
from typing import Any
from unittest import mock

import aiohttp
import pytest
from apolo_kube_client import (
    V1Container,
    V1ContainerStatus,
    V1Node,
    V1NodeStatus,
    V1NodeSystemInfo,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1PodStatus,
    V1ResourceRequirements,
)

from platform_monitoring.kube_client import (
    ContainerResources,
    ContainerStatus,
    GPUCounter,
    GPUCounters,
    JobError,
    Node,
    NodeResources,
    Pod,
    PodContainerStats,
    PodPhase,
    PodRestartPolicy,
    StatsSummary,
)
from platform_monitoring.logs import filter_out_rpc_error


class TestPod:
    def test_no_node_name(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(containers=[]),
            )
        )
        assert pod.spec.node_name is None

    def test_node_name(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(containers=[], node_name="testnode"),
            )
        )
        assert pod.spec.node_name == "testnode"

    def test_no_status(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(containers=[]),
            )
        )
        assert pod.get_container_status("testcontainer") == ContainerStatus(
            name="testcontainer"
        )

    def test_no_container_status(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(containers=[]),
                status=V1PodStatus(container_statuses=[]),
            )
        )
        assert pod.get_container_status("testcontainer") == ContainerStatus(
            name="testcontainer"
        )

    def test_container_status(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(containers=[], restart_policy="Never"),
                status=V1PodStatus(
                    container_statuses=[
                        V1ContainerStatus(
                            name="testcontainer",
                            image_id="image",
                            image="image",
                            ready=True,
                            restart_count=0,
                        )
                    ]
                ),
            )
        )
        container_status = pod.get_container_status("testcontainer")
        assert container_status == ContainerStatus(
            name="testcontainer", pod_restart_policy=PodRestartPolicy.NEVER
        )

    def test_no_container_id(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(containers=[]),
                status=V1PodStatus(
                    container_statuses=[
                        V1ContainerStatus(
                            name="testcontainer",
                            image_id="image",
                            image="image",
                            ready=True,
                            restart_count=0,
                        )
                    ]
                ),
            )
        )
        container_id = pod.get_container_id("testcontainer")
        assert container_id is None

    def test_container_id(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(containers=[]),
                status=V1PodStatus(
                    container_statuses=[
                        V1ContainerStatus(
                            name="testcontainer",
                            container_id="docker://testcontainerid",
                            image_id="image",
                            image="image",
                            ready=True,
                            restart_count=0,
                        )
                    ]
                ),
            )
        )
        container_id = pod.get_container_id("testcontainer")
        assert container_id == "testcontainerid"

    def test_phase(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(containers=[]),
                status=V1PodStatus(phase="Running"),
            )
        )
        assert pod.status.phase == PodPhase.RUNNING

    def test_phase_is_running_false(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(containers=[]),
                status=V1PodStatus(phase="Pending"),
            )
        )
        assert not pod.status.is_running

    def test_phase_is_running(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(containers=[]),
                status=V1PodStatus(phase="Running"),
            )
        )
        assert pod.status.is_running

    def test_no_resource_requests(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(containers=[V1Container(name="container_name")]),
            )
        )
        assert pod.resource_requests == ContainerResources()

    def test_resource_requests_cpu_milicores(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            name="container_name",
                            resources=V1ResourceRequirements(requests={"cpu": "100m"}),
                        )
                    ]
                ),
            )
        )
        assert pod.resource_requests == ContainerResources(cpu_m=100)

    def test_resource_requests_cpu_cores(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            name="container_name",
                            resources=V1ResourceRequirements(requests={"cpu": "1"}),
                        )
                    ]
                ),
            )
        )
        assert pod.resource_requests == ContainerResources(cpu_m=1000)

    def test_resource_requests_memory_mebibytes(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            name="container_name",
                            resources=V1ResourceRequirements(
                                requests={"memory": "1000Mi"}
                            ),
                        )
                    ]
                ),
            )
        )
        assert pod.resource_requests == ContainerResources(memory=1000 * 2**20)

    def test_resource_requests_memory_gibibytes(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            name="container_name",
                            resources=V1ResourceRequirements(
                                requests={"memory": "1Gi"}
                            ),
                        )
                    ]
                ),
            )
        )
        assert pod.resource_requests == ContainerResources(memory=1024 * 2**20)

    def test_resource_requests_memory_terabytes(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            name="container_name",
                            resources=V1ResourceRequirements(requests={"memory": "4T"}),
                        )
                    ]
                ),
            )
        )
        assert pod.resource_requests == ContainerResources(memory=4 * 10**12)

    def test_resource_requests_memory_tebibytes(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            name="container_name",
                            resources=V1ResourceRequirements(
                                requests={"memory": "4Ti"}
                            ),
                        )
                    ]
                ),
            )
        )
        assert pod.resource_requests == ContainerResources(memory=4 * 2**40)

    def test_resource_requests_gpu(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            name="container_name",
                            resources=V1ResourceRequirements(
                                requests={
                                    "nvidia.com/gpu": "1",
                                    "amd.com/gpu": "2",
                                }
                            ),
                        )
                    ]
                ),
            )
        )
        assert pod.resource_requests.has_gpu
        assert pod.resource_requests == ContainerResources(nvidia_gpu=1, amd_gpu=2)

    def test_resource_requests_for_multiple_containers(self) -> None:
        pod = Pod.from_model(
            V1Pod(
                metadata=V1ObjectMeta(name="pod"),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            name="container_name1",
                            resources=V1ResourceRequirements(
                                requests={"cpu": "0.5", "memory": "512Mi"}
                            ),
                        ),
                        V1Container(
                            name="container_name2",
                            resources=V1ResourceRequirements(
                                requests={
                                    "cpu": "1",
                                    "memory": "1Gi",
                                    "nvidia.com/gpu": "1",
                                    "amd.com/gpu": "2",
                                }
                            ),
                        ),
                    ]
                ),
            )
        )
        assert pod.resource_requests == ContainerResources(
            cpu_m=1500, memory=1536 * 2**20, nvidia_gpu=1, amd_gpu=2
        )


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
            "memory": {"workingSetBytes": 2**20},
        }
        stats = PodContainerStats.from_primitive(payload)
        assert stats == PodContainerStats(cpu=0.000001, memory=2**20)


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


class TestGPUCounters:
    def test_parse(self) -> None:
        metrics = """
# HELP DCGM_FI_DEV_GPU_UTIL GPU utilization (in %).
# TYPE DCGM_FI_DEV_GPU_UTIL gauge

# HELP DCGM_FI_DEV_FB_USED Framebuffer memory used (in MiB).
# TYPE DCGM_FI_DEV_FB_USED gauge


DCGM_FI_DEV_GPU_UTIL{gpu="0",container="job-0",namespace="platform-jobs",pod="job-0"} 1
DCGM_FI_DEV_FB_USED{gpu="0",container="job-0",namespace="platform-jobs",pod="job-0"} 10

DCGM_FI_DEV_GPU_UTIL{gpu="1",container="job-0",namespace="platform-jobs",pod="job-0"} 2
DCGM_FI_DEV_FB_USED{gpu="1",container="job-0",namespace="platform-jobs",pod="job-0"} 20

DCGM_FI_DEV_GPU_UTIL{gpu="2",container="job-1",namespace="platform-jobs",pod="job-1"} 3
DCGM_FI_DEV_FB_USED{gpu="2",container="job-1",namespace="platform-jobs",pod="job-1"} 30
"""

        counters = GPUCounters.parse(metrics)

        assert counters == GPUCounters(
            counters=[
                GPUCounter(
                    name="DCGM_FI_DEV_GPU_UTIL",
                    value=1,
                    labels={
                        "gpu": "0",
                        "namespace": "platform-jobs",
                        "pod": "job-0",
                        "container": "job-0",
                    },
                ),
                GPUCounter(
                    name="DCGM_FI_DEV_FB_USED",
                    value=10,
                    labels={
                        "gpu": "0",
                        "namespace": "platform-jobs",
                        "pod": "job-0",
                        "container": "job-0",
                    },
                ),
                GPUCounter(
                    name="DCGM_FI_DEV_GPU_UTIL",
                    value=2,
                    labels={
                        "gpu": "1",
                        "namespace": "platform-jobs",
                        "pod": "job-0",
                        "container": "job-0",
                    },
                ),
                GPUCounter(
                    name="DCGM_FI_DEV_FB_USED",
                    value=20,
                    labels={
                        "gpu": "1",
                        "namespace": "platform-jobs",
                        "pod": "job-0",
                        "container": "job-0",
                    },
                ),
                GPUCounter(
                    name="DCGM_FI_DEV_GPU_UTIL",
                    value=3,
                    labels={
                        "gpu": "2",
                        "namespace": "platform-jobs",
                        "pod": "job-1",
                        "container": "job-1",
                    },
                ),
                GPUCounter(
                    name="DCGM_FI_DEV_FB_USED",
                    value=30,
                    labels={
                        "gpu": "2",
                        "namespace": "platform-jobs",
                        "pod": "job-1",
                        "container": "job-1",
                    },
                ),
            ]
        )

    def test_get_pod_container_stats_utilization(self) -> None:
        counters = GPUCounters(
            counters=[
                GPUCounter(
                    name="DCGM_FI_DEV_GPU_UTIL",
                    value=1,
                    labels={
                        "gpu": "0",
                        "namespace": "platform-jobs",
                        "pod": "job-0",
                        "container": "job-0",
                    },
                ),
                GPUCounter(
                    name="DCGM_FI_DEV_GPU_UTIL",
                    value=4,
                    labels={
                        "gpu": "1",
                        "namespace": "platform-jobs",
                        "pod": "job-0",
                        "container": "job-0",
                    },
                ),
                GPUCounter(
                    name="DCGM_FI_DEV_GPU_UTIL",
                    value=2,
                    labels={
                        "gpu": "2",
                        "namespace": "platform-jobs",
                        "pod": "job-1",
                        "container": "job-1",
                    },
                ),
            ]
        )

        stats = counters.get_pod_container_stats(
            namespace_name="platform-jobs", pod_name="job-0", container_name="job-0"
        )
        assert stats.utilization == 2

        stats = counters.get_pod_container_stats(
            namespace_name="platform-jobs", pod_name="job-1", container_name="job-1"
        )
        assert stats.utilization == 2

    def test_get_pod_container_stats_memory_used(self) -> None:
        counters = GPUCounters(
            counters=[
                GPUCounter(
                    name="DCGM_FI_DEV_FB_USED",
                    value=1,
                    labels={
                        "gpu": "0",
                        "namespace": "platform-jobs",
                        "pod": "job-0",
                        "container": "job-0",
                    },
                ),
                GPUCounter(
                    name="DCGM_FI_DEV_FB_USED",
                    value=2,
                    labels={
                        "gpu": "1",
                        "namespace": "platform-jobs",
                        "pod": "job-0",
                        "container": "job-0",
                    },
                ),
                GPUCounter(
                    name="DCGM_FI_DEV_FB_USED",
                    value=3,
                    labels={
                        "gpu": "2",
                        "namespace": "platform-jobs",
                        "pod": "job-1",
                        "container": "job-1",
                    },
                ),
            ]
        )

        stats = counters.get_pod_container_stats(
            namespace_name="platform-jobs", pod_name="job-0", container_name="job-0"
        )
        assert stats.utilization == 0
        assert stats.memory_used == 3 * 2**20

    def test_get_pod_container_stats_unknown_job(self) -> None:
        counters = GPUCounters(
            counters=[
                GPUCounter(
                    name="DCGM_FI_DEV_GPU_UTIL",
                    value=1,
                    labels={
                        "gpu": "0",
                        "namespace": "platform-jobs",
                        "pod": "job-0",
                        "container": "job-0",
                    },
                ),
                GPUCounter(
                    name="DCGM_FI_DEV_FB_USED",
                    value=1,
                    labels={
                        "gpu": "0",
                        "namespace": "platform-jobs",
                        "pod": "job-0",
                        "container": "job-0",
                    },
                ),
            ]
        )

        stats = counters.get_pod_container_stats(
            namespace_name="platform-jobs", pod_name="job-1", container_name="job-1"
        )
        assert stats.utilization == 0
        assert stats.memory_used == 0


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
        node = Node.from_model(
            V1Node(
                metadata=V1ObjectMeta(name="default"),
                status=V1NodeStatus(
                    node_info=V1NodeSystemInfo(
                        architecture="a",
                        boot_id="b",
                        kernel_version="k",
                        kube_proxy_version="1",
                        kubelet_version="1",
                        machine_id="1",
                        operating_system="os",
                        os_image="i",
                        system_uuid="u",
                        container_runtime_version="containerd",
                    )
                ),
            )
        )
        assert node.metadata.name == "default"

    def test_labels(self) -> None:
        node = Node.from_model(
            V1Node(
                metadata=V1ObjectMeta(name="default", labels={"hello": "world"}),
                status=V1NodeStatus(
                    node_info=V1NodeSystemInfo(
                        architecture="a",
                        boot_id="b",
                        kernel_version="k",
                        kube_proxy_version="1",
                        kubelet_version="1",
                        machine_id="1",
                        operating_system="os",
                        os_image="i",
                        system_uuid="u",
                        container_runtime_version="containerd",
                    )
                ),
            )
        )
        assert node.metadata.labels == {"hello": "world"}


class TestContainerResources:
    def test_add(self) -> None:
        resources1 = ContainerResources(
            cpu_m=1,
            memory=2 * 2**20,
            nvidia_gpu=3,
            amd_gpu=4,
            nvidia_migs={"1g.5gb": 1, "3g.20gb": 1},
        )
        resources2 = ContainerResources(
            cpu_m=4,
            memory=5 * 2**20,
            nvidia_gpu=6,
            amd_gpu=7,
            nvidia_migs={"1g.5gb": 1, "2g.10gb": 1},
        )
        assert resources1 + resources2 == ContainerResources(
            cpu_m=5,
            memory=7 * 2**20,
            nvidia_gpu=9,
            amd_gpu=11,
            nvidia_migs={"1g.5gb": 2, "2g.10gb": 1, "3g.20gb": 1},
        )

    def test_sub(self) -> None:
        total = ContainerResources(
            cpu_m=1000,
            memory=1024 * 2**20,
            nvidia_gpu=2,
            amd_gpu=4,
            nvidia_migs={"1g.5gb": 7, "2g.10gb": 1},
        )
        used = ContainerResources(
            cpu_m=100,
            memory=256 * 2**20,
            nvidia_gpu=1,
            amd_gpu=2,
            nvidia_migs={"1g.5gb": 1, "3g.20gb": 1},
        )
        assert total - used == ContainerResources(
            cpu_m=900,
            memory=768 * 2**20,
            nvidia_gpu=1,
            amd_gpu=2,
            nvidia_migs={"1g.5gb": 6, "2g.10gb": 1, "3g.20gb": 0},
        )

    def test_floordiv(self) -> None:
        total = NodeResources(
            cpu_m=1000,
            memory=1024 * 2**20,
            nvidia_gpu=2,
            amd_gpu=4,
            nvidia_migs={"1g.5gb": 7},
        )

        assert (
            total
            // ContainerResources(
                cpu_m=100,
                memory=128 * 2**20,
                nvidia_gpu=1,
                amd_gpu=2,
                nvidia_migs={"1g.5gb": 1},
            )
            == 2
        )
        assert total // ContainerResources(cpu_m=100, memory=128 * 2**20) == 8
        assert total // ContainerResources(cpu_m=100) == 10
        assert total // ContainerResources(cpu_m=1100) == 0
        assert total // ContainerResources(nvidia_migs={"1g.5gb": 1}) == 7
        assert total // ContainerResources(nvidia_migs={"1g.5gb": 8}) == 0
        assert total // ContainerResources(nvidia_migs={"1g.5gb": 1, "2g.10gb": 0}) == 7
        assert total // ContainerResources(nvidia_migs={"2g.10gb": 1}) == 0
        assert total // ContainerResources() == 110
        assert ContainerResources() // ContainerResources() == 0

    def test_from_primitive(self) -> None:
        resources = ContainerResources.from_primitive(
            {
                "cpu": "1",
                "memory": 2**30,
                "nvidia.com/gpu": 1,
                "nvidia.com/mig-1g.5gb": 2,
                "amd.com/gpu": 3,
            }
        )

        assert resources == ContainerResources(
            cpu_m=1000,
            memory=2**30,
            nvidia_gpu=1,
            nvidia_migs={"1g.5gb": 2},
            amd_gpu=3,
        )
