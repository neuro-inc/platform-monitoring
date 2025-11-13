from __future__ import annotations

import asyncio
import enum
import logging
import re
import typing as t
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, replace
from datetime import datetime

import aiohttp
from apolo_kube_client import (
    KubeClientProxy,
    KubeClientSelector,
    V1Container,
    V1ContainerStatus,
    V1Node,
    V1NodeStatus,
    V1NodeSystemInfo,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1PodStatus,
)
from apolo_kube_client.apolo import generate_namespace_name

from .base import JobStats, Telemetry
from .config import KubeConfig
from .utils import format_date, parse_date


logger = logging.getLogger(__name__)

DEFAULT_MAX_PODS_PER_NODE = 110

NVIDIA_DCGM_LABEL_SELECTOR = "app=nvidia-dcgm-exporter"

type JSON = dict[str, t.Any]


class JobException(Exception):
    pass


class JobError(JobException):
    pass


class JobNotFoundException(JobException):
    pass


@dataclass(frozen=True)
class Metadata:
    name: str | None
    resource_version: str | None = None
    labels: t.Mapping[str, str] = field(default_factory=dict)
    annotations: t.Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_model(cls, model: V1ObjectMeta) -> t.Self:
        return cls(
            name=model.name,
            resource_version=model.resource_version,
            labels=model.labels,
            annotations=model.annotations,
        )


class PodPhase(enum.StrEnum):
    PENDING = "Pending"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    UNKNOWN = "Unknown"


def parse_memory(memory: str) -> int:
    try:
        return int(memory)
    except ValueError:
        pass
    for suffix, power in (("K", 1), ("M", 2), ("G", 3), ("T", 4), ("P", 5)):
        if memory.endswith(suffix):
            return int(memory[:-1]) * 1000**power
        if memory.endswith(f"{suffix}i"):
            return int(memory[:-2]) * 1024**power
    msg = f"Memory unit for {memory} is not supported"
    raise ValueError(msg)


@dataclass(frozen=True)
class ContainerResources:
    cpu_m: int = 0
    memory: int = 0
    nvidia_gpu: int = 0
    nvidia_migs: Mapping[str, int] = field(default_factory=dict)
    amd_gpu: int = 0

    nvidia_gpu_key: t.ClassVar[str] = "nvidia.com/gpu"
    nvidia_mig_key_prefix: t.ClassVar[str] = "nvidia.com/mig-"
    amd_gpu_key: t.ClassVar[str] = "amd.com/gpu"

    @property
    def cpu(self) -> float:
        return self.cpu_m / 1000

    @classmethod
    def from_primitive(cls, payload: JSON) -> t.Self:
        nvidia_migs = {}
        for key, value in payload.items():
            if key.startswith(cls.nvidia_mig_key_prefix):
                profile_name = key[len(cls.nvidia_mig_key_prefix) :]
                nvidia_migs[profile_name] = int(value)
        return cls(
            cpu_m=cls._parse_cpu_m(str(payload.get("cpu", "0"))),
            memory=parse_memory(str(payload.get("memory", "0"))),
            nvidia_gpu=int(payload.get(cls.nvidia_gpu_key, 0)),
            nvidia_migs=nvidia_migs,
            amd_gpu=int(payload.get(cls.amd_gpu_key, 0)),
        )

    @classmethod
    def _parse_cpu_m(cls, value: str) -> int:
        if value.endswith("m"):
            return int(value[:-1])
        return int(float(value) * 1000)

    @property
    def has_nvidia_gpu(self) -> bool:
        return self.nvidia_gpu != 0

    @property
    def has_nvidia_migs(self) -> bool:
        return len(self.nvidia_migs) != 0

    @property
    def has_amd_gpu(self) -> bool:
        return self.amd_gpu != 0

    @property
    def has_gpu(self) -> bool:
        return self.has_nvidia_gpu or self.has_nvidia_migs or self.has_amd_gpu

    def __bool__(self) -> bool:
        return self.cpu_m != 0 or self.memory != 0 or self.has_gpu

    def __add__(self, other: ContainerResources) -> t.Self:
        nvidia_migs = defaultdict(int, **self.nvidia_migs)
        for key, count in other.nvidia_migs.items():
            nvidia_migs[key] = nvidia_migs[key] + count
        return replace(
            self,
            cpu_m=self.cpu_m + other.cpu_m,
            memory=self.memory + other.memory,
            nvidia_gpu=self.nvidia_gpu + other.nvidia_gpu,
            nvidia_migs=nvidia_migs,
            amd_gpu=self.amd_gpu + other.amd_gpu,
        )

    def __sub__(self, used: ContainerResources) -> t.Self:
        nvidia_migs = defaultdict(int, **self.nvidia_migs)
        for key, count in used.nvidia_migs.items():
            nvidia_migs[key] = max(0, nvidia_migs[key] - count)
        return replace(
            self,
            cpu_m=max(0, self.cpu_m - used.cpu_m),
            memory=max(0, self.memory - used.memory),
            nvidia_gpu=max(0, self.nvidia_gpu - used.nvidia_gpu),
            nvidia_migs=nvidia_migs,
            amd_gpu=max(0, self.amd_gpu - used.amd_gpu),
        )

    def __floordiv__(self, resources: ContainerResources) -> int:
        if not self:
            return 0
        counts = []
        if resources.cpu_m:
            counts.append(self.cpu_m // resources.cpu_m)
        if resources.memory:
            counts.append(self.memory // resources.memory)
        if resources.nvidia_gpu:
            counts.append(self.nvidia_gpu // resources.nvidia_gpu)
        if resources.nvidia_migs:
            for key, count in resources.nvidia_migs.items():
                if count:
                    counts.append(self.nvidia_migs.get(key, 0) // count)
        if resources.amd_gpu:
            counts.append(self.amd_gpu // resources.amd_gpu)
        if not counts:
            msg = "Cannot divide by zero resources"
            raise ValueError(msg)
        return min(counts)


class PodRestartPolicy(enum.StrEnum):
    ALWAYS = "Always"
    NEVER = "Never"
    ON_FAILURE = "OnFailure"


@dataclass(frozen=True)
class Container:
    name: str
    resource_requests: ContainerResources
    stdin: bool | None
    stdin_once: bool | None
    tty: bool | None

    @classmethod
    def from_model(cls, model: V1Container) -> t.Self:
        return cls(
            name=model.name,
            resource_requests=ContainerResources.from_primitive(
                model.resources.requests or {}
            ),
            stdin=model.stdin,
            stdin_once=model.stdin_once,
            tty=model.tty,
        )


@dataclass(frozen=True)
class PodSpec:
    node_name: str | None = None
    restart_policy: PodRestartPolicy = PodRestartPolicy.ALWAYS
    containers: t.Sequence[Container] = field(default_factory=list)

    @classmethod
    def from_model(cls, model: V1PodSpec) -> t.Self:
        return cls(
            node_name=model.node_name,
            restart_policy=PodRestartPolicy(model.restart_policy or cls.restart_policy),
            containers=[Container.from_model(c) for c in model.containers],
        )


@dataclass(frozen=True)
class PodStatus:
    phase: PodPhase
    pod_ip: str | None
    host_ip: str | None
    container_statuses: t.Sequence[ContainerStatus] = field(default_factory=list)

    @classmethod
    def from_model(cls, payload: V1PodStatus) -> t.Self:
        return cls(
            phase=PodPhase(payload.phase or PodPhase.PENDING.value),
            pod_ip=payload.pod_ip,
            host_ip=payload.host_ip,
            container_statuses=[
                ContainerStatus.from_model(s)
                for s in (payload.container_statuses or ())
            ],
        )

    @property
    def is_running(self) -> bool:
        return self.phase == PodPhase.RUNNING


@dataclass(frozen=True)
class Pod:
    metadata: Metadata
    spec: PodSpec
    status: PodStatus

    @classmethod
    def from_model(cls, model: V1Pod) -> t.Self:
        assert model.spec
        return cls(
            metadata=Metadata.from_model(model.metadata),
            spec=PodSpec.from_model(model.spec),
            status=PodStatus.from_model(model.status),
        )

    def get_container_status(self, name: str) -> ContainerStatus:
        for status in self.status.container_statuses:
            if status.name == name:
                break
        else:
            status = ContainerStatus(name=name)
        return status.with_pod_restart_policy(self.spec.restart_policy)

    def get_container_id(self, name: str) -> str | None:
        for status in self.status.container_statuses:
            if status.name == name:
                return status.container_id
        return None

    @property
    def resource_requests(self) -> ContainerResources:
        return sum(
            (c.resource_requests for c in self.spec.containers), ContainerResources()
        )

    @property
    def stdin(self) -> bool:
        for container in self.spec.containers:
            if container.stdin is not None:
                return container.stdin
        return False

    @property
    def stdin_once(self) -> bool:
        for container in self.spec.containers:
            if container.stdin_once is not None:
                return container.stdin_once
        return False

    @property
    def tty(self) -> bool:
        for container in self.spec.containers:
            if container.tty is not None:
                return container.tty
        return False


@dataclass(frozen=True)
class NodeResources(ContainerResources):
    pods: int = DEFAULT_MAX_PODS_PER_NODE
    ephemeral_storage: int = 0

    @classmethod
    def from_primitive(cls, payload: JSON) -> t.Self:
        resources = super().from_primitive(payload)
        return cls(
            **{
                **{
                    field.name: getattr(resources, field.name)
                    for field in fields(resources)
                },
                "pods": int(payload.get("pods", cls.pods)),
                "ephemeral_storage": parse_memory(
                    payload.get("ephemeral-storage", cls.ephemeral_storage)
                ),
            }
        )

    def reduce_pods(self, value: int) -> t.Self:
        return replace(self, pods=max(0, self.pods - value))

    def __floordiv__(self, resources: ContainerResources) -> int:
        if not resources:
            return self.pods
        count = super().__floordiv__(resources)
        return min(self.pods or count, count)


@dataclass(frozen=True)
class NodeStatus:
    @dataclass(frozen=True)
    class NodeInfo:
        container_runtime_version: str

        @classmethod
        def from_model(cls, model: V1NodeSystemInfo) -> t.Self:
            return cls(container_runtime_version=model.container_runtime_version)

    capacity: NodeResources
    allocatable: NodeResources
    node_info: NodeInfo

    @classmethod
    def from_model(cls, model: V1NodeStatus) -> t.Self:
        assert model.node_info, "node info must be present"
        return cls(
            capacity=NodeResources.from_primitive(model.capacity),
            allocatable=NodeResources.from_primitive(model.allocatable),
            node_info=cls.NodeInfo.from_model(model.node_info),
        )


@dataclass(frozen=True)
class Node:
    metadata: Metadata
    status: NodeStatus

    @classmethod
    def from_model(cls, model: V1Node) -> Node:
        return cls(
            metadata=Metadata.from_model(model.metadata),
            status=NodeStatus.from_model(model.status),
        )


@dataclass(frozen=True)
class ContainerStatus:
    name: str
    container_id: str | None = None
    restart_count: int = 0
    state: t.Mapping[str, t.Any] = field(default_factory=dict)
    last_state: t.Mapping[str, t.Any] = field(default_factory=dict)

    pod_restart_policy: PodRestartPolicy = PodRestartPolicy.ALWAYS

    @classmethod
    def from_model(cls, payload: V1ContainerStatus) -> t.Self:
        state: dict[str, t.Any] = {}
        last_state: dict[str, t.Any] = {}

        for prop, state_attr_name in (
            (state, "state"),
            (last_state, "last_state"),
        ):
            state_attr = getattr(payload, state_attr_name)
            if state_attr.running.started_at:
                prop["running"] = {
                    "startedAt": format_date(state_attr.running.started_at)
                }
            if state_attr.terminated:
                terminated = {"exitCode": state_attr.terminated.exit_code}
                if state_attr.terminated.started_at:
                    terminated["startedAt"] = format_date(
                        state_attr.terminated.started_at
                    )
                if state_attr.terminated.finished_at:
                    terminated["finishedAt"] = format_date(
                        state_attr.terminated.finished_at
                    )
                prop["terminated"] = terminated
            if state_attr.waiting.message or state_attr.waiting.reason:
                prop["waiting"] = True
        return cls(
            name=payload.name,
            container_id=(payload.container_id or "").replace("docker://", "") or None,
            restart_count=payload.restart_count or 0,
            state=state,
            last_state=last_state,
        )

    def with_pod_restart_policy(self, value: PodRestartPolicy) -> t.Self:
        return replace(self, pod_restart_policy=value)

    @property
    def is_waiting(self) -> bool:
        return "waiting" in self.state if self.state else True

    @property
    def is_running(self) -> bool:
        return "running" in self.state

    @property
    def is_terminated(self) -> bool:
        return "terminated" in self.state

    @property
    def is_pod_terminated(self) -> bool:
        return self.is_terminated and not self.can_restart

    @property
    def can_restart(self) -> bool:
        if self.pod_restart_policy == PodRestartPolicy.NEVER:
            return False
        if self.pod_restart_policy == PodRestartPolicy.ALWAYS:
            return True
        assert self.pod_restart_policy == PodRestartPolicy.ON_FAILURE
        try:
            return self.state["terminated"]["exitCode"] != 0
        except KeyError:
            return True

    @property
    def started_at(self) -> datetime | None:
        try:
            if self.is_running:
                date_str = self.state["running"]["startedAt"]
            else:
                date_str = self.state["terminated"]["startedAt"]
                if not date_str:
                    return None
        except KeyError:
            # waiting
            return None
        return parse_date(date_str)

    @property
    def finished_at(self) -> datetime | None:
        try:
            if self.is_terminated:
                date_str = self.state["terminated"]["finishedAt"]
            else:
                date_str = self.last_state["terminated"]["finishedAt"]
            if not date_str:
                return None
        except KeyError:
            # first run
            return None
        return parse_date(date_str)


@dataclass(frozen=True)
class PodContainerStats:
    cpu: float
    memory: float

    @classmethod
    def from_primitive(cls, payload: dict[str, t.Any]) -> PodContainerStats:
        cpu = payload.get("cpu", {}).get("usageNanoCores", 0) / (10**9)
        memory = payload.get("memory", {}).get("workingSetBytes", 0)
        return cls(cpu=cpu, memory=memory)


@dataclass(frozen=True)
class PodContainerGPUStats:
    utilization: int
    memory_used: int


GPU_COUNTER_RE = r"^(?P<name>[A-Z_]+)\s*\{(?P<labels>.+)\}\s+(?P<value>\d+)"


@dataclass(frozen=True)
class GPUCounter:
    name: str
    value: int
    labels: dict[str, str]


@dataclass(frozen=True)
class GPUCounters:
    counters: list[GPUCounter] = field(default_factory=list)

    @classmethod
    def parse(cls, text: str) -> GPUCounters:
        counters = []
        for m in re.finditer(GPU_COUNTER_RE, text, re.MULTILINE):
            groups = m.groupdict()
            labels = {}
            for label in groups["labels"].split(","):
                k, v = label.strip().split("=")
                labels[k.strip()] = v.strip().strip('"')
            counters.append(
                GPUCounter(
                    name=groups["name"],
                    value=int(groups["value"]),
                    labels=labels,
                )
            )
        return cls(counters=counters)

    def get_pod_container_stats(
        self, namespace_name: str, pod_name: str, container_name: str
    ) -> PodContainerGPUStats:
        gpu_count = 0
        utilization = 0
        memory_used = 0
        for c in self.counters:
            if (
                c.name not in ("DCGM_FI_DEV_GPU_UTIL", "DCGM_FI_DEV_FB_USED")
                or c.labels.get("namespace") != namespace_name
                or c.labels.get("pod") != pod_name
                or c.labels.get("container") != container_name
            ):
                continue
            if c.name == "DCGM_FI_DEV_GPU_UTIL":
                gpu_count += 1
                utilization += c.value
            if c.name == "DCGM_FI_DEV_FB_USED":
                memory_used += c.value * 2**20
        try:
            utilization = int(utilization / gpu_count)
        except ZeroDivisionError:
            utilization = 0
        return PodContainerGPUStats(utilization=utilization, memory_used=memory_used)


class StatsSummary:
    def __init__(self, payload: dict[str, t.Any]) -> None:
        self._validate_payload(payload)
        self._payload = payload

    @staticmethod
    def _validate_payload(payload: dict[str, t.Any]) -> None:
        if "pods" not in payload:
            err_msg = "Invalid stats summary response"
            logging.error("%s: `%s`", err_msg, payload)
            raise JobError(err_msg)

    @staticmethod
    def _find_pod_in_stats_summary(
        stats_summary: dict[str, t.Any], namespace_name: str, name: str
    ) -> dict[str, t.Any]:
        for pod_stats in stats_summary["pods"]:
            ref = pod_stats["podRef"]
            if ref["namespace"] == namespace_name and ref["name"] == name:
                return pod_stats
        return {}

    @staticmethod
    def _find_container_in_pod_stats(
        pod_stats: dict[str, t.Any], name: str
    ) -> dict[str, t.Any]:
        containers = pod_stats.get("containers") or []
        for container_stats in containers:
            if container_stats["name"] == name:
                return container_stats
        return {}

    def get_pod_container_stats(
        self, namespace_name: str, pod_name: str, container_name: str
    ) -> PodContainerStats | None:
        pod_stats = self._find_pod_in_stats_summary(
            self._payload, namespace_name, pod_name
        )
        if not pod_stats:
            return None

        container_stats = self._find_container_in_pod_stats(pod_stats, container_name)
        if not container_stats:
            return None

        return PodContainerStats.from_primitive(container_stats)


class TelemetryError(Exception): ...


class KubeTelemetry(Telemetry):
    _VCLUSTER_LABEL_NAMESPACE = "vcluster.loft.sh/namespace"
    _VCLUSTER_LABEL_OBJECT_NAME = "vcluster.loft.sh/object-name"
    _VCLUSTER_LABEL_HOST_OBJECT_NAME = "vcluster.loft.sh/object-host-name"

    def __init__(
        self,
        kube_client_selector: KubeClientSelector,
        namespace_name: str,
        org_name: str,
        project_name: str,
        pod_name: str,
        container_name: str,
        kubelet_node_port: int = KubeConfig.kubelet_node_port,
        nvidia_dcgm_node_port: int | None = KubeConfig.nvidia_dcgm_node_port,
    ) -> None:
        self._kube_client_selector = kube_client_selector
        self._org_name = org_name
        self._project_name = project_name
        self._namespace_name = namespace_name
        self._pod_name = pod_name
        self._container_name = container_name
        self._kubelet_port = kubelet_node_port
        self._nvidia_dcgm_port = nvidia_dcgm_node_port

    async def _get_real_names(
        self,
        kube_client: KubeClientProxy,
    ) -> tuple[str, str]:
        """
        Returns a pair of namespace and pod names based on a fact if a kube client
        is a vcluster-based or not.
        Whenever we need a telemetry from the vcluster pod, we need to obtain a
        real host namespace name, and a host pod name, to be able to find those
        names via node proxy API.
        """
        if not kube_client.is_vcluster:
            return self._namespace_name, self._pod_name

        real_namespace_name = generate_namespace_name(
            self._org_name, self._project_name
        )

        # fetch all vcluster pods
        pods = await self._kube_client_selector.host_client.core_v1.pod.get_list(
            label_selector=self._VCLUSTER_LABEL_NAMESPACE,
            namespace=real_namespace_name,
        )
        real_pod_name = None
        for pod in pods.items:
            metadata = pod.metadata
            assert metadata, "pod must have a metadata"
            vcluster_object_name = metadata.annotations.get(
                self._VCLUSTER_LABEL_OBJECT_NAME
            )
            if not vcluster_object_name:
                continue
            if vcluster_object_name != self._pod_name:
                continue
            real_pod_name = metadata.annotations[self._VCLUSTER_LABEL_HOST_OBJECT_NAME]
            break

        if not real_pod_name:
            err_message = "Unable to obtain a telemetry"
            logger.error(
                err_message,
                extra={
                    "pod_name": self._pod_name,
                    "namespace_name": self._namespace_name,
                    "real_pod_name": real_pod_name,
                    "real_namespace": real_namespace_name,
                    "pods_found": len(pods.items),
                },
            )
            raise TelemetryError(err_message)

        return real_namespace_name, real_pod_name

    async def get_latest_stats(self) -> JobStats | None:
        async with self._kube_client_selector.get_client(
            org_name=self._org_name,
            project_name=self._project_name,
        ) as kube_client:
            pod = await get_pod(kube_client, self._pod_name)
            if not pod.spec.node_name:
                return None
            if pod.resource_requests.has_gpu:
                return await self._get_latest_gpu_pod_stats(kube_client, pod)
            return await self._get_latest_cpu_pod_stats(kube_client, pod)

    async def _get_latest_cpu_pod_stats(
        self,
        kube_client: KubeClientProxy,
        pod: Pod,
    ) -> JobStats | None:
        assert pod.spec.node_name, "pod must be scheduled on a node"
        node_name = pod.spec.node_name
        namespace_name, pod_name = await self._get_real_names(kube_client)
        pod_stats = await self._get_pod_container_stats(
            node_name=node_name,
            pod_name=pod_name,
            container_name=self._container_name,
            namespace=namespace_name,
        )
        if not pod_stats:
            return None
        return JobStats(cpu=pod_stats.cpu, memory=pod_stats.memory)

    async def _get_latest_gpu_pod_stats(
        self,
        kube_client: KubeClientProxy,
        pod: Pod,
    ) -> JobStats | None:
        assert pod.spec.node_name, "pod must be scheduled on a node"
        node_name = pod.spec.node_name
        namespace_name, pod_name = await self._get_real_names(kube_client)
        pod_stats_task = asyncio.create_task(
            self._get_pod_container_stats(
                node_name=node_name,
                pod_name=pod_name,
                container_name=self._container_name,
                namespace=namespace_name,
            )
        )
        pod_gpu_stats_task = asyncio.create_task(
            self._get_pod_container_gpu_stats(
                node_name=node_name,
                pod_name=pod_name,
                container_name=self._container_name,
                namespace=namespace_name,
            )
        )
        await asyncio.wait(
            (pod_stats_task, pod_gpu_stats_task), return_when=asyncio.FIRST_EXCEPTION
        )
        pod_stats = pod_stats_task.result()
        pod_gpu_stats = pod_gpu_stats_task.result()
        if not pod_stats:
            return None
        if not pod_gpu_stats:
            return JobStats(cpu=pod_stats.cpu, memory=pod_stats.memory)
        return JobStats(
            cpu=pod_stats.cpu,
            memory=pod_stats.memory,
            gpu_utilization=pod_gpu_stats.utilization,
            gpu_memory_used=pod_gpu_stats.memory_used,
        )

    async def _get_pod_container_stats(
        self,
        node_name: str,
        pod_name: str,
        container_name: str,
        namespace: str,
    ) -> PodContainerStats | None:
        kube_core = self._kube_client_selector.host_client.core
        url = (
            f"{kube_core.base_url}"
            f"/api/v1/nodes/{node_name}:{self._kubelet_port}/proxy/stats/summary"
        )
        try:
            async with kube_core.request(method="GET", url=url) as response:
                data = await response.json()
                summary = StatsSummary(data)
                return summary.get_pod_container_stats(
                    namespace, pod_name, container_name
                )
        except aiohttp.ClientError:
            return None

    async def _get_pod_container_gpu_stats(
        self,
        node_name: str,
        pod_name: str,
        container_name: str,
        namespace: str,
    ) -> PodContainerGPUStats | None:
        if not self._nvidia_dcgm_port:
            return None
        nvidia_dcgm_pod_ip = await self._get_nvidia_dcgm_pod_ip(node_name)
        logger.debug("Nvidia DCGM exporter pod ip: %s", nvidia_dcgm_pod_ip)
        if not nvidia_dcgm_pod_ip:
            return None
        url = f"http://{nvidia_dcgm_pod_ip}:{self._nvidia_dcgm_port}/metrics"
        logger.debug("Nvidia DCGM exporter metrics url: %s", url)
        kube_core = self._kube_client_selector.host_client.core
        try:
            async with kube_core.request(method="GET", url=url) as response:
                text = await response.text()
            gpu_counters = GPUCounters.parse(text)
            return gpu_counters.get_pod_container_stats(
                namespace, pod_name, container_name
            )
        except aiohttp.ClientError as e:
            logger.exception(e)
        return None

    async def _get_nvidia_dcgm_pod_ip(self, node_name: str) -> str | None:
        pods = await self._kube_client_selector.host_client.core_v1.pod.get_list(
            label_selector=NVIDIA_DCGM_LABEL_SELECTOR,
            field_selector=f"spec.nodeName={node_name}",
            all_namespaces=True,
        )
        if len(pods.items) != 1:
            return None
        try:
            return pods.items[0].status.pod_ip
        except AttributeError:
            return None


async def get_pod(
    kube_client: KubeClientProxy,
    name: str,
) -> Pod:
    pod = await kube_client.core_v1.pod.get(name)
    return Pod.from_model(pod)


async def get_container_status(
    kube_client: KubeClientProxy,
    name: str,
    container_name: str | None = None,
) -> ContainerStatus:
    pod = await get_pod(kube_client, name)
    container_name = container_name or name
    return pod.get_container_status(container_name)


async def wait_pod_is_not_waiting(
    kube_client: KubeClientProxy,
    pod_name: str,
    *,
    container_name: str | None = None,
    timeout_s: float = 10.0 * 60,
    interval_s: float = 1.0,
) -> ContainerStatus:
    """Wait until the pod transitions from the waiting state.
    Raise JobNotFoundException if there is no such pod.
    Raise asyncio.TimeoutError if it takes too long for the pod.
    """
    container_name = container_name or pod_name
    async with asyncio.timeout(timeout_s):
        while True:
            pod = await get_pod(kube_client, pod_name)
            status = pod.get_container_status(container_name)
            if not status.is_waiting:
                return status
            await asyncio.sleep(interval_s)


async def wait_pod_is_running(
    kube_client: KubeClientProxy,
    pod_name: str,
    *,
    container_name: str | None = None,
    timeout_s: float = 10.0 * 60,
    interval_s: float = 1.0,
) -> ContainerStatus:
    """Wait until the pod transitions to the running state.
    Raise JobNotFoundException if there is no such pod.
    Raise asyncio.TimeoutError if it takes too long for the pod.
    """
    container_name = container_name or pod_name
    async with asyncio.timeout(timeout_s):
        while True:
            pod = await get_pod(kube_client, pod_name)
            status = pod.get_container_status(container_name)
            if status.is_running:
                return status
            if status.is_terminated and not status.can_restart:
                raise JobNotFoundException
            await asyncio.sleep(interval_s)
