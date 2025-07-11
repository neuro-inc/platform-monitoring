from __future__ import annotations

import asyncio
import enum
import json
import logging
import re
import ssl
import typing as t
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus, urlsplit

import aiohttp
from aiohttp import ContentTypeError

from .base import JobStats, Telemetry
from .config import KubeClientAuthType, KubeConfig
from .utils import format_date, parse_date


logger = logging.getLogger(__name__)

DEFAULT_MAX_PODS_PER_NODE = 110

type JSON = dict[str, t.Any]


class KubeClientException(Exception):
    pass


class KubeClientUnauthorizedException(KubeClientException):
    pass


class ExpiredException(KubeClientException):
    pass


class ResourceGoneException(KubeClientException):
    pass


class ConflictException(KubeClientException):
    pass


class JobException(Exception):
    pass


class JobError(JobException):
    pass


class JobNotFoundException(JobException):
    pass


class Resource(t.Protocol):
    @classmethod
    def from_primitive(cls, payload: JSON) -> t.Self: ...


@dataclass(frozen=True)
class Metadata:
    name: str | None
    resource_version: str | None = None
    labels: t.Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_primitive(cls, payload: JSON) -> Metadata:
        return cls(
            name=payload.get("name"),
            resource_version=payload.get("resourceVersion"),
            labels=payload.get("labels", {}),
        )


@dataclass(frozen=True)
class ListResult[TResource: Resource]:
    metadata: Metadata
    items: list[TResource]

    @classmethod
    def from_primitive(
        cls, payload: JSON, *, resource_cls: type[TResource]
    ) -> ListResult[TResource]:
        return cls(
            metadata=Metadata.from_primitive(payload.get("metadata", {})),
            items=[
                resource_cls.from_primitive(item) for item in payload.get("items", ())
            ],
        )


class PodPhase(enum.StrEnum):
    PENDING = "Pending"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    UNKNOWN = "Unknown"


@dataclass(frozen=True)
class ContainerResources:
    cpu_m: int = 0
    memory: int = 0
    nvidia_gpu: int = 0
    amd_gpu: int = 0

    nvidia_gpu_key: t.ClassVar[str] = "nvidia.com/gpu"
    amd_gpu_key: t.ClassVar[str] = "amd.com/gpu"

    @classmethod
    def from_primitive(cls, payload: JSON) -> t.Self:
        return cls(
            cpu_m=cls._parse_cpu_m(str(payload.get("cpu", "0"))),
            memory=cls._parse_memory(str(payload.get("memory", "0"))),
            nvidia_gpu=int(payload.get(cls.nvidia_gpu_key, 0)),
            amd_gpu=int(payload.get(cls.amd_gpu_key, 0)),
        )

    @classmethod
    def _parse_cpu_m(cls, value: str) -> int:
        if value.endswith("m"):
            return int(value[:-1])
        return int(float(value) * 1000)

    @classmethod
    def _parse_memory(cls, memory: str) -> int:
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
        raise KubeClientException(msg)

    @property
    def has_gpu(self) -> bool:
        return self.nvidia_gpu != 0 or self.amd_gpu != 0

    def __bool__(self) -> bool:
        return (
            self.cpu_m != 0
            or self.memory != 0
            or self.nvidia_gpu != 0
            or self.amd_gpu != 0
        )

    def __add__(self, other: ContainerResources) -> t.Self:
        return replace(
            self,
            cpu_m=self.cpu_m + other.cpu_m,
            memory=self.memory + other.memory,
            nvidia_gpu=self.nvidia_gpu + other.nvidia_gpu,
            amd_gpu=self.amd_gpu + other.amd_gpu,
        )

    def __sub__(self, used: ContainerResources) -> t.Self:
        return replace(
            self,
            cpu_m=max(0, self.cpu_m - used.cpu_m),
            memory=max(0, self.memory - used.memory),
            nvidia_gpu=max(0, self.nvidia_gpu - used.nvidia_gpu),
            amd_gpu=max(0, self.amd_gpu - used.amd_gpu),
        )

    def __floordiv__(self, resources: ContainerResources) -> int:
        if not self:
            return 0
        result = DEFAULT_MAX_PODS_PER_NODE
        if resources.cpu_m:
            result = min(result, self.cpu_m // resources.cpu_m)
        if resources.memory:
            result = min(result, self.memory // resources.memory)
        if resources.nvidia_gpu:
            result = min(result, self.nvidia_gpu // resources.nvidia_gpu)
        if resources.amd_gpu:
            result = min(result, self.amd_gpu // resources.amd_gpu)
        return result


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
    def from_primitive(cls, payload: JSON) -> t.Self:
        return cls(
            name=payload["name"],
            resource_requests=ContainerResources.from_primitive(
                payload.get("resources", {}).get("requests", {})
            ),
            stdin=payload.get("stdin"),
            stdin_once=payload.get("stdinOnce"),
            tty=payload.get("tty"),
        )


@dataclass(frozen=True)
class PodSpec:
    node_name: str | None = None
    restart_policy: PodRestartPolicy = PodRestartPolicy.ALWAYS
    containers: t.Sequence[Container] = field(default_factory=list)

    @classmethod
    def from_primitive(cls, payload: JSON) -> t.Self:
        return cls(
            node_name=payload.get("nodeName"),
            restart_policy=PodRestartPolicy(
                payload.get("restartPolicy", cls.restart_policy)
            ),
            containers=[
                Container.from_primitive(c) for c in payload.get("containers", ())
            ],
        )


@dataclass(frozen=True)
class PodStatus:
    phase: PodPhase
    pod_ip: str | None
    host_ip: str | None
    container_statuses: t.Sequence[ContainerStatus] = field(default_factory=list)

    @classmethod
    def from_primitive(cls, payload: JSON) -> t.Self:
        return cls(
            phase=PodPhase(payload.get("phase", PodPhase.PENDING.value)),
            pod_ip=payload.get("podIP") or None,
            host_ip=payload.get("hostIP") or None,
            container_statuses=[
                ContainerStatus.from_primitive(s)
                for s in payload.get("containerStatuses", ())
            ],
        )

    @property
    def is_running(self) -> bool:
        return self.phase == PodPhase.RUNNING


@dataclass(frozen=True)
class Pod(Resource):
    metadata: Metadata
    spec: PodSpec
    status: PodStatus

    @classmethod
    def from_primitive(cls, payload: JSON) -> t.Self:
        return cls(
            metadata=Metadata.from_primitive(payload["metadata"]),
            spec=PodSpec.from_primitive(payload["spec"]),
            status=PodStatus.from_primitive(payload.get("status", {})),
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
                **asdict(resources),
                "pods": int(payload.get("pods", cls.pods)),
                "ephemeral_storage": cls._parse_memory(
                    payload.get("ephemeral-storage", cls.ephemeral_storage)
                ),
            }
        )

    def with_pods(self, value: int) -> t.Self:
        return replace(self, pods=max(0, value))


@dataclass(frozen=True)
class NodeStatus:
    @dataclass(frozen=True)
    class NodeInfo:
        container_runtime_version: str

        @classmethod
        def from_primitive(cls, payload: JSON) -> t.Self:
            return cls(
                container_runtime_version=payload["containerRuntimeVersion"],
            )

    capacity: NodeResources
    allocatable: NodeResources
    node_info: NodeInfo

    @classmethod
    def from_primitive(cls, payload: JSON) -> t.Self:
        return cls(
            capacity=NodeResources.from_primitive(payload.get("capacity", {})),
            allocatable=NodeResources.from_primitive(payload.get("allocatable", {})),
            node_info=cls.NodeInfo.from_primitive(payload["nodeInfo"]),
        )


@dataclass(frozen=True)
class Node(Resource):
    metadata: Metadata
    status: NodeStatus

    @classmethod
    def from_primitive(cls, payload: JSON) -> Node:
        return cls(
            metadata=Metadata.from_primitive(payload["metadata"]),
            status=NodeStatus.from_primitive(payload["status"]),
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
    def from_primitive(cls, payload: JSON) -> t.Self:
        return cls(
            name=payload["name"],
            container_id=payload.get("containerID", "").replace("docker://", "")
            or None,
            restart_count=payload.get("restartCount", 0),
            state=payload.get("state", {}),
            last_state=payload.get("lastState", {}),
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


class KubeClient:
    def __init__(
        self,
        *,
        base_url: str,
        namespace: str,
        cert_authority_path: str | None = None,
        cert_authority_data_pem: str | None = None,
        auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE,
        auth_cert_path: str | None = None,
        auth_cert_key_path: str | None = None,
        token: str | None = None,
        token_path: str | None = None,
        token_update_interval_s: int = 300,
        conn_timeout_s: int = 300,
        read_timeout_s: int = 100,
        conn_pool_size: int = 100,
        kubelet_node_port: int = KubeConfig.kubelet_node_port,
        nvidia_dcgm_node_port: int | None = KubeConfig.nvidia_dcgm_node_port,
        trace_configs: list[aiohttp.TraceConfig] | None = None,
    ) -> None:
        self._base_url = base_url
        self._namespace = namespace

        self._cert_authority_data_pem = cert_authority_data_pem
        self._cert_authority_path = cert_authority_path

        self._auth_type = auth_type
        self._auth_cert_path = auth_cert_path
        self._auth_cert_key_path = auth_cert_key_path
        self._token = token
        self._token_path = token_path
        self._token_update_interval_s = token_update_interval_s

        self._conn_timeout_s = conn_timeout_s
        self._read_timeout_s = read_timeout_s
        self._conn_pool_size = conn_pool_size

        self._kubelet_port = kubelet_node_port
        self._nvidia_dcgm_port = nvidia_dcgm_node_port

        self._trace_configs = trace_configs

        self._client: aiohttp.ClientSession | None = None
        self._token_updater_task: asyncio.Task[None] | None = None

    @property
    def _is_ssl(self) -> bool:
        return urlsplit(self._base_url).scheme == "https"

    def _create_ssl_context(self) -> bool | ssl.SSLContext:
        if not self._is_ssl:
            return True
        ssl_context = ssl.create_default_context(
            cafile=self._cert_authority_path, cadata=self._cert_authority_data_pem
        )
        if self._auth_type == KubeClientAuthType.CERTIFICATE:
            ssl_context.load_cert_chain(
                self._auth_cert_path,  # type: ignore
                self._auth_cert_key_path,
            )
        return ssl_context

    async def init(self) -> None:
        connector = aiohttp.TCPConnector(
            limit=self._conn_pool_size, ssl=self._create_ssl_context()
        )
        if self._token_path:
            self._token = Path(self._token_path).read_text()
            self._token_updater_task = asyncio.create_task(self._start_token_updater())
        timeout = aiohttp.ClientTimeout(
            connect=self._conn_timeout_s, total=self._read_timeout_s
        )
        self._client = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            trace_configs=self._trace_configs,
        )

    async def _start_token_updater(self) -> None:
        if not self._token_path:
            return
        while True:
            try:
                token = Path(self._token_path).read_text()
                if token != self._token:
                    self._token = token
                    logger.info("Kube token was refreshed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Failed to update kube token: %s", exc)
            await asyncio.sleep(self._token_update_interval_s)

    @property
    def namespace(self) -> str:
        return self._namespace

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None
        if self._token_updater_task:
            self._token_updater_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._token_updater_task
            self._token_updater_task = None

    async def __aenter__(self) -> t.Self:
        await self.init()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    @property
    def _api_v1_url(self) -> str:
        return f"{self._base_url}/api/v1"

    @property
    def _nodes_url(self) -> str:
        return f"{self._api_v1_url}/nodes"

    def _generate_node_url(self, name: str) -> str:
        return f"{self._nodes_url}/{name}"

    def _generate_namespace_url(self, namespace_name: str) -> str:
        return f"{self._api_v1_url}/namespaces/{namespace_name}"

    def _namespace_url(self, namespace: str | None) -> str:
        return self._generate_namespace_url(namespace or self._namespace)

    @property
    def _pods_url(self) -> str:
        return f"{self._api_v1_url}/pods"

    def _namespaced_pods_url(self, namespace: str | None) -> str:
        return f"{self._namespace_url(namespace)}/pods"

    def _generate_pod_url(self, pod_name: str, namespace: str | None) -> str:
        return f"{self._namespaced_pods_url(namespace)}/{pod_name}"

    def _generate_node_proxy_url(self, name: str, port: int) -> str:
        return f"{self._api_v1_url}/nodes/{name}:{port}/proxy"

    def _generate_node_stats_summary_url(self, name: str) -> str:
        proxy_url = self._generate_node_proxy_url(name, self._kubelet_port)
        return f"{proxy_url}/stats/summary"

    def _generate_node_gpu_metrics_url(self, name: str) -> str | None:
        if not self._nvidia_dcgm_port:
            return None
        proxy_url = self._generate_node_proxy_url(name, self._nvidia_dcgm_port)
        return f"{proxy_url}/metrics"

    def _generate_pod_log_url(
        self, pod_name: str, container_name: str, namespace: str | None
    ) -> str:
        url = self._generate_pod_url(pod_name, namespace)
        return f"{url}/log?container={container_name}&follow=true"

    def _create_headers(
        self, headers: dict[str, t.Any] | None = None
    ) -> dict[str, t.Any]:
        headers = dict(headers) if headers else {}
        if self._auth_type == KubeClientAuthType.TOKEN and self._token:
            headers["Authorization"] = "Bearer " + self._token
        return headers

    async def _request(self, *args: t.Any, **kwargs: t.Any) -> dict[str, t.Any]:
        headers = self._create_headers(kwargs.pop("headers", None))
        assert self._client, "client is not initialized"
        async with self._client.request(*args, headers=headers, **kwargs) as response:
            await self._check_response_status(response)
            payload = await response.json()
            logger.debug("k8s response payload: %s", payload)
            return payload

    async def get_raw_pod(
        self, pod_name: str, namespace: str | None = None
    ) -> dict[str, t.Any]:
        namespace = namespace or self._namespace
        url = self._generate_pod_url(pod_name, namespace)
        payload = await self._request(method="GET", url=url)
        self._assert_resource_kind(
            expected_kind="Pod", payload=payload, job_id=pod_name
        )
        return payload

    async def get_pod(self, pod_name: str, namespace: str | None = None) -> Pod:
        namespace = namespace or self._namespace
        payload = await self.get_raw_pod(pod_name, namespace)
        return Pod.from_primitive(payload)

    async def _get_raw_container_state(
        self,
        pod_name: str,
        container_name: str | None = None,
        namespace: str | None = None,
    ) -> dict[str, t.Any]:
        namespace = namespace or self._namespace
        pod = await self.get_pod(pod_name, namespace)
        container_name = container_name or pod_name
        container_status = pod.get_container_status(container_name)
        return {**container_status.state}

    async def get_container_status(
        self, name: str, container_name: str | None = None, namespace: str | None = None
    ) -> ContainerStatus:
        namespace = namespace or self._namespace
        pod = await self.get_pod(name, namespace)
        container_name = container_name or name
        return pod.get_container_status(container_name)

    async def wait_pod_is_running(
        self,
        pod_name: str,
        *,
        namespace: str | None = None,
        container_name: str | None = None,
        timeout_s: float = 10.0 * 60,
        interval_s: float = 1.0,
    ) -> ContainerStatus:
        """Wait until the pod transitions to the running state.

        Raise JobNotFoundException if there is no such pod.
        Raise asyncio.TimeoutError if it takes too long for the pod.
        """
        if not container_name:
            container_name = pod_name
        async with asyncio.timeout(timeout_s):
            while True:
                status = await self.get_container_status(
                    pod_name, container_name, namespace
                )
                if status.is_running:
                    return status
                if status.is_terminated and not status.can_restart:
                    raise JobNotFoundException
                await asyncio.sleep(interval_s)

    async def wait_pod_is_not_waiting(
        self,
        pod_name: str,
        *,
        namespace: str | None = None,
        container_name: str | None = None,
        timeout_s: float = 10.0 * 60,
        interval_s: float = 1.0,
    ) -> ContainerStatus:
        """Wait until the pod transitions from the waiting state.

        Raise JobNotFoundException if there is no such pod.
        Raise asyncio.TimeoutError if it takes too long for the pod.
        """
        if not container_name:
            container_name = pod_name
        async with asyncio.timeout(timeout_s):
            while True:
                status = await self.get_container_status(
                    pod_name, container_name, namespace
                )
                if not status.is_waiting:
                    return status
                await asyncio.sleep(interval_s)

    async def get_pod_container_stats(
        self,
        node_name: str,
        pod_name: str,
        container_name: str,
        namespace: str | None = None,
    ) -> PodContainerStats | None:
        """
        https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/apis/stats/v1alpha1/types.go
        """
        try:
            namespace = namespace or self._namespace
            url = self._generate_node_stats_summary_url(node_name)
            payload = await self._request(method="GET", url=url)
            summary = StatsSummary(payload)
            return summary.get_pod_container_stats(namespace, pod_name, container_name)
        except JobNotFoundException:
            return None
        except ContentTypeError as exc:
            logger.info("Failed to parse response", exc, exc_info=True)
            return None

    async def get_pod_container_gpu_stats(
        self,
        node_name: str,
        pod_name: str,
        container_name: str,
        namespace: str | None = None,
    ) -> PodContainerGPUStats | None:
        namespace = namespace or self._namespace
        url = self._generate_node_gpu_metrics_url(node_name)
        if not url:
            return None
        try:
            assert self._client
            async with self._client.get(
                url, headers=self._create_headers(), raise_for_status=True
            ) as resp:
                text = await resp.text()
                gpu_counters = GPUCounters.parse(text)
            return gpu_counters.get_pod_container_stats(
                namespace, pod_name, container_name
            )
        except aiohttp.ClientError as e:
            logger.exception(e)
        return None

    async def check_pod_exists(self, pod_name: str) -> bool:
        try:
            await self.get_raw_pod(pod_name)
            return True
        except JobNotFoundException:
            return False

    @asynccontextmanager
    async def create_pod_container_logs_stream(
        self,
        pod_name: str,
        container_name: str,
        namespace: str,
        *,
        conn_timeout_s: float = 60 * 5,
        read_timeout_s: float = 60 * 30,
        previous: bool = False,
        since: datetime | None = None,
        timestamps: bool = False,
    ) -> AsyncIterator[aiohttp.StreamReader]:
        url = self._generate_pod_log_url(pod_name, container_name, namespace)
        if previous:
            url = f"{url}&previous=true"
        if since is not None:
            since_str = quote_plus(format_date(since))
            url = f"{url}&sinceTime={since_str}"
        if timestamps:
            url = f"{url}&timestamps=true"
        client_timeout = aiohttp.ClientTimeout(
            connect=conn_timeout_s, sock_read=read_timeout_s
        )
        assert self._client
        async with self._client.get(
            url, headers=self._create_headers(), timeout=client_timeout
        ) as response:
            await self._check_response_status(response, job_id=pod_name)
            yield response.content

    async def get_pods(
        self,
        *,
        namespace: str | None = None,
        all_namespaces: bool = False,
        label_selector: str | None = None,
        field_selector: str | None = None,
    ) -> list[Pod]:
        if not namespace and not all_namespaces:
            exc_txt = "Cannot get pods without namespace or all_namespaces"
            raise Exception(exc_txt)
        url = self._pods_url if all_namespaces else self._namespaced_pods_url(namespace)
        params = {}
        if label_selector:
            params["labelSelector"] = label_selector
        if field_selector:
            params["fieldSelector"] = field_selector
        payload = await self._request(method="get", url=url, params=params)
        self._assert_resource_kind("PodList", payload)
        pod_list = ListResult.from_primitive(payload, resource_cls=Pod)
        return pod_list.items

    async def get_node(self, name: str) -> Node:
        payload = await self._request(method="get", url=self._generate_node_url(name))
        self._assert_resource_kind("Node", payload)
        return Node.from_primitive(payload)

    async def get_nodes(self, *, label_selector: str | None = None) -> list[Node]:
        params = None
        if label_selector:
            params = {"labelSelector": label_selector}
        payload = await self._request(method="get", url=self._nodes_url, params=params)
        self._assert_resource_kind("NodeList", payload)
        node_list = ListResult.from_primitive(payload, resource_cls=Node)
        return node_list.items

    async def _check_response_status(
        self, response: aiohttp.ClientResponse, job_id: str | None = None
    ) -> None:
        if not 200 <= response.status < 300:
            payload = await response.text()
            try:
                pod = json.loads(payload)
            except ValueError:
                pod = {"code": response.status, "message": payload}
            self._raise_for_status(pod, job_id=job_id)

    def _assert_resource_kind(
        self, expected_kind: str, payload: JSON, job_id: str | None = None
    ) -> None:
        kind = payload["kind"]
        if kind == "Status":
            self._raise_for_status(payload, job_id=job_id)
        elif kind != expected_kind:
            msg = f"unknown kind: {kind}"
            raise ValueError(msg)

    @staticmethod
    def _raise_for_status(payload: JSON, job_id: str | None = None) -> t.NoReturn:  # noqa: C901
        code = payload["code"]
        reason = payload.get("reason")
        if code == 400 and job_id:
            if "ContainerCreating" in payload["message"]:
                msg = f"Job '{job_id}' has not been created yet"
                raise JobNotFoundException(msg)
            if "is not available" in payload["message"]:
                msg = f"Job '{job_id}' is not available"
                raise JobNotFoundException(msg)
            if "is terminated" in payload["message"]:
                msg = f"Job '{job_id}' is terminated"
                raise JobNotFoundException(msg)
        elif code == 401:
            raise KubeClientUnauthorizedException(payload)
        elif code == 404 and job_id:
            msg = f"Job '{job_id}' not found"
            raise JobNotFoundException(msg)
        elif code == 404:
            raise JobNotFoundException(payload)
        elif code == 409 and job_id:
            msg = f"Job '{job_id}' already exists"
            raise JobError(msg)
        elif code == 409:
            raise ConflictException(payload)
        elif code == 410:
            raise ResourceGoneException(payload)
        elif code == 422:
            msg = f"Cannot create job with id '{job_id}'"
            raise JobError(msg)
        elif reason == "Expired":
            raise ExpiredException(payload)
        raise KubeClientException(payload)


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
                c.labels["namespace"] != namespace_name
                or c.labels["pod"] != pod_name
                or c.labels["container"] != container_name
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


class KubeTelemetry(Telemetry):
    def __init__(
        self,
        kube_client: KubeClient,
        namespace_name: str,
        pod_name: str,
        container_name: str,
    ) -> None:
        self._kube_client = kube_client

        self._namespace_name = namespace_name
        self._pod_name = pod_name
        self._container_name = container_name

    async def get_latest_stats(self) -> JobStats | None:
        pod = await self._kube_client.get_pod(
            self._pod_name,
            self._namespace_name,
        )
        if not pod.spec.node_name:
            return None
        if pod.resource_requests.has_gpu:
            return await self._get_latest_gpu_pod_stats(pod.spec.node_name)
        return await self._get_latest_cpu_pod_stats(pod.spec.node_name)

    async def _get_latest_cpu_pod_stats(self, node_name: str) -> JobStats | None:
        pod_stats = await self._kube_client.get_pod_container_stats(
            node_name,
            self._pod_name,
            self._container_name,
            namespace=self._namespace_name,
        )
        if not pod_stats:
            return None
        return JobStats(cpu=pod_stats.cpu, memory=pod_stats.memory)

    async def _get_latest_gpu_pod_stats(self, node_name: str) -> JobStats | None:
        pod_stats_task = asyncio.create_task(
            self._kube_client.get_pod_container_stats(
                node_name,
                self._pod_name,
                self._container_name,
                namespace=self._namespace_name,
            )
        )
        pod_gpu_stats_task = asyncio.create_task(
            self._kube_client.get_pod_container_gpu_stats(
                node_name,
                self._pod_name,
                self._container_name,
                namespace=self._namespace_name,
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
