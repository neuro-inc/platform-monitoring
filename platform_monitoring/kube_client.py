import asyncio
import enum
import json
import logging
import re
import ssl
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote_plus, urlsplit

import aiohttp
from aiohttp import ContentTypeError
from async_timeout import timeout
from yarl import URL

from .base import JobStats, Telemetry
from .config import KubeClientAuthType, KubeConfig
from .utils import format_date, parse_date

logger = logging.getLogger(__name__)

DEFAULT_MAX_PODS_PER_NODE = 110


class KubeClientException(Exception):
    pass


class KubeClientUnauthorized(KubeClientException):
    pass


class JobException(Exception):
    pass


class JobError(JobException):
    pass


class JobNotFoundException(JobException):
    pass


class PodPhase(str, enum.Enum):
    PENDING = "Pending"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    UNKNOWN = "Unknown"


@dataclass(frozen=True)
class Resources:
    cpu_m: int = 0
    memory: int = 0
    gpu: int = 0

    def add(self, other: "Resources") -> "Resources":
        return self.__class__(
            cpu_m=self.cpu_m + other.cpu_m,
            memory=self.memory + other.memory,
            gpu=self.gpu + other.gpu,
        )

    def available(self, used: "Resources") -> "Resources":
        """Get amount of unused resources.

        Returns:
            Resources: the difference between resources in {self} and {used}
        """
        return self.__class__(
            cpu_m=max(0, self.cpu_m - used.cpu_m),
            memory=max(0, self.memory - used.memory),
            gpu=max(0, self.gpu - used.gpu),
        )

    def count(self, resources: "Resources") -> int:
        """Get the number of times a client can be provided
        with the specified resources.

        Returns:
            int: count
        """
        if self.cpu_m == 0 and self.memory == 0 and self.gpu == 0:
            return 0
        result = DEFAULT_MAX_PODS_PER_NODE
        if resources.cpu_m:
            result = min(result, self.cpu_m // resources.cpu_m)
        if resources.memory:
            result = min(result, self.memory // resources.memory)
        if resources.gpu:
            result = min(result, self.gpu // resources.gpu)
        return result


@dataclass(frozen=True)
class ProxyClient:
    url: URL
    session: aiohttp.ClientSession


class Pod:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    @property
    def name(self) -> str:
        return self._payload["metadata"]["name"]

    @property
    def node_name(self) -> Optional[str]:
        return self._payload["spec"].get("nodeName")

    @property
    def restart_policy(self) -> str:
        return self._payload["spec"].get("restartPolicy") or "Never"

    @property
    def _status_payload(self) -> dict[str, Any]:
        payload = self._payload.get("status")
        if not payload:
            raise ValueError("Missing pod status")
        return payload

    def get_container_status(self, name: str) -> dict[str, Any]:
        for payload in self._status_payload.get("containerStatuses", []):
            if payload["name"] == name:
                return payload
        return {}

    def get_container_id(self, name: str) -> Optional[str]:
        id_ = self.get_container_status(name).get("containerID", "")
        # NOTE: URL(id_).host is failing because the container id is too long
        return id_.replace("docker://", "") or None

    @property
    def phase(self) -> PodPhase:
        return PodPhase(self._status_payload.get("phase", PodPhase.PENDING.value))

    @property
    def is_phase_running(self) -> bool:
        return self._status_payload.get("phase") == "Running"

    @property
    def pod_ip(self) -> str:
        return self._status_payload["podIP"]

    @property
    def host_ip(self) -> str:
        return self._status_payload["hostIP"]

    @property
    def resource_requests(self) -> Resources:
        cpu_m = 0
        memory = 0
        gpu = 0
        for container in self._payload["spec"]["containers"]:
            requests = container.get("resources", {}).get("requests")
            if requests:
                cpu_m += self._parse_cpu_m(requests.get("cpu", "0"))
                memory += self._parse_memory(requests.get("memory", "0Mi"))
                gpu += int(requests.get("nvidia.com/gpu", 0))
        return Resources(cpu_m=cpu_m, memory=memory, gpu=gpu)

    def _parse_cpu_m(self, value: str) -> int:
        if value.endswith("m"):
            return int(value[:-1])
        return int(float(value) * 1000)

    def _parse_memory(self, memory: str) -> int:
        try:
            memory_b = int(memory)
        except ValueError:
            if memory.endswith("Ki"):
                memory_b = int(memory[:-2]) * 1024
            elif memory.endswith("k"):
                memory_b = int(memory[:-1]) * 1000
            elif memory.endswith("Mi"):
                memory_b = int(memory[:-2]) * 1024**2
            elif memory.endswith("M"):
                memory_b = int(memory[:-1]) * 1000**2
            elif memory.endswith("Gi"):
                memory_b = int(memory[:-2]) * 1024**3
            elif memory.endswith("G"):
                memory_b = int(memory[:-1]) * 1000**3
            elif memory.endswith("Ti"):
                memory_b = int(memory[:-2]) * 1024**4
            elif memory.endswith("T"):
                memory_b = int(memory[:-1]) * 1000**4
            else:
                raise KubeClientException("Memory unit is not supported")
        return memory_b

    @property
    def stdin(self) -> bool:
        for container in self._payload["spec"]["containers"]:
            stdin = container.get("stdin")
            if stdin is not None:
                return stdin
        return False

    @property
    def stdin_once(self) -> bool:
        for container in self._payload["spec"]["containers"]:
            stdin_once = container.get("stdinOnce")
            if stdin_once is not None:
                return stdin_once
        return False

    @property
    def tty(self) -> bool:
        for container in self._payload["spec"]["containers"]:
            tty = container.get("tty")
            if tty is not None:
                return tty
        return False


class Node:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    @property
    def name(self) -> str:
        return self._payload["metadata"]["name"]

    @property
    def container_runtime_version(self) -> str:
        return self._payload["status"]["nodeInfo"]["containerRuntimeVersion"]

    def get_label(self, key: str) -> Optional[str]:
        return self._payload["metadata"].get("labels", {}).get(key)


class ContainerStatus:
    def __init__(self, payload: dict[str, Any], restart_policy: str) -> None:
        self._payload = payload
        self._restart_policy = restart_policy

    @property
    def is_waiting(self) -> bool:
        state = self._payload.get("state")
        return "waiting" in state if state else True

    @property
    def is_running(self) -> bool:
        state = self._payload.get("state")
        return "running" in state if state else False

    @property
    def is_terminated(self) -> bool:
        state = self._payload.get("state")
        return "terminated" in state if state else False

    @property
    def is_pod_terminated(self) -> bool:
        return self.is_terminated and not self.can_restart

    @property
    def can_restart(self) -> bool:
        if self._restart_policy == "Never":
            return False
        if self._restart_policy == "Always":
            return True
        assert self._restart_policy == "OnFailure"
        try:
            return self._payload["state"]["terminated"]["exitCode"] != 0
        except KeyError:
            return True

    @property
    def restart_count(self) -> int:
        return self._payload.get("restartCount") or 0

    @property
    def started_at(self) -> Optional[datetime]:
        try:
            if self.is_running:
                date_str = self._payload["state"]["running"]["startedAt"]
            else:
                date_str = self._payload["state"]["terminated"]["startedAt"]
                if not date_str:
                    return None
        except KeyError:
            # waiting
            return None
        return parse_date(date_str)

    @property
    def finished_at(self) -> Optional[datetime]:
        try:
            if self.is_terminated:
                date_str = self._payload["state"]["terminated"]["finishedAt"]
            else:
                date_str = self._payload["lastState"]["terminated"]["finishedAt"]
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
        cert_authority_path: Optional[str] = None,
        cert_authority_data_pem: Optional[str] = None,
        auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE,
        auth_cert_path: Optional[str] = None,
        auth_cert_key_path: Optional[str] = None,
        token: Optional[str] = None,
        token_path: Optional[str] = None,
        token_update_interval_s: int = 300,
        conn_timeout_s: int = 300,
        read_timeout_s: int = 100,
        conn_pool_size: int = 100,
        kubelet_node_port: int = KubeConfig.kubelet_node_port,
        nvidia_dcgm_node_port: Optional[int] = KubeConfig.nvidia_dcgm_node_port,
        trace_configs: Optional[list[aiohttp.TraceConfig]] = None,
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

        self._client: Optional[aiohttp.ClientSession] = None
        self._token_updater_task: Optional[asyncio.Task[None]] = None

    @property
    def _is_ssl(self) -> bool:
        return urlsplit(self._base_url).scheme == "https"

    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        if not self._is_ssl:
            return None
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

    async def __aenter__(self) -> "KubeClient":
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
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

    @property
    def _namespace_url(self) -> str:
        return self._generate_namespace_url(self._namespace)

    @property
    def _pods_url(self) -> str:
        return f"{self._namespace_url}/pods"

    def _generate_pod_url(self, pod_name: str) -> str:
        return f"{self._pods_url}/{pod_name}"

    def _generate_node_proxy_url(self, name: str, port: int) -> str:
        return f"{self._api_v1_url}/nodes/{name}:{port}/proxy"

    def _generate_node_stats_summary_url(self, name: str) -> str:
        proxy_url = self._generate_node_proxy_url(name, self._kubelet_port)
        return f"{proxy_url}/stats/summary"

    def _generate_node_gpu_metrics_url(self, name: str) -> Optional[str]:
        if not self._nvidia_dcgm_port:
            return None
        proxy_url = self._generate_node_proxy_url(name, self._nvidia_dcgm_port)
        return f"{proxy_url}/metrics"

    def _generate_pod_log_url(self, pod_name: str, container_name: str) -> str:
        url = self._generate_pod_url(pod_name)
        url = f"{url}/log?container={pod_name}&follow=true"
        return url

    def _create_headers(
        self, headers: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        headers = dict(headers) if headers else {}
        if self._auth_type == KubeClientAuthType.TOKEN and self._token:
            headers["Authorization"] = "Bearer " + self._token
        return headers

    async def _request(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        headers = self._create_headers(kwargs.pop("headers", None))
        assert self._client, "client is not initialized"
        async with self._client.request(*args, headers=headers, **kwargs) as response:
            await self._check_response_status(response)
            payload = await response.json()
            logger.debug("k8s response payload: %s", payload)
            return payload

    async def get_raw_pod(self, pod_name: str) -> dict[str, Any]:
        url = self._generate_pod_url(pod_name)
        payload = await self._request(method="GET", url=url)
        self._assert_resource_kind(
            expected_kind="Pod", payload=payload, job_id=pod_name
        )
        return payload

    async def get_pod(self, pod_name: str) -> Pod:
        return Pod(await self.get_raw_pod(pod_name))

    async def _get_raw_container_state(self, pod_name: str) -> dict[str, Any]:
        pod = await self.get_pod(pod_name)
        container_status = pod.get_container_status(pod_name)
        return container_status.get("state", {})

    async def get_container_status(self, name: str) -> ContainerStatus:
        pod = await self.get_pod(name)
        return ContainerStatus(
            pod.get_container_status(name),
            restart_policy=pod.restart_policy,
        )

    async def wait_pod_is_running(
        self, pod_name: str, *, timeout_s: float = 10.0 * 60, interval_s: float = 1.0
    ) -> ContainerStatus:
        """Wait until the pod transitions to the running state.

        Raise JobNotFoundException if there is no such pod.
        Raise asyncio.TimeoutError if it takes too long for the pod.
        """
        async with timeout(timeout_s):
            while True:
                status = await self.get_container_status(pod_name)
                if status.is_running:
                    return status
                if status.is_terminated and not status.can_restart:
                    raise JobNotFoundException
                await asyncio.sleep(interval_s)

    async def wait_pod_is_not_waiting(
        self, pod_name: str, *, timeout_s: float = 10.0 * 60, interval_s: float = 1.0
    ) -> ContainerStatus:
        """Wait until the pod transitions from the waiting state.

        Raise JobNotFoundException if there is no such pod.
        Raise asyncio.TimeoutError if it takes too long for the pod.
        """
        async with timeout(timeout_s):
            while True:
                status = await self.get_container_status(pod_name)
                if not status.is_waiting:
                    return status
                await asyncio.sleep(interval_s)

    async def get_pod_container_stats(
        self, node_name: str, pod_name: str, container_name: str
    ) -> Optional["PodContainerStats"]:
        """
        https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/apis/stats/v1alpha1/types.go
        """
        try:
            url = self._generate_node_stats_summary_url(node_name)
            payload = await self._request(method="GET", url=url)
            summary = StatsSummary(payload)
            return summary.get_pod_container_stats(
                self._namespace, pod_name, container_name
            )
        except JobNotFoundException:
            return None
        except ContentTypeError as e:
            logger.info(f"Failed to parse response: {e}", exc_info=True)
            return None

    async def get_pod_container_gpu_stats(
        self,
        node_name: str,
        pod_name: str,
        container_name: str,
    ) -> Optional["PodContainerGPUStats"]:
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
                self._namespace, pod_name, container_name
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
        *,
        conn_timeout_s: float = 60 * 5,
        read_timeout_s: float = 60 * 30,
        previous: bool = False,
        since: Optional[datetime] = None,
        timestamps: bool = False,
    ) -> AsyncIterator[aiohttp.StreamReader]:
        url = self._generate_pod_log_url(pod_name, container_name)
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
        self, label_selector: str = "", field_selector: str = ""
    ) -> Sequence[Pod]:
        params = {}
        if label_selector:
            params["labelSelector"] = label_selector
        if field_selector:
            params["fieldSelector"] = field_selector
        payload = await self._request(method="get", url=self._pods_url, params=params)
        self._assert_resource_kind("PodList", payload)
        return [Pod(p) for p in payload["items"]]

    async def get_node(self, name: str) -> Node:
        payload = await self._request(method="get", url=self._generate_node_url(name))
        self._assert_resource_kind("Node", payload)
        return Node(payload)

    async def get_nodes(self, label_selector: str = "") -> Sequence[Node]:
        params = None
        if label_selector:
            params = {"labelSelector": label_selector}
        payload = await self._request(method="get", url=self._nodes_url, params=params)
        self._assert_resource_kind("NodeList", payload)
        return [Node(item) for item in payload["items"]]

    async def _check_response_status(
        self, response: aiohttp.ClientResponse, job_id: str = ""
    ) -> None:
        if not 200 <= response.status < 300:
            payload = await response.text()
            try:
                pod = json.loads(payload)
            except ValueError:
                pod = {"code": response.status, "message": payload}
            self._raise_for_status(pod, job_id=job_id)
            raise KubeClientException(payload)

    def _assert_resource_kind(
        self, expected_kind: str, payload: dict[str, Any], job_id: str = ""
    ) -> None:
        kind = payload["kind"]
        if kind == "Status":
            self._raise_for_status(payload, job_id=job_id)
            raise JobError("unexpected error")
        elif kind != expected_kind:
            raise ValueError(f"unknown kind: {kind}")

    def _raise_for_status(self, payload: dict[str, Any], job_id: Optional[str]) -> None:
        if payload["code"] == 400:
            if "ContainerCreating" in payload["message"]:
                raise JobNotFoundException(f"job '{job_id}' was not created yet")
            if "is not available" in payload["message"]:
                raise JobNotFoundException(f"job '{job_id}' has not created yet")
            if "is terminated" in payload["message"]:
                raise JobNotFoundException(f"job '{job_id}' is terminated")
        elif payload["code"] == 401:
            raise KubeClientUnauthorized(payload)
        elif payload["code"] == 404:
            raise JobNotFoundException(f"job '{job_id}' was not found")
        elif payload["code"] == 409:
            raise JobError(f"job '{job_id}' already exist")
        elif payload["code"] == 422:
            raise JobError(f"can not create job with id '{job_id}'")


@dataclass(frozen=True)
class PodContainerStats:
    cpu: float
    memory: float

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "PodContainerStats":
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
    def parse(cls, text: str) -> "GPUCounters":
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
    def __init__(self, payload: dict[str, Any]) -> None:
        self._validate_payload(payload)
        self._payload = payload

    def _validate_payload(self, payload: dict[str, Any]) -> None:
        if "pods" not in payload:
            err_msg = "Invalid stats summary response"
            logging.error(err_msg + f": `{payload}`")
            raise JobError(err_msg)

    def _find_pod_in_stats_summary(
        self, stats_summary: dict[str, Any], namespace_name: str, name: str
    ) -> dict[str, Any]:
        for pod_stats in stats_summary["pods"]:
            ref = pod_stats["podRef"]
            if ref["namespace"] == namespace_name and ref["name"] == name:
                return pod_stats
        return {}

    def _find_container_in_pod_stats(
        self, pod_stats: dict[str, Any], name: str
    ) -> dict[str, Any]:
        containers = pod_stats.get("containers") or []
        for container_stats in containers:
            if container_stats["name"] == name:
                return container_stats
        return {}

    def get_pod_container_stats(
        self, namespace_name: str, pod_name: str, container_name: str
    ) -> Optional[PodContainerStats]:
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

    async def get_latest_stats(self) -> Optional[JobStats]:
        pod = await self._kube_client.get_pod(self._pod_name)
        if not pod.node_name:
            return None
        if pod.resource_requests.gpu:
            return await self._get_latest_gpu_pod_stats(pod.node_name)
        return await self._get_latest_cpu_pod_stats(pod.node_name)

    async def _get_latest_cpu_pod_stats(self, node_name: str) -> Optional[JobStats]:
        pod_stats = await self._kube_client.get_pod_container_stats(
            node_name, self._pod_name, self._container_name
        )
        if not pod_stats:
            return None
        return JobStats(cpu=pod_stats.cpu, memory=pod_stats.memory)

    async def _get_latest_gpu_pod_stats(self, node_name: str) -> Optional[JobStats]:
        pod_stats_task = asyncio.create_task(
            self._kube_client.get_pod_container_stats(
                node_name, self._pod_name, self._container_name
            )
        )
        pod_gpu_stats_task = asyncio.create_task(
            self._kube_client.get_pod_container_gpu_stats(
                node_name, self._pod_name, self._container_name
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
