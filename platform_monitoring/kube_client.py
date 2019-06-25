import asyncio
import logging
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, NoReturn, Optional
from urllib.parse import urlsplit

import aiohttp
from aiohttp import ContentTypeError
from async_timeout import timeout

from .base import JobStats, Telemetry
from .config import KubeClientAuthType


logger = logging.getLogger(__name__)


class JobException(Exception):
    pass


class JobError(JobException):
    pass


class JobNotFoundException(JobException):
    pass


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
        conn_timeout_s: int = 300,
        read_timeout_s: int = 100,
        conn_pool_size: int = 100,
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

        self._conn_timeout_s = conn_timeout_s
        self._read_timeout_s = read_timeout_s
        self._conn_pool_size = conn_pool_size
        self._client: Optional[aiohttp.ClientSession] = None

        self._kubelet_port = 10255

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
            ssl_context.load_cert_chain(  # type: ignore
                self._auth_cert_path, self._auth_cert_key_path
            )
        return ssl_context

    async def init(self) -> None:
        connector = aiohttp.TCPConnector(
            limit=self._conn_pool_size, ssl=self._create_ssl_context()
        )
        if self._auth_type == KubeClientAuthType.TOKEN:
            token = self._token
            if not token:
                assert self._token_path is not None
                token = Path(self._token_path).read_text()
            headers = {"Authorization": "Bearer " + token}
        else:
            headers = {}
        timeout = aiohttp.ClientTimeout(
            connect=self._conn_timeout_s, total=self._read_timeout_s
        )
        self._client = aiohttp.ClientSession(
            connector=connector, timeout=timeout, headers=headers
        )

    @property
    def namespace(self) -> str:
        return self._namespace

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    async def __aenter__(self) -> "KubeClient":
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    @property
    def _api_v1_url(self) -> str:
        return f"{self._base_url}/api/v1"

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

    async def _request(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        assert self._client, "client is not initialized"
        async with self._client.request(*args, **kwargs) as response:
            # TODO (A Danshyn 05/21/18): check status code etc
            payload = await response.json()
            logger.debug("k8s response payload: %s", payload)
            return payload

    async def get_raw_pod(self, pod_name: str) -> Dict[str, Any]:
        url = self._generate_pod_url(pod_name)
        payload = await self._request(method="GET", url=url)
        self._assert_resource_kind(expected_kind="Pod", payload=payload)
        return payload

    async def _get_raw_container_state(self, pod_name: str) -> Dict[str, Any]:
        payload = await self.get_raw_pod(pod_name)
        pod_status = payload.get("status")
        if not pod_status:
            raise ValueError("Missing pod status")
        container_status: Dict[str, Any] = {}
        if "containerStatuses" in pod_status:
            container_status = pod_status["containerStatuses"][0]
        state = container_status.get("state", {})
        return state

    async def is_container_waiting(self, pod_name: str) -> bool:
        state = await self._get_raw_container_state(pod_name)
        is_waiting = not state or "waiting" in state
        return is_waiting

    async def wait_pod_is_running(
        self, pod_name: str, timeout_s: float = 10.0 * 60, interval_s: float = 1.0
    ) -> None:
        """Wait until the pod transitions from the waiting state.

        Raise JobError if there is no such pod.
        Raise asyncio.TimeoutError if it takes too long for the pod.
        """
        async with timeout(timeout_s):
            while True:
                is_waiting = await self.is_container_waiting(pod_name)
                if not is_waiting:
                    return
                await asyncio.sleep(interval_s)

    def _parse_node_name(self, payload: Dict[str, Any]) -> Optional[str]:
        return payload["spec"].get("nodeName")

    async def get_pod_container_stats(
        self, pod_name: str, container_name: str
    ) -> Optional["PodContainerStats"]:
        """
        https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/apis/stats/v1alpha1/types.go
        """
        raw_pod = await self.get_raw_pod(pod_name)
        node_name = self._parse_node_name(raw_pod)
        if not node_name:
            return None
        url = self._generate_node_stats_summary_url(node_name)
        try:
            payload = await self._request(method="GET", url=url)
            summary = StatsSummary(payload)
            return summary.get_pod_container_stats(
                self._namespace, pod_name, container_name
            )
        except ContentTypeError as e:
            logger.info(f"Failed to parse response: {e}", exc_info=True)
            return None

    def _assert_resource_kind(
        self, expected_kind: str, payload: Dict[str, Any]
    ) -> None:
        kind = payload["kind"]
        if kind == "Status":
            self._raise_status_job_exception(payload, job_id="")
        elif kind != expected_kind:
            raise ValueError(f"unknown kind: {kind}")

    def _raise_status_job_exception(
        self, pod: Dict[str, Any], job_id: Optional[str]
    ) -> NoReturn:
        if pod["code"] == 409:
            raise JobError(f"job '{job_id}' already exist")
        elif pod["code"] == 404:
            raise JobNotFoundException(f"job '{job_id}' was not found")
        elif pod["code"] == 422:
            raise JobError(f"can not create job with id '{job_id}'")
        else:
            raise JobError("unexpected error")


@dataclass(frozen=True)
class PodContainerStats:
    cpu: float
    memory: float
    # TODO (A Danshyn): group into a single attribute
    gpu_duty_cycle: Optional[int] = None
    gpu_memory: Optional[float] = None

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "PodContainerStats":
        cpu = payload.get("cpu", {}).get("usageNanoCores", 0) / (10 ** 9)
        memory = payload.get("memory", {}).get("workingSetBytes", 0) / (2 ** 20)  # MB
        gpu_memory = None
        gpu_duty_cycle = None
        accelerators = payload.get("accelerators") or []
        if accelerators:
            gpu_memory = sum(acc["memoryUsed"] for acc in accelerators) / (
                2 ** 20
            )  # MB
            gpu_duty_cycle_total = sum(acc["dutyCycle"] for acc in accelerators)
            gpu_duty_cycle = int(gpu_duty_cycle_total / len(accelerators))  # %
        return cls(
            cpu=cpu, memory=memory, gpu_duty_cycle=gpu_duty_cycle, gpu_memory=gpu_memory
        )


class StatsSummary:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload

    def _find_pod_in_stats_summary(
        self, stats_summary: Dict[str, Any], namespace_name: str, name: str
    ) -> Dict[str, Any]:
        for pod_stats in stats_summary["pods"]:
            ref = pod_stats["podRef"]
            if ref["namespace"] == namespace_name and ref["name"] == name:
                return pod_stats
        return {}

    def _find_container_in_pod_stats(
        self, pod_stats: Dict[str, Any], name: str
    ) -> Dict[str, Any]:
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
        pod_stats = await self._kube_client.get_pod_container_stats(
            self._pod_name, self._container_name
        )
        if not pod_stats:
            return None

        return JobStats(
            cpu=pod_stats.cpu,
            memory=pod_stats.memory,
            gpu_duty_cycle=pod_stats.gpu_duty_cycle,
            gpu_memory=pod_stats.gpu_memory,
        )
