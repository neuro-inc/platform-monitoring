import asyncio
import logging
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Dict, NoReturn, Optional
from urllib.parse import urlsplit

import aiohttp
from aiohttp import ContentTypeError
from async_generator import asynccontextmanager
from async_timeout import timeout
from yarl import URL

from .base import JobStats, Telemetry
from .config import KubeClientAuthType


logger = logging.getLogger(__name__)


KUBELET_NODE_PORT = 10255


class KubeClientException(Exception):
    pass


class JobException(Exception):
    pass


class JobError(JobException):
    pass


class JobNotFoundException(JobException):
    pass


@dataclass(frozen=True)
class ProxyClient:
    url: URL
    session: aiohttp.ClientSession


class Pod:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload

    @property
    def node_name(self) -> Optional[str]:
        return self._payload["spec"].get("nodeName")

    @property
    def _status_payload(self) -> Dict[str, Any]:
        payload = self._payload.get("status")
        if not payload:
            raise ValueError("Missing pod status")
        return payload

    def get_container_status(self, name: str) -> Dict[str, Any]:
        for payload in self._status_payload.get("containerStatuses", []):
            if payload["name"] == name:
                return payload
        return {}

    def get_container_id(self, name: str) -> Optional[str]:
        id_ = self.get_container_status(name).get("containerID", "")
        # NOTE: URL(id_).host is failing because the container id is too long
        return id_.replace("docker://", "") or None

    @property
    def is_phase_running(self) -> bool:
        return self._status_payload.get("phase") == "Running"


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

        self._kubelet_port = KUBELET_NODE_PORT

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

    def _generate_pod_log_url(self, pod_name: str, container_name: str) -> str:
        return (
            f"{self._generate_pod_url(pod_name)}/log"
            f"?container={pod_name}&follow=true"
        )

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

    async def get_pod(self, pod_name: str) -> Pod:
        return Pod(await self.get_raw_pod(pod_name))

    async def _get_raw_container_state(self, pod_name: str) -> Dict[str, Any]:
        pod = await self.get_pod(pod_name)
        container_status = pod.get_container_status(pod_name)
        return container_status.get("state", {})

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

    def _get_node_proxy_url(self, host: str, port: int) -> URL:
        return URL(self._generate_node_proxy_url(host, port))

    @asynccontextmanager
    async def get_node_proxy_client(
        self, host: str, port: int
    ) -> AsyncIterator[ProxyClient]:
        assert self._client
        yield ProxyClient(
            url=self._get_node_proxy_url(host, port), session=self._client
        )

    async def get_pod_container_stats(
        self, pod_name: str, container_name: str
    ) -> Optional["PodContainerStats"]:
        """
        https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/apis/stats/v1alpha1/types.go
        """
        pod = await self.get_pod(pod_name)
        if not pod.node_name:
            return None
        url = self._generate_node_stats_summary_url(pod.node_name)
        try:
            payload = await self._request(method="GET", url=url)
            summary = StatsSummary(payload)
            return summary.get_pod_container_stats(
                self._namespace, pod_name, container_name
            )
        except ContentTypeError as e:
            logger.info(f"Failed to parse response: {e}", exc_info=True)
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
        conn_timeout_s: float = 60 * 5,
        read_timeout_s: float = 60 * 30,
    ) -> AsyncIterator[aiohttp.StreamReader]:
        url = self._generate_pod_log_url(pod_name, container_name)
        client_timeout = aiohttp.ClientTimeout(
            connect=conn_timeout_s, sock_read=read_timeout_s
        )
        async with self._client.get(  # type: ignore
            url, timeout=client_timeout
        ) as response:
            await self._check_response_status(response)
            yield response.content

    async def _check_response_status(self, response: aiohttp.ClientResponse) -> None:
        if response.status != 200:
            payload = await response.text()
            raise KubeClientException(payload)

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
        self._validate_payload(payload)
        self._payload = payload

    def _validate_payload(self, payload: Dict[str, Any]) -> None:
        if "pods" not in payload:
            err_msg = "Invalid stats summary response"
            logging.error(err_msg + f": `{payload}`")
            raise JobError(err_msg)

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
