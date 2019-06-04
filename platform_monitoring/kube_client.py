import asyncio
import logging
import ssl
from pathlib import Path
from typing import Any, Dict, NoReturn, Optional
from urllib.parse import urlsplit

import aiohttp
from async_timeout import timeout

from .config import KubeClientAuthType


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

    def _generate_pod_url(self, pod_id: str) -> str:
        return f"{self._pods_url}/{pod_id}"

    def _generate_node_proxy_url(self, name: str, port: int) -> str:
        return f"{self._api_v1_url}/nodes/{name}:{port}/proxy"

    def _generate_node_stats_summary_url(self, name: str) -> str:
        proxy_url = self._generate_node_proxy_url(name, self._kubelet_port)
        return f"{proxy_url}/stats/summary"

    # TODO: _check_pod_exists
    # TODO: _generate_pod_log_url

    async def _request(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        assert self._client, "client is not initialized"
        async with self._client.request(*args, **kwargs) as response:
            # TODO (A Danshyn 05/21/18): check status code etc
            payload = await response.json()
            logging.debug("k8s response payload: %s", payload)
            return payload

    async def get_raw_pod(self, pod_id: str) -> Dict[str, Any]:
        url = self._generate_pod_url(pod_id)
        payload = await self._request(method="GET", url=url)
        self._assert_resource_kind(expected_kind="Pod", payload=payload)
        return payload

    async def get_pod_status(self, pod_id: str) -> "PodStatus":
        payload = await self.get_raw_pod(pod_id)
        if "status" in payload:
            return PodStatus.from_primitive(payload["status"])
        raise ValueError("Missing pod status")

    async def get_node_name(self, pod_id: str) -> str:
        payload = await self.get_raw_pod(pod_id)
        return payload["spec"].get("nodeName")

    async def _is_pod_waiting(self, pod_name: str) -> bool:
        payload = await self.get_raw_pod(pod_name)
        state = payload.get("state", {})
        return not state or "waiting" in state

    async def wait_pod_is_running(
        self, pod_name: str, timeout_s: float = 10.0 * 60, interval_s: float = 1.0
    ) -> None:
        """Wait until the pod transitions from the waiting state.

        Raise JobError if there is no such pod.
        Raise asyncio.TimeoutError if it takes too long for the pod.
        """
        async with timeout(timeout_s):
            while True:
                pod_status = await self.get_pod_status(pod_name)
                if not pod_status.container_status.is_waiting:
                    return
                await asyncio.sleep(interval_s)

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
            raise JobError(f"job {job_id} already exist")
        elif pod["code"] == 404:
            raise JobNotFoundException(f"job '{job_id}' was not found")
        elif pod["code"] == 422:
            raise JobError(f"can not create job with id {job_id}")
        else:
            raise JobError("unexpected")


class ContainerStatus:
    def __init__(self, payload: Optional[Dict[str, Any]] = None) -> None:
        self._payload = payload or {}

    @property
    def _state(self) -> Dict[str, Any]:
        return self._payload.get("state", {})

    @property
    def is_waiting(self) -> bool:
        return not self._state or "waiting" in self._state

    @property
    def is_terminated(self) -> bool:
        return bool(self._state) and "terminated" in self._state

    @property
    def reason(self) -> Optional[str]:
        """Return the reason of the current state.

        'waiting' reasons:
            'PodInitializing'
            'ContainerCreating'
            'ErrImagePull'
        see
        https://github.com/kubernetes/kubernetes/blob/29232e3edc4202bb5e34c8c107bae4e8250cd883/pkg/kubelet/kubelet_pods.go#L1463-L1468
        https://github.com/kubernetes/kubernetes/blob/886e04f1fffbb04faf8a9f9ee141143b2684ae68/pkg/kubelet/images/types.go#L25-L43

        'terminated' reasons:
            'OOMKilled'
            'Completed'
            'Error'
            'ContainerCannotRun'
        see
        https://github.com/kubernetes/kubernetes/blob/c65f65cf6aea0f73115a2858a9d63fc2c21e5e3b/pkg/kubelet/dockershim/docker_container.go#L306-L409
        """
        for state in self._state.values():
            return state.get("reason")
        return None

    @property
    def message(self) -> Optional[str]:
        for state in self._state.values():
            return state.get("message")
        return None

    @property
    def exit_code(self) -> Optional[int]:
        assert self.is_terminated
        return self._state["terminated"]["exitCode"]

    @property
    def is_creating(self) -> bool:
        # TODO (A Danshyn 07/20/18): handle PodInitializing
        # TODO (A Danshyn 07/20/18): consider handling other reasons
        # https://github.com/kubernetes/kubernetes/blob/886e04f1fffbb04faf8a9f9ee141143b2684ae68/pkg/kubelet/images/types.go#L25-L43
        return self.is_waiting and self.reason in (None, "ContainerCreating")


class PodStatus:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload
        self._container_status = self._init_container_status()

    def _init_container_status(self) -> ContainerStatus:
        payload = None
        if "containerStatuses" in self._payload:
            payload = self._payload["containerStatuses"][0]
        return ContainerStatus(payload=payload)

    @property
    def phase(self) -> str:
        """
        "Pending", "Running", "Succeeded", "Failed", "Unknown"
        """
        return self._payload["phase"]

    @property
    def is_phase_pending(self) -> bool:
        return self.phase == "Pending"

    @property
    def is_scheduled(self) -> bool:
        # TODO (A Danshyn 11/16/18): we should consider using "conditions"
        # type="PodScheduled" reason="unschedulable" instead.
        return not self.is_phase_pending or self.is_container_status_available

    @property
    def reason(self) -> Optional[str]:
        """

        If kubelet decides to evict the pod, it sets the "Failed" phase along with
        the "Evicted" reason.
        https://github.com/kubernetes/kubernetes/blob/a3ccea9d8743f2ff82e41b6c2af6dc2c41dc7b10/pkg/kubelet/eviction/eviction_manager.go#L543-L566
        If a node the pod scheduled on fails, node lifecycle controller sets
        the "NodeList" reason.
        https://github.com/kubernetes/kubernetes/blob/a3ccea9d8743f2ff82e41b6c2af6dc2c41dc7b10/pkg/controller/util/node/controller_utils.go#L109-L126
        """
        # the pod status reason has a greater priority
        return self._payload.get("reason") or self._container_status.reason

    @property
    def message(self) -> Optional[str]:
        return self._payload.get("message") or self._container_status.message

    @property
    def container_status(self) -> ContainerStatus:
        return self._container_status

    @property
    def is_container_creating(self) -> bool:
        return self._container_status.is_creating

    @property
    def is_container_status_available(self) -> bool:
        return "containerStatuses" in self._payload

    @property
    def is_node_lost(self) -> bool:
        return self.reason == "NodeLost"

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "PodStatus":
        return cls(payload)
