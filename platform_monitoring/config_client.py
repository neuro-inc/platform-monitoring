from types import TracebackType
from typing import Any, Dict, Optional, Sequence, Type

import aiohttp
from multidict import CIMultiDict
from yarl import URL


class NodePool:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload

    @property
    def name(self) -> str:
        return self._payload["name"]

    @property
    def zones(self) -> Sequence[str]:
        return self._payload.get("zones", [])

    @property
    def zones_count(self) -> int:
        return len(self.zones)

    @property
    def max_size(self) -> int:
        return self._payload["max_size"]

    @property
    def is_preemptible(self) -> bool:
        return self._payload.get("is_preemptible", False)

    @property
    def available_cpu_m(self) -> int:
        return int(self._payload["available_cpu"] * 1000)

    @property
    def available_memory_mb(self) -> int:
        return self._payload["available_memory_mb"]

    @property
    def gpu(self) -> int:
        return self._payload.get("gpu", 0)

    @property
    def gpu_model(self) -> str:
        return self._payload.get("gpu_model", "")


class Cluster:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload

    @property
    def zones(self) -> Sequence[str]:
        return self._payload["cloud_provider"].get("zones", [])

    @property
    def zones_count(self) -> int:
        return len(self.zones)

    @property
    def node_pools(self) -> Sequence[NodePool]:
        return [NodePool(np) for np in self._payload["cloud_provider"]["node_pools"]]


class ConfigClient:
    def __init__(
        self,
        api_url: URL,
        token: Optional[str] = None,
        timeout: aiohttp.ClientTimeout = aiohttp.client.DEFAULT_TIMEOUT,
        trace_config: Optional[aiohttp.TraceConfig] = None,
    ):
        self._clusters_url = api_url / "clusters"
        self._token = token
        self._timeout = timeout
        self._trace_config = trace_config
        self._client: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "ConfigClient":
        await self._init()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        assert self._client
        await self._client.close()

    async def _init(self) -> None:
        if self._trace_config:
            trace_configs = [self._trace_config]
        else:
            trace_configs = []
        client = aiohttp.ClientSession(
            headers=self._generate_headers(self._token),
            timeout=self._timeout,
            trace_configs=trace_configs,
        )
        self._client = await client.__aenter__()

    def _generate_headers(self, token: Optional[str] = None) -> "CIMultiDict[str]":
        headers: "CIMultiDict[str]" = CIMultiDict()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def get_cluster(self, cluster_name: str) -> Cluster:
        assert self._client
        async with self._client.get(
            self._clusters_url / cluster_name,
            params={"include": "cloud_provider_infra"},
        ) as response:
            response.raise_for_status()
            payload = await response.json()
            return Cluster(payload)
