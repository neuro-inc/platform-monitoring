from dataclasses import dataclass
from types import TracebackType
from typing import Any

import aiohttp
from yarl import URL


@dataclass(frozen=True)
class AppInstance:
    id: str
    name: str
    namespace: str


def _create_app_instance(payload: dict[str, Any]) -> AppInstance:
    return AppInstance(
        id=payload["id"],
        name=payload["name"],
        namespace=payload["namespace"],
    )


class AppsApiClient:
    def __init__(
        self,
        url: URL,
        token: str | None = None,
        timeout: aiohttp.ClientTimeout = aiohttp.client.DEFAULT_TIMEOUT,
        trace_configs: list[aiohttp.TraceConfig] | None = None,
    ):
        super().__init__()

        self._base_url = url / "apis/apps/v1"
        self._token = token
        self._timeout = timeout
        self._trace_configs = trace_configs

    async def __aenter__(self) -> "AppsApiClient":
        self._client = self._create_http_client()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def _create_http_client(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            headers=self._create_default_headers(),
            timeout=self._timeout,
            trace_configs=self._trace_configs,
            raise_for_status=self._raise_for_status,
        )

    @staticmethod
    async def _raise_for_status(response: aiohttp.ClientResponse) -> None:
        if not 200 <= response.status < 300:
            text = await response.text()
            exc_text = f"Platform-apps response status is not 2xx. Response: {text}"
            raise Exception(exc_text)
        return

    async def aclose(self) -> None:
        assert self._client
        await self._client.close()

    def _create_default_headers(self) -> dict[str, str]:
        result = {}
        if self._token:
            result["Authorization"] = f"Bearer {self._token}"
        return result

    async def get_app(
        self, app_instance_id: str, cluster_name: str, org_name: str, project_name: str
    ) -> AppInstance:
        async with self._client.get(
            self._base_url
            / "cluster"
            / cluster_name
            / "org"
            / org_name
            / "project"
            / project_name
            / "instances"
            / app_instance_id
        ) as response:
            response_json = await response.json()
            return _create_app_instance(response_json)
