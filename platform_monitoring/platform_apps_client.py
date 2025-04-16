from dataclasses import dataclass
from types import TracebackType
from typing import Any

import aiohttp
from yarl import URL


@dataclass(frozen=True)
class AppInstance:
    id: str
    name: str
    org_name: str
    project_name: str
    namespace: str


class AppsApiException(Exception):
    def __init__(self, *, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def _create_app_instance(payload: dict[str, Any]) -> AppInstance:
    return AppInstance(
        id=payload["id"],
        name=payload["name"],
        org_name=payload["org_name"],
        project_name=payload["project_name"],
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
            timeout=self._timeout,
            trace_configs=self._trace_configs,
            raise_for_status=self._raise_for_status,
        )

    @staticmethod
    async def _raise_for_status(response: aiohttp.ClientResponse) -> None:
        exc_text = None
        match response.status:
            case 401:
                exc_text = "Platform-apps api response: Unauthorized"
            case 402:
                exc_text = "Platform-apps api response: Payment Required"
            case 403:
                exc_text = "Platform-apps api response: Forbidden"
            case _ if not 200 <= response.status < 300:
                text = await response.text()
                exc_text = (
                    f"Platform-apps api response status is not 2xx. "
                    f"Status: {response.status} Response: {text}"
                )
        if exc_text:
            raise AppsApiException(code=response.status, message=exc_text)
        return

    async def aclose(self) -> None:
        assert self._client
        await self._client.close()

    def _create_default_headers(self, token: str | None) -> dict[str, str]:
        result = {}
        if self._token:
            result["Authorization"] = f"Bearer {token or self._token}"
        return result

    async def get_app(
        self,
        app_instance_id: str,
        cluster_name: str,
        org_name: str,
        project_name: str,
        token: str | None = None,
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
            / app_instance_id,
            headers=self._create_default_headers(token),
        ) as response:
            response_json = await response.json()
            return _create_app_instance(response_json)
