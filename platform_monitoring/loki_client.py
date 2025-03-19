import logging
from collections.abc import AsyncIterator
from itertools import chain
from operator import itemgetter
from typing import Any, NoReturn, Self

import aiohttp
from yarl import URL


logger = logging.getLogger(__name__)


class LokiClient:
    def __init__(
        self,
        *,
        base_url: URL,
        conn_timeout_s: int,
        read_timeout_s: int,
        conn_pool_size: int,
        archive_delay_s: int,
    ) -> None:
        self._base_url = base_url

        self._conn_timeout_s = conn_timeout_s
        self._read_timeout_s = read_timeout_s
        self._conn_pool_size = conn_pool_size
        self._archive_delay_s = archive_delay_s

        self._session: aiohttp.ClientSession | None = None

    async def init(self) -> None:
        connector = aiohttp.TCPConnector(limit=self._conn_pool_size)
        timeout = aiohttp.ClientTimeout(
            connect=self._conn_timeout_s, total=self._read_timeout_s
        )

        async def custom_check(response: aiohttp.ClientResponse) -> None | NoReturn:
            if not 200 <= response.status < 300:
                text = await response.text()
                exc_text = f"Loki response status is not 2xx. Response: {text}"
                raise Exception(exc_text)
            return None

        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            # trace_configs=self._trace_configs,
            raise_for_status=custom_check,
        )

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def __aenter__(self) -> Self:
        await self.init()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    @property
    def _api_v1_url(self) -> URL:
        return self._base_url / "loki/api/v1/"

    @property
    def _query_range_url(self) -> URL:
        return self._api_v1_url / "query_range"

    async def _request(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        assert self._session
        async with self._session.request(*args, **kwargs) as response:
            payload = await response.json()
            logger.debug("Loki response payload: %s", payload)
            return payload

    async def query_range(self, **params: Any) -> dict[str, Any]:
        params = {k: v for k, v in params.items() if v is not None}  # TODO
        start, end = params.get("start"), params.get("end")
        if start and end and int(start) > int(end):
            exc_text = f"Invalid range: {start=} > {end=}"
            raise ValueError(exc_text)
        url = str(self._query_range_url)
        result = await self._request(method="GET", url=url, params=params)
        # comment
        data_result = sorted(
            chain.from_iterable(
                log_data["values"] for log_data in result["data"]["result"]
            ),
            key=itemgetter(0),
            reverse=params.get("direction") == "backward",
        )

        result["data"]["result"] = data_result

        return result

    async def query_range_page_iterate(
        self,
        query: str,
        start: str | int | None,
        end: str | int | None,
        direction: str = "forward",
        limit: int = 100,
    ) -> AsyncIterator[dict[str, Any]]:
        while True:
            response = await self.query_range(
                query=query, start=start, end=end, limit=limit, direction=direction
            )

            yield response

            if response["data"]["stats"]["summary"]["totalEntriesReturned"] < limit:
                break

            if direction == "forward":
                start = int(response["data"]["result"][-1][0]) + 1
            else:
                end = int(response["data"]["result"][-1][0]) - 1
