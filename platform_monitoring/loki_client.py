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
        trace_configs: list[aiohttp.TraceConfig] | None = None,
    ) -> None:
        self._base_url = base_url

        self._conn_timeout_s = conn_timeout_s
        self._read_timeout_s = read_timeout_s
        self._conn_pool_size = conn_pool_size
        self._archive_delay_s = archive_delay_s

        self._session: aiohttp.ClientSession | None = None
        self._trace_configs = trace_configs

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
            trace_configs=self._trace_configs,
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

    @property
    def _labels_url(self) -> URL:
        return self._api_v1_url / "labels"

    def _label_values_url(self, label: str) -> URL:
        return self._api_v1_url / "label" / label / "values"

    async def _request(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        assert self._session
        self._validate_range(kwargs.get("params", {}))
        async with self._session.request(*args, **kwargs) as response:
            payload = await response.json()
            # print(args, kwargs)
            logger.debug("Loki response payload: %s", payload)
            # print(444444, len(payload["data"]["result"]))
            return payload

    @staticmethod
    def _validate_range(params: dict[str, Any]) -> None | NoReturn:
        start = params.get("start")
        end = params.get("end")
        if start and end and int(start) > int(end):
            exc_text = f"Invalid range: {start=} > {end=}"
            raise ValueError(exc_text)
        return None

    @staticmethod
    def _build_params(**kwargs: Any) -> dict[str, Any]:
        return {k: v for k, v in kwargs.items() if v is not None}

    async def query_range(
        self,
        *,
        query: str,
        start: str | int,
        end: str | int | None = None,
        direction: str = "backward",
        limit: int = 5000,
        add_stream_to_log_entries: bool = True,
    ) -> dict[str, Any]:
        params = self._build_params(
            query=query, start=start, end=end, direction=direction, limit=limit
        )

        url = str(self._query_range_url)
        # print(url)
        result = await self._request(method="GET", url=url, params=params)

        # add stream data to each log entry
        if add_stream_to_log_entries:
            for stream_result in result["data"]["result"]:
                for log in stream_result["values"]:
                    log.append(stream_result["stream"])

        data_result = sorted(
            chain.from_iterable(
                log_data["values"] for log_data in result["data"]["result"]
            ),
            key=itemgetter(0),
            reverse=direction == "backward",
        )

        result["data"]["result"] = data_result
        # print(result["data"]["result"])
        return result

    async def query_range_page_iterate(
        self,
        *,
        query: str,
        start: str | int,
        end: str | int | None = None,
        direction: str = "backward",  # or can be "forward"
        limit: int = 5000,
    ) -> AsyncIterator[dict[str, Any]]:
        # print(22222222, "query_range_page_iterate", start, end, direction)
        while True:
            response = await self.query_range(
                query=query,
                start=start,
                end=end,
                limit=limit,
                direction=direction,
            )
            # print(44444444, response)
            yield response
            # print(start, end, direction, limit, (end - start) / 1000000000,
            # response["data"]["stats"]["summary"]["totalEntriesReturned"])
            if response["data"]["stats"]["summary"]["totalEntriesReturned"] < limit:
                break

            if direction == "forward":
                start = int(response["data"]["result"][-1][0]) + 1
            else:
                end = int(response["data"]["result"][-1][0]) - 1

    async def labels(
        self, query: str, start: str | int, end: str | int | None
    ) -> dict[str, Any]:
        return await self._request(
            method="GET",
            url=str(self._labels_url),
            params=self._build_params(query=query, start=start, end=end),
        )

    async def label_values(
        self,
        label: str,
        query: str,
        start: str | int,
        end: str | int | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            method="GET",
            url=str(self._label_values_url(label)),
            params=self._build_params(query=query, start=start, end=end),
        )
