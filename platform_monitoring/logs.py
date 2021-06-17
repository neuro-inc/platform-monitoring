import asyncio
import io
import json
import logging
import warnings
import zlib
from collections import namedtuple
from os.path import basename
from types import TracebackType
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
)

import aiohttp
from aiobotocore.client import AioBaseClient
from aiobotocore.response import StreamingBody
from aioelasticsearch import Elasticsearch
from aioelasticsearch.helpers import Scan
from platform_logging import trace

from .base import LogReader
from .kube_client import KubeClient


logger = logging.getLogger(__name__)


async def filter_out_rpc_error(stream: aiohttp.StreamReader) -> AsyncIterator[bytes]:
    # https://github.com/neuromation/platform-api/issues/131
    # k8s API (and the underlying docker API) sometimes returns an rpc
    # error as the last log line. it says that the corresponding container
    # does not exist. we should try to not expose such internals, but only
    # if it is the last line indeed.

    _is_line_start = True

    async def read_chunk(*, min_line_length: int) -> Tuple[bytes, bool]:
        nonlocal _is_line_start
        chunk = io.BytesIO()
        is_line_start = _is_line_start
        while chunk.tell() < min_line_length:
            data = await stream.readany()
            if not data:
                break

            n_pos = data.find(b"\n") + 1
            _is_line_start = bool(n_pos)
            if n_pos:
                line, tail = data[:n_pos], data[n_pos:]
                if tail:
                    unreadline(tail)
                chunk.write(line)
                break

            chunk.write(data)
            if not is_line_start:
                # if this chunk is somewhere in the middle of the line, we
                # want to return immediately without waiting for the rest of
                # `min_chunk_length`
                break

        return chunk.getvalue(), is_line_start

    async def readline() -> bytes:
        line = await stream.readline()
        nonlocal _is_line_start
        _is_line_start = True
        return line

    def unreadline(data: bytes) -> None:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            stream.unread_data(data)
        nonlocal _is_line_start
        _is_line_start = True

    while True:
        error_prefix = b"rpc error: code ="
        chunk, is_line_start = await read_chunk(min_line_length=len(error_prefix))
        # 1. `chunk` may not be a whole line, ending with "\n";
        # 2. `chunk` may be the beginning of a line with the min length of
        # `len(error_prefix)`.
        if is_line_start and chunk.startswith(error_prefix):
            unreadline(chunk)
            line = await readline()
            next_chunk, _ = await read_chunk(min_line_length=1)
            if next_chunk:
                logging.warning("An rpc error line was not at the end of the log")
                chunk = line
                unreadline(next_chunk)
            else:
                logging.info("Skipping an rpc error line at the end of the log")
                break
        if not chunk:
            break
        yield chunk


class PodContainerLogReader(LogReader):
    def __init__(
        self,
        client: KubeClient,
        pod_name: str,
        container_name: str,
        client_conn_timeout_s: Optional[float] = None,
        client_read_timeout_s: Optional[float] = None,
        *,
        previous: bool = False,
    ) -> None:
        self._client = client
        self._pod_name = pod_name
        self._container_name = container_name
        self._client_conn_timeout_s = client_conn_timeout_s
        self._client_read_timeout_s = client_read_timeout_s
        self._previous = previous

        self._stream_cm: Optional[AsyncContextManager[aiohttp.StreamReader]] = None

    async def __aenter__(self) -> AsyncIterator[bytes]:
        await self._client.wait_pod_is_running(self._pod_name)
        kwargs = {}
        if self._client_conn_timeout_s is not None:
            kwargs["conn_timeout_s"] = self._client_conn_timeout_s
        if self._client_read_timeout_s is not None:
            kwargs["read_timeout_s"] = self._client_read_timeout_s
        if self._previous:
            kwargs["previous"] = True
        self._stream_cm = self._client.create_pod_container_logs_stream(
            pod_name=self._pod_name, container_name=self._container_name, **kwargs
        )
        assert self._stream_cm
        stream = await self._stream_cm.__aenter__()
        return filter_out_rpc_error(stream)

    async def __aexit__(self, *args: Any) -> None:
        assert self._stream_cm
        stream_cm = self._stream_cm
        self._stream_cm = None
        await stream_cm.__aexit__(*args)


class ElasticsearchLogReader(LogReader):
    def __init__(
        self,
        es_client: Elasticsearch,
        namespace_name: str,
        pod_name: str,
        container_name: str,
    ) -> None:
        self._es_client = es_client
        self._index = "logstash-*"
        self._doc_type = "fluent-bit"

        self._namespace_name = namespace_name
        self._pod_name = pod_name
        self._container_name = container_name

        self._scan: Optional[Scan] = None

    def _combine_search_query(self) -> Dict[str, Any]:
        terms = [
            {"term": {"kubernetes.namespace_name.keyword": self._namespace_name}},
            {"term": {"kubernetes.pod_name.keyword": self._pod_name}},
            {"term": {"kubernetes.container_name.keyword": self._container_name}},
        ]
        return {"query": {"bool": {"must": terms}}, "sort": [{"@timestamp": "asc"}]}

    async def __aenter__(self) -> AsyncIterator[bytes]:
        query = self._combine_search_query()
        self._scan = Scan(
            self._es_client,
            index=self._index,
            doc_type=self._doc_type,
            # scroll="1m" means that the requested search context will be
            # preserved in the ES cluster for at most 1 minutes. in other
            # words, our client code has up to 1 minute to process previous
            # results and fetch next ones.
            scroll="1m",
            raise_on_error=False,
            query=query,
            preserve_order=True,
            size=100,
        )
        await self._scan.__aenter__()
        return (doc["_source"]["log"].encode() async for doc in self._scan)

    async def __aexit__(self, *args: Any) -> None:
        assert self._scan
        scan = self._scan
        self._scan = None
        await scan.__aexit__(*args)


class S3LogReader(LogReader):
    def __init__(
        self,
        s3_client: AioBaseClient,
        bucket_name: str,
        prefix_format: str,
        namespace_name: str,
        pod_name: str,
        container_name: str,
    ) -> None:
        self._s3_client = s3_client
        self._bucket_name = bucket_name
        self._prefix_format = prefix_format
        self._namespace_name = namespace_name
        self._pod_name = pod_name
        self._container_name = container_name
        self._key_iterator: Iterator[str] = iter(())
        self._response_body: Optional[StreamingBody] = None
        self._line_iterator: Optional[AsyncIterator[bytes]] = None

    def _get_prefix(self) -> str:
        return self._prefix_format.format(
            namespace_name=self._namespace_name,
            pod_name=self._pod_name,
            container_name=self._container_name,
        )

    async def __aenter__(self) -> AsyncIterator[bytes]:
        await self._load_log_keys()
        return self._iter()

    @trace
    async def _load_log_keys(self) -> None:
        paginator = self._s3_client.get_paginator("list_objects_v2")
        Key = namedtuple("Key", ["value", "time_slice"])
        keys: List[Key] = []
        async for page in paginator.paginate(
            Bucket=self._bucket_name, Prefix=self._get_prefix()
        ):
            for obj in page.get("Contents", ()):
                s3_key = obj["Key"]
                # get time slice from s3 key
                time_slice_str = basename(s3_key).split(".")[0].split("_")
                time_slice = (int(time_slice_str[0]), int(time_slice_str[1]))
                keys.append(Key(value=s3_key, time_slice=time_slice))
        keys.sort(key=lambda k: k.time_slice)  # order keys by time slice
        self._key_iterator = iter(key.value for key in keys)

    async def __aexit__(self, *args: Any) -> None:
        self._key_iterator = iter(())
        await self._end_read_file(*args)

    async def _iter(self) -> AsyncIterator[bytes]:
        while await self._start_read_file():
            assert self._line_iterator
            async for line in self._line_iterator:
                event = json.loads(line)
                yield event["log"].encode()
            await self._end_read_file()

    async def _start_read_file(self) -> bool:
        if self._line_iterator:
            return True
        key = next(self._key_iterator, "")
        if not key:
            return False
        response = await self._s3_client.get_object(Bucket=self._bucket_name, Key=key)
        self._response_body = response["Body"]
        await self._response_body.__aenter__()

        if response["ContentType"] == "application/x-gzip":
            self._line_iterator = self._iter_decompressed_lines(self._response_body)
        else:
            self._line_iterator = self._response_body.iter_lines()
        return True

    @classmethod
    async def _iter_decompressed_lines(
        cls, body: StreamingBody
    ) -> AsyncIterator[bytes]:
        loop = asyncio.get_event_loop()
        decompress_obj = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
        pending = b""
        async for chunk in body.iter_chunks():
            chunk_d = await loop.run_in_executor(
                None, lambda: decompress_obj.decompress(chunk)
            )
            lines = (pending + chunk_d).splitlines(True)
            for line in lines[:-1]:
                yield line.splitlines()[0]
            pending = lines[-1]
        if pending:
            yield pending.splitlines()[0]

    async def _end_read_file(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
    ) -> None:
        self._line_iterator = None
        if self._response_body:
            await self._response_body.__aexit__(exc_type, exc_val, exc_tb)
            self._response_body = None
