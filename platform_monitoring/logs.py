from __future__ import annotations

import abc
import asyncio
import io
import logging
import sys
import time
import zlib
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Sequence,
)
from contextlib import AbstractAsyncContextManager, aclosing, suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import IO, Any, Self, cast

import aiohttp
import botocore.exceptions
import orjson
from aiobotocore.client import AioBaseClient
from aiobotocore.response import StreamingBody
from aioitertools.asyncio import as_generated
from cachetools import LRUCache
from elasticsearch import AsyncElasticsearch, RequestError
from elasticsearch.helpers import async_scan
from iso8601 import ParseError
from neuro_logging import trace, trace_cm

from .base import LogReader
from .kube_client import ContainerStatus, JobNotFoundException, KubeClient
from .loki_client import LokiClient
from .utils import asyncgeneratorcontextmanager, parse_date


logger = logging.getLogger(__name__)


DEFAULT_ARCHIVE_DELAY = 3.0 * 60
ZLIB_WBITS = 16 + zlib.MAX_WBITS

error_prefixes = (
    b"rpc error: code =",
    # failed to try resolving symlinks in path "/var/log/pods/xxx.log":
    # lstat /var/log/pods/xxx.log: no such file or directory
    b"failed to try resolving",
    # Unable to retrieve container logs for docker://xxxx
    b"Unable to retrieve",
)
max_error_prefix_len = max(map(len, error_prefixes))


async def filter_out_rpc_error(stream: aiohttp.StreamReader) -> AsyncIterator[bytes]:  # noqa: C901
    # https://github.com/neuromation/platform-api/issues/131
    # k8s API (and the underlying docker API) sometimes returns an rpc
    # error as the last log line. it says that the corresponding container
    # does not exist. we should try to not expose such internals, but only
    # if it is the last line indeed.

    _is_line_start = True
    unread_buffer = b""

    async def read_chunk(*, min_line_length: int) -> tuple[bytes, bool]:
        nonlocal _is_line_start, unread_buffer
        chunk = io.BytesIO()
        is_line_start = _is_line_start
        while True:
            if unread_buffer:
                data = unread_buffer
                unread_buffer = b""
            else:
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
            if not is_line_start or chunk.tell() >= min_line_length:
                # if this chunk is somewhere in the middle of the line, we
                # want to return immediately without waiting for the rest of
                # `min_chunk_length`
                break

        return chunk.getvalue(), is_line_start

    async def readline() -> bytes:
        nonlocal _is_line_start, unread_buffer
        _is_line_start = True
        if unread_buffer:
            n_pos = unread_buffer.find(b"\n") + 1
            if n_pos:
                line = unread_buffer[:n_pos]
                unread_buffer = unread_buffer[n_pos:]
            else:
                line = unread_buffer
                unread_buffer = b""
                line += await stream.readline()
        else:
            line = await stream.readline()
        return line

    def unreadline(data: bytes) -> None:
        nonlocal _is_line_start, unread_buffer
        _is_line_start = True
        unread_buffer = data + unread_buffer

    while True:
        chunk, is_line_start = await read_chunk(min_line_length=max_error_prefix_len)
        # 1. `chunk` may not be a whole line, ending with "\n";
        # 2. `chunk` may be the beginning of a line with the min length of
        # `max_error_prefix_len`.
        if is_line_start and chunk.startswith(error_prefixes):
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
        namespace: str,
        client_conn_timeout_s: float | None = None,
        client_read_timeout_s: float | None = None,
        *,
        previous: bool = False,
        since: datetime | None = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> None:
        self._client = client
        self._pod_name = pod_name
        self._container_name = container_name
        self._namespace = namespace
        self._client_conn_timeout_s = client_conn_timeout_s
        self._client_read_timeout_s = client_read_timeout_s
        self._previous = previous
        self._since = since
        self._timestamps = timestamps
        self._debug = debug

        self._stream_cm: (AbstractAsyncContextManager[aiohttp.StreamReader]) | None = (
            None
        )
        self._iterator: AsyncIterator[bytes] | None = None

    async def __aenter__(self) -> AsyncIterator[bytes]:
        await self._client.wait_pod_is_not_waiting(
            self._pod_name,
            container_name=self._container_name,
            namespace=self._namespace,
        )
        kwargs: dict[str, Any] = {}
        if self._client_conn_timeout_s is not None:
            kwargs["conn_timeout_s"] = self._client_conn_timeout_s
        if self._client_read_timeout_s is not None:
            kwargs["read_timeout_s"] = self._client_read_timeout_s
        if self._previous:
            kwargs["previous"] = True
        if self._since:
            kwargs["since"] = self._since
        if self._timestamps:
            kwargs["timestamps"] = True
        self._stream_cm = self._client.create_pod_container_logs_stream(
            pod_name=self._pod_name,
            container_name=self._container_name,
            namespace=self._namespace,
            **kwargs,
        )
        assert self._stream_cm
        stream = await self._stream_cm.__aenter__()
        if self._debug:
            self._iterator = stream.iter_any()
        else:
            self._iterator = filter_out_rpc_error(stream)
        return self._iterator

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        assert self._iterator
        if hasattr(self._iterator, "aclose"):
            await self._iterator.aclose()
        assert self._stream_cm
        stream_cm = self._stream_cm
        self._stream_cm = None
        await stream_cm.__aexit__(exc_type, exc_value, traceback)


class ElasticsearchLogReader(LogReader):
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        namespace_name: str,
        pod_name: str,
        container_name: str,
        *,
        since: datetime | None = None,
        timestamps: bool = False,
    ) -> None:
        super().__init__(timestamps=timestamps)

        self._es_client = es_client
        self._index = "logstash-*"
        self._doc_type = "fluent-bit"
        self._namespace_name = namespace_name
        self._pod_name = pod_name
        self._container_name = container_name
        self._since = since
        self._scan_cm: (
            AbstractAsyncContextManager[AsyncGenerator[dict[str, Any]]] | None
        ) = None
        self._scan: AsyncGenerator[dict[str, Any]] | None = None
        self._iterator: AsyncIterator[bytes] | None = None

    def _combine_search_query(self) -> dict[str, Any]:
        terms = [
            {"term": {"kubernetes.namespace_name.keyword": self._namespace_name}},
            {"term": {"kubernetes.pod_name.keyword": self._pod_name}},
            {"term": {"kubernetes.container_name.keyword": self._container_name}},
        ]
        return {"query": {"bool": {"must": terms}}, "sort": [{"@timestamp": "asc"}]}

    async def __aenter__(self) -> AsyncIterator[bytes]:
        query = self._combine_search_query()
        scan = cast(
            AsyncGenerator[Any],
            async_scan(
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
            ),
        )
        try:
            self._scan_cm = aclosing(scan)
            self._scan = await self._scan_cm.__aenter__()
        except RequestError:
            pass
        self._iterator = self._iterate()
        return self._iterator

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        assert self._iterator
        await self._iterator.aclose()  # type: ignore
        scan_cm = self._scan_cm
        self._scan_cm = None
        self._scan = None
        if scan_cm is not None:
            await scan_cm.__aexit__(exc_type, exc_value, traceback)

    async def _iterate(self) -> AsyncIterator[bytes]:
        if self._scan is None:
            return
        async for doc in self._scan:
            try:
                source = doc["_source"]
                time_str = source.get("@timestamp", source.get("time"))
                time = parse_date(time_str)
                if self._since is not None and time < self._since:
                    continue
                self.last_time = time
                log = source["log"]
                yield self.encode_log(time_str, log)
            except Exception:
                logger.exception("Invalid log entry: %r", doc)
                raise


@dataclass(frozen=True)
class S3LogFile:
    key: str
    records_count: int
    size: int
    first_record_time: datetime
    last_record_time: datetime

    def to_primitive(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "records_count": self.records_count,
            "size": self.size,
            "first_record_time": self.first_record_time.isoformat(),
            "last_record_time": self.last_record_time.isoformat(),
        }

    @classmethod
    def from_primitive(cls, data: dict[str, Any]) -> S3LogFile:
        return cls(
            key=data["key"],
            records_count=data["records_count"],
            size=data["size"],
            first_record_time=datetime.fromisoformat(data["first_record_time"]),
            last_record_time=datetime.fromisoformat(data["last_record_time"]),
        )


@dataclass(frozen=True)
class S3LogsMetadata:
    log_files: Sequence[S3LogFile] = field(default_factory=list, repr=False)
    last_compaction_time: datetime | None = None
    last_merged_key: str | None = None

    def get_log_keys(self, *, since: datetime | None = None) -> list[str]:
        if since is None:
            return [f.key for f in self.log_files]
        return [f.key for f in self.log_files if since <= f.last_record_time]

    def add_log_files(
        self,
        files: Iterable[S3LogFile],
        *,
        last_merged_key: str,
        compaction_time: datetime,
    ) -> S3LogsMetadata:
        return replace(
            self,
            log_files=[*self.log_files, *files],
            last_merged_key=last_merged_key,
            last_compaction_time=compaction_time,
        )

    def delete_last_log_file(self) -> S3LogsMetadata:
        return replace(self, log_files=self.log_files[:-1])

    def to_primitive(self) -> dict[str, Any]:
        return {
            "log_files": [f.to_primitive() for f in self.log_files],
            "last_compaction_time": (
                self.last_compaction_time.isoformat()
                if self.last_compaction_time
                else None
            ),
            "last_merged_key": self.last_merged_key,
        }

    @classmethod
    def from_primitive(cls, data: dict[str, Any]) -> S3LogsMetadata:
        return cls(
            log_files=[S3LogFile.from_primitive(f) for f in data["log_files"]],
            last_compaction_time=(
                datetime.fromisoformat(data["last_compaction_time"])
                if data.get("last_compaction_time")
                else None
            ),
            last_merged_key=data["last_merged_key"],
        )


class S3LogsMetadataStorage:
    METADATA_KEY_PREFIX = "metadata"

    def __init__(
        self,
        s3_client: AioBaseClient,
        bucket_name: str,
        *,
        cache_metadata: bool = False,
        max_cache_size: int = 1000,
    ) -> None:
        self._s3_client = s3_client
        self._bucket_name = bucket_name
        self._cache_metadata = cache_metadata
        self._cache: LRUCache[str, S3LogsMetadata] = LRUCache(max_cache_size)

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    @classmethod
    def get_metadata_key(cls, pod_name: str) -> str:
        return f"{cls.METADATA_KEY_PREFIX}/{pod_name}.json"

    async def get(self, pod_name: str) -> S3LogsMetadata:
        metadata = None
        if self._cache_metadata:
            metadata = self._cache.get(pod_name)
        if metadata is None:
            async with trace_cm("get_from_s3"):
                try:
                    metadata = await self._get_from_s3(pod_name)
                except s3_client_error("NoSuchKey"):
                    metadata = S3LogsMetadata()
                if self._cache_metadata:
                    self._cache[pod_name] = metadata
        return metadata

    async def _get_from_s3(self, pod_name: str) -> S3LogsMetadata:
        response = await self._s3_client.get_object(
            Bucket=self._bucket_name,
            Key=self.get_metadata_key(pod_name),
        )
        async with response["Body"]:
            raw_data = await response["Body"].read()
            data = orjson.loads(raw_data)
            return S3LogsMetadata.from_primitive(data)

    async def put(self, pod_name: str, metadata: S3LogsMetadata) -> None:
        await self._put_to_s3(pod_name, metadata)
        if self._cache_metadata:
            self._cache[pod_name] = metadata

    @trace
    async def _put_to_s3(self, pod_name: str, metadata: S3LogsMetadata) -> None:
        await self._s3_client.put_object(
            Bucket=self._bucket_name,
            Key=self.get_metadata_key(pod_name),
            Body=orjson.dumps(metadata.to_primitive()),
        )


class S3LogsMetadataService:
    RAW_LOG_KEY_PREFIX = "kube.var.log.containers."
    RAW_LOG_KEY_PREFIX_FORMAT = (
        "kube.var.log.containers.{pod_name}_{namespace_name}_{container_name}"
    )
    CLEANUP_KEY_PREFIX = "cleanup"

    def __init__(
        self,
        s3_client: AioBaseClient,
        metadata_storage: S3LogsMetadataStorage,
        kube_namespace_name: str,
    ) -> None:
        self._s3_client = s3_client
        self._metadata_storage = metadata_storage
        self._kube_namespace_name = kube_namespace_name

    @property
    def bucket_name(self) -> str:
        return self._metadata_storage.bucket_name

    def _get_prefix(self, pod_name: str) -> str:
        return self.RAW_LOG_KEY_PREFIX_FORMAT.format(
            namespace_name=self._kube_namespace_name,
            pod_name=pod_name,
            container_name=pod_name,
        )

    @trace
    async def get_metadata(self, pod_name: str) -> S3LogsMetadata:
        return await self._metadata_storage.get(pod_name)

    @trace
    async def update_metadata(self, pod_name: str, metadata: S3LogsMetadata) -> None:
        await self._metadata_storage.put(pod_name, metadata)

    @trace
    async def get_log_keys(
        self, pod_name: str, *, since: datetime | None = None
    ) -> list[str]:
        metadata = await self._metadata_storage.get(pod_name)
        keys = metadata.get_log_keys(since=since)
        raw_keys = await self.get_raw_log_keys(pod_name, since=since)
        if metadata.last_merged_key and keys:
            try:
                last_merged_key_index = raw_keys.index(metadata.last_merged_key)
                raw_keys = raw_keys[last_merged_key_index + 1 :]
            except ValueError:
                pass
        keys.extend(raw_keys)
        return keys

    @trace
    async def get_raw_log_keys(
        self, pod_name: str, *, since: datetime | None = None
    ) -> list[str]:
        since_time_str = f"{since:%Y%m%d%H%M}" if since else ""
        paginator = self._s3_client.get_paginator("list_objects_v2")
        keys = []
        async for page in paginator.paginate(
            Bucket=self.bucket_name, Prefix=self._get_prefix(pod_name)
        ):
            for obj in page.get("Contents", ()):
                key = obj["Key"]
                # get time slice from s3 key
                time_slice_str = Path(key).name.split(".")[0].split("_")
                start_time_str = time_slice_str[0]
                index = int(time_slice_str[-1])
                if start_time_str >= since_time_str:
                    keys.append((start_time_str, index, key))
        keys.sort()  # order keys by time slice
        return [key[-1] for key in keys]

    @trace
    async def get_pods_compact_queue(self, compact_interval: float = 3600) -> list[str]:
        time_threshold = _utcnow() - timedelta(seconds=compact_interval)
        paginator = self._s3_client.get_paginator("list_objects_v2")
        pod_names = []
        async for page in paginator.paginate(
            Bucket=self.bucket_name, Prefix=self.RAW_LOG_KEY_PREFIX, Delimiter="_"
        ):
            for prefix in page.get("CommonPrefixes", ()):
                prefix = prefix["Prefix"]
                pod_name = prefix[len(self.RAW_LOG_KEY_PREFIX) : -1]
                metadata = await self._metadata_storage.get(pod_name)
                if (
                    metadata.last_compaction_time is None
                    or metadata.last_compaction_time <= time_threshold
                ):
                    pod_names.append(pod_name)
        return pod_names

    @trace
    async def get_pods_cleanup_queue(self, cleanup_interval: float = 3600) -> list[str]:
        time_threshold = _utcnow() - timedelta(seconds=cleanup_interval)
        paginator = self._s3_client.get_paginator("list_objects_v2")
        pod_names = []
        async for page in paginator.paginate(
            Bucket=self.bucket_name, Prefix=f"{self.CLEANUP_KEY_PREFIX}/"
        ):
            for obj in page.get("Contents", ()):
                key = obj["Key"]
                pod_name = key[len(self.CLEANUP_KEY_PREFIX) + 1 :]
                metadata = await self._metadata_storage.get(pod_name)
                if (
                    metadata.last_compaction_time is None
                    or metadata.last_compaction_time <= time_threshold
                ):
                    pod_names.append(pod_name)
        return pod_names

    @trace
    async def add_pod_to_cleanup_queue(self, pod_name: str) -> None:
        await self._s3_client.put_object(
            Bucket=self.bucket_name,
            Key=f"{self.CLEANUP_KEY_PREFIX}/{pod_name}",
            Body=b"",
        )

    @trace
    async def remove_pod_from_cleanup_queue(self, pod_name: str) -> None:
        await self._s3_client.delete_object(
            Bucket=self.bucket_name, Key=f"{self.CLEANUP_KEY_PREFIX}/{pod_name}"
        )


@dataclass(frozen=True)
class S3LogRecord:
    time: datetime
    time_str: str
    message: str
    container_id: str
    stream: str = "stdout"

    @classmethod
    def parse(
        cls, line: bytes, fallback_time: datetime, *, container_id: str
    ) -> S3LogRecord:
        data = orjson.loads(line.decode(errors="replace"))
        time_str, time = cls._parse_time(data, fallback_time)
        return cls(
            time_str=time_str,
            time=time,
            message=cls._parse_message(data),
            container_id=container_id,
            stream=data.get("stream", cls.stream),
        )

    @classmethod
    def _parse_time(
        cls, data: dict[str, Any], default_time: datetime
    ) -> tuple[str, datetime]:
        time_str = data.get("time")
        if time_str:
            return (time_str, parse_date(time_str))
        # There are cases when unparsed log records are pushed to
        # object storage, the reasons of this behavior are unknown for now.
        # If there is no time key then record is considered not parsed by Fluent Bit.
        # We can try to parse time from container runtime log message.
        msg = data.get("log", "")
        if result := cls._parse_time_from_message(msg):
            return result
        time_str = default_time.strftime("%Y-%m-%dT%H:%M:%S.%f")
        return (time_str, default_time)

    @classmethod
    def _parse_time_from_message(cls, msg: str) -> tuple[str, datetime] | None:
        time_end = msg.find(" ")
        if time_end > 0:
            time_str = msg[:time_end]
            with suppress(ParseError):
                return (time_str, parse_date(time_str))
        return None

    @classmethod
    def _parse_message(cls, data: dict[str, Any]) -> str:
        log = data.get("log", "")
        if "time" in data:
            return log
        # If there is no time key then record is considered not parsed by Fluent Bit.
        # We need to extract raw message from container runtime log message.
        log_arr = log.split(" ", 3)
        return log_arr[-1] if len(log_arr) == 4 else log


class S3FileReader:
    def __init__(
        self,
        s3_client: AioBaseClient,
        bucket_name: str,
        key: str,
        chunk_size: int = 1024,
    ) -> None:
        self._s3_client = s3_client
        self._bucket_name = bucket_name
        self._key = key
        self._chunk_size = chunk_size
        self._loop = asyncio.get_event_loop()

    @classmethod
    def _is_compressed(cls, response: Any) -> bool:
        return response["ContentType"] == "application/x-gzip"

    async def iter_lines(self) -> AsyncIterator[bytes]:
        response = await self._s3_client.get_object(
            Bucket=self._bucket_name, Key=self._key
        )

        async with response["Body"]:
            if self._is_compressed(response):
                line_iterator = self._iter_decompressed_lines(response["Body"])
            else:
                line_iterator = response["Body"].iter_lines(chunk_size=self._chunk_size)

            async with aclosing(line_iterator):
                async for line in line_iterator:
                    yield line

    async def _iter_decompressed_lines(
        self, body: StreamingBody
    ) -> AsyncGenerator[bytes]:
        decompress_obj = zlib.decompressobj(wbits=ZLIB_WBITS)
        pending = b""
        async for chunk in body.iter_chunks(chunk_size=self._chunk_size):
            chunk_d = await asyncio.to_thread(decompress_obj.decompress, chunk)
            if chunk_d:
                lines = chunk_d.splitlines()
                lines[0] = pending + lines[0]
                for i in range(len(lines) - 1):
                    yield lines[i]
                if chunk_d.endswith(b"\n"):
                    pending = b""
                    yield lines[-1]
                else:
                    pending = lines[-1]
        chunk_d = await asyncio.to_thread(decompress_obj.flush)
        if chunk_d:
            pending += chunk_d
        if pending:
            for line in pending.splitlines():
                yield line


class S3LogRecordsWriter:
    LOGS_KEY_PREFIX = "logs"

    def __init__(
        self,
        s3_client: AioBaseClient,
        bucket_name: str,
        pod_name: str,
        size_limit: int = 10 * 1024**2,
        buffer: IO[bytes] | None = None,
        write_buffer_attempts: int = 4,
        write_buffer_timeout: int = 10,
    ) -> None:
        self._s3_client = s3_client
        self._bucket_name = bucket_name
        self._size_limit = size_limit
        self._write_buffer_attempts = write_buffer_attempts
        self._write_buffer_timeout = write_buffer_timeout

        self._buffer = io.BytesIO() if buffer is None else buffer
        self._buffer.seek(0)
        self._buffer.truncate(0)
        self._compress_obj = zlib.compressobj(wbits=ZLIB_WBITS)

        self._key_prefix = self.get_key_prefix(pod_name)
        self._time = _utcnow().strftime("%Y%m%d%H%M")
        self._container_id: str
        self._size = 0
        self._records_count = 0
        self._first_record_time: datetime
        self._last_record_time: datetime
        self._output_files: list[S3LogFile] = []

    @classmethod
    def get_key_prefix(cls, pod_name: str) -> str:
        return f"{cls.LOGS_KEY_PREFIX}/{pod_name}"

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, *args: object
    ) -> None:
        if exc_type is None:
            await self._flush()

    async def _flush(self) -> None:
        if self._size == 0:
            return

        self._buffer.write(self._compress_obj.flush())
        self._buffer.seek(0)

        key = (
            f"{self._key_prefix}/{self._container_id}/"
            f"{self._time}_{time.monotonic_ns()}.gz"
        )
        file = S3LogFile(
            key=key,
            records_count=self._records_count,
            size=self._size,
            first_record_time=self._first_record_time,
            last_record_time=self._last_record_time,
        )
        logger.info("Flushing records to %s", file)
        await self._put_buffer_to_s3(key)
        self._output_files.append(file)
        self._compress_obj = zlib.compressobj(wbits=ZLIB_WBITS)
        self._buffer.seek(0)
        self._buffer.truncate(0)
        self._records_count = 0
        self._size = 0

    async def _put_buffer_to_s3(self, key: str) -> None:
        last_err = None
        for _ in range(self._write_buffer_attempts):
            try:
                await asyncio.wait_for(
                    self._s3_client.put_object(
                        Bucket=self._bucket_name,
                        Key=key,
                        Body=self._buffer,
                        ContentType="application/x-gzip",
                    ),
                    self._write_buffer_timeout,
                )
                return
            except TimeoutError as err:
                last_err = err
                logger.warning("Timeout while flushing records")
        else:
            assert last_err
            raise last_err

    async def write(self, record: S3LogRecord) -> None:
        if self._size and self._container_id != record.container_id:
            await self._flush()

        self._container_id = record.container_id
        record_encoded = self.encode_record(record)

        if self._size + len(record_encoded) > self._size_limit:
            await self._flush()

        if self._records_count == 0:
            self._first_record_time = record.time

        data = self._compress_obj.compress(record_encoded)
        self._buffer.write(data)
        self._size += len(record_encoded)
        self._records_count += 1
        self._last_record_time = record.time

    def encode_record(self, record: S3LogRecord) -> bytes:
        body = {"time": record.time_str, "log": record.message}
        if record.stream != S3LogRecord.stream:
            body["stream"] = record.stream
        return orjson.dumps(body) + b"\n"

    def get_output_files(self) -> list[S3LogFile]:
        return self._output_files.copy()


class S3LogRecordsReader:
    def __init__(self, s3_client: AioBaseClient, bucket_name: str) -> None:
        self._s3_client = s3_client
        self._bucket_name = bucket_name

    async def iter_records(
        self, keys: str | Iterable[str]
    ) -> AsyncIterator[S3LogRecord]:
        if isinstance(keys, str):
            keys = [keys]

        fallback_time = None

        for key in keys:
            reader = S3FileReader(self._s3_client, self._bucket_name, key)
            container_id = self._get_key_container_id(key)

            if fallback_time is None:
                fallback_time = self._get_key_time(key)
            else:
                fallback_time = max(fallback_time, self._get_key_time(key))

            async for line in reader.iter_lines():
                try:
                    record = S3LogRecord.parse(
                        line, fallback_time, container_id=container_id
                    )
                    fallback_time = record.time
                    yield record
                except Exception:
                    logger.exception("Invalid log entry: %r", line)
                    raise

    @classmethod
    def _get_key_time(cls, key: str) -> datetime:
        time_str = Path(key).name.split("_")[0]
        return datetime.strptime(time_str, "%Y%m%d%H%M").replace(tzinfo=UTC)

    @classmethod
    def _get_key_container_id(cls, key: str) -> str:
        container_id = key.split("/")[-2].split("-")[-1]
        if container_id.endswith(".log"):
            return container_id[:-4]
        return container_id


class S3LogReader(LogReader):
    def __init__(
        self,
        s3_client: AioBaseClient,
        metadata_service: S3LogsMetadataService,
        pod_name: str,
        *,
        since: datetime | None = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> None:
        super().__init__(timestamps=timestamps)

        self._record_reader = S3LogRecordsReader(
            s3_client, metadata_service.bucket_name
        )
        self._metadata_service = metadata_service
        self._pod_name = pod_name
        self._since = since
        self._debug = debug
        self._iterator: AsyncIterator[bytes] | None = None

    async def __aenter__(self) -> AsyncIterator[bytes]:
        self._iterator = self._iterate()
        return self._iterator

    async def __aexit__(self, *args: object) -> None:
        assert self._iterator
        await self._iterator.aclose()  # type: ignore

    async def _iterate(self) -> AsyncIterator[bytes]:
        keys = await self._metadata_service.get_log_keys(
            self._pod_name, since=self._since
        )

        for key in keys:
            async for record in self._record_reader.iter_records(key):
                if self._since is not None and record.time < self._since:
                    continue
                self.last_time = record.time
                if self._debug and key:
                    yield f"~~~ From file {Path(key).name}\n".encode()
                    key = ""
                yield self.encode_log(record.time_str, record.message)


class LogsService(abc.ABC):
    @asyncgeneratorcontextmanager
    async def get_pod_log_reader(  # noqa: C901
        self,
        pod_name: str,
        namespace: str,
        *,
        since: datetime | None = None,
        separator: bytes | None = None,
        timestamps: bool = False,
        timeout_s: float = 10.0 * 60,
        interval_s: float = 1.0,
        archive_delay_s: float = DEFAULT_ARCHIVE_DELAY,
        debug: bool = False,
        stop_func: Callable[[], Awaitable[bool]] | None = None,
    ) -> AsyncGenerator[bytes]:
        archive_delay = timedelta(seconds=archive_delay_s)
        if stop_func is None:

            async def stop_func() -> bool:
                await asyncio.sleep(interval_s)
                return False

        def get_last_start(status: ContainerStatus) -> datetime | None:
            if status.is_running:
                return status.started_at
            if status.is_pod_terminated:
                finish = status.finished_at
                if finish is not None and _utcnow() - finish < archive_delay:
                    return status.started_at
            return None

        try:
            status = await self.get_container_status(pod_name, namespace)
            start = get_last_start(status)
        except JobNotFoundException:
            start = None

        has_archive = False
        is_pod_terminated = False
        prev_finish = _utcnow()
        until = start
        while True:
            request_time = _utcnow()
            until = until or request_time
            log_reader = self.get_pod_archive_log_reader(
                pod_name, namespace, since=since, timestamps=timestamps, debug=debug
            )
            if debug:
                yield (
                    f"~~~ Archive logs from {since} to {until} (started at {start})\n"
                ).encode()
            async with log_reader as it:
                async for chunk in it:
                    assert log_reader.last_time
                    if log_reader.last_time >= until:
                        try:
                            status = await self.get_container_status(
                                pod_name, namespace
                            )
                            start = get_last_start(status)
                        except JobNotFoundException:
                            start = None
                        if start is not None:
                            if start > until:
                                until = start
                            else:
                                first = await self.get_first_log_entry_time(
                                    pod_name, namespace, timeout_s=archive_delay_s
                                )
                                if (
                                    first
                                    and self.__class__.__name__
                                    == "ElasticsearchLogsService"
                                ):
                                    # Es time logs precision is 1ms, so we micro -> ms
                                    first = first.replace(
                                        microsecond=first.microsecond // 1000 * 1000
                                    )
                                until = first or start
                                if log_reader.last_time >= until:
                                    since = until
                                    # There is a line in the container logs,
                                    # and it is already archived. All lines
                                    # before that line are already read from
                                    # archive and output. Stop reading from
                                    # archive and start reading from container.
                                    break
                        else:
                            until = _utcnow()
                    has_archive = True
                    yield chunk
                else:
                    # No log line with timestamp >= until is found.
                    if log_reader.last_time:
                        prev_finish = log_reader.last_time
                        since = log_reader.last_time + datetime.resolution
                    try:
                        status = await self.get_container_status(pod_name, namespace)
                        start = get_last_start(status)
                        prev_finish = status.finished_at or prev_finish
                        is_pod_terminated = status.is_pod_terminated
                    except JobNotFoundException:
                        start = None
                    if start is not None:
                        if start > until:
                            until = start
                            continue
                        first = await self.get_first_log_entry_time(
                            pod_name, namespace, timeout_s=archive_delay_s
                        )
                        if first is not None:
                            if first > until:
                                until = first
                                continue
                            # Start reading from container.
                            break
                        if is_pod_terminated and first is None:
                            return
                    elif is_pod_terminated:
                        return

                    if request_time - prev_finish < archive_delay:
                        assert stop_func is not None
                        if not await stop_func():
                            until = None
                            continue
            # Start reading from container.
            break

        if is_pod_terminated and start is None:
            return

        if not has_archive:
            separator = None

        try:
            if start is None:
                status = await self.wait_pod_is_running(
                    pod_name,
                    start,
                    namespace=namespace,
                    timeout_s=timeout_s,
                    interval_s=interval_s,
                )
                start = status.started_at
            if start is not None and (since is None or since < start):
                since = start

            while True:
                async with self.get_pod_live_log_reader(
                    pod_name, namespace, since=since, timestamps=timestamps, debug=debug
                ) as it:
                    if debug:
                        if separator:
                            yield separator + b"\n"
                            separator = None
                        yield (
                            f"~~~ Live logs from {since} (started at {start})\n"
                        ).encode()
                    async for chunk in it:
                        if separator:
                            yield separator + b"\n"
                            separator = None
                        yield chunk

                if not status.can_restart:
                    break
                status = await self.wait_pod_is_running(
                    pod_name,
                    start,
                    namespace=namespace,
                    timeout_s=timeout_s,
                    interval_s=interval_s,
                )
                since = start = status.started_at
        except JobNotFoundException:
            pass

    @abc.abstractmethod
    async def get_first_log_entry_time(
        self, pod_name: str, namespace: str, *, timeout_s: float = 2.0 * 60
    ) -> datetime | None:
        pass  # pragma: no cover

    async def wait_pod_is_running(
        self,
        name: str,
        old_start: datetime | None,
        namespace: str | None = None,
        *,
        timeout_s: float = 10.0 * 60,
        interval_s: float = 1.0,
    ) -> ContainerStatus:
        async with asyncio.timeout(timeout_s):
            while True:
                status = await self.get_container_status(name, namespace)
                if not status.is_waiting:
                    if status.started_at != old_start:
                        return status
                    if status.is_pod_terminated:
                        raise JobNotFoundException
                await asyncio.sleep(interval_s)

    @abc.abstractmethod
    async def get_container_status(
        self, name: str, namespace: str | None
    ) -> ContainerStatus:
        pass  # pragma: no cover

    @abc.abstractmethod
    def get_pod_live_log_reader(
        self,
        pod_name: str,
        namespace: str,
        *,
        since: datetime | None = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> LogReader:
        pass  # pragma: no cover

    @abc.abstractmethod
    def get_pod_archive_log_reader(
        self,
        pod_name: str,
        namespace: str,
        *,
        since: datetime | None = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> LogReader:
        pass  # pragma: no cover


class BaseLogsService(LogsService):
    count = 0

    def __init__(self, kube_client: KubeClient) -> None:
        self._kube_client = kube_client

    async def get_container_status(
        self, name: str, namespace: str | None
    ) -> ContainerStatus:
        return await self._kube_client.get_container_status(name, namespace=namespace)

    def get_pod_live_log_reader(
        self,
        pod_name: str,
        namespace: str,
        *,
        container_name: str | None = None,
        since: datetime | None = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> LogReader:
        if not container_name:
            container_name = pod_name
        return PodContainerLogReader(
            client=self._kube_client,
            pod_name=pod_name,
            namespace=namespace,
            container_name=container_name,
            since=since,
            timestamps=timestamps,
            debug=debug,
        )

    async def get_first_log_entry_time(
        self, pod_name: str, namespace: str, *, timeout_s: float = 2.0 * 60
    ) -> datetime | None:
        return await get_first_log_entry_time(
            self._kube_client, pod_name, namespace, timeout_s=timeout_s
        )


class ElasticsearchLogsService(BaseLogsService):
    # TODO (A Yushkovskiy 07-Jun-2019) Add another abstraction layer joining together
    #  kube-client and elasticsearch-client (in platform-api it's KubeOrchestrator)
    #  and move there method `get_pod_log_reader`

    def __init__(self, kube_client: KubeClient, es_client: AsyncElasticsearch) -> None:
        super().__init__(kube_client)
        self._es_client = es_client

    def get_pod_archive_log_reader(
        self,
        pod_name: str,
        namespace: str,
        *,
        since: datetime | None = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> LogReader:
        return ElasticsearchLogReader(
            es_client=self._es_client,
            namespace_name=namespace,
            pod_name=pod_name,
            container_name=pod_name,
            since=since,
            timestamps=timestamps,
        )


class S3LogsService(BaseLogsService):
    def __init__(
        self,
        kube_client: KubeClient,
        s3_client: AioBaseClient,
        metadata_service: S3LogsMetadataService,
        log_file_size_limit: int = 10 * 1024**2,
    ) -> None:
        super().__init__(kube_client)
        self._s3_client = s3_client
        self._metadata_service = metadata_service
        self._log_file_size_limit = log_file_size_limit
        self._write_buffer = io.BytesIO()

    @property
    def _bucket_name(self) -> str:
        return self._metadata_service.bucket_name

    def get_pod_archive_log_reader(
        self,
        pod_name: str,
        namespace: str,
        *,
        since: datetime | None = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> LogReader:
        return S3LogReader(
            s3_client=self._s3_client,
            metadata_service=self._metadata_service,
            pod_name=pod_name,
            since=since,
            timestamps=timestamps,
            debug=debug,
        )

    @trace
    async def compact_all(
        self,
        compact_interval: float = 3600,
        cleanup_interval: float = 3600,
        pod_names: Sequence[str] | None = None,  # for testing
    ) -> None:
        compact_queue = await self._metadata_service.get_pods_compact_queue(
            compact_interval=compact_interval
        )
        for pod_name in compact_queue:
            if pod_names is None or pod_name in pod_names:
                await self.compact_one(pod_name)

        cleanup_queue = await self._metadata_service.get_pods_cleanup_queue(
            cleanup_interval=cleanup_interval
        )
        for pod_name in cleanup_queue:
            if pod_names is None or pod_name in pod_names:
                await self.cleanup_one(pod_name)

    @trace
    async def compact_one(self, pod_name: str) -> None:
        await self._metadata_service.add_pod_to_cleanup_queue(pod_name)
        metadata = await self._metadata_service.get_metadata(pod_name)
        logger.info("Compacting pod %s with %s", pod_name, metadata)
        raw_keys = await self._metadata_service.get_raw_log_keys(pod_name)
        raw_keys = await self._delete_merged_keys(metadata, raw_keys)
        await self._delete_orphaned_keys(pod_name, metadata)
        metadata = await self._merge_raw_keys(pod_name, metadata, raw_keys)
        await self._metadata_service.update_metadata(pod_name, metadata)

    @trace
    async def cleanup_one(self, pod_name: str) -> None:
        metadata = await self._metadata_service.get_metadata(pod_name)
        logger.info("Cleaning pod %s with %s", pod_name, metadata)
        raw_keys = await self._metadata_service.get_raw_log_keys(pod_name)
        raw_keys = await self._delete_merged_keys(metadata, raw_keys)
        await self._delete_orphaned_keys(pod_name, metadata)
        await self._metadata_service.remove_pod_from_cleanup_queue(pod_name)

    @trace
    async def _merge_raw_keys(
        self, pod_name: str, metadata: S3LogsMetadata, raw_keys: Sequence[str]
    ) -> S3LogsMetadata:
        if not raw_keys:
            return metadata

        logger.info("Merging %d files", len(raw_keys))

        now = _utcnow()
        log_writing_resumed = False

        async with self._create_log_record_writer(pod_name) as writer:
            reader = S3LogRecordsReader(self._s3_client, self._bucket_name)

            async for record in reader.iter_records(raw_keys):
                if log_writing_resumed:
                    await writer.write(record)
                else:
                    metadata = await self._resume_log_writing(writer, metadata, record)
                    log_writing_resumed = True

        return metadata.add_log_files(
            writer.get_output_files(), last_merged_key=raw_keys[-1], compaction_time=now
        )

    def _create_log_record_writer(self, pod_name: str) -> S3LogRecordsWriter:
        # NOTE: write_buffer_timeout must be less than s3_client read_timeout,
        # which is 60s by default. Buffer flushing can happen while compactor
        # is reading the raw log file. So it should be acceptable for flushing
        # to be retried several times while the file is being read.
        return S3LogRecordsWriter(
            self._s3_client,
            self._bucket_name,
            pod_name,
            size_limit=self._log_file_size_limit,
            buffer=self._write_buffer,
        )

    @trace
    async def _resume_log_writing(
        self, writer: S3LogRecordsWriter, metadata: S3LogsMetadata, record: S3LogRecord
    ) -> S3LogsMetadata:
        if metadata.log_files:
            last_file = metadata.log_files[-1]
            record_encoded = writer.encode_record(record)

            if last_file.size + len(record_encoded) <= self._log_file_size_limit:
                logger.info("Resuming log writing from the last file %s", last_file)
                metadata = metadata.delete_last_log_file()
                reader = S3LogRecordsReader(self._s3_client, self._bucket_name)

                async for r in reader.iter_records(last_file.key):
                    await writer.write(r)
        await writer.write(record)

        return metadata

    @trace
    async def _delete_merged_keys(
        self, metadata: S3LogsMetadata, keys: list[str]
    ) -> list[str]:
        if not metadata.last_merged_key:
            return keys
        try:
            last_merged_key_index = keys.index(metadata.last_merged_key)
        except ValueError:
            return keys
        merged_keys = keys[: last_merged_key_index + 1]
        logger.info("Deleting %d merged keys", len(merged_keys))
        await self._delete_keys(merged_keys)
        return keys[last_merged_key_index + 1 :]

    @trace
    async def _delete_orphaned_keys(
        self, pod_name: str, metadata: S3LogsMetadata
    ) -> None:
        log_keys = {f.key for f in metadata.log_files}
        orphaned_keys = []
        paginator = self._s3_client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(
            Bucket=self._bucket_name,
            Prefix=f"{S3LogRecordsWriter.get_key_prefix(pod_name)}/",
        ):
            for obj in page.get("Contents", ()):
                if obj["Key"] not in log_keys:
                    orphaned_keys.append(obj["Key"])
        if orphaned_keys:
            logger.info("Deleting %d orphaned keys", len(orphaned_keys))
            await self._delete_keys(orphaned_keys)

    @trace
    async def _delete_keys(self, keys: Iterable[str]) -> None:
        for key in keys:
            await self._delete_key(key)

    @trace
    async def _delete_key(self, key: str) -> None:
        await self._s3_client.delete_object(Bucket=self._bucket_name, Key=key)


async def get_first_log_entry_time(
    kube_client: KubeClient,
    pod_name: str,
    namespace: str,
    *,
    timeout_s: float = 60 * 2.0,
) -> datetime | None:
    """Return the timestamp of the first container log line from Kubernetes.

    Return None if the container is not created yet, or there are no logs yet,
    or if it takes too long for reading the first timestamp.
    """
    time_str = b""
    try:
        async with kube_client.create_pod_container_logs_stream(
            pod_name=pod_name,
            container_name=pod_name,
            namespace=namespace,
            timestamps=True,
            read_timeout_s=timeout_s,
        ) as stream:
            async with asyncio.timeout(timeout_s):
                while True:
                    chunk = await stream.readany()
                    if not chunk:
                        break
                    pos = chunk.find(b" "[0])
                    if pos >= 0:
                        time_str += chunk[:pos]
                        break
                    time_str += chunk
    except (TimeoutError, JobNotFoundException):
        return None
    try:
        return parse_date(time_str.decode())
    except ValueError:
        return None


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


class UnknownS3Error(Exception):
    pass


def s3_client_error(code: str | int) -> type[Exception]:
    e = sys.exc_info()[1]
    if isinstance(e, botocore.exceptions.ClientError) and (
        e.response["Error"]["Code"] == code
        or e.response["ResponseMetadata"]["HTTPStatusCode"] == code
    ):
        return botocore.exceptions.ClientError
    return UnknownS3Error


class LokiLogReader(LogReader):
    def __init__(
        self,
        loki_client: LokiClient,
        query: str,
        *,
        start: int,
        end: int | None = None,
        direction: str = "forward",  # or can be "backward"
        timestamps: bool = False,
        prefix: bool = False,
        as_ndjson: bool = False,
        split_time_range_count: int = 25,
        concurrent_factor: int = 5,
    ) -> None:
        super().__init__(timestamps=timestamps)

        self._loki_client = loki_client
        self._query = query
        self._start = start
        self._end = end or int(datetime.now(UTC).timestamp()) * 1_000_000_000
        self._direction = direction
        self._prefix = prefix
        self._iterator: AsyncIterator[bytes] | None = None
        self._as_ndjson = as_ndjson

    async def __aenter__(self) -> AsyncIterator[bytes]:
        self._iterator = self._iterate()
        return self._iterator

    async def __aexit__(self, *args: object) -> None:
        assert self._iterator
        await self._iterator.aclose()  # type: ignore

    def encode_and_handle_log(self, log_data: list[Any]) -> bytes:
        log = orjson.loads(log_data[1])["_entry"]

        log = log if log.endswith("\n") else log + "\n"

        stream = log_data[2]

        if self._timestamps:
            log_dt = datetime.fromtimestamp(int(log_data[0]) / 1_000_000_000, tz=UTC)
            log = f"{log_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')}Z {log}"
        if self._prefix:
            log = f"{stream['pod']}/{stream['container']} {log}"

        if self._as_ndjson:
            return (
                orjson.dumps(
                    {
                        "container": stream["container"],
                        "log": log,
                        "pod": stream["pod"],
                        "namespace": stream["namespace"],
                    }
                )
                + b"\n"  # bring to ndjson format
            )

        return log.encode()

    async def _iterate(self) -> AsyncIterator[bytes]:
        async for res in self._loki_client.query_range_page_iterate(
            query=self._query,
            start=self._start,
            end=self._end,
            direction=self._direction,
        ):
            for log_data in res["data"]["result"]:
                yield self.encode_and_handle_log(log_data)


class LokiLogsService(BaseLogsService):
    def __init__(
        self,
        kube_client: KubeClient,
        loki_client: LokiClient,
        max_query_lookback_s: int,
    ) -> None:
        super().__init__(kube_client)
        self._loki_client = loki_client
        if max_query_lookback_s < 60 * 60 * 24:
            exc_txt = "Retention period should be at least 1 day"
            raise ValueError(exc_txt)
        self._max_query_lookback_s = max_query_lookback_s

    @asyncgeneratorcontextmanager
    async def get_pod_log_reader(  # noqa: C901
        self,
        pod_name: str,
        namespace: str,
        *,
        since: datetime | None = None,
        separator: bytes | None = None,
        timestamps: bool = False,
        timeout_s: float = 10.0 * 60,
        interval_s: float = 1.0,
        archive_delay_s: float = 5,
        debug: bool = False,
        stop_func: Callable[[], Awaitable[bool]] | None = None,
    ) -> AsyncGenerator[bytes]:
        now_dt = datetime.now(UTC)
        start_dt = (
            now_dt - timedelta(seconds=self._max_query_lookback_s) + timedelta(hours=1)
        )  # +1 hour prevent max query length error
        if since:
            start_dt = max(start_dt, since)

        archive_border_dt = (now_dt - timedelta(seconds=archive_delay_s)).replace(
            microsecond=0
        )  # kube api log can't work with microseconds

        should_get_archive_logs = True
        should_get_live_logs = True
        try:
            status = await self._kube_client.wait_pod_is_not_waiting(
                pod_name,
                namespace=namespace,
                timeout_s=timeout_s,
                interval_s=interval_s,
            )

            if start_dt >= archive_border_dt:
                archive_border_dt = start_dt

            if (
                status.is_running
                and status.started_at
                and status.started_at > archive_border_dt
            ):
                archive_border_dt = status.started_at

            if status.is_terminated and status.finished_at:
                archive_border_dt = status.finished_at

            # There is a case f.e. when pod terminated at 12:00:00.666 and last log was
            # at 12:00:00.555 but finished_at truncated to 12:00:00.000 by api,
            # so this logs appeared as live logs and not appeared in archive.
        except JobNotFoundException:
            should_get_live_logs = False
            archive_border_dt = now_dt

        has_archive = False

        if start_dt >= archive_border_dt:
            should_get_archive_logs = False

        if should_get_archive_logs:
            start = int(start_dt.timestamp() * 1_000_000_000)
            end = int(archive_border_dt.timestamp() * 1_000_000_000) - 1
            async with self.get_pod_archive_log_reader(
                # Note: For jobs container store the same value as pod_name.
                # Pod name located in json log entry, while container is in label.
                # So we filter by container label, it faster.
                f'{{namespace="{namespace}", container="{pod_name}"}}',
                start=start,
                end=end,
                timestamps=timestamps,
                direction="forward",
            ) as it:
                async for chunk in it:
                    has_archive = True
                    yield chunk

        if not has_archive:
            separator = None

        if should_get_live_logs:
            since = archive_border_dt
            try:
                while True:
                    async with self.get_pod_live_log_reader(
                        pod_name,
                        namespace,
                        since=since,
                        timestamps=timestamps,
                        debug=debug,
                    ) as it:
                        if debug:
                            yield f"=== Live logs from {since=} ===\n".encode()
                        async for chunk in it:
                            if separator:
                                yield separator + b"\n"
                                separator = None
                            yield chunk

                    if not status.can_restart:
                        break

                    status = await self.wait_pod_is_running(
                        pod_name,
                        status.started_at,
                        namespace=namespace,
                        timeout_s=timeout_s,
                        interval_s=interval_s,
                    )
                    since = status.started_at
            except JobNotFoundException:
                pass

    async def get_pod_container_live_log_reader(
        self,
        *,
        pod_name: str,
        container_name: str,
        namespace: str,
        since: datetime | None = None,
        timestamps: bool = False,
        debug: bool = False,
        prefix: bool = True,
        as_ndjson: bool = False,
    ) -> AsyncIterator[bytes]:
        try:
            while True:
                status = await self._kube_client.get_container_status(
                    name=pod_name, container_name=container_name, namespace=namespace
                )
                if status.is_pod_terminated:
                    break

                async with self.get_pod_live_log_reader(
                    pod_name,
                    namespace,
                    container_name=container_name,
                    since=since,
                    timestamps=timestamps,
                    debug=debug,
                ) as it:
                    if debug:
                        yield (
                            f"=== Live logs from "
                            f"{since=} {pod_name=} {container_name=} ===\n"
                        ).encode()
                    async for chunk in it:
                        if prefix:
                            chunk = f"[{pod_name}/{container_name}] ".encode() + chunk

                        if as_ndjson:
                            chunk = (
                                orjson.dumps(
                                    {
                                        "pod": pod_name,
                                        "container": container_name,
                                        "log": chunk.decode(),
                                        "namespace": namespace,
                                    }
                                )
                                + b"\n"  # bring to ndjson format
                            )

                        yield chunk

                if not status.can_restart:
                    break

                status = await self.wait_pod_is_running(
                    pod_name,
                    status.started_at,
                    namespace=namespace,
                )
                since = status.started_at
        except JobNotFoundException:
            pass
        except Exception as e:
            exc_txt = (
                f"Error while get_pod_container_live_log_reader "
                f"{pod_name=} {container_name=}\nException: {e}"
            )
            raise Exception(exc_txt) from e

    async def get_pod_containers_live_log_reader(
        self,
        *,
        containers: list[str],
        k8s_label_selector: dict[str, str],
        namespace: str,
        since: datetime | None = None,
        separator: bytes | None = None,
        timestamps: bool = False,
        debug: bool = False,
        prefix: bool = True,
        as_ndjson: bool = False,
    ) -> AsyncIterator[bytes]:
        label_selector = ",".join(
            f"{key}={value}" for key, value in k8s_label_selector.items()
        )

        pods = await self._kube_client.get_pods(
            namespace=namespace, label_selector=label_selector
        )

        async for chunk in as_generated(
            [
                self.get_pod_container_live_log_reader(
                    pod_name=pod.metadata.name,
                    container_name=container.name,
                    namespace=namespace,
                    since=since,
                    timestamps=timestamps,
                    debug=debug,
                    prefix=prefix,
                    as_ndjson=as_ndjson,
                )
                for pod in pods
                if pod.metadata.name
                for container in pod.spec.containers
                if not containers or container.name in containers
            ],
            return_exceptions=True,
        ):
            if isinstance(chunk, Exception):
                logger.error(str(chunk))
                continue
            if separator:
                yield separator + b"\n"
                separator = None
            yield chunk

    @staticmethod
    def _build_loki_labels_filter_query(
        exactly_equal_labels: dict[str, Any],
        regex_matches_labels: dict[str, Any] | None = None,
    ) -> str:
        """
        Example: {app="myapp", environment="dev", container=~"myapp-.*|myapp-2.*"}
        """
        exactly_equal_labels_list = [
            f'{key}="{value}"' for key, value in exactly_equal_labels.items()
        ]
        regex_matches_labels = regex_matches_labels or {}
        regex_matches_labels_list = [
            f'{key}=~"{value}"' for key, value in regex_matches_labels.items()
        ]
        labels_filter = ",".join(exactly_equal_labels_list + regex_matches_labels_list)
        return f"{{{labels_filter}}}"

    @asyncgeneratorcontextmanager
    async def get_pod_log_reader_by_containers(  # noqa: C901
        self,
        containers: list[str] | None,
        loki_label_selector: dict[str, str] | None,
        k8s_label_selector: dict[str, str] | None,
        namespace: str,
        *,
        since: datetime | None = None,
        separator: bytes | None = None,
        timestamps: bool = False,
        archive_delay_s: float = 5,
        debug: bool = False,
        prefix: bool = False,
        as_ndjson: bool = False,
    ) -> AsyncGenerator[bytes]:
        containers = containers or []
        loki_label_selector = loki_label_selector or {}
        k8s_label_selector = k8s_label_selector or {}
        now_dt = datetime.now(UTC)
        start_dt = (
            now_dt - timedelta(seconds=self._max_query_lookback_s) + timedelta(hours=1)
        )  # +1 hour prevent max query length error
        if since:
            start_dt = max(start_dt, since)

        archive_border_dt = (now_dt - timedelta(seconds=archive_delay_s)).replace(
            microsecond=0
        )  # kube api log can't work with microseconds

        should_get_archive_logs = True
        should_get_live_logs = True

        has_archive = False

        if start_dt >= archive_border_dt:
            should_get_archive_logs = False

        if should_get_archive_logs:
            start = int(start_dt.timestamp() * 1_000_000_000)
            end = int(archive_border_dt.timestamp() * 1_000_000_000) - 1

            loki_regex_matches_labels = (
                {"container": "|".join(containers)} if containers else {}
            )
            query = self._build_loki_labels_filter_query(
                exactly_equal_labels=loki_label_selector,
                regex_matches_labels=loki_regex_matches_labels,
            )

            async with self.get_pod_archive_log_reader(
                query,
                start=start,
                end=end,
                timestamps=timestamps,
                prefix=prefix,
                as_ndjson=as_ndjson,
            ) as it:
                async for chunk in it:
                    if not has_archive:
                        has_archive = True
                    yield chunk

        if not has_archive:
            separator = None

        if should_get_live_logs:
            async for chunk in self.get_pod_containers_live_log_reader(
                containers=containers,
                k8s_label_selector=k8s_label_selector,
                namespace=namespace,
                since=archive_border_dt,
                separator=separator,
                timestamps=timestamps,
                debug=debug,
                prefix=prefix,
                as_ndjson=as_ndjson,
            ):
                yield chunk

    def get_pod_archive_log_reader(  # type: ignore
        self,
        query: str,
        *,
        start: int,
        end: int | None = None,
        direction: str = "forward",  # or can be "backward"
        timestamps: bool = False,
        prefix: bool = False,
        as_ndjson: bool = False,
    ) -> LogReader:
        # have another set of params unlike base method, need # type: ignore
        return LokiLogReader(
            loki_client=self._loki_client,
            query=query,
            start=start,
            end=end,
            direction=direction,
            timestamps=timestamps,
            prefix=prefix,
            as_ndjson=as_ndjson,
        )

    async def get_label_values(
        self,
        label: str,
        loki_label_selector: dict[str, str],
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[str]:
        query = self._build_loki_labels_filter_query(
            exactly_equal_labels=loki_label_selector
        )

        now_dt = datetime.now(UTC)
        start_dt = (
            now_dt - timedelta(seconds=self._max_query_lookback_s) + timedelta(hours=1)
        )  # +1 hour prevent max query length error
        if since:
            start_dt = max(start_dt, since)

        start = int(start_dt.timestamp() * 1_000_000_000)
        end = int(until.timestamp() * 1_000_000_000) if until else None

        result = await self._loki_client.label_values(
            label=label, query=query, start=start, end=end
        )

        return result.get("data", [])
