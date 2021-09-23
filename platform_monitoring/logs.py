import abc
import asyncio
import io
import json
import logging
import zlib
from datetime import datetime, timedelta, timezone
from os.path import basename
from typing import Any, AsyncContextManager, AsyncIterator, Dict, List, Optional, Tuple

import aiohttp
from aiobotocore.client import AioBaseClient
from aiobotocore.response import StreamingBody
from aioelasticsearch import Elasticsearch
from aioelasticsearch.helpers import Scan
from async_timeout import timeout
from neuro_logging import trace

from .base import LogReader
from .kube_client import ContainerStatus, JobNotFoundException, KubeClient
from .utils import aclosing, asyncgeneratorcontextmanager, parse_date


logger = logging.getLogger(__name__)


DEFAULT_ARCHIVE_DELAY = 3.0 * 60

error_prefixes = (
    b"rpc error: code =",
    # failed to try resolving symlinks in path "/var/log/pods/xxx.log":
    # lstat /var/log/pods/xxx.log: no such file or directory
    b"failed to try resolving",
    # Unable to retrieve container logs for docker://xxxx
    b"Unable to retrieve",
)
max_error_prefix_len = max(map(len, error_prefixes))


async def filter_out_rpc_error(stream: aiohttp.StreamReader) -> AsyncIterator[bytes]:
    # https://github.com/neuromation/platform-api/issues/131
    # k8s API (and the underlying docker API) sometimes returns an rpc
    # error as the last log line. it says that the corresponding container
    # does not exist. we should try to not expose such internals, but only
    # if it is the last line indeed.

    _is_line_start = True
    unread_buffer = b""

    async def read_chunk(*, min_line_length: int) -> Tuple[bytes, bool]:
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
        client_conn_timeout_s: Optional[float] = None,
        client_read_timeout_s: Optional[float] = None,
        *,
        previous: bool = False,
        since: Optional[datetime] = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> None:
        self._client = client
        self._pod_name = pod_name
        self._container_name = container_name
        self._client_conn_timeout_s = client_conn_timeout_s
        self._client_read_timeout_s = client_read_timeout_s
        self._previous = previous
        self._since = since
        self._timestamps = timestamps
        self._debug = debug

        self._stream_cm: Optional[AsyncContextManager[aiohttp.StreamReader]] = None
        self._iterator: Optional[AsyncIterator[bytes]] = None

    async def __aenter__(self) -> AsyncIterator[bytes]:
        await self._client.wait_pod_is_not_waiting(self._pod_name)
        kwargs: Dict[str, Any] = {}
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
            pod_name=self._pod_name, container_name=self._container_name, **kwargs
        )
        assert self._stream_cm
        stream = await self._stream_cm.__aenter__()
        if self._debug:
            self._iterator = stream.iter_any()
        else:
            self._iterator = filter_out_rpc_error(stream)
        return self._iterator

    async def __aexit__(self, *args: Any) -> None:
        assert self._iterator
        if hasattr(self._iterator, "aclose"):
            await self._iterator.aclose()  # type: ignore
        assert self._stream_cm
        stream_cm = self._stream_cm
        self._stream_cm = None
        await stream_cm.__aexit__(*args)


class ElasticsearchLogReader(LogReader):
    def __init__(
        self,
        es_client: Elasticsearch,
        container_runtime: str,
        namespace_name: str,
        pod_name: str,
        container_name: str,
        *,
        since: Optional[datetime] = None,
        timestamps: bool = False,
    ) -> None:
        super().__init__(container_runtime=container_runtime, timestamps=timestamps)

        self._es_client = es_client
        self._index = "logstash-*"
        self._doc_type = "fluent-bit"
        self._namespace_name = namespace_name
        self._pod_name = pod_name
        self._container_name = container_name
        self._since = since
        self._scan: Optional[Scan] = None
        self._iterator: Optional[AsyncIterator[bytes]] = None

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
        self._iterator = self._iterate()
        return self._iterator

    async def __aexit__(self, *args: Any) -> None:
        assert self._iterator
        await self._iterator.aclose()  # type: ignore
        assert self._scan
        scan = self._scan
        self._scan = None
        await scan.__aexit__(*args)

    async def _iterate(self) -> AsyncIterator[bytes]:
        assert self._scan
        async for doc in self._scan:
            try:
                source = doc["_source"]
                time_str = source["time"]
                time = parse_date(time_str)
                if self._since is not None and time < self._since:
                    continue
                self.last_time = time
                log = source["log"]
                yield self.encode_log(time_str, log)
            except Exception:
                logger.exception("Invalid log entry: %r", doc)
                raise


class S3LogReader(LogReader):
    def __init__(
        self,
        s3_client: AioBaseClient,
        container_runtime: str,
        bucket_name: str,
        prefix_format: str,
        namespace_name: str,
        pod_name: str,
        container_name: str,
        *,
        since: Optional[datetime] = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> None:
        super().__init__(container_runtime=container_runtime, timestamps=timestamps)

        self._s3_client = s3_client
        self._bucket_name = bucket_name
        self._prefix_format = prefix_format
        self._namespace_name = namespace_name
        self._pod_name = pod_name
        self._container_name = container_name
        self._since = since
        self._debug = debug
        self._iterator: Optional[AsyncIterator[bytes]] = None

    @staticmethod
    def get_prefix(
        prefix_format: str, namespace_name: str, pod_name: str, container_name: str
    ) -> str:
        return prefix_format.format(
            namespace_name=namespace_name,
            pod_name=pod_name,
            container_name=container_name,
        )

    def _get_prefix(self) -> str:
        return self.get_prefix(
            prefix_format=self._prefix_format,
            namespace_name=self._namespace_name,
            pod_name=self._pod_name,
            container_name=self._container_name,
        )

    async def __aenter__(self) -> AsyncIterator[bytes]:
        self._iterator = self._iterate()
        return self._iterator

    @trace
    async def _load_log_keys(self, since: Optional[datetime]) -> List[str]:
        since_time_str = f"{since:%Y%m%d%H%M}" if since else ""
        paginator = self._s3_client.get_paginator("list_objects_v2")
        keys: List[Tuple[int, int, str]] = []
        async for page in paginator.paginate(
            Bucket=self._bucket_name, Prefix=self._get_prefix()
        ):
            for obj in page.get("Contents", ()):
                s3_key = obj["Key"]
                # get time slice from s3 key
                time_slice_str = basename(s3_key).split(".")[0].split("_")
                start_time_str = time_slice_str[0]
                index = int(time_slice_str[1])
                if start_time_str >= since_time_str:
                    keys.append((start_time_str, index, s3_key))
        keys.sort()  # order keys by time slice
        return [key[-1] for key in keys]

    async def __aexit__(self, *args: Any) -> None:
        assert self._iterator
        await self._iterator.aclose()  # type: ignore

    async def _iterate(self) -> AsyncIterator[bytes]:
        since = self._since
        keys = await self._load_log_keys(since)
        for key in keys:
            response = await self._s3_client.get_object(
                Bucket=self._bucket_name, Key=key
            )
            response_body = response["Body"]
            async with response_body:
                if response["ContentType"] == "application/x-gzip":
                    line_iterator = self._iter_decompressed_lines(response_body)
                else:
                    line_iterator = response_body.iter_lines()
                async with aclosing(line_iterator):
                    async for line in line_iterator:
                        try:
                            event = json.loads(line)
                            time_str = event["time"]
                            time = parse_date(time_str)
                            if since is not None and time < since:
                                continue
                            self.last_time = time
                            if self._debug and key:
                                yield f"~~~ From file {basename(key)}\n".encode()
                                key = ""
                            log = event["log"]
                            yield self.encode_log(time_str, log)
                        except Exception:
                            logger.exception("Invalid log entry: %r", line)
                            raise

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


class LogsService(abc.ABC):
    @asyncgeneratorcontextmanager
    async def get_pod_log_reader(
        self,
        pod_name: str,
        *,
        since: Optional[datetime] = None,
        separator: Optional[bytes] = None,
        timestamps: bool = False,
        timeout_s: float = 10.0 * 60,
        interval_s: float = 1.0,
        archive_delay_s: float = DEFAULT_ARCHIVE_DELAY,
        debug: bool = False,
    ) -> AsyncIterator[bytes]:
        archive_delay = timedelta(seconds=archive_delay_s)

        def get_last_start(status: ContainerStatus) -> Optional[datetime]:
            if status.is_running:
                return status.started_at
            elif status.is_pod_terminated:
                finish = status.finished_at
                if finish is not None and _utcnow() - finish < archive_delay:
                    return status.started_at
            return None

        try:
            status = await self.get_container_status(pod_name)
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
                pod_name, since=since, timestamps=timestamps, debug=debug
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
                            status = await self.get_container_status(pod_name)
                            start = get_last_start(status)
                        except JobNotFoundException:
                            start = None
                        if start is not None:
                            if start > until:
                                until = start
                            else:
                                first = await self.get_first_log_entry_time(
                                    pod_name, timeout_s=archive_delay_s
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
                        status = await self.get_container_status(pod_name)
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
                            pod_name, timeout_s=archive_delay_s
                        )
                        if first is not None:
                            if first > until:
                                until = first
                                continue
                            # Start reading from container.
                            break

                    if request_time - prev_finish < archive_delay:
                        until = None
                        await asyncio.sleep(interval_s)
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
                    pod_name, start, timeout_s=timeout_s, interval_s=interval_s
                )
                start = status.started_at
            if start is not None and (since is None or since < start):
                since = start

            while True:
                async with self.get_pod_live_log_reader(
                    pod_name, since=since, timestamps=timestamps, debug=debug
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
                    pod_name, start, timeout_s=timeout_s, interval_s=interval_s
                )
                since = start = status.started_at
        except JobNotFoundException:
            pass

    @abc.abstractmethod
    async def get_first_log_entry_time(
        self, pod_name: str, *, timeout_s: float = 2.0 * 60
    ) -> Optional[datetime]:
        pass  # pragma: no cover

    async def wait_pod_is_running(
        self,
        name: str,
        old_start: Optional[datetime],
        *,
        timeout_s: float = 10.0 * 60,
        interval_s: float = 1.0,
    ) -> ContainerStatus:
        async with timeout(timeout_s):
            while True:
                status = await self.get_container_status(name)
                if not status.is_waiting:
                    if status.started_at != old_start:
                        return status
                    if status.is_pod_terminated:
                        raise JobNotFoundException
                await asyncio.sleep(interval_s)

    @abc.abstractmethod
    async def get_container_status(self, name: str) -> ContainerStatus:
        pass  # pragma: no cover

    @abc.abstractmethod
    def get_pod_live_log_reader(
        self,
        pod_name: str,
        *,
        since: Optional[datetime] = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> LogReader:
        pass  # pragma: no cover

    @abc.abstractmethod
    def get_pod_archive_log_reader(
        self,
        pod_name: str,
        *,
        since: Optional[datetime] = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> LogReader:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def drop_logs(self, pod_name: str) -> None:
        pass  # pragma: no cover


class BaseLogsService(LogsService):
    count = 0

    def __init__(self, kube_client: KubeClient) -> None:
        self._kube_client = kube_client

    async def get_container_status(self, name: str) -> ContainerStatus:
        return await self._kube_client.get_container_status(name)

    def get_pod_live_log_reader(
        self,
        pod_name: str,
        *,
        since: Optional[datetime] = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> LogReader:
        return PodContainerLogReader(
            client=self._kube_client,
            pod_name=pod_name,
            container_name=pod_name,
            since=since,
            timestamps=timestamps,
            debug=debug,
        )

    async def get_first_log_entry_time(
        self, pod_name: str, *, timeout_s: float = 2.0 * 60
    ) -> Optional[datetime]:
        return await get_first_log_entry_time(
            self._kube_client, pod_name, timeout_s=timeout_s
        )


class ElasticsearchLogsService(BaseLogsService):
    # TODO (A Yushkovskiy 07-Jun-2019) Add another abstraction layer joining together
    #  kube-client and elasticsearch-client (in platform-api it's KubeOrchestrator)
    #  and move there method `get_pod_log_reader`

    def __init__(
        self, kube_client: KubeClient, es_client: Elasticsearch, container_runtime: str
    ) -> None:
        super().__init__(kube_client)
        self._es_client = es_client
        self._container_runtime = container_runtime

    def get_pod_archive_log_reader(
        self,
        pod_name: str,
        *,
        since: Optional[datetime] = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> LogReader:
        return ElasticsearchLogReader(
            es_client=self._es_client,
            container_runtime=self._container_runtime,
            namespace_name=self._kube_client.namespace,
            pod_name=pod_name,
            container_name=pod_name,
            since=since,
            timestamps=timestamps,
        )

    async def drop_logs(self, pod_name: str) -> None:
        raise NotImplementedError("Dropping logs for Elasticsearch is not implemented")


class S3LogsService(BaseLogsService):
    def __init__(
        self,
        kube_client: KubeClient,
        s3_client: AioBaseClient,
        container_runtime: str,
        bucket_name: str,
        key_prefix_format: str,
    ) -> None:
        super().__init__(kube_client)
        self._s3_client = s3_client
        self._container_runtime = container_runtime
        self._bucket_name = bucket_name
        self._key_prefix_format = key_prefix_format

    def get_pod_archive_log_reader(
        self,
        pod_name: str,
        *,
        since: Optional[datetime] = None,
        timestamps: bool = False,
        debug: bool = False,
    ) -> LogReader:
        return S3LogReader(
            s3_client=self._s3_client,
            container_runtime=self._container_runtime,
            bucket_name=self._bucket_name,
            prefix_format=self._key_prefix_format,
            namespace_name=self._kube_client.namespace,
            pod_name=pod_name,
            container_name=pod_name,
            since=since,
            timestamps=timestamps,
            debug=debug,
        )

    async def drop_logs(self, pod_name: str) -> None:
        paginator = self._s3_client.get_paginator("list_objects_v2")

        async for page in paginator.paginate(
            Bucket=self._bucket_name,
            Prefix=S3LogReader.get_prefix(
                self._key_prefix_format,
                namespace_name=self._kube_client.namespace,
                pod_name=pod_name,
                container_name=pod_name,
            ),
        ):
            for obj in page.get("Contents", ()):
                await self._s3_client.delete_object(
                    Bucket=self._bucket_name, Key=obj["Key"]
                )


async def get_first_log_entry_time(
    kube_client: KubeClient, pod_name: str, *, timeout_s: float = 60 * 2.0
) -> Optional[datetime]:
    """Return the timestamp of the first container log line from Kubernetes.

    Return None if the container is not created yet, or there are no logs yet,
    or if it takes too long for reading the first timestamp.
    """
    time_str = b""
    try:
        async with kube_client.create_pod_container_logs_stream(
            pod_name=pod_name,
            container_name=pod_name,
            timestamps=True,
            read_timeout_s=timeout_s,
        ) as stream:
            async with timeout(timeout_s):
                while True:
                    chunk = await stream.readany()
                    if not chunk:
                        break
                    pos = chunk.find(b" ")
                    if pos >= 0:
                        time_str += chunk[:pos]
                        break
                    else:
                        time_str += chunk
                else:
                    return None
    except (asyncio.TimeoutError, JobNotFoundException):
        return None
    try:
        return parse_date(time_str.decode())
    except ValueError:
        return None


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)
