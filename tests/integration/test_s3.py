from __future__ import annotations

import json
import logging
import uuid
import zlib
from collections.abc import (
    Awaitable,
    Callable,
    Iterable,
    Sequence,
)
from datetime import UTC, datetime, timedelta
from io import BytesIO
from unittest import mock

import botocore.exceptions
import pytest
from aiobotocore.client import AioBaseClient

from platform_monitoring.logs import (
    ZLIB_WBITS,
    S3FileReader,
    S3LogFile,
    S3LogRecord,
    S3LogRecordsReader,
    S3LogRecordsWriter,
    S3LogsMetadata,
    S3LogsMetadataService,
    S3LogsMetadataStorage,
    S3LogsService,
)


logger = logging.getLogger(__name__)


@pytest.fixture
async def write_lines_to_s3(
    s3_client: AioBaseClient, s3_logs_bucket: str
) -> Callable[..., Awaitable[None]]:
    async def _put(key: str, *lines: str, compress: bool = False) -> None:
        body = "\n".join(lines).encode()
        if compress:
            compress_obj = zlib.compressobj(wbits=ZLIB_WBITS)
            body = compress_obj.compress(body) + compress_obj.flush()
        kwargs = {
            "Bucket": s3_logs_bucket,
            "Key": key,
            "Body": body,
        }
        if compress:
            kwargs["ContentType"] = "application/x-gzip"
        await s3_client.put_object(**kwargs)

    return _put


class TestS3FileReader:
    async def test_iter_lines__without_compression(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        key = f"tests/{uuid.uuid4()}"
        reader = S3FileReader(s3_client, s3_logs_bucket, key)

        await write_lines_to_s3(key, "1", "2", "3")

        result = [line async for line in reader.iter_lines()]

        assert result == [b"1", b"2", b"3"]

    async def test_iter_lines__with_compression(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        key = f"tests/{uuid.uuid4()}"
        # chunk_size = 1 will for DecompressObj to periodically
        # keep decompressed data in internal buffer and not return
        # it to the caller.
        reader = S3FileReader(s3_client, s3_logs_bucket, key, chunk_size=1)

        await write_lines_to_s3(key, "1", "2", "3", compress=True)

        result = [line async for line in reader.iter_lines()]

        assert result == [b"1", b"2", b"3"]


class TestS3LogsMetadataStorage:
    @pytest.mark.parametrize("cache_metadata", [(True,), (False,)])
    async def test_get(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        cache_metadata: bool,  # noqa: FBT001
    ) -> None:
        pod_name = f"test-{uuid.uuid4()}"
        metadata = S3LogsMetadata(
            last_compaction_time=datetime(2023, 1, 2), last_merged_key="key"
        )
        storage = S3LogsMetadataStorage(
            s3_client, s3_logs_bucket, cache_metadata=cache_metadata
        )

        await storage.put(pod_name, metadata)
        result = await storage.get(pod_name)

        assert result == metadata

    @pytest.mark.parametrize("cache_metadata", [(True,), (False,)])
    async def test_get__no_key(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        cache_metadata: bool,  # noqa: FBT001
    ) -> None:
        pod_name = f"test-{uuid.uuid4()}"
        storage = S3LogsMetadataStorage(
            s3_client, s3_logs_bucket, cache_metadata=cache_metadata
        )

        result = await storage.get(pod_name)

        assert result == S3LogsMetadata()

    @pytest.mark.parametrize("cache_metadata", [(True,), (False,)])
    async def test_put(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        cache_metadata: bool,  # noqa: FBT001
    ) -> None:
        pod_name = f"test-{uuid.uuid4()}"
        metadata = S3LogsMetadata(
            last_compaction_time=datetime(2023, 1, 2), last_merged_key="key"
        )
        storage = S3LogsMetadataStorage(
            s3_client, s3_logs_bucket, cache_metadata=cache_metadata
        )

        await storage.put(pod_name, metadata)
        result = await storage.get(pod_name)

        assert result == metadata

        resp = await s3_client.get_object(
            Bucket=s3_logs_bucket, Key=storage.get_metadata_key(pod_name)
        )
        resp_body = await resp["Body"].read()

        assert json.loads(resp_body) == metadata.to_primitive()


def _create_raw_log_key_prefix(pod_name: str) -> str:
    return S3LogsMetadataService.RAW_LOG_KEY_PREFIX_FORMAT.format(
        namespace_name="default", pod_name=pod_name, container_name=pod_name
    )


def _create_merged_key_prefix(pod_name: str) -> str:
    return f"{S3LogRecordsWriter.LOGS_KEY_PREFIX}/{pod_name}"


def _create_log_file(key: str) -> S3LogFile:
    return S3LogFile(
        key=key,
        records_count=1,
        size=2,
        first_record_time=datetime(2023, 1, 1, 0, 0),
        last_record_time=datetime(2023, 1, 1, 0, 0, 30),
    )


class TestS3LogsMetadataService:
    async def test_get_log_keys__raw_keys_not_deleted_after_merge(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        merged_log_key_prefix = _create_merged_key_prefix(pod_name)
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        metadata = S3LogsMetadata(
            log_files=[_create_log_file(f"{merged_log_key_prefix}/202301010000_0.gz")],
            last_merged_key=f"{raw_log_key_prefix}/202301010000_0.gz",
        )
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_1.gz", '{"log":"4"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_0.gz", '{"log":"3"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_1.gz", '{"log":"2"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_0.gz", '{"log":"1"}'
        )

        keys = await s3_logs_metadata_service.get_log_keys(pod_name)

        assert keys == [
            f"{merged_log_key_prefix}/202301010000_0.gz",
            f"{raw_log_key_prefix}/202301010000_1.gz",
            f"{raw_log_key_prefix}/202301010001_0.gz",
            f"{raw_log_key_prefix}/202301010001_1.gz",
        ]

    async def test_get_log_keys__raw_keys_deleted_after_merge(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        merged_log_key_prefix = _create_merged_key_prefix(pod_name)
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        metadata = S3LogsMetadata(
            log_files=[_create_log_file(f"{merged_log_key_prefix}/202301010000_0.gz")],
            last_merged_key=f"{raw_log_key_prefix}/202301010000_0.gz",
        )
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_1.gz", '{"log":"4"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_0.gz", '{"log":"3"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_1.gz", '{"log":"2"}'
        )

        keys = await s3_logs_metadata_service.get_log_keys(pod_name)

        assert keys == [
            f"{merged_log_key_prefix}/202301010000_0.gz",
            f"{raw_log_key_prefix}/202301010000_1.gz",
            f"{raw_log_key_prefix}/202301010001_0.gz",
            f"{raw_log_key_prefix}/202301010001_1.gz",
        ]

    async def test_get_log_keys__no_raw_keys(
        self, s3_logs_metadata_service: S3LogsMetadataService
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_merged_key_prefix(pod_name)
        metadata = S3LogsMetadata(
            log_files=[_create_log_file(f"{raw_log_key_prefix}/202301010000_0.gz")],
            last_merged_key=f"{raw_log_key_prefix}/202301010000_0.gz",
        )
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)

        keys = await s3_logs_metadata_service.get_log_keys(pod_name)

        assert keys == [f"{raw_log_key_prefix}/202301010000_0.gz"]

    async def test_get_log_keys_since(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        merged_log_key_prefix = _create_merged_key_prefix(pod_name)
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        metadata = S3LogsMetadata(
            log_files=[_create_log_file(f"{merged_log_key_prefix}/202301010000_0.gz")],
            last_merged_key=f"{raw_log_key_prefix}/202301010000_0.gz",
        )
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_1.gz", '{"log":"4"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_0.gz", '{"log":"3"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_1.gz", '{"log":"2"}'
        )

        all_keys = [
            f"{merged_log_key_prefix}/202301010000_0.gz",
            f"{raw_log_key_prefix}/202301010000_1.gz",
            f"{raw_log_key_prefix}/202301010001_0.gz",
            f"{raw_log_key_prefix}/202301010001_1.gz",
        ]

        keys = await s3_logs_metadata_service.get_log_keys(
            pod_name, since=datetime(2023, 1, 1, 0, 0, 30)
        )
        assert keys == all_keys

        keys = await s3_logs_metadata_service.get_log_keys(
            pod_name, since=datetime(2023, 1, 1, 0, 0, 31)
        )
        assert keys == all_keys[1:]

        keys = await s3_logs_metadata_service.get_log_keys(
            pod_name, since=datetime(2023, 1, 1, 0, 1)
        )
        assert keys == all_keys[2:]

    async def test_get_raw_log_keys(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_1.gz", '{"log":"3"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_0.gz", '{"log":"2"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_0.gz", '{"log":"1"}'
        )

        keys = await s3_logs_metadata_service.get_raw_log_keys(pod_name)

        assert keys == [
            f"{raw_log_key_prefix}/202301010000_0.gz",
            f"{raw_log_key_prefix}/202301010001_0.gz",
            f"{raw_log_key_prefix}/202301010001_1.gz",
        ]

    async def test_get_raw_log_keys_since(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_1.gz", '{"log":"3"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010001_0.gz", '{"log":"2"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_0.gz", '{"log":"1"}'
        )

        all_keys = [
            f"{raw_log_key_prefix}/202301010000_0.gz",
            f"{raw_log_key_prefix}/202301010001_0.gz",
            f"{raw_log_key_prefix}/202301010001_1.gz",
        ]

        keys = await s3_logs_metadata_service.get_raw_log_keys(
            pod_name, since=datetime(2023, 1, 1, 0, 0)
        )
        assert keys == all_keys

        keys = await s3_logs_metadata_service.get_raw_log_keys(
            pod_name, since=datetime(2023, 1, 1, 0, 1)
        )
        assert keys == all_keys[1:]

        keys = await s3_logs_metadata_service.get_raw_log_keys(
            pod_name, since=datetime(2023, 1, 1, 0, 2)
        )
        assert keys == []

    async def test_get_pods_compact_queue__old_logs(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        metadata = S3LogsMetadata(
            last_compaction_time=datetime.now(UTC) - timedelta(hours=1)
        )
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_0.gz", '{"log":"1"}'
        )
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_1.gz", '{"log":"2"}'
        )

        queue = await s3_logs_metadata_service.get_pods_compact_queue(
            compact_interval=60
        )

        assert pod_name in queue

    async def test_get_pods_compact_queue__recent_logs(
        self,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        metadata = S3LogsMetadata(last_compaction_time=datetime.now(UTC))
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await write_lines_to_s3(
            f"{raw_log_key_prefix}/202301010000_0.gz", '{"log":"1"}'
        )

        queue = await s3_logs_metadata_service.get_pods_compact_queue(
            compact_interval=60
        )

        # Recent logs should wait compaction_interval
        assert pod_name not in queue

    async def test_add_pod_to_cleanup_queue(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_logs_metadata_service: S3LogsMetadataService,
    ) -> None:
        pod_name = str(uuid.uuid4())
        await s3_logs_metadata_service.add_pod_to_cleanup_queue(pod_name)

        await s3_client.get_object(
            Bucket=s3_logs_bucket,
            Key=f"{S3LogsMetadataService.CLEANUP_KEY_PREFIX}/{pod_name}",
        )

    async def test_remove_pod_from_cleanup_queue(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_logs_metadata_service: S3LogsMetadataService,
    ) -> None:
        pod_name = str(uuid.uuid4())
        await s3_logs_metadata_service.add_pod_to_cleanup_queue(pod_name)
        await s3_client.get_object(
            Bucket=s3_logs_bucket,
            Key=f"{S3LogsMetadataService.CLEANUP_KEY_PREFIX}/{pod_name}",
        )
        await s3_logs_metadata_service.remove_pod_from_cleanup_queue(pod_name)

        with pytest.raises(botocore.exceptions.ClientError):
            await s3_client.get_object(
                Bucket=s3_logs_bucket,
                Key=f"{S3LogsMetadataService.CLEANUP_KEY_PREFIX}/{pod_name}",
            )

    async def test_add_pod_to_cleanup_queue__old_logs(
        self, s3_logs_metadata_service: S3LogsMetadataService
    ) -> None:
        pod_name = str(uuid.uuid4())
        metadata = S3LogsMetadata(
            last_compaction_time=datetime.now(UTC) - timedelta(hours=1)
        )
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await s3_logs_metadata_service.add_pod_to_cleanup_queue(pod_name)

        queue = await s3_logs_metadata_service.get_pods_cleanup_queue(
            cleanup_interval=60
        )

        assert pod_name in queue

    async def test_add_pod_to_cleanup_queue__recent_logs(
        self, s3_logs_metadata_service: S3LogsMetadataService
    ) -> None:
        pod_name = str(uuid.uuid4())
        metadata = S3LogsMetadata(last_compaction_time=datetime.now(UTC))
        await s3_logs_metadata_service.update_metadata(pod_name, metadata)
        await s3_logs_metadata_service.add_pod_to_cleanup_queue(pod_name)

        queue = await s3_logs_metadata_service.get_pods_cleanup_queue()

        assert pod_name not in queue


class TestS3LogRecordReader:
    @pytest.fixture
    def reader(
        self, s3_client: AioBaseClient, s3_logs_bucket: str
    ) -> S3LogRecordsReader:
        return S3LogRecordsReader(s3_client, s3_logs_bucket)

    async def test_iter_records__raw_keys(
        self,
        reader: S3LogRecordsReader,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
        ]
        await write_lines_to_s3(
            pod_keys[0], '{"time":"2023-01-01T12:34:56.123456","log":"1"}'
        )
        await write_lines_to_s3(
            pod_keys[1], '{"time":"2023-01-01T12:34:57.123456","log":"2"}'
        )

        result = [r async for r in reader.iter_records(pod_keys)]

        assert result == [
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 56, 123456, UTC),
                time_str="2023-01-01T12:34:56.123456",
                message="1",
                container_id="c1",
            ),
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 57, 123456, UTC),
                time_str="2023-01-01T12:34:57.123456",
                message="2",
                container_id="c1",
            ),
        ]

    async def test_iter_records__merged_keys(
        self,
        reader: S3LogRecordsReader,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_merged_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}/c1/202301011234_0.gz",
            f"{raw_log_key_prefix}/c1/202301011234_1.gz",
        ]
        await write_lines_to_s3(
            pod_keys[0], '{"time":"2023-01-01T12:34:56.123456","log":"1"}'
        )
        await write_lines_to_s3(
            pod_keys[1], '{"time":"2023-01-01T12:34:57.123456","log":"2"}'
        )

        result = [r async for r in reader.iter_records(pod_keys)]

        assert result == [
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 56, 123456, UTC),
                time_str="2023-01-01T12:34:56.123456",
                message="1",
                container_id="c1",
            ),
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 57, 123456, UTC),
                time_str="2023-01-01T12:34:57.123456",
                message="2",
                container_id="c1",
            ),
        ]

    async def test_iter_records__fallback_to_key_time(
        self,
        reader: S3LogRecordsReader,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [f"{raw_log_key_prefix}-c1.log/202301011234_0.gz"]
        await write_lines_to_s3(pod_keys[0], '{"log":"1"}')

        result = [r async for r in reader.iter_records(pod_keys)]

        assert result == [
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, tzinfo=UTC),
                time_str="2023-01-01T12:34:00.000000",
                message="1",
                container_id="c1",
            )
        ]

    async def test_iter_records__fallback_to_last_record_time(
        self,
        reader: S3LogRecordsReader,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
        ]
        await write_lines_to_s3(
            pod_keys[0],
            '{"time":"2023-01-01T12:34:56.123456","log":"1"}',
            '{"log":"2"}',
        )
        await write_lines_to_s3(pod_keys[1], '{"log":"3"}')

        result = [r async for r in reader.iter_records(pod_keys)]

        assert result == [
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 56, 123456, UTC),
                time_str="2023-01-01T12:34:56.123456",
                message="1",
                container_id="c1",
            ),
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 56, 123456, UTC),
                time_str="2023-01-01T12:34:56.123456",
                message="2",
                container_id="c1",
            ),
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 56, 123456, UTC),
                time_str="2023-01-01T12:34:56.123456",
                message="3",
                container_id="c1",
            ),
        ]


class TestS3LogRecordWriter:
    @pytest.fixture
    def records(self) -> list[S3LogRecord]:
        return [
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 56, 123456, UTC),
                time_str="2023-01-01T12:34:56.123456",
                message="1",
                container_id="c1",
            ),
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 57, 123456, UTC),
                time_str="2023-01-01T12:34:57.123456",
                message="2",
                container_id="c1",
            ),
            S3LogRecord(
                time=datetime(2023, 1, 1, 12, 34, 58, 123456, UTC),
                time_str="2023-01-01T12:34:58.123456",
                message="2",
                container_id="c1",
                stream="stderr",
            ),
        ]

    async def _assert_records_written(
        self, writer: S3LogRecordsWriter, expected_records: Sequence[S3LogRecord]
    ) -> None:
        reader = S3LogRecordsReader(writer._s3_client, writer._bucket_name)
        keys = [f.key for f in writer.get_output_files()]
        result = [r async for r in reader.iter_records(keys)]
        assert result == expected_records

    async def test_write__single_file(
        self, s3_client: AioBaseClient, s3_logs_bucket: str, records: list[S3LogRecord]
    ) -> None:
        async with S3LogRecordsWriter(
            s3_client, s3_logs_bucket, str(uuid.uuid4())
        ) as writer:
            for record in records:
                await writer.write(record)

        assert writer.get_output_files() == [
            S3LogFile(
                key=mock.ANY,
                records_count=3,
                size=162,
                first_record_time=records[0].time,
                last_record_time=records[2].time,
            )
        ]

        await self._assert_records_written(writer, records)

    async def test_write__multiple_files(
        self, s3_client: AioBaseClient, s3_logs_bucket: str, records: list[S3LogRecord]
    ) -> None:
        async with S3LogRecordsWriter(
            s3_client, s3_logs_bucket, str(uuid.uuid4()), size_limit=110
        ) as writer:
            for record in records:
                await writer.write(record)

        assert writer.get_output_files() == [
            S3LogFile(
                key=mock.ANY,
                records_count=2,
                size=96,
                first_record_time=records[0].time,
                last_record_time=records[1].time,
            ),
            S3LogFile(
                key=mock.ANY,
                records_count=1,
                size=66,
                first_record_time=records[2].time,
                last_record_time=records[2].time,
            ),
        ]

        await self._assert_records_written(writer, records)

    async def test_write__long_record(
        self, s3_client: AioBaseClient, s3_logs_bucket: str, records: list[S3LogRecord]
    ) -> None:
        async with S3LogRecordsWriter(
            s3_client, s3_logs_bucket, str(uuid.uuid4()), size_limit=1
        ) as writer:
            for record in records:
                await writer.write(record)

        assert writer.get_output_files() == [
            S3LogFile(
                key=mock.ANY,
                records_count=1,
                size=48,
                first_record_time=records[0].time,
                last_record_time=records[0].time,
            ),
            S3LogFile(
                key=mock.ANY,
                records_count=1,
                size=48,
                first_record_time=records[1].time,
                last_record_time=records[1].time,
            ),
            S3LogFile(
                key=mock.ANY,
                records_count=1,
                size=66,
                first_record_time=records[2].time,
                last_record_time=records[2].time,
            ),
        ]

        await self._assert_records_written(writer, records)

    async def test_write__shared_buffer(
        self, s3_client: AioBaseClient, s3_logs_bucket: str, records: list[S3LogRecord]
    ) -> None:
        buffer = BytesIO()

        async with S3LogRecordsWriter(
            s3_client, s3_logs_bucket, str(uuid.uuid4()), buffer=buffer
        ) as writer:
            for record in records:
                await writer.write(record)

        async with S3LogRecordsWriter(
            s3_client, s3_logs_bucket, str(uuid.uuid4()), buffer=buffer
        ) as writer:
            for record in records:
                await writer.write(record)

        assert writer.get_output_files() == [
            S3LogFile(
                key=mock.ANY,
                records_count=3,
                size=162,
                first_record_time=records[0].time,
                last_record_time=records[2].time,
            ),
        ]

        await self._assert_records_written(writer, records)


class TestS3LogsService:
    @pytest.fixture
    def assert_records_written(
        self, s3_client: AioBaseClient, s3_logs_bucket: str
    ) -> Callable[..., Awaitable[None]]:
        async def _assert(keys: Iterable[str], expected_records: Sequence[str]) -> None:
            result = []
            for key in keys:
                reader = S3FileReader(s3_client, s3_logs_bucket, key)
                result.extend([line.decode() async for line in reader.iter_lines()])
            assert result == expected_records

        return _assert

    async def test_compact_one(
        self,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
        assert_records_written: Callable[..., Awaitable[None]],
    ) -> None:
        now = datetime.now(UTC)
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
        ]
        records = [
            '{"time":"2023-01-01T12:34:56.123456","log":"1"}',
            '{"time":"2023-01-01T12:34:57.123456","log":"2"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await write_lines_to_s3(pod_keys[1], records[1])
        await s3_log_service.compact_one(pod_name)

        metadata = await s3_logs_metadata_service.get_metadata(pod_name)
        assert metadata.last_merged_key == pod_keys[1]
        assert metadata.last_compaction_time
        assert metadata.last_compaction_time >= now
        await assert_records_written(metadata.get_log_keys(), records)

        # merged keys should not be deleted after merge
        raw_keys = await s3_logs_metadata_service.get_raw_log_keys(pod_name)
        assert raw_keys == pod_keys

        # pod should be added to clean up queue every time logs are merged
        cleanup_queue = await s3_logs_metadata_service.get_pods_cleanup_queue(
            cleanup_interval=0
        )
        assert pod_name in cleanup_queue

    async def test_compact_one__resume_write_to_last_file(
        self,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
        assert_records_written: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
        ]
        records = [
            '{"time":"2023-01-01T12:34:56.123456","log":"1"}',
            '{"time":"2023-01-01T12:34:57.123456","log":"2"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await s3_log_service.compact_one(pod_name)

        metadata = await s3_logs_metadata_service.get_metadata(pod_name)
        last_merged_key = metadata.get_log_keys()[0]
        assert len(metadata.get_log_keys()) == 1
        await assert_records_written(metadata.get_log_keys(), [records[0]])

        await write_lines_to_s3(pod_keys[1], records[1])
        await s3_log_service.compact_one(pod_name)

        metadata = await s3_logs_metadata_service.get_metadata(pod_name)
        assert len(metadata.get_log_keys()) == 1
        assert last_merged_key != metadata.get_log_keys()[0]
        await assert_records_written(metadata.get_log_keys(), records)

    async def test_compact_one__merged_keys_deleted(
        self,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
        ]
        records = [
            '{"time": "2023-01-01T12:34:56.123456", "log": "1"}',
            '{"time": "2023-01-01T12:34:57.123456", "log": "2"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await s3_log_service.compact_one(pod_name)

        await write_lines_to_s3(pod_keys[1], records[1])
        await s3_log_service.compact_one(pod_name)

        raw_keys = await s3_logs_metadata_service.get_raw_log_keys(pod_name)
        assert raw_keys == pod_keys[1:]

        await s3_log_service.compact_one(pod_name)

        raw_keys = await s3_logs_metadata_service.get_raw_log_keys(pod_name)
        assert raw_keys == []

    async def test_compact_one__orphaned_keys_deleted(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_2.gz",
        ]
        records = [
            '{"time": "2023-01-01T12:34:56.123456", "log": "1"}',
            '{"time": "2023-01-01T12:34:57.123456", "log": "2"}',
            '{"time": "2023-01-01T12:34:58.123456", "log": "3"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await s3_log_service.compact_one(pod_name)

        metadata = await s3_logs_metadata_service.get_metadata(pod_name)
        orphaned_key = metadata.get_log_keys()[0]

        # The current log file has space for more log records.
        # After merge, its content will be copied to a new log file
        # and it will become orphaned.
        await write_lines_to_s3(pod_keys[1], records[1])
        await s3_log_service.compact_one(pod_name)

        await write_lines_to_s3(pod_keys[2], records[2])
        await s3_log_service.compact_one(pod_name)

        with pytest.raises(botocore.exceptions.ClientError):
            await s3_client.get_object(Bucket=s3_logs_bucket, Key=orphaned_key)

    async def test_cleanup_one__merged_keys_deleted(
        self,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
        ]
        records = [
            '{"time": "2023-01-01T12:34:56.123456", "log": "1"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await s3_log_service.compact_one(pod_name)

        raw_keys = await s3_logs_metadata_service.get_raw_log_keys(pod_name)
        assert raw_keys

        await s3_log_service.cleanup_one(pod_name)

        raw_keys = await s3_logs_metadata_service.get_raw_log_keys(pod_name)
        assert raw_keys == []

        queue = await s3_logs_metadata_service.get_pods_cleanup_queue(
            cleanup_interval=0
        )
        assert pod_name not in queue

    async def test_cleanup_one__orphaned_keys_deleted(
        self,
        s3_client: AioBaseClient,
        s3_logs_bucket: str,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
            f"{raw_log_key_prefix}-c1.log/202301011234_1.gz",
        ]
        records = [
            '{"time": "2023-01-01T12:34:56.123456", "log": "1"}',
            '{"time": "2023-01-01T12:34:57.123456", "log": "2"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await s3_log_service.compact_one(pod_name)

        metadata = await s3_logs_metadata_service.get_metadata(pod_name)
        orphaned_key = metadata.get_log_keys()[0]

        # Create an orphaned file
        await write_lines_to_s3(pod_keys[1], records[1])
        await s3_log_service.compact_one(pod_name)

        # Orphaned should still exist
        await s3_client.get_object(Bucket=s3_logs_bucket, Key=orphaned_key)

        await s3_log_service.cleanup_one(pod_name)

        with pytest.raises(botocore.exceptions.ClientError):
            await s3_client.get_object(Bucket=s3_logs_bucket, Key=orphaned_key)

    async def test_compact_all(
        self,
        s3_log_service: S3LogsService,
        s3_logs_metadata_service: S3LogsMetadataService,
        write_lines_to_s3: Callable[..., Awaitable[None]],
    ) -> None:
        pod_name = str(uuid.uuid4())
        raw_log_key_prefix = _create_raw_log_key_prefix(pod_name)
        pod_keys = [
            f"{raw_log_key_prefix}-c1.log/202301011234_0.gz",
        ]
        records = [
            '{"time": "2023-01-01T12:34:56.123456", "log": "1"}',
        ]

        await write_lines_to_s3(pod_keys[0], records[0])
        await s3_log_service.compact_all(pod_names=[pod_name])

        queue = await s3_logs_metadata_service.get_pods_cleanup_queue(
            cleanup_interval=0
        )
        assert pod_name in queue

        await s3_log_service.compact_all(cleanup_interval=0, pod_names=[pod_name])

        queue = await s3_logs_metadata_service.get_pods_cleanup_queue(
            cleanup_interval=0
        )
        assert pod_name not in queue
