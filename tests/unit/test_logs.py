from datetime import datetime

from iso8601 import UTC

from platform_monitoring.logs import S3LogFile, S3LogRecord, S3LogsMetadata


class TestS3LogRecord:
    def test_parse(self) -> None:
        record = S3LogRecord.parse(
            b'{"time": "2023-10-01T12:34:56.123456789", '
            b'"log": "hello", "stream": "stderr"}',
            datetime.now(),
            container_id="container",
        )

        assert record == S3LogRecord(
            time=datetime(2023, 10, 1, 12, 34, 56, 123456, UTC),
            time_str="2023-10-01T12:34:56.123456789",
            message="hello",
            container_id="container",
            stream="stderr",
        )

    def test_parse__no_log(self) -> None:
        record = S3LogRecord.parse(
            b'{"time": "2023-10-01T12:34:56.123456789"}',
            datetime.now(),
            container_id="container",
        )

        assert record == S3LogRecord(
            time=datetime(2023, 10, 1, 12, 34, 56, 123456, UTC),
            time_str="2023-10-01T12:34:56.123456789",
            message="",
            container_id="container",
        )

    def test_parse__invalid_unicode(self) -> None:
        record = S3LogRecord.parse(
            b'{"time": "2023-10-01T12:34:56.123456789", "log": "hello\xff"}',
            datetime.now(),
            container_id="container",
        )

        assert record == S3LogRecord(
            time=datetime(2023, 10, 1, 12, 34, 56, 123456, UTC),
            time_str="2023-10-01T12:34:56.123456789",
            message="hello�",
            container_id="container",
        )

    def test_parse__no_time_key(self) -> None:
        record = S3LogRecord.parse(
            b'{"log": "2023-10-01T12:34:56.123456789 stdout P hello"}',
            datetime.now(),
            container_id="container",
        )

        assert record == S3LogRecord(
            time=datetime(2023, 10, 1, 12, 34, 56, 123456, UTC),
            time_str="2023-10-01T12:34:56.123456789",
            message="hello",
            container_id="container",
        )

    def test_parse__no_time_key__invalid_time(self) -> None:
        fallback_time = datetime(2023, 10, 1, 12, 34, 56, 123456, UTC)
        record = S3LogRecord.parse(
            b'{"log": "invalid stdout P hello"}',
            fallback_time,
            container_id="container",
        )

        assert record == S3LogRecord(
            time=fallback_time,
            time_str="2023-10-01T12:34:56.123456",
            message="hello",
            container_id="container",
        )

    def test_parse__no_time_key__no_message(self) -> None:
        record = S3LogRecord.parse(
            b'{"log": "2023-10-01T12:34:56.123456789 stdout P "}',
            datetime.now(),
            container_id="container",
        )

        assert record == S3LogRecord(
            time=datetime(2023, 10, 1, 12, 34, 56, 123456, UTC),
            time_str="2023-10-01T12:34:56.123456789",
            message="",
            container_id="container",
        )

    def test_parse__no_time_key__no_time_in_message(self) -> None:
        fallback_time = datetime(2023, 10, 1, 12, 34, 56, 123456, UTC)
        record = S3LogRecord.parse(
            b'{"log": "hello"}',
            fallback_time,
            container_id="container",
        )

        assert record == S3LogRecord(
            time=fallback_time,
            time_str="2023-10-01T12:34:56.123456",
            message="hello",
            container_id="container",
        )


class TestS3LogFile:
    def test_to_primitive(self) -> None:
        file = S3LogFile(
            key="s3_key",
            records_count=123,
            size=456,
            first_record_time=datetime(2023, 1, 2, 3, 4, 5),
            last_record_time=datetime(2023, 6, 7, 8, 9, 10),
        )

        assert file.to_primitive() == {
            "key": "s3_key",
            "records_count": 123,
            "size": 456,
            "first_record_time": "2023-01-02T03:04:05",
            "last_record_time": "2023-06-07T08:09:10",
        }

    def test_from_primitive(self) -> None:
        file = S3LogFile.from_primitive(
            {
                "key": "s3_key",
                "records_count": 123,
                "size": 456,
                "first_record_time": "2023-01-02T03:04:05",
                "last_record_time": "2023-06-07T08:09:10",
            }
        )

        assert file == S3LogFile(
            key="s3_key",
            records_count=123,
            size=456,
            first_record_time=datetime(2023, 1, 2, 3, 4, 5),
            last_record_time=datetime(2023, 6, 7, 8, 9, 10),
        )


class TestS3LogsMetadata:
    def test_to_primitive_defaults(self) -> None:
        metadata = S3LogsMetadata()

        assert metadata.to_primitive() == {
            "log_files": [],
            "last_compaction_time": None,
            "last_merged_key": None,
        }

    def test_to_primitive_custom(self) -> None:
        metadata = S3LogsMetadata(
            log_files=[
                S3LogFile(
                    key="s3_key",
                    records_count=123,
                    size=456,
                    first_record_time=datetime(2023, 1, 2, 3, 4, 5),
                    last_record_time=datetime(2023, 6, 7, 8, 9, 10),
                )
            ],
            last_compaction_time=datetime(2023, 1, 2, 3, 4, 5),
            last_merged_key="last-s3-key",
        )

        assert metadata.to_primitive() == {
            "log_files": [
                {
                    "first_record_time": "2023-01-02T03:04:05",
                    "key": "s3_key",
                    "last_record_time": "2023-06-07T08:09:10",
                    "records_count": 123,
                    "size": 456,
                }
            ],
            "last_compaction_time": "2023-01-02T03:04:05",
            "last_merged_key": "last-s3-key",
        }

    def test_get_log_keys(self) -> None:
        file1 = S3LogFile(
            key="s3_key1",
            records_count=123,
            size=456,
            first_record_time=datetime(2023, 1, 1, 0, 1),
            last_record_time=datetime(2023, 1, 1, 0, 2),
        )
        file2 = S3LogFile(
            key="s3_key2",
            records_count=123,
            size=456,
            first_record_time=datetime(2023, 1, 1, 0, 3),
            last_record_time=datetime(2023, 1, 1, 0, 4),
        )
        metadata = S3LogsMetadata(log_files=[file1, file2])

        keys = metadata.get_log_keys()

        assert keys == ["s3_key1", "s3_key2"]

    def test_get_log_keys_since(self) -> None:
        file1 = S3LogFile(
            key="s3_key1",
            records_count=123,
            size=456,
            first_record_time=datetime(2023, 1, 1, 0, 1),
            last_record_time=datetime(2023, 1, 1, 0, 2),
        )
        file2 = S3LogFile(
            key="s3_key2",
            records_count=123,
            size=456,
            first_record_time=datetime(2023, 1, 1, 0, 3),
            last_record_time=datetime(2023, 1, 1, 0, 5),
        )
        metadata = S3LogsMetadata(log_files=[file1, file2])

        keys = metadata.get_log_keys(since=datetime(2023, 1, 1, 0, 0))
        assert keys == ["s3_key1", "s3_key2"]

        keys = metadata.get_log_keys(since=datetime(2023, 1, 1, 0, 4))
        assert keys == ["s3_key2"]

        keys = metadata.get_log_keys(since=datetime(2023, 1, 1, 0, 5))
        assert keys == ["s3_key2"]

        keys = metadata.get_log_keys(since=datetime(2023, 1, 1, 0, 6))
        assert keys == []

    def test_add_log_files(self) -> None:
        file = S3LogFile(
            key="s3_key",
            records_count=123,
            size=456,
            first_record_time=datetime(2023, 1, 2, 3, 4, 5),
            last_record_time=datetime(2023, 6, 7, 8, 9, 10),
        )
        metadata = S3LogsMetadata().add_log_files(
            [file],
            last_merged_key="last-s3-key",
            compaction_time=datetime(2023, 1, 2, 3, 4, 5),
        )

        assert metadata == S3LogsMetadata(
            log_files=[file],
            last_compaction_time=datetime(2023, 1, 2, 3, 4, 5),
            last_merged_key="last-s3-key",
        )

    def test_delete_last_log_file(self) -> None:
        metadata = S3LogsMetadata(
            log_files=[
                S3LogFile(
                    key="s3_key",
                    records_count=123,
                    size=456,
                    first_record_time=datetime(2023, 1, 2, 3, 4, 5),
                    last_record_time=datetime(2023, 6, 7, 8, 9, 10),
                )
            ],
        )

        metadata = metadata.delete_last_log_file()

        assert metadata == S3LogsMetadata(log_files=[])
