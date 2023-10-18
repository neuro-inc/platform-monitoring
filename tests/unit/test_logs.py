import json
from collections.abc import AsyncIterator, Callable, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest import mock

import pytest

from platform_monitoring.logs import S3LogReader


class TestS3LogReader:
    @pytest.fixture
    def s3_client(self) -> mock.Mock:
        return mock.MagicMock()

    @pytest.fixture
    def setup_s3_pages(
        self, s3_client: mock.Mock
    ) -> Callable[[Sequence[dict[str, Any]]], None]:
        def _setup(pages: Sequence[dict[str, Any]]) -> None:
            async def paginate(
                *args: Any, **kwargs: Any
            ) -> AsyncIterator[dict[str, Any]]:
                for page in pages:
                    yield page

            paginator = mock.MagicMock()
            paginator.paginate.side_effect = paginate
            s3_client.get_paginator.return_value = paginator

        return _setup

    @pytest.fixture
    def setup_s3_key_content(
        self,
        s3_client: mock.Mock,
    ) -> Callable[[dict[str, list[str]]], None]:
        def _setup(content: dict[str, list[str]]) -> None:
            async def get_object(Key: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
                async def _iter() -> AsyncIterator[str]:
                    for line in content[Key]:
                        yield line

                body = mock.AsyncMock()
                body.iter_lines = mock.MagicMock()
                body.iter_lines.return_value = mock.AsyncMock()
                body.iter_lines.return_value.__aiter__.side_effect = _iter
                return {"ContentType": "", "Body": body}

            s3_client.get_object = get_object

        return _setup

    @pytest.fixture
    def log_reader(self, s3_client: mock.Mock) -> S3LogReader:
        return S3LogReader(s3_client, "", "", "", "", "", "")

    async def test_keys_sorted_by_time(
        self,
        log_reader: S3LogReader,
        setup_s3_pages: Callable[[Sequence[dict[str, Any]]], None],
    ) -> None:
        setup_s3_pages(
            [
                {
                    "Contents": [
                        {"Key": "s3-key/202101311202_0.gz"},
                        {"Key": "s3-key/202101311201_1.gz"},
                    ]
                },
                {"Contents": [{"Key": "s3-key/202101311201_0.gz"}]},
            ]
        )

        async with log_reader:
            assert list(await log_reader._load_log_keys(None)) == [
                "s3-key/202101311201_0.gz",
                "s3-key/202101311201_1.gz",
                "s3-key/202101311202_0.gz",
            ]
            dt = datetime(2021, 1, 31, 12, 2, 30, tzinfo=timezone.utc)
            assert list(await log_reader._load_log_keys(dt)) == [
                "s3-key/202101311202_0.gz",
            ]
            dt = datetime(2021, 1, 31, 12, 3, 0, tzinfo=timezone.utc)
            assert list(await log_reader._load_log_keys(dt)) == []

    async def test_iterate_log_chunks(
        self,
        log_reader: S3LogReader,
        setup_s3_pages: Callable[[Sequence[dict[str, Any]]], None],
        setup_s3_key_content: Callable[[dict[str, Any]], None],
    ) -> None:
        now = datetime.now(tz=timezone.utc)
        now_as_key = now.strftime("%Y%m%d%H%M")
        setup_s3_pages([{"Contents": [{"Key": f"s3-key/{now_as_key}_0.gz"}]}])

        log_lines = ["qwe\n", "line2", "line3", "", "\n\n\n", "something here"]

        def later_iso(sec: int = 0) -> str:
            return (now + timedelta(seconds=sec)).isoformat()

        stored_lines = []
        for i, line in enumerate(log_lines):
            stored_line = {"time": later_iso(i)}
            if line:
                stored_line["log"] = line
            stored_lines.append(json.dumps(stored_line))
        setup_s3_key_content({f"s3-key/{now_as_key}_0.gz": stored_lines})

        res = []
        async with log_reader as it:
            async for chunk in it:
                res.append(chunk.decode()[:-1])  # -1 implies removal of \n
        assert log_lines == res
