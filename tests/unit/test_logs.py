from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Dict, Sequence
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
    ) -> Callable[[Sequence[Dict[str, Any]]], None]:
        def _setup(pages: Sequence[Dict[str, Any]]) -> None:
            async def paginate(
                *args: Any, **kwargs: Any
            ) -> AsyncIterator[Dict[str, Any]]:
                for page in pages:
                    yield page

            paginator = mock.MagicMock()
            paginator.paginate.side_effect = paginate
            s3_client.get_paginator.return_value = paginator

        return _setup

    @pytest.fixture
    def log_reader(self, s3_client: mock.Mock) -> S3LogReader:
        return S3LogReader(s3_client, "", "", "", "", "", "")

    @pytest.mark.asyncio
    async def test_keys_sorted_by_time(
        self,
        log_reader: S3LogReader,
        setup_s3_pages: Callable[[Sequence[Dict[str, Any]]], None],
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
