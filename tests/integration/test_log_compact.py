from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import replace

import pytest

from platform_monitoring.config import Config, LogsCompactConfig
from platform_monitoring.log_compact import App, run_task
from platform_monitoring.logs import S3LogsService


class MyS3LogService(S3LogsService):
    def __init__(self) -> None:
        super().__init__(None, None, None, None)  # type: ignore

        self.call_count = 0
        self.raise_error = False

    async def compact_all(
        self,
        compact_interval: float = 3600,
        cleanup_interval: float = 3600,
        pod_names: Sequence[str] | None = None,
    ) -> None:
        self.call_count += 1
        if self.raise_error:
            msg = "error"
            raise Exception(msg)


@pytest.fixture
def config(config: Config) -> Config:
    return replace(
        config,
        logs=replace(config.logs, compact=LogsCompactConfig(run_interval=0.1)),
    )


async def test_run(config: Config) -> None:
    async with App().init(config) as app:
        app.service = MyS3LogService()

        async with run_task(app.run()):
            await asyncio.sleep(1)

        assert app.service.call_count >= 10


async def test_run__errors_ignored(config: Config) -> None:
    async with App().init(config) as app:
        app.service = MyS3LogService()
        app.service.raise_error = True

        async with run_task(app.run()):
            await asyncio.sleep(1)

        assert app.service.call_count >= 10
