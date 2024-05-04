from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import AsyncIterator, Coroutine
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from typing import Any, NoReturn

from neuro_logging import init_logging

from .api import (
    create_kube_client,
    create_s3_client,
    create_s3_logs_bucket,
    create_s3_logs_service,
)
from .config import Config
from .config_factory import EnvironConfigFactory
from .logs import S3LogsService


logger = logging.getLogger(__name__)


class App:
    config: Config
    service: S3LogsService

    @asynccontextmanager
    async def init(self, config: Config) -> AsyncIterator[App]:
        self.config = config

        async with AsyncExitStack() as exit_stack:
            logger.info("Initializing Kube client")
            kube_client = await exit_stack.enter_async_context(
                create_kube_client(config.kube)
            )

            logger.info("Initializing S3 client")
            assert config.s3
            s3_client = await exit_stack.enter_async_context(
                create_s3_client(config.s3)
            )
            await create_s3_logs_bucket(s3_client, config.s3)

            logger.info("Initializing S3 logs service")
            # NOTE: it is safe to cache log metadata by compactor because there's only
            # one instance of it running and only compactor can update
            # metadata in s3.
            self.service = create_s3_logs_service(
                config, kube_client, s3_client, cache_log_metadata=True
            )

            yield self

    async def run(self) -> None:
        while True:
            try:
                logger.info("Starting compact iteration")
                await self.service.compact_all(
                    compact_interval=self.config.logs.compact.compact_interval,
                    cleanup_interval=self.config.logs.compact.cleanup_interval,
                )
            except Exception:
                logger.exception("Unhandled error")
            logger.info(
                "Finished compact iteration, sleeping for %.1f seconds",
                self.config.logs.compact.run_interval,
            )
            await asyncio.sleep(self.config.logs.compact.run_interval)


@asynccontextmanager
async def run_task(coro: Coroutine[Any, Any, None]) -> AsyncIterator[None]:
    task = asyncio.create_task(coro)
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def _raise_graceful_exit() -> NoReturn:  # pragma: no cover
    err = SystemExit()
    err.code = 1
    raise err


def _setup() -> None:  # pragma: no cover
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, _raise_graceful_exit)
    loop.add_signal_handler(signal.SIGTERM, _raise_graceful_exit)


def _cleanup() -> None:  # pragma: no cover
    loop = asyncio.get_event_loop()
    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)


async def run_app(config: Config) -> None:  # pragma: no cover
    _setup()

    try:
        async with App().init(config) as app:
            async with run_task(app.run()):
                while True:
                    await asyncio.sleep(3600)
    finally:
        _cleanup()


def main() -> None:  # pragma: no cover
    init_logging()
    config = EnvironConfigFactory().create()
    logger.info("Loaded config: %r", config)
    asyncio.run(run_app(config))
