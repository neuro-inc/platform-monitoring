from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from typing import NoReturn

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


@asynccontextmanager
async def run_app(config: Config) -> AsyncIterator[None]:
    async with AsyncExitStack() as exit_stack:
        logger.info("Initializing Kube client")
        kube_client = await exit_stack.enter_async_context(
            create_kube_client(config.kube)
        )

        logger.info("Initializing S3 client")
        assert config.s3
        s3_client = await exit_stack.enter_async_context(create_s3_client(config.s3))
        await create_s3_logs_bucket(s3_client, config.s3)

        logger.info("Initializing S3 logs service")
        service = create_s3_logs_service(config, kube_client, s3_client)

        task = asyncio.create_task(
            run_compact(
                service,
                run_interval=config.logs_compact.run_interval,
                compact_interval=config.logs_compact.compact_interval,
                cleanup_interval=config.logs_compact.cleanup_interval,
            )
        )

        yield

        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def run_compact(
    service: S3LogsService,
    *,
    run_interval: float,
    compact_interval: float,
    cleanup_interval: float,
) -> None:
    while True:
        try:
            await service.compact_all(
                compact_interval=compact_interval, cleanup_interval=cleanup_interval
            )
        except Exception:
            logger.exception("Unhandled error")
        await asyncio.sleep(run_interval)


class GracefulExitError(SystemExit):
    code = 1


def _raise_graceful_exit() -> NoReturn:
    raise GracefulExitError()


def setup() -> None:
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, _raise_graceful_exit)
    loop.add_signal_handler(signal.SIGTERM, _raise_graceful_exit)


def cleanup() -> None:
    loop = asyncio.get_event_loop()
    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)


async def run(config: Config) -> None:
    setup()

    try:
        async with run_app(config):
            while True:
                await asyncio.sleep(3600)
    finally:
        cleanup()


def main() -> None:
    init_logging()
    config = EnvironConfigFactory().create()
    logger.info("Loaded config: %r", config)
    asyncio.run(run(config))
