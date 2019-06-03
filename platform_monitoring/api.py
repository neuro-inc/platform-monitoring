import logging
from pathlib import Path
from tempfile import mktemp
from typing import AsyncIterator, Awaitable, Callable

import aiohttp
import aiohttp.web
from aiohttp.abc import StreamResponse
from aiohttp.web import Request, Response
from aiohttp.web_middlewares import middleware
from aiohttp.web_response import json_response
from async_exit_stack import AsyncExitStack
from async_generator import asynccontextmanager
from neuromation.api import (
    Client as PlatformApiClient,
    Factory as PlatformClientFactory,
)
from platform_monitoring.config import Config, PlatformApiConfig
from platform_monitoring.config_factory import EnvironConfigFactory
from platform_monitoring.monitoring_service import MonitoringService


logger = logging.getLogger(__name__)


def init_logging() -> None:
    logging.basicConfig(
        # TODO (A Yushkovskiy after A Danshyn 06/01/18): expose in the Config
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


class MonitoringApiHandler:
    def __init__(self, app: aiohttp.web.Application) -> None:
        self._app = app

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes([aiohttp.web.get("/ping", self.handle_ping)])

    @property
    def monitoring_service(self) -> MonitoringService:
        return self._app["monitoring_service"]

    async def handle_ping(self, request: Request) -> Response:
        return Response(text="Pong")


@middleware
async def handle_exceptions(
    request: Request, handler: Callable[[Request], Awaitable[StreamResponse]]
) -> StreamResponse:
    try:
        return await handler(request)
    except ValueError as e:
        payload = {"error": str(e)}
        return json_response(payload, status=aiohttp.web.HTTPBadRequest.status_code)
    except aiohttp.web.HTTPException:
        raise
    except Exception as e:
        msg_str = (
            f"Unexpected exception: {str(e)}. " f"Path with query: {request.path_qs}."
        )
        logging.exception(msg_str)
        payload = {"error": msg_str}
        return json_response(
            payload, status=aiohttp.web.HTTPInternalServerError.status_code
        )


async def create_api_v1_app() -> aiohttp.web.Application:  # pragma: no coverage
    api_v1_app = aiohttp.web.Application()
    return api_v1_app


async def create_monitoring_app() -> aiohttp.web.Application:  # pragma: no coverage
    monitoring_app = aiohttp.web.Application()
    notifications_handler = MonitoringApiHandler(monitoring_app)
    notifications_handler.register(monitoring_app)
    return monitoring_app


@asynccontextmanager
async def create_platform_api_client(config: PlatformApiConfig) -> PlatformApiClient:
    tmp_config = Path(mktemp())
    platform_api_factory = PlatformClientFactory(tmp_config)
    await platform_api_factory.login_with_token(url=config.url, token=config.token)
    client = None
    try:
        client = await platform_api_factory.get()
        yield client
    finally:
        if client:
            await client.close()


async def create_app(config: Config) -> aiohttp.web.Application:  # pragma: no coverage
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config
    async with AsyncExitStack() as exit_stack:

        async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
            logger.info("Initializing Platform API client")
            platform_client = await exit_stack.enter_async_context(
                create_platform_api_client(config.platform_api)
            )

            logger.info("Initializing JobsService")
            monitoring_service = MonitoringService(platform_client=platform_client)
            app["monitoring_app"]["monitoring_service"] = monitoring_service

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = await create_api_v1_app()
    app["api_v1_app"] = api_v1_app

    monitoring_app = await create_monitoring_app()
    app["monitoring_app"] = monitoring_app
    api_v1_app.add_subapp("/jobs", monitoring_app)

    app.add_subapp("/api/v1", api_v1_app)
    return app


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
    aiohttp.web.run_app(
        create_app(config), host=config.server.host, port=config.server.port
    )
