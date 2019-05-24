import logging
from typing import Awaitable, Callable

import aiohttp
from aiohttp.abc import StreamResponse
from aiohttp.web import Request, Response
from aiohttp.web_middlewares import middleware
from aiohttp.web_response import json_response
from platform_monitoring.config import Config
from platform_monitoring.config_factory import EnvironConfigFactory


logger = logging.getLogger(__name__)


def init_logging() -> None:
    logging.basicConfig(
        # TODO (A Yushkovskiy after A Danshyn 06/01/18): expose in the Config
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


class ApiHandler:
    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes([aiohttp.web.get("/ping", self.handle_ping)])

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


async def create_api_v1_app() -> aiohttp.web.Application:
    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)
    return api_v1_app


async def create_app(config: Config) -> aiohttp.web.Application:  # pragma: no coverage
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

    api_v1_app = await create_api_v1_app()
    app["api_v1_app"] = api_v1_app

    app.add_subapp("/api/v1", api_v1_app)

    return app


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
    aiohttp.web.run_app(
        create_app(config), host=config.server.host, port=config.server.port
    )
