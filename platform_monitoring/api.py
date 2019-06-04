import asyncio
import logging
from pathlib import Path
from tempfile import mktemp
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Optional

import aiohttp
import aiohttp.web
from aiohttp import BasicAuth
from aiohttp.abc import StreamResponse
from aiohttp.web import Request, Response
from aiohttp.web_middlewares import middleware
from aiohttp.web_response import json_response
from async_exit_stack import AsyncExitStack
from async_generator import asynccontextmanager
from neuromation.api import (
    Client as PlatformApiClient,
    Factory as PlatformClientFactory,
    IllegalArgumentError,
    JobDescription as Job,
    JobStatus,
)
from platform_monitoring.config import (
    Config,
    ElasticsearchConfig,
    KubeConfig,
    PlatformApiConfig,
)
from platform_monitoring.config_factory import EnvironConfigFactory
from platform_monitoring.kube_base import JobStats
from platform_monitoring.logs import LogReaderFactory

from .base import JobStats, Telemetry
from .config import Config, KubeConfig, PlatformApiConfig
from .config_factory import EnvironConfigFactory
from .kube_client import KubeClient, KubeTelemetry


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


class MonitoringApiHandler:
    def __init__(self, app: aiohttp.web.Application) -> None:
        self._app = app

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            [
                aiohttp.web.get("/{job_id}/log", self.stream_log),
                aiohttp.web.get("/{job_id}/top", self.stream_top),
            ]
        )

    @property
    def _platform_client(self) -> PlatformApiClient:
        return self._app["platform_client"]

    @property
    def _kube_client(self) -> KubeClient:
        return self._app["kube_client"]

    @property
    def log_reader_factory(self) -> LogReaderFactory:
        return self._app["log_reader_factory"]

    async def stream_log(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        job_id = request.match_info["job_id"]
        job = await self._get_job(job_id)
        await self._check_job_read_permissions(job_id, job.owner)

        log_reader = await self.log_reader_factory.get_job_log_reader(job_id)
        # TODO: expose. make configurable
        chunk_size = 1024

        response = aiohttp.web.StreamResponse(status=200)
        response.enable_chunked_encoding()
        response.enable_compression(aiohttp.web.ContentCoding.identity)
        response.content_type = "text/plain"
        response.charset = "utf-8"
        await response.prepare(request)

        async with log_reader:
            while True:
                chunk = await log_reader.read(size=chunk_size)
                if not chunk:
                    break
                await response.write(chunk)

        await response.write_eof()
        return response

    async def stream_top(self, request: Request) -> aiohttp.web.WebSocketResponse:
        job_id = request.match_info["job_id"]

        # TODO (A Yushkovskiy, 06-Jun-2019) check READ permissions on the job

        logger.info("Websocket connection starting")
        ws = aiohttp.web.WebSocketResponse()
        await ws.prepare(request)
        logger.info("Websocket connection ready")

        try:
            job = await self._get_job(job_id)
        except IllegalArgumentError as e:
            ws.set_status(aiohttp.web.HTTPBadRequest.status_code, reason=str(e))
            await ws.close()
            return ws

        # TODO expose configuration
        sleep_timeout = 1

        telemetry = await self._get_job_telemetry(job)

        # TODO (truskovskiyk 09/12/18) remove CancelledError
        # https://github.com/aio-libs/aiohttp/issues/3443

        async with telemetry:

            try:
                while True:
                    # client closed connection
                    assert request.transport is not None
                    if request.transport.is_closing():
                        break

                    # TODO (A Yushkovskiy 06-Jun-2019) don't make slow HTTP requests to
                    #  platform-api to check job's status every iteration: we better
                    #  retrieve this information directly form kubernetes
                    job = await self._get_job(job_id)

                    if self._is_job_running(job):
                        job_stats = await telemetry.get_latest_stats()
                        if job_stats:
                            message = self._convert_job_stats_to_ws_message(job_stats)
                            await ws.send_json(message)

                    if self._is_job_finished(job):
                        await ws.close()
                        break

                    await asyncio.sleep(sleep_timeout)

            except asyncio.CancelledError as ex:
                logger.info(f"got cancelled error {ex}")

        return ws

    async def _get_job(self, job_id: str) -> Job:
        return await self._platform_client.jobs.status(job_id)

    def _is_job_running(self, job: Job) -> bool:
        return job.status == JobStatus.RUNNING

    def _is_job_finished(self, job: Job) -> bool:
        return job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED)

    def _get_job_pod_name(self, job: Job) -> str:
        # TODO (A Danshyn 11/15/18): we will need to start storing jobs'
        # kube pod names explicitly at some point
        return job.id

    async def _get_job_telemetry(self, job: Job) -> Telemetry:
        pod_name = self._get_job_pod_name(job)
        return KubeTelemetry(
            self._kube_client,
            namespace_name=self._kube_client.namespace,
            pod_name=pod_name,
            container_name=pod_name,
        )

    def _convert_job_stats_to_ws_message(self, job_stats: JobStats) -> Dict[str, Any]:
        message = {
            "cpu": job_stats.cpu,
            "memory": job_stats.memory,
            "timestamp": job_stats.timestamp,
        }
        if job_stats.gpu_duty_cycle is not None:
            message["gpu_duty_cycle"] = job_stats.gpu_duty_cycle
        if job_stats.gpu_memory is not None:
            message["gpu_memory"] = job_stats.gpu_memory
        return message


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
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)
    return api_v1_app


async def create_monitoring_app() -> aiohttp.web.Application:  # pragma: no coverage
    monitoring_app = aiohttp.web.Application()
    notifications_handler = MonitoringApiHandler(monitoring_app)
    notifications_handler.register(monitoring_app)
    return monitoring_app


@asynccontextmanager
async def create_platform_api_client(
    config: PlatformApiConfig
) -> AsyncIterator[PlatformApiClient]:
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


@asynccontextmanager
async def create_kube_client(config: KubeConfig) -> AsyncIterator[KubeClient]:
    client = KubeClient(
        base_url=config.endpoint_url,
        namespace=config.namespace,
        cert_authority_path=config.cert_authority_path,
        cert_authority_data_pem=config.cert_authority_data_pem,
        auth_type=config.auth_type,
        auth_cert_path=config.auth_cert_path,
        auth_cert_key_path=config.auth_cert_key_path,
        token=config.token,
        token_path=None,  # TODO (A Yushkovskiy) add support for token_path or drop
        conn_timeout_s=config.client_conn_timeout_s,
        read_timeout_s=config.client_read_timeout_s,
        conn_pool_size=config.client_conn_pool_size,
    )
    try:
        await client.init()
        yield client
    finally:
        await client.close()


@asynccontextmanager
async def create_elasticsearch_client(
    config: ElasticsearchConfig
) -> AsyncIterator[ElasticsearchClient]:
    http_auth: Optional[BasicAuth]
    if config.user:
        http_auth = BasicAuth(config.user, config.password)  # type: ignore  # noqa
    else:
        http_auth = None

    async with ElasticsearchClient(hosts=config.hosts, http_auth=http_auth) as client:
        yield client


async def create_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            logger.info("Initializing Platform API client")
            platform_client = await exit_stack.enter_async_context(
                create_platform_api_client(config.platform_api)
            )
            app["monitoring_app"]["platform_client"] = platform_client

            logger.info("Initializing Elasticsearc client")
            es_client = await exit_stack.enter_async_context(
                create_elasticsearch_client(config.elasticsearch)
            )

            logger.info("Initializing Kubernetes client")
            kube_client = await exit_stack.enter_async_context(
                create_kube_client(config.kube)
            )
            app["monitoring_app"]["kube_client"] = kube_client

            log_reader_factory = LogReaderFactory(kube_client, es_client)
            app["monitoring_app"]["log_reader_factory"] = log_reader_factory

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
