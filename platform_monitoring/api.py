import asyncio
import json
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from tempfile import mktemp
from typing import Any, AsyncIterator, Awaitable, Callable, Dict

import aiohttp
import aiohttp.web
from aioelasticsearch import Elasticsearch
from aiohttp.web import (
    HTTPBadRequest,
    HTTPInternalServerError,
    Request,
    Response,
    StreamResponse,
)
from aiohttp.web_middlewares import middleware
from aiohttp.web_response import json_response
from aiohttp_security import check_authorized
from neuro_auth_client import AuthClient, Permission, check_permissions
from neuro_auth_client.security import AuthScheme, setup_security
from neuromation.api import (
    Client as PlatformApiClient,
    Factory as PlatformClientFactory,
    JobDescription as Job,
)
from platform_logging import init_logging
from platform_monitoring.user import untrusted_user

from .base import JobStats, Telemetry
from .config import (
    Config,
    ElasticsearchConfig,
    KubeConfig,
    PlatformApiConfig,
    PlatformAuthConfig,
)
from .config_factory import EnvironConfigFactory
from .jobs_service import Container, JobException, JobsService
from .kube_client import JobError, KubeClient, KubeTelemetry
from .utils import JobsHelper, KubeHelper, LogReaderFactory
from .validators import create_save_request_payload_validator


logger = logging.getLogger(__name__)


class ApiHandler:
    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            [
                aiohttp.web.get("/ping", self.handle_ping),
                aiohttp.web.get("/secured-ping", self.handle_secured_ping),
            ]
        )

    async def handle_ping(self, request: Request) -> Response:
        return Response(text="Pong")

    async def handle_secured_ping(self, request: Request) -> Response:
        await check_authorized(request)
        return aiohttp.web.Response(text=f"Secured Pong")


class MonitoringApiHandler:
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config
        self._jobs_helper = JobsHelper(cluster_name=config.cluster_name)
        self._kube_helper = KubeHelper()

        self._save_request_payload_validator = create_save_request_payload_validator(
            config.registry.host
        )

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            [
                aiohttp.web.get("/{job_id}/log", self.stream_log),
                aiohttp.web.get("/{job_id}/top", self.stream_top),
                aiohttp.web.post("/{job_id}/save", self.stream_save),
            ]
        )

    @property
    def _jobs_service(self) -> JobsService:
        return self._app["jobs_service"]

    @property
    def _kube_client(self) -> KubeClient:
        return self._app["kube_client"]

    @property
    def _log_reader_factory(self) -> LogReaderFactory:
        return self._app["log_reader_factory"]

    async def stream_log(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        user = await untrusted_user(request)
        job_id = request.match_info["job_id"]
        job = await self._get_job(job_id)

        permission = Permission(uri=self._jobs_helper.job_to_uri(job), action="read")
        logger.info("Checking whether %r has %r", user, permission)
        await check_permissions(request, [permission])

        pod_name = self._kube_helper.get_job_pod_name(job)
        log_reader = await self._log_reader_factory.get_pod_log_reader(pod_name)

        # TODO (A Danshyn, 05.07.2018): expose. make configurable
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
        user = await untrusted_user(request)
        job_id = request.match_info["job_id"]
        job = await self._get_job(job_id)

        permission = Permission(uri=self._jobs_helper.job_to_uri(job), action="read")
        logger.info("Checking whether %r has %r", user, permission)
        await check_permissions(request, [permission])

        logger.info("Websocket connection starting")
        ws = aiohttp.web.WebSocketResponse()
        await ws.prepare(request)
        logger.info("Websocket connection ready")

        # TODO (truskovskiyk 09/12/18) remove CancelledError
        # https://github.com/aio-libs/aiohttp/issues/3443

        # TODO expose configuration
        sleep_timeout = 1

        telemetry = await self._get_job_telemetry(job)

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

                    if self._jobs_helper.is_job_running(job):
                        job_stats = await telemetry.get_latest_stats()
                        if job_stats:
                            message = self._convert_job_stats_to_ws_message(job_stats)
                            await ws.send_json(message)

                    if self._jobs_helper.is_job_finished(job):
                        break

                    await asyncio.sleep(sleep_timeout)

            except JobError as e:
                raise JobError(f"Failed to get telemetry for job {job.id}: {e}") from e

            except asyncio.CancelledError as ex:
                logger.info(f"got cancelled error {ex}")

            finally:
                if not ws.closed:
                    await ws.close()

        return ws

    async def _get_job(self, job_id: str) -> Job:
        return await self._jobs_service.get(job_id)

    async def _get_job_telemetry(self, job: Job) -> Telemetry:
        pod_name = self._kube_helper.get_job_pod_name(job)
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

    async def stream_save(self, request: Request) -> StreamResponse:
        user = await untrusted_user(request)
        job_id = request.match_info["job_id"]
        job = await self._get_job(job_id)

        permission = Permission(uri=self._jobs_helper.job_to_uri(job), action="write")
        logger.info("Checking whether %r has %r", user, permission)
        await check_permissions(request, [permission])

        container = await self._parse_save_container(request)

        # Following docker engine API, the response should conform ndjson
        # see https://github.com/ndjson/ndjson-spec
        encoding = "utf-8"
        response = StreamResponse(status=200)
        response.enable_compression(aiohttp.web.ContentCoding.identity)
        response.content_type = "application/x-ndjson"
        response.charset = encoding
        await response.prepare(request)

        try:
            async for chunk in self._jobs_service.save(job, user, container):
                await response.write(self._serialize_chunk(chunk, encoding))
        except JobException as e:
            # Serialize an exception in a similar way as docker does:
            chunk = {"error": str(e), "errorDetail": {"message": str(e)}}
            await response.write(self._serialize_chunk(chunk, encoding))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # middleware don't work for prepared StreamResponse, so we need to
            # catch a general exception and send it as a chunk
            msg_str = f"Unexpected error: {e}"
            logging.exception(msg_str)
            chunk = {"error": msg_str}
            await response.write(self._serialize_chunk(chunk, encoding))
        finally:
            return response

    def _serialize_chunk(self, chunk: Dict[str, Any], encoding: str = "utf-8") -> bytes:
        chunk_str = json.dumps(chunk) + "\r\n"
        return chunk_str.encode(encoding)

    async def _parse_save_container(self, request: Request) -> Container:
        payload = await request.json()
        payload = self._save_request_payload_validator.check(payload)

        image = payload["container"]["image"]
        if image.domain != self._config.registry.host:
            raise ValueError("Unknown registry host")

        return Container(image=image)


@middleware
async def handle_exceptions(
    request: Request, handler: Callable[[Request], Awaitable[StreamResponse]]
) -> StreamResponse:
    try:
        return await handler(request)
    except ValueError as e:
        payload = {"error": str(e)}
        return json_response(payload, status=HTTPBadRequest.status_code)
    except aiohttp.web.HTTPException:
        raise
    except Exception as e:
        msg_str = (
            f"Unexpected exception: {str(e)}. " f"Path with query: {request.path_qs}."
        )
        logging.exception(msg_str)
        payload = {"error": msg_str}
        return json_response(payload, status=HTTPInternalServerError.status_code)


async def create_api_v1_app() -> aiohttp.web.Application:
    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)
    return api_v1_app


async def create_monitoring_app(config: Config) -> aiohttp.web.Application:
    monitoring_app = aiohttp.web.Application()
    notifications_handler = MonitoringApiHandler(monitoring_app, config)
    notifications_handler.register(monitoring_app)
    return monitoring_app


@asynccontextmanager
async def create_platform_api_client(
    config: PlatformApiConfig,
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
async def create_auth_client(config: PlatformAuthConfig) -> AsyncIterator[AuthClient]:
    async with AuthClient(config.url, config.token) as client:
        yield client


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
        kubelet_node_port=config.kubelet_node_port,
    )
    try:
        await client.init()
        yield client
    finally:
        await client.close()


@asynccontextmanager
async def create_elasticsearch_client(
    config: ElasticsearchConfig,
) -> AsyncIterator[Elasticsearch]:
    async with Elasticsearch(hosts=config.hosts) as client:
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

            logger.info("Initializing Auth client")
            auth_client = await exit_stack.enter_async_context(
                create_auth_client(config.platform_auth)
            )

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )

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

            app["monitoring_app"]["jobs_service"] = JobsService(
                jobs_client=platform_client.jobs,
                kube_client=kube_client,
                docker_config=config.docker,
            )

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = await create_api_v1_app()
    app["api_v1_app"] = api_v1_app

    monitoring_app = await create_monitoring_app(config)
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
