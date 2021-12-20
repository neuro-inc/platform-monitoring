import asyncio
import json
import logging
import random
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from pathlib import Path
from tempfile import mktemp
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Union

import aiobotocore.session
import aiohttp
import aiohttp.hdrs
import aiohttp.web
import aiohttp_cors
import pkg_resources
from aiobotocore.client import AioBaseClient
from aioelasticsearch import Elasticsearch
from aiohttp.client_ws import ClientWebSocketResponse
from aiohttp.web import (
    HTTPBadRequest,
    HTTPInternalServerError,
    HTTPNotFound,
    Request,
    Response,
    StreamResponse,
    WebSocketResponse,
    json_response,
    middleware,
)
from aiohttp.web_urldispatcher import AbstractRoute
from aiohttp_security import check_authorized
from aiohttp_security.api import AUTZ_KEY
from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.security import AuthScheme, setup_security
from neuro_config_client import ConfigClient
from neuro_logging import (
    init_logging,
    make_sentry_trace_config,
    make_zipkin_trace_config,
    notrace,
    setup_sentry,
    setup_zipkin,
    setup_zipkin_tracer,
)
from neuro_sdk import (
    Client as PlatformApiClient,
    Factory as PlatformClientFactory,
    JobDescription as Job,
)
from yarl import URL

from .base import JobStats, Telemetry
from .config import (
    Config,
    CORSConfig,
    ElasticsearchConfig,
    KubeConfig,
    LogsStorageType,
    S3Config,
)
from .config_factory import EnvironConfigFactory
from .container_runtime_client import (
    ContainerNotFoundError,
    ContainerRuntimeClientError,
    ContainerRuntimeClientRegistry,
)
from .jobs_service import JobException, JobNotRunningException, JobsService
from .kube_client import JobError, KubeClient, KubeTelemetry
from .log_cleanup_poller import LogCleanupPoller
from .logs import (
    DEFAULT_ARCHIVE_DELAY,
    ElasticsearchLogsService,
    LogsService,
    S3LogsService,
)
from .user import untrusted_user
from .utils import JobsHelper, KubeHelper, parse_date
from .validators import (
    create_exec_create_request_payload_validator,
    create_save_request_payload_validator,
)


WS_ATTACH_PROTOCOL = "v2.channels.neu.ro"


logger = logging.getLogger(__name__)


class ApiHandler:
    def register(self, app: aiohttp.web.Application) -> List[AbstractRoute]:
        return app.add_routes(
            [
                aiohttp.web.get("/ping", self.handle_ping),
                aiohttp.web.get("/secured-ping", self.handle_secured_ping),
            ]
        )

    @notrace
    async def handle_ping(self, request: Request) -> Response:
        return Response(text="Pong")

    @notrace
    async def handle_secured_ping(self, request: Request) -> Response:
        await check_authorized(request)
        return Response(text="Secured Pong")


class MonitoringApiHandler:
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config
        self._jobs_helper = JobsHelper()
        self._kube_helper = KubeHelper()

        self._save_request_payload_validator = create_save_request_payload_validator(
            config.registry.host
        )

        self._exec_create_request_payload_validator = (
            create_exec_create_request_payload_validator()
        )

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            [
                aiohttp.web.post("/available", self.get_capacity),  # deprecated
                aiohttp.web.get("/capacity", self.get_capacity),
                aiohttp.web.get("/{job_id}/log", self.stream_log),
                aiohttp.web.delete("/{job_id}/log", self.drop_log),
                aiohttp.web.get("/{job_id}/top", self.stream_top),
                aiohttp.web.post("/{job_id}/save", self.stream_save),
                aiohttp.web.post("/{job_id}/attach", self.ws_attach),
                aiohttp.web.get("/{job_id}/attach", self.ws_attach),
                aiohttp.web.post("/{job_id}/exec", self.ws_exec),
                aiohttp.web.get("/{job_id}/exec", self.ws_exec),
                aiohttp.web.post("/{job_id}/kill", self.kill),
                aiohttp.web.post("/{job_id}/port_forward/{port}", self.port_forward),
                aiohttp.web.get("/{job_id}/port_forward/{port}", self.port_forward),
            ]
        )

    @property
    def _jobs_service(self) -> JobsService:
        return self._app["jobs_service"]

    @property
    def _kube_client(self) -> KubeClient:
        return self._app["kube_client"]

    @property
    def _logs_service(self) -> LogsService:
        return self._app["logs_service"]

    async def get_capacity(self, request: Request) -> Response:
        # Check that user has access to the cluster
        user = await untrusted_user(request)
        await check_any_permissions(
            request,
            [
                Permission(
                    uri=f"job://{self._config.cluster_name}/{user.name}", action="read"
                ),
                Permission(
                    uri=f"cluster://{self._config.cluster_name}/access", action="read"
                ),
            ],
        )
        result = await self._jobs_service.get_available_jobs_counts()
        return json_response(result)

    async def stream_log(self, request: Request) -> StreamResponse:
        timestamps = _get_bool_param(request, "timestamps", False)
        debug = _get_bool_param(request, "debug", False)
        since_str = request.query.get("since")
        since = parse_date(since_str) if since_str else None
        archive_delay_s = float(
            request.query.get("archive_delay", DEFAULT_ARCHIVE_DELAY)
        )
        job = await self._resolve_job(request, "read")

        pod_name = self._kube_helper.get_job_pod_name(job)
        separator = request.query.get("separator")
        if separator is None:
            separator = "=== Live logs ===" + _getrandbytes(30).hex()

        response = StreamResponse(status=200)
        response.enable_chunked_encoding()
        response.enable_compression(aiohttp.web.ContentCoding.identity)
        response.content_type = "text/plain"
        response.charset = "utf-8"
        response.headers["X-Separator"] = separator
        await response.prepare(request)

        async with self._logs_service.get_pod_log_reader(
            pod_name,
            separator=separator.encode(),
            since=since,
            timestamps=timestamps,
            debug=debug,
            archive_delay_s=archive_delay_s,
        ) as it:
            async for chunk in it:
                await response.write(chunk)

        await response.write_eof()
        return response

    async def drop_log(self, request: Request) -> Response:
        job = await self._resolve_job(request, "write")

        pod_name = self._kube_helper.get_job_pod_name(job)
        await self._logs_service.drop_logs(pod_name)
        return Response(status=aiohttp.web.HTTPNoContent.status_code)

    async def stream_top(self, request: Request) -> WebSocketResponse:
        job = await self._resolve_job(request, "read")

        logger.info("Websocket connection starting")
        ws = WebSocketResponse()
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
                    job = await self._get_job(job.id)

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

    async def _resolve_job(self, request: Request, action: str) -> Job:
        user = await untrusted_user(request)
        job_id = request.match_info["job_id"]
        job = await self._get_job(job_id)

        # XXX (serhiy 23-May-2020) Maybe check permissions on the
        # platform-api side?
        permissions = [Permission(uri=str(job.uri), action=action)]
        if job.name:
            permissions.append(
                Permission(
                    uri=str(_job_uri_with_name(job.uri, job.name)), action=action
                )
            )
        logger.info("Checking whether %r has %r", user, permissions)
        await check_any_permissions(request, permissions)
        return job

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
        job = await self._resolve_job(request, "write")

        payload = await request.json()
        payload = self._save_request_payload_validator.check(payload)

        image = payload["container"]["image"]

        # Following docker engine API, the response should conform ndjson
        # see https://github.com/ndjson/ndjson-spec
        encoding = "utf-8"
        response = StreamResponse(status=200)
        response.enable_compression(aiohttp.web.ContentCoding.identity)
        response.content_type = "application/x-ndjson"
        response.charset = encoding
        await response.prepare(request)

        try:
            async with self._jobs_service.save(job, user, image) as it:
                async for chunk in it:
                    await response.write(chunk)
        except JobException as e:
            # Serialize an exception in a similar way as docker does:
            error = {"error": str(e), "errorDetail": {"message": str(e)}}
            await response.write(self._serialize_chunk(error, encoding))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # middleware don't work for prepared StreamResponse, so we need to
            # catch a general exception and send it as a chunk
            msg_str = f"Unexpected error: {e}"
            logging.exception(msg_str)
            error = {"error": msg_str}
            await response.write(self._serialize_chunk(error, encoding))
        finally:
            return response

    def _serialize_chunk(self, chunk: Dict[str, Any], encoding: str = "utf-8") -> bytes:
        chunk_str = json.dumps(chunk) + "\r\n"
        return chunk_str.encode(encoding)

    async def kill(self, request: Request) -> Response:
        job = await self._resolve_job(request, "write")
        await self._jobs_service.kill(job)
        return json_response(None, status=204)

    async def ws_attach(self, request: Request) -> StreamResponse:
        tty = _get_bool_param(request, "tty", False)
        stdin = _get_bool_param(request, "stdin", False)
        stdout = _get_bool_param(request, "stdout", True)
        stderr = _get_bool_param(request, "stderr", True)

        if not (stdin or stdout or stderr):
            raise ValueError("Required at least one of stdin, stdout or stderr")

        if tty and stdout and stderr:
            raise ValueError("Stdin and stdout cannot be multiplexed in tty mode")

        job = await self._resolve_job(request, "write")

        response = WebSocketResponse(protocols=[WS_ATTACH_PROTOCOL])

        async with self._jobs_service.attach(
            job, tty=tty, stdin=stdin, stdout=stdout, stderr=stderr
        ) as ws:
            await response.prepare(request)
            transfer = Transfer(response, ws, stdin, stdout or stderr)
            await transfer.transfer()

        return response

    async def ws_exec(self, request: Request) -> StreamResponse:
        cmd = request.query.get("cmd")
        tty = _get_bool_param(request, "tty", False)
        stdin = _get_bool_param(request, "stdin", False)
        stdout = _get_bool_param(request, "stdout", True)
        stderr = _get_bool_param(request, "stderr", True)

        if not cmd:
            raise ValueError("Command is required")

        if not (stdin or stdout or stderr):
            raise ValueError("Required at least one of stdin, stdout or stderr")

        if tty and stdout and stderr:
            raise ValueError("Stdin and stdout cannot be multiplexed in tty mode")

        job = await self._resolve_job(request, "write")

        response = WebSocketResponse(protocols=[WS_ATTACH_PROTOCOL])

        async with self._jobs_service.exec(
            job, cmd=cmd, tty=tty, stdin=stdin, stdout=stdout, stderr=stderr
        ) as ws:
            await response.prepare(request)
            transfer = Transfer(response, ws, stdin, stdout or stderr)
            await transfer.transfer()

        return response

    async def port_forward(self, request: Request) -> StreamResponse:
        sport = request.match_info["port"]
        try:
            port = int(sport)
        except (TypeError, ValueError):
            payload = json.dumps({"msg": f"Invalid port number {sport!r}"})
            raise aiohttp.web.HTTPBadRequest(
                text=payload,
                content_type="application/json",
                headers={"X-Error": payload},
            )

        job = await self._resolve_job(request, "write")

        # Connect before web socket handshake,
        # it allows returning 400 Bad Request
        try:
            reader, writer = await self._jobs_service.port_forward(job, port)
        except OSError as exc:
            payload = json.dumps(
                {"msg": f"Cannot connect to port {port}", "error": repr(exc)}
            )
            raise aiohttp.web.HTTPBadRequest(
                text=payload,
                content_type="application/json",
                headers={"X-Error": payload},
            )

        try:
            response = WebSocketResponse()
            await response.prepare(request)

            tasks = []
            try:
                tasks.append(asyncio.create_task(_forward_reading(response, reader)))
                tasks.append(asyncio.create_task(_forward_writing(response, writer)))

                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

                return response
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                        with suppress(asyncio.CancelledError):
                            await task

        finally:
            writer.close()
            await writer.wait_closed()


async def _forward_reading(ws: WebSocketResponse, reader: asyncio.StreamReader) -> None:
    while True:
        # 4-6 MB is the typical default socket receive buffer size of Lunix
        data = await reader.read(4 * 1024 * 1024)
        if not data:
            break
        await ws.send_bytes(data)


async def _forward_writing(ws: WebSocketResponse, writer: asyncio.StreamWriter) -> None:
    async for msg in ws:
        assert msg.type == aiohttp.WSMsgType.BINARY
        writer.write(msg.data)
        await writer.drain()


class Transfer:
    def __init__(
        self,
        resp: WebSocketResponse,
        client_resp: ClientWebSocketResponse,
        handle_input: bool,
        handle_output: bool,
    ) -> None:
        self._resp = resp
        self._client_resp = client_resp
        self._handle_input = handle_input
        self._handle_output = handle_output
        self._closing = False

    async def transfer(self) -> None:
        tasks = [
            asyncio.create_task(self._proxy(self._resp, self._client_resp)),
            asyncio.create_task(self._proxy(self._client_resp, self._resp)),
        ]

        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            await self._resp.close()
            await self._client_resp.close()

            for task in tasks:
                if not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task

    async def _proxy(
        self,
        src: Union[WebSocketResponse, ClientWebSocketResponse],
        dst: Union[WebSocketResponse, ClientWebSocketResponse],
    ) -> None:
        try:
            async for msg in src:
                if self._closing:
                    break

                if msg.type == aiohttp.WSMsgType.BINARY:
                    await dst.send_bytes(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    exc = src.exception()
                    logger.error(
                        "WS connection closed with exception %s", exc, exc_info=exc
                    )
                else:
                    raise ValueError(f"Unsupported WS message type {msg.type}")
        except StopAsyncIteration:
            pass
        finally:
            self._closing = True


@middleware
async def handle_exceptions(
    request: Request, handler: Callable[[Request], Awaitable[StreamResponse]]
) -> StreamResponse:
    try:
        return await handler(request)
    except ValueError as e:
        payload = {"error": str(e)}
        return json_response(payload, status=HTTPBadRequest.status_code)
    except ContainerNotFoundError as e:
        payload = {"error": str(e)}
        return json_response(payload, status=HTTPNotFound.status_code)
    except JobNotRunningException as e:
        payload = {"error": str(e)}
        return json_response(payload, status=HTTPNotFound.status_code)
    except JobException as e:
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


async def create_monitoring_app(config: Config) -> aiohttp.web.Application:
    monitoring_app = aiohttp.web.Application()
    monitoring_handler = MonitoringApiHandler(monitoring_app, config)
    monitoring_handler.register(monitoring_app)
    return monitoring_app


@asynccontextmanager
async def create_platform_api_client(
    url: URL, token: str, trace_configs: Optional[List[aiohttp.TraceConfig]] = None
) -> AsyncIterator[PlatformApiClient]:
    tmp_config = Path(mktemp())
    platform_api_factory = PlatformClientFactory(
        tmp_config, trace_configs=trace_configs
    )
    await platform_api_factory.login_with_token(url=url, token=token)
    client = None
    try:
        client = await platform_api_factory.get()
        yield client
    finally:
        if client:
            await client.close()


@asynccontextmanager
async def create_kube_client(
    config: KubeConfig, trace_configs: Optional[List[aiohttp.TraceConfig]] = None
) -> AsyncIterator[KubeClient]:
    client = KubeClient(
        base_url=config.endpoint_url,
        namespace=config.namespace,
        cert_authority_path=config.cert_authority_path,
        cert_authority_data_pem=config.cert_authority_data_pem,
        auth_type=config.auth_type,
        auth_cert_path=config.auth_cert_path,
        auth_cert_key_path=config.auth_cert_key_path,
        token=config.token,
        token_path=config.token_path,
        conn_timeout_s=config.client_conn_timeout_s,
        read_timeout_s=config.client_read_timeout_s,
        conn_pool_size=config.client_conn_pool_size,
        kubelet_node_port=config.kubelet_node_port,
        trace_configs=trace_configs,
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
        await client.ping()
        yield client


def create_s3_client(config: S3Config) -> AioBaseClient:
    kwargs: Dict[str, str] = {}
    if config.access_key_id:
        kwargs["aws_access_key_id"] = config.access_key_id
    if config.secret_access_key:
        kwargs["aws_secret_access_key"] = config.secret_access_key
    if config.endpoint_url:
        kwargs["endpoint_url"] = str(config.endpoint_url)
    if config.region:
        kwargs["region_name"] = config.region
    session = aiobotocore.session.get_session()
    return session.create_client("s3", **kwargs)


def create_logs_service(
    config: Config,
    kube_client: KubeClient,
    es_client: Optional[Elasticsearch] = None,
    s3_client: Optional[AioBaseClient] = None,
) -> LogsService:
    if config.logs.storage_type == LogsStorageType.ELASTICSEARCH:
        assert es_client
        return ElasticsearchLogsService(
            kube_client,
            es_client,
            container_runtime=config.container_runtime.name,
        )

    if config.logs.storage_type == LogsStorageType.S3:
        assert config.s3
        assert s3_client
        return S3LogsService(
            kube_client,
            s3_client,
            container_runtime=config.container_runtime.name,
            bucket_name=config.s3.job_logs_bucket_name,
            key_prefix_format=config.s3.job_logs_key_prefix_format,
        )

    raise ValueError(
        f"{config.logs.storage_type} storage is not supported"
    )  # pragma: nocover


def _setup_cors(app: aiohttp.web.Application, config: CORSConfig) -> None:
    if not config.allowed_origins:
        return

    logger.info(f"Setting up CORS with allowed origins: {config.allowed_origins}")
    default_options = aiohttp_cors.ResourceOptions(
        allow_credentials=True,
        expose_headers="*",
        allow_headers="*",
    )
    cors = aiohttp_cors.setup(
        app, defaults={origin: default_options for origin in config.allowed_origins}
    )
    for route in app.router.routes():
        logger.debug(f"Setting up CORS for {route}")
        cors.add(route)


package_version = pkg_resources.get_distribution("platform-monitoring").version


async def add_version_to_header(request: Request, response: StreamResponse) -> None:
    response.headers["X-Service-Version"] = f"platform-monitoring/{package_version}"


def make_tracing_trace_configs(config: Config) -> List[aiohttp.TraceConfig]:
    trace_configs = []

    if config.zipkin:
        trace_configs.append(make_zipkin_trace_config())

    if config.sentry:
        trace_configs.append(make_sentry_trace_config())

    return trace_configs


async def create_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            logger.info("Initializing Platform API client")
            platform_client = await exit_stack.enter_async_context(
                create_platform_api_client(
                    config.platform_api.url,
                    config.platform_api.token,
                    make_tracing_trace_configs(config),
                )
            )

            logger.info("Initializing Auth client")
            auth_client = await exit_stack.enter_async_context(
                AuthClient(
                    config.platform_auth.url or None,
                    config.platform_auth.token,
                    make_tracing_trace_configs(config),
                )
            )

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )

            es_client: Optional[Elasticsearch] = None
            if config.elasticsearch:
                logger.info("Initializing Elasticsearch client")
                es_client = await exit_stack.enter_async_context(
                    create_elasticsearch_client(config.elasticsearch)
                )

            s3_client: Optional[AioBaseClient] = None
            if config.s3:
                logger.info("Initializing S3 client")
                s3_client = await exit_stack.enter_async_context(
                    create_s3_client(config.s3)
                )

            logger.info("Initializing Kubernetes client")
            kube_client = await exit_stack.enter_async_context(
                create_kube_client(config.kube)
            )
            app["monitoring_app"]["kube_client"] = kube_client

            logger.info("Initializing Platform Config client")
            config_client = await exit_stack.enter_async_context(
                ConfigClient(
                    config.platform_config.url,
                    config.platform_config.token,
                    trace_configs=make_tracing_trace_configs(config),
                )
            )
            app["monitoring_app"]["config_client"] = config_client

            logs_service = create_logs_service(
                config, kube_client, es_client, s3_client
            )
            app["monitoring_app"]["logs_service"] = logs_service

            container_runtime_client_registry = await exit_stack.enter_async_context(
                ContainerRuntimeClientRegistry(
                    container_runtime_port=config.container_runtime.port,
                    trace_configs=make_tracing_trace_configs(config),
                )
            )
            jobs_service = JobsService(
                config_client=config_client,
                jobs_client=platform_client.jobs,
                kube_client=kube_client,
                container_runtime_client_registry=container_runtime_client_registry,
                cluster_name=config.cluster_name,
                kube_job_label=config.kube.job_label,
                kube_node_pool_label=config.kube.node_pool_label,
            )
            app["monitoring_app"]["jobs_service"] = jobs_service

            await exit_stack.enter_async_context(
                LogCleanupPoller(
                    jobs_service=jobs_service,
                    logs_service=logs_service,
                    interval_sec=config.logs.cleanup_interval_sec,
                )
            )

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    probes_routes = api_v1_handler.register(api_v1_app)
    app["api_v1_app"] = api_v1_app

    monitoring_app = await create_monitoring_app(config)
    app["monitoring_app"] = monitoring_app
    api_v1_app.add_subapp("/jobs", monitoring_app)

    app.add_subapp("/api/v1", api_v1_app)

    _setup_cors(app, config.cors)

    app.on_response_prepare.append(add_version_to_header)

    if config.zipkin:
        setup_zipkin(app, skip_routes=probes_routes)

    return app


def setup_tracing(config: Config) -> None:
    if config.zipkin:
        setup_zipkin_tracer(
            config.zipkin.app_name,
            config.server.host,
            config.server.port,
            config.zipkin.url,
            config.zipkin.sample_rate,
            ignored_exceptions=[ContainerRuntimeClientError],
        )

    if config.sentry:
        setup_sentry(
            config.sentry.dsn,
            app_name=config.sentry.app_name,
            cluster_name=config.sentry.cluster_name,
            sample_rate=config.sentry.sample_rate,
            exclude=[ContainerRuntimeClientError],
        )


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
    setup_tracing(config)
    aiohttp.web.run_app(
        create_app(config), host=config.server.host, port=config.server.port
    )


async def check_any_permissions(
    request: aiohttp.web.Request, permissions: List[Permission]
) -> None:
    user_name = await check_authorized(request)
    auth_policy = request.config_dict.get(AUTZ_KEY)
    if not auth_policy:
        raise RuntimeError("Auth policy not configured")

    try:
        missing = await auth_policy.get_missing_permissions(user_name, permissions)
    except aiohttp.ClientError as e:
        # re-wrap in order not to expose the client
        raise RuntimeError(e) from e

    if len(missing) >= len(permissions):
        payload = {"missing": [_permission_to_primitive(p) for p in missing]}
        raise aiohttp.web.HTTPForbidden(
            text=json.dumps(payload), content_type="application/json"
        )


def _permission_to_primitive(perm: Permission) -> Dict[str, str]:
    return {"uri": perm.uri, "action": perm.action}


def _job_uri_with_name(uri: URL, name: str) -> URL:
    assert name
    assert uri.host
    assert uri.name
    return uri.with_name(name)


def _get_bool_param(request: Request, name: str, default: bool = False) -> bool:
    param = request.query.get(name)
    if param is None:
        return default
    param = param.lower()
    if param in ("1", "true"):
        return True
    if param in ("0", "false"):
        return False
    raise ValueError(f'"{name}" request parameter can be "true"/"1" or "false"/"0"')


def _getrandbytes(size: int) -> bytes:
    return random.getrandbits(size * 8).to_bytes(size, "big")
