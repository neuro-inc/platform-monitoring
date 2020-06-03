import asyncio
import json
import logging
import shlex
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from pathlib import Path
from tempfile import mktemp
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List

import aiohttp
import aiohttp.web
import aiohttp_cors
from aiodocker.stream import Stream
from aioelasticsearch import Elasticsearch
from aiohttp.web import (
    HTTPBadRequest,
    HTTPInternalServerError,
    Request,
    Response,
    StreamResponse,
    WebSocketResponse,
    json_response,
    middleware,
)
from aiohttp_security import check_authorized
from aiohttp_security.api import AUTZ_KEY
from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.security import AuthScheme, setup_security
from neuromation.api import (
    Client as PlatformApiClient,
    Factory as PlatformClientFactory,
    JobDescription as Job,
)
from platform_logging import init_logging
from platform_monitoring.user import untrusted_user
from yarl import URL

from .base import JobStats, Telemetry
from .config import (
    Config,
    CORSConfig,
    ElasticsearchConfig,
    KubeConfig,
    PlatformApiConfig,
    PlatformAuthConfig,
)
from .config_factory import EnvironConfigFactory
from .jobs_service import Container, ExecCreate, JobException, JobsService
from .kube_client import JobError, KubeClient, KubeTelemetry
from .utils import JobsHelper, KubeHelper, LogReaderFactory
from .validators import (
    create_exec_create_request_payload_validator,
    create_save_request_payload_validator,
)


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
                aiohttp.web.get("/{job_id}/log", self.stream_log),
                aiohttp.web.get("/{job_id}/top", self.stream_top),
                aiohttp.web.post("/{job_id}/save", self.stream_save),
                aiohttp.web.post("/{job_id}/attach", self.ws_attach),
                aiohttp.web.get("/{job_id}/attach", self.ws_attach),
                aiohttp.web.post("/{job_id}/resize", self.resize),
                aiohttp.web.post("/{job_id}/kill", self.kill),
                aiohttp.web.post("/{job_id}/exec_create", self.exec_create),
                aiohttp.web.post("/{job_id}/port_forward/{port}", self.port_forward),
                aiohttp.web.get("/{job_id}/port_forward/{port}", self.port_forward),
                aiohttp.web.post("/{job_id}/{exec_id}/exec_resize", self.exec_resize),
                aiohttp.web.get("/{job_id}/{exec_id}/exec_inspect", self.exec_inspect),
                aiohttp.web.post("/{job_id}/{exec_id}/exec_start", self.exec_start),
                aiohttp.web.get("/{job_id}/{exec_id}/exec_start", self.exec_start),
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

    async def stream_log(self, request: Request) -> StreamResponse:
        job = await self._resolve_job(request, "read")

        pod_name = self._kube_helper.get_job_pod_name(job)
        log_reader = await self._log_reader_factory.get_pod_log_reader(pod_name)

        # TODO (A Danshyn, 05.07.2018): expose. make configurable
        chunk_size = 1024
        response = StreamResponse(status=200)
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

    async def resize(self, request: Request) -> Response:
        w = int(request.query.get("w", "80"))
        h = int(request.query.get("h", "25"))

        job = await self._resolve_job(request, "write")

        await self._jobs_service.resize(job, w=w, h=h)
        return json_response(None)

    async def kill(self, request: Request) -> Response:
        signal = request.query.get("signal", "SIGKILL")

        job = await self._resolve_job(request, "write")

        await self._jobs_service.kill(job, signal)
        return json_response(None, status=204)

    async def ws_attach(self, request: Request) -> StreamResponse:
        stdin = _parse_bool(request.query.get("stdin", "0"))
        stdout = _parse_bool(request.query.get("stdout", "1"))
        stderr = _parse_bool(request.query.get("stderr", "1"))
        logs = _parse_bool(request.query.get("logs", "1"))

        if not (stdin or stdout or stderr):
            raise ValueError("Required at least one of stdin, stdout or stderr")

        job = await self._resolve_job(request, "write")

        response = WebSocketResponse()
        await response.prepare(request)

        async with self._jobs_service.attach(
            job, stdin=stdin, stdout=stdout, stderr=stderr, logs=logs
        ) as stream:
            transfer = Transfer(response, stream, stdin, stdout or stderr)
            await transfer.transfer()

        return response

    async def exec_create(self, request: Request) -> StreamResponse:
        exe = await self._parse_exec_create(request)

        job = await self._resolve_job(request, "write")

        exec_id = await self._jobs_service.exec_create(
            job,
            cmd=exe.cmd,
            stdin=exe.stdin,
            stdout=exe.stdout,
            stderr=exe.stderr,
            tty=exe.tty,
        )
        return json_response({"job_id": job.id, "exec_id": exec_id})

    async def _parse_exec_create(self, request: Request) -> ExecCreate:
        payload = await request.json()
        payload = self._exec_create_request_payload_validator.check(payload)

        return ExecCreate(
            cmd=payload["command"],
            stdin=payload["stdin"],
            stdout=payload["stdout"],
            stderr=payload["stderr"],
            tty=payload["tty"],
        )

    async def exec_resize(self, request: Request) -> StreamResponse:
        exec_id = request.match_info["exec_id"]

        job = await self._resolve_job(request, "write")

        w = int(request.query["w"])
        h = int(request.query["h"])
        await self._jobs_service.exec_resize(job, exec_id, w=w, h=h)

        return json_response(None)

    async def exec_inspect(self, request: Request) -> StreamResponse:
        exec_id = request.match_info["exec_id"]

        job = await self._resolve_job(request, "write")

        ret = await self._jobs_service.exec_inspect(job, exec_id)
        cmd = " ".join(shlex.quote(arg) for arg in ret["ProcessConfig"]["arguments"])
        return json_response(
            {
                "id": ret["ID"],
                "running": ret["Running"],
                "exit_code": ret["ExitCode"],
                "job_id": job.id,
                "tty": ret["ProcessConfig"]["tty"],
                "entrypoint": ret["ProcessConfig"]["entrypoint"],
                "command": cmd,
            }
        )

    async def exec_start(self, request: Request) -> StreamResponse:
        exec_id = request.match_info["exec_id"]

        job = await self._resolve_job(request, "write")

        data = await self._jobs_service.exec_inspect(job, exec_id)

        response = WebSocketResponse()
        await response.prepare(request)

        async with self._jobs_service.exec_start(job, exec_id) as stream:
            transfer = Transfer(
                response,
                stream,
                data["OpenStdin"],
                data["OpenStdout"] or data["OpenStderr"],
            )
            await transfer.transfer()

        return response

    async def port_forward(self, request: Request) -> StreamResponse:
        sport = request.match_info["port"]
        try:
            port = int(sport)
        except (TypeError, ValueError):
            payload = {"msg": f"Invalid port number {sport!r}"}
            raise aiohttp.web.HTTPBadRequest(
                text=json.dumps(payload), content_type="application/json"
            )

        job = await self._resolve_job(request, "write")

        # Connect before web socket handshake,
        # it allows returning 400 Bad Request
        try:
            reader, writer = await self._jobs_service.port_forward(job, port)
        except OSError as exc:
            payload = {"msg": f"Cannot connect to port {port}", "error": repr(exc)}
            raise aiohttp.web.HTTPBadRequest(
                text=json.dumps(payload), content_type="application/json"
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
        data = await reader.read()
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
        ws: WebSocketResponse,
        stream: Stream,
        handle_input: bool,
        handle_output: bool,
    ) -> None:
        self._ws = ws
        self._stream = stream
        self._handle_input = handle_input
        self._handle_output = handle_output
        self._closing = False

    async def transfer(self) -> None:
        tasks = []
        if self._handle_input:
            tasks.append(asyncio.create_task(self._do_input()))
        if self._handle_output:
            tasks.append(asyncio.create_task(self._do_output()))

        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except:  # noqa: E722
            await self._ws.close()
            await self._stream.close()
            for task in tasks:
                if not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            raise

    async def _do_input(self) -> None:
        async for msg in self._ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                await self._stream.write_in(msg.data)
            elif msg.type in (
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSING,
                aiohttp.WSMsgType.CLOSED,
            ):
                self._closing = True
                self._stream.close()
            elif msg.type == aiohttp.WSMsgType.ERROR:
                exc = self._ws.exception()
                logger.error(
                    "WS connection closed with exception %s", exc, exc_info=exc
                )
            else:
                raise ValueError(f"Unsupported WS message type {msg.type}")

    async def _do_output(self) -> None:
        while True:
            data = await self._stream.read_out()
            if data is None:
                self._closing = True
                await self._ws.close()
                return
            if not self._closing:
                await self._ws.send_bytes(bytes([data.stream]) + data.data)


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
        await client.ping()
        yield client


def _setup_cors(app: aiohttp.web.Application, config: CORSConfig) -> None:
    if not config.allowed_origins:
        return

    logger.info(f"Setting up CORS with allowed origins: {config.allowed_origins}")
    default_options = aiohttp_cors.ResourceOptions(
        allow_credentials=True, expose_headers="*", allow_headers="*",
    )
    cors = aiohttp_cors.setup(
        app, defaults={origin: default_options for origin in config.allowed_origins}
    )
    for route in app.router.routes():
        logger.debug(f"Setting up CORS for {route}")
        cors.add(route)


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

    _setup_cors(app, config.cors)
    return app


def _parse_bool(value: str) -> bool:
    if value == "0":
        return False
    elif value == "1":
        return True
    else:
        raise ValueError('Required "0" or "1"')


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
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
