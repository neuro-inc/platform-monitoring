import asyncio
import json
import logging
import random
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from typing import Any

import aiobotocore.session
import aiohttp
import aiohttp.hdrs
import aiohttp.web
from aiobotocore.client import AioBaseClient
from aiobotocore.config import AioConfig
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
from elasticsearch import AsyncElasticsearch
from neuro_auth_client import AuthClient, Permission, check_permissions
from neuro_auth_client.security import AuthScheme, setup_security
from neuro_config_client import ConfigClient
from neuro_logging import init_logging, setup_sentry
from yarl import URL

from .base import JobStats, Telemetry
from .config import (
    Config,
    ElasticsearchConfig,
    KubeConfig,
    LogsStorageType,
    LokiConfig,
    S3Config,
)
from .config_factory import EnvironConfigFactory
from .container_runtime_client import (
    ContainerNotFoundError,
    ContainerRuntimeClientError,
    ContainerRuntimeClientRegistry,
)
from .jobs_service import JobException, JobNotRunningException, JobsService
from .kube_client import KubeClient, KubeTelemetry
from .log_cleanup_poller import LogCleanupPoller
from .logs import (
    DEFAULT_ARCHIVE_DELAY,
    ElasticsearchLogsService,
    LogsService,
    LokiLogsService,
    S3LogsMetadataService,
    S3LogsMetadataStorage,
    S3LogsService,
    s3_client_error,
)
from .loki_client import LokiClient
from .platform_api_client import ApiClient, Job
from .platform_apps_client import AppInstance, AppsApiClient, AppsApiException
from .user import untrusted_user
from .utils import JobsHelper, KubeHelper, parse_date
from .validators import (
    create_exec_create_request_payload_validator,
    create_save_request_payload_validator,
)


WS_ATTACH_PROTOCOL = "v2.channels.neu.ro"
WS_ATTACH_PROTOCOL2 = "attach.apolo.us"
WS_LOGS_PROTOCOL = "logs.apolo.us"
WS_TOP_PROTOCOL = "top.apolo.us"
HEARTBEAT = 30


# k8s labels
K8S_LABEL_APOLO_ORG = "platform.apolo.us/org"
K8S_LABEL_APOLO_PROJECT = "platform.apolo.us/project"
K8S_LABEL_APOLO_APP_ID = "platform.apolo.us/app"


CONFIG_KEY = aiohttp.web.AppKey("config", Config)
KUBE_CLIENT_KEY = aiohttp.web.AppKey("kube_client", KubeClient)
LOKI_CLIENT_KEY = aiohttp.web.AppKey("loki_client", LokiClient)
JOBS_SERVICE_KEY = aiohttp.web.AppKey("jobs_service", JobsService)
LOGS_SERVICE_KEY = aiohttp.web.AppKey("logs_service", LogsService)
LOGS_STORAGE_TYPE_KEY = aiohttp.web.AppKey("logs_storage_type", LogsStorageType)
CONFIG_CLIENT_KEY = aiohttp.web.AppKey("config_client", ConfigClient)
MONITORING_APP_KEY = aiohttp.web.AppKey("monitoring_app", aiohttp.web.Application)
APPS_MONITORING_APP_KEY = aiohttp.web.AppKey(
    "apps_monitoring_app", aiohttp.web.Application
)
APPS_API_CLIENT_KEY = aiohttp.web.AppKey("apps_api_client", AppsApiClient)

logger = logging.getLogger(__name__)


class ApiHandler:
    def register(self, app: aiohttp.web.Application) -> list[AbstractRoute]:
        return app.add_routes(
            [
                aiohttp.web.get("/ping", self.handle_ping),
            ]
        )

    async def handle_ping(self, request: Request) -> Response:
        return Response(text="Pong")


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
                aiohttp.web.get("/{job_id}/log_ws", self.ws_log),
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
        return self._app[JOBS_SERVICE_KEY]

    @property
    def _kube_client(self) -> KubeClient:
        return self._app[KUBE_CLIENT_KEY]

    @property
    def _loki_client(self) -> LokiClient:
        return self._app[LOKI_CLIENT_KEY]

    @property
    def _logs_service(self) -> LogsService:
        return self._app[LOGS_SERVICE_KEY]

    async def get_capacity(self, request: Request) -> Response:
        # Check that user has access to the cluster
        user = await untrusted_user(request)
        permissions = [
            Permission(
                uri=f"job://{self._config.cluster_name}/{user.name}", action="read"
            ),
            Permission(
                uri=f"cluster://{self._config.cluster_name}/access", action="read"
            ),
        ]
        await check_permissions(request, [permissions])
        result = await self._jobs_service.get_available_jobs_counts()
        return json_response(result)

    def get_archive_delay(self) -> float:
        if self._config.logs.storage_type == LogsStorageType.LOKI:
            assert self._config.loki
            return self._config.loki.archive_delay_s
        return DEFAULT_ARCHIVE_DELAY

    async def stream_log(self, request: Request) -> StreamResponse:
        timestamps = _get_bool_param(request, "timestamps", default=False)
        debug = _get_bool_param(request, "debug", default=False)
        since_str = request.query.get("since")
        archive_delay_s = float(
            request.query.get("archive_delay", self.get_archive_delay())
        )
        job = await self._resolve_job(request, "read")
        since = parse_date(since_str) if since_str else parse_date(job.created_at)

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

        async def stop_func() -> bool:
            return self._jobs_helper.is_job_finished(await self._get_job(job.id))

        async with self._logs_service.get_pod_log_reader(
            pod_name,
            job.namespace or self._config.kube.namespace,
            separator=separator.encode(),
            since=since,
            timestamps=timestamps,
            debug=debug,
            archive_delay_s=archive_delay_s,
            stop_func=stop_func,
        ) as it:
            async for chunk in it:
                await response.write(chunk)

        await response.write_eof()
        return response

    async def ws_log(self, request: Request) -> StreamResponse:
        timestamps = _get_bool_param(request, "timestamps", default=False)
        debug = _get_bool_param(request, "debug", default=False)
        since_str = request.query.get("since")
        archive_delay_s = float(
            request.query.get("archive_delay", self.get_archive_delay())
        )
        job = await self._resolve_job(request, "read")
        since = parse_date(since_str) if since_str else parse_date(job.created_at)

        pod_name = self._kube_helper.get_job_pod_name(job)
        separator = request.query.get("separator")
        if separator is None:
            separator = "=== Live logs ===" + _getrandbytes(30).hex()

        async def stop_func() -> bool:
            return self._jobs_helper.is_job_finished(await self._get_job(job.id))

        async with self._logs_service.get_pod_log_reader(
            pod_name,
            job.namespace or self._config.kube.namespace,
            separator=separator.encode(),
            since=since,
            timestamps=timestamps,
            debug=debug,
            archive_delay_s=archive_delay_s,
            stop_func=stop_func,
        ) as it:
            response = WebSocketResponse(
                protocols=[WS_LOGS_PROTOCOL],
                heartbeat=HEARTBEAT,
            )
            await response.prepare(request)
            await _run_concurrently(
                _listen(response),
                _forward_bytes_iterating(response, it),
            )
            return response

    async def drop_log(self, request: Request) -> Response:
        job = await self._resolve_job(request, "write")

        pod_name = self._kube_helper.get_job_pod_name(job)
        await self._logs_service.drop_logs(pod_name)
        return Response(status=aiohttp.web.HTTPNoContent.status_code)

    async def stream_top(self, request: Request) -> WebSocketResponse:
        job = await self._resolve_job(request, "read")

        telemetry = await self._get_job_telemetry(job)
        async with telemetry:
            response = WebSocketResponse(protocols=[WS_TOP_PROTOCOL])
            await response.prepare(request)
            await _run_concurrently(
                _listen(response),
                self._send_telemetry(response, job.id, telemetry),
            )
            return response

    async def _send_telemetry(
        self, ws: WebSocketResponse, job_id: str, telemetry: Telemetry
    ) -> None:
        # TODO expose configuration
        sleep_timeout = 1

        while not ws.closed:
            # TODO (A Yushkovskiy 06-Jun-2019) don't make slow HTTP requests to
            #  platform-api to check job's status every iteration: we better
            #  retrieve this information directly from kubernetes
            job = await self._get_job(job_id)
            if ws.closed:
                break

            if self._jobs_helper.is_job_running(job):
                job_stats = await telemetry.get_latest_stats()
                if ws.closed:
                    break
                if job_stats:
                    message = self._convert_job_stats_to_ws_message(job_stats)
                    await ws.send_json(message)
                    if ws.closed:
                        break

            if self._jobs_helper.is_job_finished(job):
                break

            await asyncio.sleep(sleep_timeout)

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
        await check_permissions(request, [permissions])
        return job

    async def _get_job_telemetry(self, job: Job) -> Telemetry:
        pod_name = self._kube_helper.get_job_pod_name(job)
        return KubeTelemetry(
            self._kube_client,
            namespace_name=self._kube_client.namespace,
            pod_name=pod_name,
            container_name=pod_name,
        )

    def _convert_job_stats_to_ws_message(self, job_stats: JobStats) -> dict[str, Any]:
        message = {
            "cpu": job_stats.cpu,
            "memory": job_stats.memory // 2**20,
            "memory_bytes": job_stats.memory,
            "timestamp": job_stats.timestamp,
        }
        if job_stats.gpu_utilization is not None:
            message["gpu_duty_cycle"] = job_stats.gpu_utilization
        if job_stats.gpu_memory_used is not None:
            message["gpu_memory"] = job_stats.gpu_memory_used // 2**20
            message["gpu_memory_bytes"] = job_stats.gpu_memory_used
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
        return response

    def _serialize_chunk(self, chunk: dict[str, Any], encoding: str = "utf-8") -> bytes:
        chunk_str = json.dumps(chunk) + "\r\n"
        return chunk_str.encode(encoding)

    async def kill(self, request: Request) -> Response:
        job = await self._resolve_job(request, "write")
        await self._jobs_service.kill(job)
        return json_response(None, status=204)

    async def ws_attach(self, request: Request) -> StreamResponse:
        tty = _get_bool_param(request, "tty", default=False)
        stdin = _get_bool_param(request, "stdin", default=False)
        stdout = _get_bool_param(request, "stdout", default=True)
        stderr = _get_bool_param(request, "stderr", default=True)

        if not (stdin or stdout or stderr):
            msg = "Required at least one of stdin, stdout or stderr"
            raise ValueError(msg)

        if tty and stdout and stderr:
            msg = "Stdout and stderr cannot be multiplexed in tty mode"
            raise ValueError(msg)

        job = await self._resolve_job(request, "write")

        response = WebSocketResponse(
            protocols=[WS_ATTACH_PROTOCOL, WS_ATTACH_PROTOCOL2], heartbeat=HEARTBEAT
        )

        async with self._jobs_service.attach(
            job, tty=tty, stdin=stdin, stdout=stdout, stderr=stderr
        ) as ws:
            await response.prepare(request)
            transfer = Transfer(
                response, ws, handle_input=stdin, handle_output=stdout or stderr
            )
            await transfer.transfer()

        return response

    async def ws_exec(self, request: Request) -> StreamResponse:
        cmd = request.query.get("cmd")
        tty = _get_bool_param(request, "tty", default=False)
        stdin = _get_bool_param(request, "stdin", default=False)
        stdout = _get_bool_param(request, "stdout", default=True)
        stderr = _get_bool_param(request, "stderr", default=True)

        if not cmd:
            msg = "Command is required"
            raise ValueError(msg)

        if not (stdin or stdout or stderr):
            msg = "Required at least one of stdin, stdout or stderr"
            raise ValueError(msg)

        if tty and stdout and stderr:
            msg = "Stdout and stderr cannot be multiplexed in tty mode"
            raise ValueError(msg)

        job = await self._resolve_job(request, "write")

        response = WebSocketResponse(
            protocols=[WS_ATTACH_PROTOCOL], heartbeat=HEARTBEAT
        )

        async with self._jobs_service.exec(
            job, cmd=cmd, tty=tty, stdin=stdin, stdout=stdout, stderr=stderr
        ) as ws:
            await response.prepare(request)
            transfer = Transfer(
                response, ws, handle_input=stdin, handle_output=stdout or stderr
            )
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
            ) from None

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
            ) from None

        try:
            response = WebSocketResponse(heartbeat=HEARTBEAT)
            await response.prepare(request)
            await _run_concurrently(
                _forward_reading(response, reader),
                _forward_writing(response, writer),
            )
            return response
        finally:
            writer.close()
            await writer.wait_closed()


class AppsMonitoringApiHandler:
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            [
                aiohttp.web.get("/{app_id}/containers", self.containers),
                aiohttp.web.get("/{app_id}/log", self.stream_log),
                aiohttp.web.get("/{app_id}/log_ws", self.ws_log),
            ]
        )

    @property
    def _logs_service(self) -> LogsService:
        return self._app[LOGS_SERVICE_KEY]

    @property
    def _apps_api_client(self) -> AppsApiClient:
        return self._app[APPS_API_CLIENT_KEY]

    @property
    def _kube_client(self) -> KubeClient:
        return self._app[KUBE_CLIENT_KEY]

    @property
    def _logs_storage_type(self) -> LogsStorageType:
        return self._app[LOGS_STORAGE_TYPE_KEY]

    def get_archive_delay(self) -> float:
        if self._config.logs.storage_type == LogsStorageType.LOKI:
            assert self._config.loki
            return self._config.loki.archive_delay_s
        return DEFAULT_ARCHIVE_DELAY

    def _check_logs_backend(self) -> None:
        if self._logs_storage_type != LogsStorageType.LOKI or not isinstance(
            self._logs_service, LokiLogsService
        ):
            exc_txt = "Loki as logs backend required"
            raise Exception(exc_txt)

    async def containers(self, request: Request) -> Response:
        app_instance = await self._resolve_app_instance(request=request)

        k8s_label_selector = {
            K8S_LABEL_APOLO_ORG: app_instance.org_name,
            K8S_LABEL_APOLO_PROJECT: app_instance.project_name,
            K8S_LABEL_APOLO_APP_ID: app_instance.id,
        }

        label_selector = ",".join(
            f"{key}={value}" for key, value in k8s_label_selector.items()
        )

        pods = await self._kube_client.get_pods(
            namespace=app_instance.namespace, label_selector=label_selector
        )

        containers = [
            container.name
            for pod in pods
            if pod.metadata.name
            for container in pod.spec.containers
        ]

        return json_response(containers)

    async def stream_log(self, request: Request) -> StreamResponse:
        app_instance = await self._resolve_app_instance(request=request)

        timestamps = _get_bool_param(request, "timestamps", default=False)
        debug = _get_bool_param(request, "debug", default=False)
        prefix = _get_bool_param(request, "prefix", default=False)
        since_str = request.query.get("since")
        try:
            containers = request.query.getall("container")
        except KeyError:
            containers = []
        archive_delay_s = float(
            request.query.get("archive_delay", self.get_archive_delay())
        )

        since = (
            parse_date(since_str)
            if since_str
            else datetime.now(UTC)
            - timedelta(
                minutes=self._config.platform_apps.default_since_timedelta_minutes
            )
        )
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

        loki_label_selector = {
            "org": app_instance.org_name,
            "project": app_instance.project_name,
            "app_instance_id": app_instance.id,
        }
        k8s_label_selector = {
            K8S_LABEL_APOLO_ORG: app_instance.org_name,
            K8S_LABEL_APOLO_PROJECT: app_instance.project_name,
            K8S_LABEL_APOLO_APP_ID: app_instance.id,
        }

        assert isinstance(self._logs_service, LokiLogsService)
        async with self._logs_service.get_pod_log_reader_by_containers(
            containers,
            loki_label_selector,
            k8s_label_selector,
            app_instance.namespace,
            separator=separator.encode(),
            since=since,
            timestamps=timestamps,
            debug=debug,
            archive_delay_s=archive_delay_s,
            prefix=prefix,
        ) as it:
            async for chunk in it:
                await response.write(chunk)
            await response.write_eof()
            return response

    async def ws_log(self, request: Request) -> StreamResponse:
        app_instance = await self._resolve_app_instance(request=request)

        timestamps = _get_bool_param(request, "timestamps", default=False)
        debug = _get_bool_param(request, "debug", default=False)
        prefix = _get_bool_param(request, "prefix", default=False)
        since_str = request.query.get("since")
        try:
            containers = request.query.getall("container")
        except KeyError:
            containers = []
        archive_delay_s = float(
            request.query.get("archive_delay", self.get_archive_delay())
        )

        since = (
            parse_date(since_str)
            if since_str
            else datetime.now(UTC)
            - timedelta(
                minutes=self._config.platform_apps.default_since_timedelta_minutes
            )
        )
        separator = request.query.get("separator")
        if separator is None:
            separator = "=== Live logs ===" + _getrandbytes(30).hex()

        loki_label_selector = {
            "org": app_instance.org_name,
            "project": app_instance.project_name,
            "app_instance_id": app_instance.id,
        }
        k8s_label_selector = {
            K8S_LABEL_APOLO_ORG: app_instance.org_name,
            K8S_LABEL_APOLO_PROJECT: app_instance.project_name,
            K8S_LABEL_APOLO_APP_ID: app_instance.id,
        }

        assert isinstance(self._logs_service, LokiLogsService)
        async with self._logs_service.get_pod_log_reader_by_containers(
            containers,
            loki_label_selector,
            k8s_label_selector,
            app_instance.namespace,
            separator=separator.encode(),
            since=since,
            timestamps=timestamps,
            debug=debug,
            archive_delay_s=archive_delay_s,
            prefix=prefix,
        ) as it:
            response = WebSocketResponse(
                protocols=[WS_LOGS_PROTOCOL],
                heartbeat=HEARTBEAT,
            )
            await response.prepare(request)
            await _run_concurrently(
                _listen(response),
                _forward_bytes_iterating(response, it),
            )
            return response

    async def _resolve_app_instance(self, request: Request) -> AppInstance:
        self._check_logs_backend()

        user = await untrusted_user(request)

        instance_id = request.match_info["app_id"]
        cluster_name = request.query.get("cluster_name")
        org_name = request.query.get("org_name")
        project_name = request.query.get("project_name")

        if not all([instance_id, cluster_name, org_name, project_name]):
            exc_txt = "Instance_id, cluster_name, org_name and project_name required"
            raise Exception(exc_txt)

        return await self._apps_api_client.get_app(
            app_instance_id=instance_id,
            cluster_name=cluster_name,  # type: ignore
            org_name=org_name,  # type: ignore
            project_name=project_name,  # type: ignore
            token=user.token,
        )


async def _listen(ws: WebSocketResponse) -> None:
    # Maintain the WebSocket connection.
    # Process ping-pong game and perform closing handshake.
    async for msg in ws:
        logger.info(f"Received unexpected WebSocket message: {msg!r}")  # noqa: G004


async def _forward_bytes_iterating(
    ws: WebSocketResponse, it: AsyncIterator[bytes]
) -> None:
    with suppress(ConnectionResetError):
        async for chunk in it:
            if ws.closed:
                break
            await ws.send_bytes(chunk)
            if ws.closed:
                break


async def _forward_reading(ws: WebSocketResponse, reader: asyncio.StreamReader) -> None:
    while not ws.closed:
        # 4-6 MB is the typical default socket receive buffer size on Linux
        data = await reader.read(4 * 1024 * 1024)
        if not data or ws.closed:
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
        *,
        handle_input: bool,
        handle_output: bool,
    ) -> None:
        self._resp = resp
        self._client_resp = client_resp
        self._handle_input = handle_input
        self._handle_output = handle_output
        self._closing = False

    async def transfer(self) -> None:
        try:
            await _run_concurrently(
                self._proxy(self._resp, self._client_resp),
                self._proxy(self._client_resp, self._resp),
            )
        finally:
            await self._resp.close()
            await self._client_resp.close()

    async def _proxy(
        self,
        src: WebSocketResponse | ClientWebSocketResponse,
        dst: WebSocketResponse | ClientWebSocketResponse,
    ) -> None:
        try:
            async for msg in src:
                if self._closing or dst.closed:
                    break

                if msg.type == aiohttp.WSMsgType.BINARY:
                    await dst.send_bytes(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    exc = src.exception()
                    logger.error(
                        "WS connection closed with exception %s", exc, exc_info=exc
                    )
                else:
                    err_msg = f"Unsupported WS message type {msg.type}"
                    raise ValueError(err_msg)
        finally:
            self._closing = True


@middleware
async def handle_exceptions(
    request: Request, handler: Callable[[Request], Awaitable[StreamResponse]]
) -> StreamResponse:
    ws_request = (
        request.headers.get("Connection", "").lower() == "upgrade"
        and request.headers.get("Upgrade", "").lower() == "websocket"
    )
    try:
        return await handler(request)
    except ValueError as e:
        payload = {"error": str(e)}
        return json_response(
            payload,
            status=HTTPBadRequest.status_code,
            headers={"X-Error": json.dumps(payload)} if ws_request else None,
        )
    except ContainerNotFoundError as e:
        payload = {"error": str(e)}
        return json_response(
            payload,
            status=HTTPNotFound.status_code,
            headers={"X-Error": json.dumps(payload)} if ws_request else None,
        )
    except ContainerRuntimeClientError as e:
        payload = {"error": str(e)}
        return json_response(
            payload,
            status=HTTPNotFound.status_code,
            headers={"X-Error": json.dumps(payload)} if ws_request else None,
        )
    except JobNotRunningException as e:
        payload = {"error": str(e)}
        return json_response(
            payload,
            status=HTTPNotFound.status_code,
            headers={"X-Error": json.dumps(payload)} if ws_request else None,
        )
    except JobException as e:
        payload = {"error": str(e)}
        return json_response(
            payload,
            status=HTTPBadRequest.status_code,
            headers={"X-Error": json.dumps(payload)} if ws_request else None,
        )
    except AppsApiException as e:
        payload = {"error": e.message}
        return json_response(
            payload,
            status=e.code if e.code in (401, 403, 404) else HTTPBadRequest.status_code,
            headers={"X-Error": json.dumps(payload)} if ws_request else None,
        )
    except aiohttp.web.HTTPException as e:
        if ws_request and e.text:
            e.headers["X-Error"] = e.text
        raise e
    except Exception as e:
        msg_str = f"Unexpected exception: {str(e)}. Path with query: {request.path_qs}."
        logger.exception(msg_str)
        payload = {"error": msg_str}
        return json_response(
            payload,
            status=HTTPInternalServerError.status_code,
            headers={"X-Error": json.dumps(payload)} if ws_request else None,
        )


async def create_monitoring_app(config: Config) -> aiohttp.web.Application:
    monitoring_app = aiohttp.web.Application()
    monitoring_handler = MonitoringApiHandler(monitoring_app, config)
    monitoring_handler.register(monitoring_app)
    return monitoring_app


async def create_apps_monitoring_app(config: Config) -> aiohttp.web.Application:
    apps_monitoring_app = aiohttp.web.Application()
    apps_monitoring_handler = AppsMonitoringApiHandler(apps_monitoring_app, config)
    apps_monitoring_handler.register(apps_monitoring_app)
    return apps_monitoring_app


@asynccontextmanager
async def create_platform_api_client(
    url: URL, token: str, trace_configs: list[aiohttp.TraceConfig] | None = None
) -> AsyncIterator[ApiClient]:
    async with ApiClient(url=url, token=token, trace_configs=trace_configs) as client:
        yield client


@asynccontextmanager
async def create_platform_apps_api_client(
    url: URL, token: str, trace_configs: list[aiohttp.TraceConfig] | None = None
) -> AsyncIterator[AppsApiClient]:
    async with AppsApiClient(
        url=url, token=token, trace_configs=trace_configs
    ) as client:
        yield client


@asynccontextmanager
async def create_kube_client(
    config: KubeConfig, trace_configs: list[aiohttp.TraceConfig] | None = None
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
        nvidia_dcgm_node_port=config.nvidia_dcgm_node_port,
        trace_configs=trace_configs,
    )
    try:
        await client.init()
        yield client
    finally:
        await client.close()


@asynccontextmanager
async def create_loki_client(config: LokiConfig) -> AsyncIterator[LokiClient]:
    client = LokiClient(
        base_url=config.endpoint_url,
        conn_timeout_s=config.client_conn_timeout_s,
        read_timeout_s=config.client_read_timeout_s,
        conn_pool_size=config.client_conn_pool_size,
        archive_delay_s=config.archive_delay_s,
    )
    try:
        await client.init()
        yield client
    finally:
        await client.close()


@asynccontextmanager
async def create_elasticsearch_client(
    config: ElasticsearchConfig,
) -> AsyncIterator[AsyncElasticsearch]:
    async with AsyncElasticsearch(hosts=config.hosts) as client:
        await client.ping()
        yield client


def create_s3_client(config: S3Config) -> AioBaseClient:
    kwargs: dict[str, Any] = {}
    if config.access_key_id:
        kwargs["aws_access_key_id"] = config.access_key_id
    if config.secret_access_key:
        kwargs["aws_secret_access_key"] = config.secret_access_key
    if config.endpoint_url:
        kwargs["endpoint_url"] = str(config.endpoint_url)
    if config.region:
        kwargs["region_name"] = config.region
    kwargs["config"] = AioConfig(
        retries={"mode": "standard"},  # 3 retries by default
    )
    session = aiobotocore.session.get_session()
    return session.create_client("s3", **kwargs)


async def create_s3_logs_bucket(client: AioBaseClient, config: S3Config) -> None:
    try:
        await client.create_bucket(Bucket=config.job_logs_bucket_name)
        logger.info("Bucket %r created", config.job_logs_bucket_name)
    except s3_client_error(409):
        logger.info("Bucket %r already exists", config.job_logs_bucket_name)


def create_logs_service(
    config: Config,
    kube_client: KubeClient,
    es_client: AsyncElasticsearch | None = None,
    s3_client: AioBaseClient | None = None,
    loki_client: LokiClient | None = None,
) -> LogsService:
    if config.logs.storage_type == LogsStorageType.ELASTICSEARCH:
        assert es_client
        return ElasticsearchLogsService(kube_client, es_client)

    if config.logs.storage_type == LogsStorageType.S3:
        assert s3_client
        return create_s3_logs_service(
            config, kube_client, s3_client, cache_log_metadata=False
        )

    if config.logs.storage_type == LogsStorageType.LOKI:
        assert loki_client
        assert config.loki
        return LokiLogsService(
            kube_client, loki_client, config.loki.max_query_lookback_s
        )

    msg = f"{config.logs.storage_type} storage is not supported"
    raise ValueError(msg)  # pragma: nocover


def create_s3_logs_service(
    config: Config,
    kube_client: KubeClient,
    s3_client: AioBaseClient,
    *,
    cache_log_metadata: bool = False,
) -> S3LogsService:
    assert config.s3
    metadata_storage = S3LogsMetadataStorage(
        s3_client,
        bucket_name=config.s3.job_logs_bucket_name,
        cache_metadata=cache_log_metadata,
    )
    metadata_service = S3LogsMetadataService(
        s3_client, metadata_storage, kube_namespace_name=config.kube.namespace
    )
    return S3LogsService(kube_client, s3_client, metadata_service)


package_version = version(__package__)


async def add_version_to_header(request: Request, response: StreamResponse) -> None:
    response.headers["X-Service-Version"] = f"platform-monitoring/{package_version}"


async def create_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app[CONFIG_KEY] = config

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            logger.info("Initializing Platform API client")
            platform_api_client = await exit_stack.enter_async_context(
                create_platform_api_client(
                    config.platform_api.url, config.platform_api.token
                )
            )

            logger.info("Initializing Auth client")
            auth_client = await exit_stack.enter_async_context(
                AuthClient(config.platform_auth.url or None, config.platform_auth.token)
            )

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )

            es_client: AsyncElasticsearch | None = None
            if config.elasticsearch:
                logger.info("Initializing Elasticsearch client")
                es_client = await exit_stack.enter_async_context(
                    create_elasticsearch_client(config.elasticsearch)
                )

            s3_client: AioBaseClient | None = None
            if config.s3:
                logger.info("Initializing S3 client")
                s3_client = await exit_stack.enter_async_context(
                    create_s3_client(config.s3)
                )
                await create_s3_logs_bucket(s3_client, config.s3)

            loki_client: LokiClient | None = None
            if config.loki:
                logger.info("Initializing Loki client")
                loki_client = await exit_stack.enter_async_context(
                    create_loki_client(config.loki)
                )

            logger.info("Initializing Kubernetes client")
            kube_client = await exit_stack.enter_async_context(
                create_kube_client(config.kube)
            )
            app[MONITORING_APP_KEY][KUBE_CLIENT_KEY] = kube_client

            logger.info("Initializing Platform Config client")
            config_client = await exit_stack.enter_async_context(
                ConfigClient(config.platform_config.url, config.platform_config.token)
            )
            app[MONITORING_APP_KEY][CONFIG_CLIENT_KEY] = config_client

            logs_service = create_logs_service(
                config, kube_client, es_client, s3_client, loki_client
            )
            app[MONITORING_APP_KEY][LOGS_SERVICE_KEY] = logs_service
            app[APPS_MONITORING_APP_KEY][LOGS_SERVICE_KEY] = logs_service
            app[APPS_MONITORING_APP_KEY][KUBE_CLIENT_KEY] = kube_client
            app[APPS_MONITORING_APP_KEY][LOGS_STORAGE_TYPE_KEY] = (
                config.logs.storage_type
            )

            container_runtime_client_registry = await exit_stack.enter_async_context(
                ContainerRuntimeClientRegistry(
                    container_runtime_port=config.container_runtime.port
                )
            )
            jobs_service = JobsService(
                config_client=config_client,
                jobs_client=platform_api_client,
                kube_client=kube_client,
                container_runtime_client_registry=container_runtime_client_registry,
                cluster_name=config.cluster_name,
                kube_node_pool_label=config.kube.node_pool_label,
            )
            app[MONITORING_APP_KEY][JOBS_SERVICE_KEY] = jobs_service

            await exit_stack.enter_async_context(
                LogCleanupPoller(
                    jobs_service=jobs_service,
                    logs_service=logs_service,
                    interval_sec=config.logs.cleanup_interval_sec,
                )
            )

            platform_apps_api_client = await exit_stack.enter_async_context(
                create_platform_apps_api_client(
                    config.platform_apps.url, config.platform_apps.token
                )
            )
            app[APPS_MONITORING_APP_KEY][APPS_API_CLIENT_KEY] = platform_apps_api_client

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)

    monitoring_app = await create_monitoring_app(config)
    app[MONITORING_APP_KEY] = monitoring_app
    api_v1_app.add_subapp("/jobs", monitoring_app)

    apps_monitoring_app = await create_apps_monitoring_app(config)
    app[APPS_MONITORING_APP_KEY] = apps_monitoring_app
    api_v1_app.add_subapp("/apps", apps_monitoring_app)

    app.add_subapp("/api/v1", api_v1_app)

    app.on_response_prepare.append(add_version_to_header)

    return app


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
    setup_sentry()
    aiohttp.web.run_app(
        create_app(config), host=config.server.host, port=config.server.port
    )


def _permission_to_primitive(perm: Permission) -> dict[str, str]:
    return {"uri": perm.uri, "action": perm.action}


def _job_uri_with_name(uri: URL, name: str) -> URL:
    assert name
    assert uri.host
    assert uri.name
    return uri.with_name(name)


def _get_bool_param(request: Request, name: str, *, default: bool = False) -> bool:
    param = request.query.get(name)
    if param is None:
        return default
    param = param.lower()
    if param in ("1", "true"):
        return True
    if param in ("0", "false"):
        return False
    msg = f'"{name}" request parameter can be "true"/"1" or "false"/"0"'
    raise ValueError(msg)


def _getrandbytes(size: int) -> bytes:
    return random.getrandbits(size * 8).to_bytes(size, "big")


async def _run_concurrently(*coros: Coroutine[Any, Any, None]) -> None:
    tasks: list[asyncio.Task[None]] = []
    try:
        for coro in coros:
            tasks.append(asyncio.create_task(coro))
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
