import asyncio
import json
import logging
import re
import shlex
import signal
import textwrap
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock
from uuid import uuid4

import aiohttp
import aiohttp.hdrs
import pytest
from aiohttp import WSServerHandshakeError
from aiohttp.web import HTTPOk
from aiohttp.web_exceptions import (
    HTTPAccepted,
    HTTPBadRequest,
    HTTPForbidden,
    HTTPNoContent,
    HTTPNotFound,
    HTTPUnauthorized,
)
from yarl import URL

from platform_monitoring.api import create_app
from platform_monitoring.config import (
    Config,
    ContainerRuntimeConfig,
    LokiConfig,
    PlatformApiConfig,
)
from platform_monitoring.platform_apps_client import (
    AppInstance,
    AppsApiClient,
    AppsApiException,
)

from .conftest import ApiAddress, create_local_app_server, random_str
from .conftest_admin import ProjectUser
from .conftest_kube import MyKubeClient


logger = logging.getLogger(__name__)


async def expect_prompt(ws: aiohttp.ClientWebSocketResponse) -> bytes:
    _ansi_re = re.compile(rb"\033\[[;?0-9]*[a-zA-Z]")
    _exit_re = re.compile(rb"exit \d+")
    try:
        ret: bytes = b""
        async with asyncio.timeout(3):
            async for msg in ws:
                assert msg.type == aiohttp.WSMsgType.BINARY
                assert msg.data[0] == 1
                ret += _ansi_re.sub(b"", msg.data[1:])
                ret_strip = ret.strip()
                if ret_strip.endswith(b"#") or _exit_re.fullmatch(ret_strip):
                    break
            return ret
    except TimeoutError:
        err_msg = f"[Timeout] {ret!r}"
        raise AssertionError(err_msg) from None


@dataclass(frozen=True)
class MonitoringApiEndpoints:
    address: ApiAddress

    @property
    def api_v1_endpoint(self) -> URL:
        return URL(f"http://{self.address.host}:{self.address.port}/api/v1")

    @property
    def ping_url(self) -> URL:
        return self.api_v1_endpoint / "ping"

    @property
    def endpoint(self) -> URL:
        return self.api_v1_endpoint / "jobs"

    @property
    def jobs_capacity_url(self) -> URL:
        return self.endpoint / "capacity"

    def generate_top_url(self, job_id: str) -> URL:
        return self.endpoint / job_id / "top"

    def generate_log_url(self, job_id: str) -> URL:
        return self.endpoint / job_id / "log"

    def generate_log_ws_url(self, job_id: str) -> URL:
        return self.endpoint / job_id / "log_ws"

    def generate_save_url(self, job_id: str) -> URL:
        return self.endpoint / job_id / "save"

    def generate_kill_url(self, job_id: str) -> URL:
        return self.endpoint / job_id / "kill"

    def generate_attach_url(
        self,
        job_id: str,
        *,
        tty: bool = False,
        stdin: bool = False,
        stdout: bool = True,
        stderr: bool = True,
        logs: bool = False,
    ) -> URL:
        url = self.endpoint / job_id / "attach"
        return url.with_query(
            tty=int(tty),
            stdin=int(stdin),
            stdout=int(stdout),
            stderr=int(stderr),
            logs=int(logs),
        )

    def generate_resize_url(self, job_id: str, *, w: int, h: int) -> URL:
        url = self.endpoint / job_id / "resize"
        return url.with_query(w=w, h=h)

    def generate_exec_create_url(self, job_id: str) -> URL:
        return self.endpoint / job_id / "exec_create"

    def generate_exec_resize_url(
        self, job_id: str, exec_id: str, *, w: int, h: int
    ) -> URL:
        url = self.endpoint / job_id / exec_id / "exec_resize"
        return url.with_query(w=w, h=h)

    def generate_exec_inspect_url(self, job_id: str, exec_id: str) -> URL:
        return self.endpoint / job_id / exec_id / "exec_inspect"

    def generate_exec_start_url(self, job_id: str, exec_id: str) -> URL:
        return self.endpoint / job_id / exec_id / "exec_start"

    def generate_exec_url(
        self,
        job_id: str,
        cmd: str,
        *,
        tty: bool = False,
        stdin: bool = False,
        stdout: bool = True,
        stderr: bool = True,
    ) -> URL:
        url = self.endpoint / job_id / "exec"
        return url.with_query(
            cmd=cmd,
            tty=int(tty),
            stdin=int(stdin),
            stdout=int(stdout),
            stderr=int(stderr),
        )

    def generate_port_forward_url(self, job_id: str, port: int | str) -> URL:
        return self.endpoint / job_id / "port_forward" / str(port)


@dataclass(frozen=True)
class AppsMonitoringApiEndpoints:
    address: ApiAddress

    @property
    def api_v1_endpoint(self) -> URL:
        return URL(f"http://{self.address.host}:{self.address.port}/api/v1")

    @property
    def endpoint(self) -> URL:
        return self.api_v1_endpoint / "apps"

    def generate_log_url(self, app_id: str) -> URL:
        return self.endpoint / app_id / "log"

    def generate_log_ws_url(self, app_id: str) -> URL:
        return self.endpoint / app_id / "log_ws"

    def generate_containers_url(self, app_id: str) -> URL:
        return self.endpoint / app_id / "containers"


@dataclass(frozen=True)
class PlatformApiEndpoints:
    url: URL

    @property
    def api_v1_endpoint(self) -> URL:
        return self.url / "api/v1"

    @property
    def platform_config_url(self) -> URL:
        return self.api_v1_endpoint / "config"

    @property
    def jobs_base_url(self) -> URL:
        return self.api_v1_endpoint / "jobs"

    def generate_job_url(self, job_id: str) -> URL:
        return self.jobs_base_url / job_id


@pytest.fixture
async def monitoring_api(config: Config) -> AsyncIterator[MonitoringApiEndpoints]:
    app = await create_app(config)
    async with create_local_app_server(app, port=8080) as address:
        yield MonitoringApiEndpoints(address=address)


@pytest.fixture
async def apps_monitoring_api(
    config_with_loki_logs_backend: Config,
) -> AsyncIterator[AppsMonitoringApiEndpoints]:
    app = await create_app(config_with_loki_logs_backend)
    async with create_local_app_server(app, port=8080) as address:
        yield AppsMonitoringApiEndpoints(address=address)


@pytest.fixture(scope="session")
async def monitoring_api_ep(
    platform_monitoring_api_address: ApiAddress,
) -> MonitoringApiEndpoints:
    return MonitoringApiEndpoints(address=platform_monitoring_api_address)


@pytest.fixture
async def monitoring_api_s3_storage(
    config_s3_storage: Config,
) -> AsyncIterator[MonitoringApiEndpoints]:
    app = await create_app(config_s3_storage)
    async with create_local_app_server(app, port=8080) as address:
        yield MonitoringApiEndpoints(address=address)


@pytest.fixture
def platform_api(
    platform_api_config: PlatformApiConfig,
) -> PlatformApiEndpoints:
    return PlatformApiEndpoints(url=platform_api_config.url)


class JobsClient:
    def __init__(
        self,
        platform_api: PlatformApiEndpoints,
        client: aiohttp.ClientSession,
        user: ProjectUser,
    ) -> None:
        self._platform_api = platform_api
        self._client = client
        self._user = user

    @property
    def user(self) -> ProjectUser:
        return self._user

    @property
    def headers(self) -> dict[str, str]:
        return self._user.headers

    async def get_job_by_id(self, job_id: str) -> dict[str, Any]:
        url = self._platform_api.generate_job_url(job_id)
        async with self._client.get(url, headers=self.headers) as response:
            response_text = await response.text()
            assert response.status == HTTPOk.status_code, response_text
            return await response.json()

    async def get_job_materialized_by_id(
        self,
        job_id: str,
    ) -> bool:
        url = self._platform_api.generate_job_url(job_id).with_query(
            _tests_check_materialized="True"
        )
        async with self._client.get(url, headers=self.headers) as response:
            response_text = await response.text()
            assert response.status == HTTPOk.status_code, response_text
            return (await response.json())["materialized"]

    async def run_job(self, job_submit: dict[str, Any]) -> str:
        headers = self.headers
        url = self._platform_api.jobs_base_url
        job_submit["cluster_name"] = self._user.cluster_name
        job_submit["org_name"] = self._user.org_name
        job_submit["project_name"] = self._user.project_name
        async with self._client.post(url, headers=headers, json=job_submit) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]
            if "name" in job_submit:
                assert result["name"] == job_submit["name"]
        return job_id

    async def long_polling_by_job_id(
        self, job_id: str, status: str, interval_s: float = 0.5, max_time: float = 180
    ) -> dict[str, Any]:
        t0 = time.monotonic()
        while True:
            response = await self.get_job_by_id(job_id)
            if response["status"] == status:
                return response
            await asyncio.sleep(max(interval_s, time.monotonic() - t0))
            current_time = time.monotonic() - t0
            if current_time > max_time:
                pytest.fail(f"too long: {current_time:.3f} sec; resp: {response}")
            interval_s *= 1.5

    async def wait_job_dematerialized(
        self, job_id: str, interval_s: float = 0.5, max_time: float = 300
    ) -> None:
        t0 = time.monotonic()
        while True:
            is_materialized = await self.get_job_materialized_by_id(job_id)
            if not is_materialized:
                return
            await asyncio.sleep(max(interval_s, time.monotonic() - t0))
            current_time = time.monotonic() - t0
            if current_time > max_time:
                pytest.fail(f"too long: {current_time:.3f} sec;")
            interval_s *= 1.5

    async def delete_job(self, job_id: str, *, assert_success: bool = True) -> None:
        url = self._platform_api.generate_job_url(job_id)
        async with self._client.delete(url, headers=self.headers) as response:
            if assert_success:
                assert response.status == HTTPNoContent.status_code

    async def drop_job(self, job_id: str, *, assert_success: bool = True) -> None:
        url = self._platform_api.generate_job_url(job_id) / "drop"
        async with self._client.post(url, headers=self.headers) as response:
            if assert_success:
                assert response.status == HTTPNoContent.status_code


@pytest.fixture
def jobs_client_factory(
    platform_api: PlatformApiEndpoints, client: aiohttp.ClientSession
) -> Callable[[ProjectUser], JobsClient]:
    def impl(user: ProjectUser) -> JobsClient:
        return JobsClient(platform_api, client, user=user)

    return impl


@pytest.fixture
async def jobs_client(
    regular_user1: ProjectUser,
    jobs_client_factory: Callable[[ProjectUser], JobsClient],
) -> JobsClient:
    return jobs_client_factory(regular_user1)


@pytest.fixture
def job_request_factory() -> Callable[[], dict[str, Any]]:
    def _factory() -> dict[str, Any]:
        return {
            "container": {
                "image": "ubuntu:20.10",
                "command": "true",
                "resources": {"cpu": 0.1, "memory": 128 * 2**20},
            }
        }

    return _factory


@pytest.fixture
async def job_submit(
    job_request_factory: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    return job_request_factory()


@pytest.fixture
async def job_factory(
    jobs_client: JobsClient,
    job_request_factory: Callable[[], dict[str, Any]],
) -> AsyncIterator[Callable[[str], Awaitable[str]]]:
    jobs: list[str] = []

    async def _f(command: str, name: str = "") -> str:
        request_payload = job_request_factory()
        request_payload["container"]["command"] = command
        if name:
            request_payload["name"] = name
        job_id = await jobs_client.run_job(request_payload)
        jobs.append(job_id)
        await jobs_client.long_polling_by_job_id(job_id, status="running")

        return job_id

    yield _f

    for job_id in jobs:
        await jobs_client.delete_job(job_id)
    for job_id in jobs:
        job = await jobs_client.get_job_by_id(job_id)
        if job["status"] == "cancelled":
            # Wait until job is deleted from k8s
            await jobs_client.wait_job_dematerialized(job_id)


@pytest.fixture
async def infinite_job(job_factory: Callable[[str], Awaitable[str]]) -> str:
    return await job_factory("tail -f /dev/null")


@pytest.fixture
def job_name() -> str:
    return f"test-job-{random_str()}"


@pytest.fixture
async def named_infinite_job(
    job_factory: Callable[[str, str], Awaitable[str]], job_name: str
) -> str:
    return await job_factory("tail -f /dev/null", job_name)


class TestApi:
    async def test_ping(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        config: Config,
    ) -> None:
        async with client.get(monitoring_api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == "Pong"

    async def test_ping_includes_version(
        self, monitoring_api: MonitoringApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(monitoring_api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
            assert "platform-monitoring" in resp.headers["X-Service-Version"]

    async def test_get_capacity(
        self,
        monitoring_api: MonitoringApiEndpoints,
        regular_user1: ProjectUser,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.get(
            monitoring_api.jobs_capacity_url,
            headers=regular_user1.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            result = await resp.json()
            assert "cpu-small" in result

    async def test_get_capacity_forbidden(
        self,
        monitoring_api: MonitoringApiEndpoints,
        regular_user_factory: Callable[..., Awaitable[ProjectUser]],
        client: aiohttp.ClientSession,
        cluster_name: str,
    ) -> None:
        user = await regular_user_factory(cluster_name="default2")
        async with client.get(
            monitoring_api.jobs_capacity_url, headers=user.headers
        ) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            result = await resp.json()
            assert {
                "uri": f"job://{cluster_name}/{user.name}",
                "action": "read",
            } in result["missing"]
            assert {
                "uri": f"cluster://{cluster_name}/access",
                "action": "read",
            } in result["missing"]


class TestTopApi:
    async def test_top_ok(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        num_request = 2
        records = []

        url = monitoring_api.generate_top_url(job_id=infinite_job)
        async with client.ws_connect(url, headers=jobs_client.headers) as ws:
            # TODO move this ws communication to JobClient
            async for msg in ws:
                assert msg.type == aiohttp.WSMsgType.TEXT
                records.append(json.loads(msg.data))
                if len(records) == num_request:
                    break

        assert len(records) == num_request
        for message in records:
            assert message == {
                "cpu": mock.ANY,
                "memory": mock.ANY,
                "memory_bytes": mock.ANY,
                "timestamp": mock.ANY,
            }

    async def test_top_shared_by_name(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_name: str,
        named_infinite_job: str,
        regular_user2: ProjectUser,
        share_job: Callable[..., Awaitable[None]],
    ) -> None:
        await share_job(jobs_client.user, regular_user2, job_name)

        url = monitoring_api.generate_top_url(named_infinite_job)
        async with client.ws_connect(url, headers=regular_user2.headers):
            pass

    async def test_top_no_permissions_unauthorized(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        infinite_job: str,
        regular_user1: ProjectUser,
        regular_user2: ProjectUser,
    ) -> None:
        url = monitoring_api.generate_top_url(infinite_job)
        with pytest.raises(WSServerHandshakeError) as err:
            async with client.ws_connect(url, headers=regular_user2.headers):
                pass
        assert err.value.message == "Invalid response status"
        assert err.value.status == HTTPForbidden.status_code

    async def test_top_no_auth_token_provided_unauthorized(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        infinite_job: str,
    ) -> None:
        url = monitoring_api.generate_top_url(job_id=infinite_job)
        with pytest.raises(WSServerHandshakeError) as err:
            async with client.ws_connect(url):
                pass
        assert err.value.message == "Invalid response status"
        assert err.value.status == HTTPUnauthorized.status_code

    async def test_top_non_running_job(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        await jobs_client.delete_job(infinite_job)
        await jobs_client.long_polling_by_job_id(
            job_id=infinite_job, status="cancelled"
        )

        url = monitoring_api.generate_top_url(job_id=infinite_job)
        async with client.ws_connect(url, headers=jobs_client.headers) as ws:
            msg = await ws.receive()
            assert msg.type == aiohttp.WSMsgType.CLOSE

    async def test_top_non_existing_job(
        self,
        platform_api: PlatformApiEndpoints,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
    ) -> None:
        headers = jobs_client.headers
        job_id = f"job-{uuid4()}"

        url = platform_api.generate_job_url(job_id)
        async with client.get(url, headers=headers) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.text()
            assert "no such job" in payload

        url = monitoring_api.generate_top_url(job_id=job_id)
        with pytest.raises(WSServerHandshakeError) as err:
            async with client.ws_connect(url, headers=headers):
                pass
        assert err.value.message == "Invalid response status"
        assert err.value.status == HTTPBadRequest.status_code

    async def test_top_silently_wait_when_job_pending(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: dict[str, Any],
    ) -> None:
        command = 'bash -c "for i in {1..10}; do echo $i; sleep 1; done"'
        job_submit["container"]["command"] = command
        job_id = await jobs_client.run_job(job_submit)

        num_request = 2
        records: list[Any] = []

        headers = jobs_client.headers
        job_top_url = monitoring_api.generate_top_url(job_id)
        async with client.ws_connect(job_top_url, headers=headers) as ws:
            job = await jobs_client.get_job_by_id(job_id=job_id)
            assert job["status"] == "pending"

            async for msg in ws:
                assert msg.type == aiohttp.WSMsgType.TEXT
                if not records:  # First message.
                    job = await jobs_client.get_job_by_id(job_id=job_id)
                    assert job["status"] == "running"
                records.append(json.loads(msg.data))
                if len(records) == num_request:
                    break

        assert len(records) == num_request
        for message in records:
            assert message == {
                "cpu": mock.ANY,
                "memory": mock.ANY,
                "memory_bytes": mock.ANY,
                "timestamp": mock.ANY,
            }

        await jobs_client.delete_job(job_id=job_id)

    async def test_top_close_when_job_succeeded(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: dict[str, Any],
    ) -> None:
        command = 'bash -c "for i in {1..2}; do echo $i; sleep 1; done"'
        job_submit["container"]["command"] = command
        job_id = await jobs_client.run_job(job_submit)
        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")

        headers = jobs_client.headers
        job_top_url = monitoring_api.generate_top_url(job_id)
        async with client.ws_connect(job_top_url, headers=headers) as ws:
            msg = await ws.receive()
            assert msg.type == aiohttp.WSMsgType.CLOSE
            job = await jobs_client.get_job_by_id(job_id=job_id)
            assert job["status"] == "succeeded"

        await jobs_client.delete_job(job_id=job_id)


class TestLogApi:
    async def test_log_no_permissions_forbidden(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user1: ProjectUser,
        regular_user2: ProjectUser,
        infinite_job: str,
        cluster_name: str,
    ) -> None:
        url = monitoring_api.generate_log_url(infinite_job)
        async with client.get(url, headers=regular_user2.headers) as resp:
            assert resp.status == HTTPForbidden.status_code
            result = await resp.json()
            job_uri = (
                f"job://{cluster_name}/{regular_user1.org_name}/"
                f"{regular_user1.project_name}/{infinite_job}"
            )
            assert result == {"missing": [{"uri": job_uri, "action": "read"}]}

    async def test_log_no_auth_token_provided_unauthorized(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        url = monitoring_api.generate_log_url(job_id=infinite_job)
        async with client.get(url) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    async def test_job_log(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: dict[str, Any],
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        job_submit["container"]["command"] = command
        job_id = await jobs_client.run_job(job_submit)
        await jobs_client.long_polling_by_job_id(job_id, "succeeded")

        headers = jobs_client.headers
        url = monitoring_api.generate_log_url(job_id)
        async with client.get(url, headers=headers) as response:
            assert response.status == HTTPOk.status_code
            assert response.content_type == "text/plain"
            assert response.charset == "utf-8"
            assert response.headers["Transfer-Encoding"] == "chunked"
            assert "Content-Encoding" not in response.headers
            actual_payload = await response.read()
            expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
            assert actual_payload == expected_payload.encode()

    async def test_job_log_ws(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: dict[str, Any],
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        job_submit["container"]["command"] = command
        job_id = await jobs_client.run_job(job_submit)
        await jobs_client.long_polling_by_job_id(job_id, "succeeded")

        headers = jobs_client.headers
        url = monitoring_api.generate_log_ws_url(job_id)

        async with client.ws_connect(url, headers=headers) as ws:
            ws_data = []
            async for msg in ws:
                assert msg.type == aiohttp.WSMsgType.BINARY
                ws_data.append(msg.data)

        actual_payload = b"".join(ws_data)
        expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
        assert actual_payload == expected_payload.encode()

    async def test_log_shared_by_name(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        named_infinite_job: str,
        job_name: str,
        regular_user1: ProjectUser,
        regular_user2: ProjectUser,
        share_job: Callable[[ProjectUser, ProjectUser, str], Awaitable[None]],
    ) -> None:
        await share_job(regular_user1, regular_user2, job_name)

        url = monitoring_api.generate_log_url(named_infinite_job)
        async with client.get(url, headers=regular_user2.headers) as resp:
            assert resp.status == HTTPOk.status_code

    async def test_job_log_cleanup(
        self,
        monitoring_api_s3_storage: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: dict[str, Any],
        kube_client: MyKubeClient,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; done; sleep 100"'
        job_submit["container"]["command"] = command
        job_id = await jobs_client.run_job(job_submit)

        # Jobs is canceled so its pod removed immediately
        await jobs_client.long_polling_by_job_id(job_id, "running")
        await jobs_client.delete_job(job_id)
        await kube_client.wait_pod_is_deleted(job_id)

        headers = jobs_client.headers
        url = monitoring_api_s3_storage.generate_log_url(job_id)
        async with client.get(url, headers=headers) as response:
            actual_payload = await response.read()
            expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
            assert actual_payload == expected_payload.encode()

        async with client.delete(url, headers=headers) as response:
            assert response.status == HTTPNoContent.status_code

        async with client.get(url, headers=headers) as response:
            actual_payload = await response.read()
            assert actual_payload == b""

    async def test_job_logs_removed_on_drop(
        self,
        monitoring_api_s3_storage: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: dict[str, Any],
        kube_client: MyKubeClient,
    ) -> None:
        job_id = await jobs_client.run_job(job_submit)
        await jobs_client.long_polling_by_job_id(job_id, "succeeded")

        # Drop request
        await jobs_client.drop_job(job_id)

        async def _wait_no_job() -> None:
            while True:
                try:
                    await jobs_client.get_job_by_id(job_id)
                except AssertionError:
                    return

        await asyncio.wait_for(_wait_no_job(), timeout=10)


class TestSaveApi:
    async def test_save_no_permissions_forbidden(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        infinite_job: str,
        regular_user1: ProjectUser,
        regular_user2: ProjectUser,
        cluster_name: str,
    ) -> None:
        url = monitoring_api.generate_save_url(infinite_job)
        async with client.post(url, headers=regular_user2.headers) as resp:
            assert resp.status == HTTPForbidden.status_code
            result = await resp.json()
            job_uri = (
                f"job://{cluster_name}/{regular_user1.org_name}/"
                f"{regular_user1.project_name}/{infinite_job}"
            )
            assert result == {"missing": [{"uri": job_uri, "action": "write"}]}

    async def test_save_no_auth_token_provided_unauthorized(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        infinite_job: str,
    ) -> None:
        url = monitoring_api.generate_save_url(job_id=infinite_job)
        async with client.post(url) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    async def test_save_non_existing_job(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
    ) -> None:
        headers = jobs_client.headers
        job_id = f"job-{uuid4()}"

        url = monitoring_api.generate_save_url(job_id=job_id)
        async with client.post(url, headers=headers) as resp:
            assert resp.status == HTTPBadRequest.status_code, str(resp)
            assert "no such job" in await resp.text()

    async def test_save_unknown_registry_host(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        url = monitoring_api.generate_save_url(job_id=infinite_job)
        headers = jobs_client.headers
        payload = {"container": {"image": "unknown:5000/alpine:latest"}}
        async with client.post(url, headers=headers, json=payload) as resp:
            assert resp.status == HTTPBadRequest.status_code, str(resp)
            resp_payload = await resp.json()
            assert "Unknown registry host" in resp_payload["error"]

    async def test_save_not_running_job(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
        config: Config,
        kube_client: MyKubeClient,
    ) -> None:
        await jobs_client.delete_job(infinite_job)
        await kube_client.wait_pod_is_terminated(
            pod_name=infinite_job, allow_pod_not_exists=True
        )

        url = monitoring_api.generate_save_url(job_id=infinite_job)
        headers = jobs_client.headers
        payload = {
            "container": {"image": f"{config.registry.host}/alpine:{infinite_job}"}
        }
        async with client.post(url, headers=headers, json=payload) as resp:
            assert resp.status == HTTPOk.status_code, str(resp)
            chunks = [
                json.loads(chunk.decode("utf-8"))
                async for chunk in resp.content
                if chunk
            ]
            debug = f"Received chunks: `{chunks}`"

            assert len(chunks) == 1, debug
            assert "not running" in chunks[0]["error"], debug

    async def test_save_push_failed_job_exception_raised(
        self,
        config_factory: Callable[..., Config],
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        invalid_runtime_config = ContainerRuntimeConfig(port=1)
        config = config_factory(container_runtime=invalid_runtime_config)

        app = await create_app(config)
        async with create_local_app_server(app, port=8080) as address:
            monitoring_api = MonitoringApiEndpoints(address=address)
            url = monitoring_api.generate_save_url(job_id=infinite_job)

            headers = jobs_client.headers
            image = f"{config.registry.host}/alpine:{infinite_job}"
            payload = {"container": {"image": image}}
            async with client.post(url, headers=headers, json=payload) as resp:
                assert resp.status == HTTPOk.status_code, str(resp)
                chunks = [
                    json.loads(chunk.decode("utf-8"))
                    async for chunk in resp.content
                    if chunk
                ]
                debug = f"Received chunks: `{chunks}`"

                assert len(chunks) == 1, debug

                error = chunks[0]["error"]
                assert "Unexpected error: Cannot connect to host" in error, debug
                assert "Connect call failed" in error, debug

    async def test_save_ok(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
        config: Config,
    ) -> None:
        url = monitoring_api.generate_save_url(job_id=infinite_job)
        headers = jobs_client.headers
        repository = f"{config.registry.host}/alpine"
        image = f"{repository}:{infinite_job}"
        payload = {"container": {"image": image}}

        async with client.post(url, headers=headers, json=payload) as resp:
            assert resp.status == HTTPOk.status_code, str(resp)
            chunks = [
                json.loads(chunk.decode("utf-8"))
                async for chunk in resp.content
                if chunk
            ]
            debug = f"Received chunks: `{chunks}`"
            assert isinstance(chunks, list), debug
            assert all(isinstance(s, dict) for s in chunks), debug
            assert len(chunks) >= 4, debug  # 2 for commit(), >=2 for push()

            # here we rely on chunks to be received in correct order

            assert chunks[0]["status"] == "CommitStarted", debug
            assert chunks[0]["details"]["image"] == image, debug
            assert re.match(r"\w{64}", chunks[0]["details"]["container"]), debug

            assert chunks[1] == {"status": "CommitFinished"}, debug

            msg = f"The push refers to repository [{repository}]"
            assert chunks[2].get("status") == msg, debug

            assert chunks[-1].get("aux", {}).get("Tag") == infinite_job, debug

    async def test_save_shared_by_name(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_name: str,
        named_infinite_job: str,
        regular_user2: ProjectUser,
        share_job: Callable[..., Awaitable[None]],
        config: Config,
    ) -> None:
        await share_job(jobs_client.user, regular_user2, job_name, action="write")

        url = monitoring_api.generate_save_url(job_id=named_infinite_job)
        repository = f"{config.registry.host}/alpine"
        image = f"{repository}:{named_infinite_job}"
        payload = {"container": {"image": image}}

        async with client.post(
            url, headers=regular_user2.headers, json=payload
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()


class TestAttachApi:
    async def test_attach_forbidden(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
    ) -> None:
        url = monitoring_api.generate_attach_url(
            job_id="anything", stdout=True, stderr=True
        )

        with pytest.raises(WSServerHandshakeError) as exc_info:
            async with client.ws_connect(url):
                pass

        assert exc_info.value.headers
        assert exc_info.value.headers.get("X-Error")
        assert exc_info.value.message == "Invalid response status"
        assert exc_info.value.status == HTTPUnauthorized.status_code

    async def test_attach_nontty_stdout(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: dict[str, Any],
    ) -> None:
        command = 'bash -c "for i in {0..9}; do echo $i; sleep 1; done"'
        job_submit["container"]["command"] = command
        job_id = await jobs_client.run_job(job_submit)
        await jobs_client.long_polling_by_job_id(job_id=job_id, status="running")

        headers = jobs_client.headers
        url = monitoring_api.generate_attach_url(
            job_id=job_id, stdout=True, stderr=True
        )

        async with client.ws_connect(url, headers=headers) as ws:
            await ws.receive_bytes(timeout=5)  # empty message is sent to stdout

            content = []
            async for msg in ws:
                assert msg.type == aiohttp.WSMsgType.BINARY
                content.append(msg.data)

        expected = (
            b"".join(f"\x01{i}\n".encode("ascii") for i in range(10))
            + b'\x03{"exit_code": 0}'
        )
        assert b"".join(content) in expected

        await jobs_client.delete_job(job_id)

    async def test_attach_nontty_stdout_shared_by_name(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: dict[str, Any],
        job_name: str,
        regular_user2: ProjectUser,
        share_job: Callable[..., Awaitable[None]],
    ) -> None:
        command = 'bash -c "for i in {0..9}; do echo $i; sleep 1; done"'
        job_submit["container"]["command"] = command
        job_submit["name"] = job_name
        job_id = await jobs_client.run_job(job_submit)

        await share_job(jobs_client.user, regular_user2, job_name, action="write")

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="running")

        url = monitoring_api.generate_attach_url(
            job_id=job_id, stdout=True, stderr=True
        )

        async with client.ws_connect(url, headers=regular_user2.headers) as ws:
            await ws.receive_bytes(timeout=5)  # empty message is sent to stdout

            content = []
            async for msg in ws:
                assert msg.type == aiohttp.WSMsgType.BINARY
                content.append(msg.data)

        expected = (
            b"".join(f"\x01{i}\n".encode("ascii") for i in range(10))
            + b'\x03{"exit_code": 0}'
        )

        logger.info("content: %s", content)

        assert b"".join(content) in expected

        await jobs_client.delete_job(job_id)

    async def test_attach_nontty_stderr(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: dict[str, Any],
    ) -> None:
        command = 'bash -c "for i in {0..9}; do echo $i >&2; sleep 1; done"'
        job_submit["container"]["command"] = command
        job_id = await jobs_client.run_job(job_submit)
        await jobs_client.long_polling_by_job_id(job_id=job_id, status="running")

        headers = jobs_client.headers
        url = monitoring_api.generate_attach_url(
            job_id=job_id, stdout=True, stderr=True
        )

        async with client.ws_connect(url, headers=headers) as ws:
            await ws.receive_bytes(timeout=5)  # empty message is sent to stdout

            content = []
            async for msg in ws:
                assert msg.type == aiohttp.WSMsgType.BINARY
                content.append(msg.data)

        expected = (
            b"".join(f"\x02{i}\n".encode("ascii") for i in range(10))
            + b'\x03{"exit_code": 0}'
        )
        assert b"".join(content) in expected

        await jobs_client.delete_job(job_id)

    async def test_attach_tty(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: dict[str, Any],
    ) -> None:
        command = "sh"
        job_submit["container"]["command"] = command
        job_submit["container"]["tty"] = True
        job_id = await jobs_client.run_job(job_submit)
        await jobs_client.long_polling_by_job_id(job_id=job_id, status="running")

        headers = jobs_client.headers
        url = monitoring_api.generate_attach_url(
            job_id=job_id, tty=True, stdin=True, stdout=True, stderr=False
        )

        async with client.ws_connect(url, headers=headers) as ws:
            await ws.receive_bytes(timeout=5)  # empty message is sent to stdout

            await ws.send_bytes(b"\x00\n")
            assert await expect_prompt(ws) == b"\r\n# "
            await ws.send_bytes(b"\x00echo 'abc'\n")
            assert await expect_prompt(ws) == b"echo 'abc'\r\nabc\r\n# "

        await jobs_client.delete_job(job_id)

    async def test_attach_tty_exit_code(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: dict[str, Any],
    ) -> None:
        command = "sh"
        job_submit["container"]["command"] = command
        job_submit["container"]["tty"] = True
        job_id = await jobs_client.run_job(job_submit)
        await jobs_client.long_polling_by_job_id(job_id=job_id, status="running")

        headers = jobs_client.headers
        url = monitoring_api.generate_attach_url(
            job_id=job_id, tty=True, stdin=True, stdout=True, stderr=False
        )

        async with client.ws_connect(url, headers=headers) as ws:
            await ws.receive_bytes(timeout=5)  # empty message is sent to stdout

            await ws.send_bytes(b"\x00\n")
            assert await expect_prompt(ws) == b"\r\n# "
            await ws.send_bytes(b"\x00exit 1\n")
            assert await expect_prompt(ws) == b"exit 1\r\n"

            while True:
                msg = await ws.receive(timeout=5)

                if msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.CLOSED,
                ):
                    break

                assert msg.type == aiohttp.WSMsgType.BINARY
                if msg.data[0] == 3:
                    payload = json.loads(msg.data[1:])
                    # Zero code is returned even if we exited with non zero code.
                    # The same behavior as in kubectl.
                    assert payload["exit_code"] == 0
                    break

        await jobs_client.delete_job(job_id)

    async def test_reattach_just_after_exit(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_submit: dict[str, Any],
    ) -> None:
        command = "sh"
        job_submit["container"]["command"] = command
        job_submit["container"]["tty"] = True
        job_id = await jobs_client.run_job(job_submit)
        await jobs_client.long_polling_by_job_id(job_id=job_id, status="running")

        headers = jobs_client.headers
        url = monitoring_api.generate_attach_url(
            job_id=job_id, tty=True, stdin=True, stdout=True, stderr=False
        )

        async with client.ws_connect(url, headers=headers) as ws:
            await ws.receive_bytes(timeout=5)  # empty message is sent to stdout

            await ws.send_bytes(b"\x00\n")
            assert await expect_prompt(ws) == b"\r\n# "
            await ws.send_bytes(b"\x00exit 1\n")
            assert await expect_prompt(ws) == b"exit 1\r\n"

        await asyncio.sleep(2)  # Allow poller to collect pod

        with pytest.raises(WSServerHandshakeError) as err:
            async with client.ws_connect(url, headers=headers):
                pass
        assert err.value.status == HTTPNotFound.status_code

        await jobs_client.long_polling_by_job_id(job_id, status="failed")

        await jobs_client.delete_job(job_id)


class TestExecApi:
    async def test_exec_notty_stdout(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        headers = jobs_client.headers

        url = monitoring_api.generate_exec_url(
            infinite_job,
            "sh -c 'sleep 5; echo abc'",
            tty=False,
            stdin=False,
            stdout=True,
            stderr=True,
        )
        async with client.ws_connect(url, headers=headers) as ws:
            await ws.receive_bytes(timeout=5)
            data = await ws.receive_bytes()
            assert data == b"\x01abc\n"

    async def test_exec_notty_stdout_shared_by_name(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_name: str,
        named_infinite_job: str,
        regular_user2: ProjectUser,
        share_job: Callable[..., Awaitable[None]],
    ) -> None:
        await share_job(jobs_client.user, regular_user2, job_name, action="write")

        url = monitoring_api.generate_exec_url(
            named_infinite_job,
            "sh -c 'sleep 5; echo abc'",
            tty=False,
            stdin=False,
            stdout=True,
            stderr=True,
        )
        async with client.ws_connect(url, headers=regular_user2.headers) as ws:
            await ws.receive_bytes(timeout=5)
            data = await ws.receive_bytes()
            assert data == b"\x01abc\n"

    async def test_exec_notty_stderr(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        headers = jobs_client.headers

        url = monitoring_api.generate_exec_url(
            infinite_job,
            "sh -c 'sleep 5; echo abc 1>&2'",
            tty=False,
            stdin=False,
            stdout=True,
            stderr=True,
        )
        async with client.ws_connect(url, headers=headers) as ws:
            await ws.receive_bytes(timeout=5)
            data = await ws.receive_bytes()
            assert data == b"\x02abc\n"

    async def test_exec_tty(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        headers = jobs_client.headers

        url = monitoring_api.generate_exec_url(
            infinite_job,
            "sh",
            tty=True,
            stdin=True,
            stdout=True,
            stderr=False,
        )
        async with client.ws_connect(url, headers=headers) as ws:
            await ws.receive_bytes(timeout=5)

            assert (await expect_prompt(ws)).strip() == b"#"
            await ws.send_bytes(b"\x00echo 'abc'\n")
            assert await expect_prompt(ws) == b"echo 'abc'\r\nabc\r\n# "

    async def test_exec_tty_exit_code(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        headers = jobs_client.headers

        url = monitoring_api.generate_exec_url(
            infinite_job,
            "sh",
            tty=True,
            stdin=True,
            stdout=True,
            stderr=False,
        )
        async with client.ws_connect(url, headers=headers) as ws:
            await ws.receive_bytes(timeout=5)

            assert (await expect_prompt(ws)).strip() == b"#"
            await ws.send_bytes(b"\x00exit 42\n")
            assert await expect_prompt(ws) == b"exit 42\r\n"

            while True:
                msg = await ws.receive(timeout=5)

                if msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.CLOSED,
                ):
                    break

                assert msg.type == aiohttp.WSMsgType.BINARY
                if msg.data[0] == 3:
                    payload = json.loads(msg.data[1:])
                    assert payload["exit_code"] == 42
                    break


class TestKillApi:
    async def test_kill(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        headers = jobs_client.headers

        url = monitoring_api.generate_kill_url(infinite_job)
        async with client.post(url, headers=headers) as response:
            assert response.status == HTTPNoContent.status_code, await response.text()

        result = await jobs_client.long_polling_by_job_id(infinite_job, status="failed")
        assert result["history"]["exit_code"] == 128 + signal.SIGKILL, result

    async def test_kill_shared_by_name(
        self,
        monitoring_api: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        job_name: str,
        named_infinite_job: str,
        regular_user2: ProjectUser,
        share_job: Callable[..., Awaitable[None]],
    ) -> None:
        await share_job(jobs_client.user, regular_user2, job_name, action="write")

        url = monitoring_api.generate_kill_url(named_infinite_job)
        async with client.post(url, headers=regular_user2.headers) as response:
            assert response.status == HTTPNoContent.status_code, await response.text()

        result = await jobs_client.long_polling_by_job_id(
            named_infinite_job, status="failed"
        )
        assert result["history"]["exit_code"] == 128 + signal.SIGKILL, result


class TestPortForward:
    async def test_port_forward_bad_port(
        self,
        monitoring_api_ep: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        headers = jobs_client.headers

        # String port is invalid
        url = monitoring_api_ep.generate_port_forward_url(infinite_job, "abc")

        async with client.get(url, headers=headers) as response:
            assert response.status == HTTPBadRequest.status_code, await response.text()

    async def test_port_forward_cannot_connect(
        self,
        monitoring_api_ep: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        headers = jobs_client.headers

        # Port 60001 is not handled
        url = monitoring_api_ep.generate_port_forward_url(infinite_job, 60001)

        async with client.get(url, headers=headers) as response:
            assert response.status == HTTPBadRequest.status_code, await response.text()

    async def test_port_forward_ok(
        self,
        monitoring_api_ep: MonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        job_submit: dict[str, Any],
        jobs_client: JobsClient,
    ) -> None:
        py = textwrap.dedent(
            """\
            import socket

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("0.0.0.0", 60002))
            sock.listen()
            cli, addr = sock.accept()
            while True:
                data = cli.recv(1024)
                if not data:
                    break
                cli.sendall(b"rep-"+data)
        """
        )
        command = f"python -c '{py}'"
        job_submit["container"]["command"] = command
        job_submit["container"]["image"] = "python:latest"
        job_id = await jobs_client.run_job(job_submit)
        await jobs_client.long_polling_by_job_id(job_id=job_id, status="running")

        headers = jobs_client.headers

        url = monitoring_api_ep.generate_port_forward_url(job_id, 60002)

        async with client.ws_connect(url, headers=headers) as ws:
            for i in range(3):
                data = str(i).encode("ascii")
                await ws.send_bytes(data)
                ret = await ws.receive_bytes()
                assert ret == b"rep-" + data
            await ws.close()

        await jobs_client.delete_job(job_id)


class MyAppsPodDescriptor:
    def __init__(self, pod_name: str, **kwargs: dict[str, Any]) -> None:
        self._payload: dict[str, Any] = {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {
                "name": pod_name,
                "labels": {"platform.apolo.us/app": "app_instance_id"},
                "namespace": "default",
            },
            "spec": {
                "containers": [
                    {
                        "name": "container1",
                        "image": "busybox",
                        "command": [
                            "/bin/sh",
                            "-c",
                            "for i in $(seq 1 5); do echo container1_$i; sleep 1; done",
                        ],
                    },
                    {
                        "name": "container2",
                        "image": "busybox",
                        "command": [
                            "/bin/sh",
                            "-c",
                            "for i in $(seq 1 5); do echo container2_$i; sleep 1; done",
                        ],
                    },
                ],
                "restartPolicy": "Never",
                "imagePullSecrets": [],
                "tolerations": [],
            },
            **kwargs,
        }

    def set_command(self, command: str) -> None:
        for container in self._payload["spec"]["containers"]:
            container["command"] = shlex.split(command)

    def set_restart_policy(self, policy: str) -> None:
        self._payload["spec"]["restartPolicy"] = policy

    def add_labels(self, labels: dict[str, str]) -> None:
        self._payload["metadata"]["labels"].update(labels)

    @property
    def payload(self) -> dict[str, Any]:
        return self._payload

    @property
    def name(self) -> str:
        return self._payload["metadata"]["name"]

    @property
    def containers(self) -> list[str]:
        return [container["name"] for container in self._payload["spec"]["containers"]]


@pytest.fixture
async def apps_basic_pod(
    kube_client: MyKubeClient,
    regular_user1: ProjectUser,
) -> AsyncIterator[MyAppsPodDescriptor]:
    pod_name = f"test-pod-{uuid4()}"
    apps_pod_description = MyAppsPodDescriptor(pod_name)
    apps_pod_description.add_labels(
        {
            "platform.apolo.us/org": regular_user1.org_name,
            "platform.apolo.us/project": regular_user1.project_name,
        }
    )
    await kube_client.create_pod(apps_pod_description.payload)
    yield apps_pod_description
    await kube_client.delete_pod(apps_pod_description.name)


@pytest.fixture
def _get_app_mock(
    monkeypatch: pytest.MonkeyPatch,
    regular_user1: ProjectUser,
    kube_client: MyKubeClient,
) -> None:
    async def mock_get_app(*args: Any, **kwargs: Any) -> AppInstance:
        return AppInstance(
            id="app_instance_id",
            name="test-apps-instance",
            org_name=regular_user1.org_name,
            project_name=regular_user1.project_name,
            namespace=kube_client.namespace,
        )

    monkeypatch.setattr(AppsApiClient, "get_app", mock_get_app)


class TestAppsLogApi:
    @staticmethod
    async def read_ws(ws: aiohttp.ClientWebSocketResponse) -> bytes:
        ws_actual_payload = b""
        async for msg in ws:
            assert msg.type == aiohttp.WSMsgType.BINARY
            ws_actual_payload += msg.data
        return ws_actual_payload

    async def response_read_task(
        self,
        client: aiohttp.ClientSession,
        url: URL,
        headers: dict[str, Any],
        params: dict[str, Any],
        resp_type: str,
    ) -> bytes:
        if resp_type == "stream":
            async with client.get(url, headers=headers, params=params) as response:
                actual_log = await response.read()
        elif resp_type == "ws":
            async with client.ws_connect(url, headers=headers, params=params) as ws:
                actual_log = await self.read_ws(ws)
        return actual_log

    @staticmethod
    def assert_archive_logs(
        actual_log: bytes,
        container_count_start: int,
        container_count_end: int,
        logs_count_start: int,
        logs_count_end: int,
        re_log_template: str,
    ) -> None:
        for c_number in range(
            container_count_start, container_count_end + 1
        ):  # iterate over containers
            for l_number in range(
                logs_count_start, logs_count_end + 1
            ):  # iterate over logs
                log_pattern = re.compile(
                    re_log_template.replace("[c_number]", str(c_number))
                    .replace("[l_number]", str(l_number))
                    .replace("[time]", r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{,9}Z")
                    .encode()
                )
                actual_log, replace_count = log_pattern.subn(b"", actual_log)
                assert replace_count == 1
        assert actual_log == b""

    @staticmethod
    def assert_archive_logs_as_json(
        actual_log: bytes,
        pod_name: str,
        namespace: str,
        container_count_start: int,
        container_count_end: int,
        logs_count_start: int,
        logs_count_end: int,
        re_log_template: str,
    ) -> None:
        actual_log_list = [
            json.loads(obj) for obj in re.findall(rb"\{.*?\}", actual_log)
        ]
        for c_number in range(
            container_count_start, container_count_end + 1
        ):  # iterate over containers
            for l_number in range(
                logs_count_start, logs_count_end + 1
            ):  # iterate over logs
                log_pattern = re.compile(
                    re_log_template.replace("[c_number]", str(c_number))
                    .replace("[l_number]", str(l_number))
                    .replace("[time]", r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{,9}Z")
                )
                log = next(
                    filter(lambda x: log_pattern.match(x["log"]), actual_log_list)
                )
                assert log["pod"] == pod_name
                assert log["namespace"] == namespace
                assert log["container"] == f"container{c_number}"
                actual_log_list.remove(log)
        assert actual_log_list == []

    @pytest.mark.usefixtures("_get_app_mock")
    async def test_apps_only_loki_log(
        self,
        apps_monitoring_api: AppsMonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user1: ProjectUser,
        apps_basic_pod: MyAppsPodDescriptor,
        kube_client: MyKubeClient,
        loki_config: LokiConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        since = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        pod_name = apps_basic_pod.name

        for container_name in apps_basic_pod.containers:
            await kube_client.wait_pod_is_terminated(pod_name, container_name)

        await asyncio.sleep(loki_config.archive_delay_s)

        headers = regular_user1.headers
        url = apps_monitoring_api.generate_log_url(app_id="app_instance_id")
        url_ws = apps_monitoring_api.generate_log_ws_url(app_id="app_instance_id")
        base_params = {
            "since": since,
            "cluster_name": "default",
            "org_name": regular_user1.org_name,
            "project_name": regular_user1.project_name,
        }

        # test base params
        async with client.get(url, headers=headers, params=base_params) as response:
            assert response.status == HTTPOk.status_code
            assert response.content_type == "text/plain"
            assert response.charset == "utf-8"
            assert response.headers["Transfer-Encoding"] == "chunked"
            assert "Content-Encoding" not in response.headers
            actual_log = await response.read()
        async with client.ws_connect(url_ws, headers=headers, params=base_params) as ws:
            ws_actual_log = await self.read_ws(ws)
        assert actual_log == ws_actual_log
        self.assert_archive_logs(
            actual_log=actual_log,
            container_count_start=1,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=r"container[c_number]_[l_number]\n",
        )

        # test with prefix
        params = base_params.copy()
        params["prefix"] = "true"
        async with client.get(url, headers=headers, params=params) as response:
            actual_log = await response.read()
        async with client.ws_connect(url_ws, headers=headers, params=params) as ws:
            ws_actual_log = await self.read_ws(ws)
        assert actual_log == ws_actual_log
        self.assert_archive_logs(
            actual_log=actual_log,
            container_count_start=1,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=rf"\[{pod_name}/container[c_number]\] "
            f"container[c_number]_[l_number]\n",
        )

        # test with timestamps
        params = base_params.copy()
        params["timestamps"] = "true"
        async with client.get(url, headers=headers, params=params) as response:
            actual_log = await response.read()
        async with client.ws_connect(url_ws, headers=headers, params=params) as ws:
            ws_actual_log = await self.read_ws(ws)
        assert actual_log == ws_actual_log
        self.assert_archive_logs(
            actual_log=actual_log,
            container_count_start=1,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=r"[time] container[c_number]_[l_number]\n",
        )

        # test with containers filter
        params = base_params.copy()
        params["container"] = "container2"
        async with client.get(url, headers=headers, params=params) as response:
            actual_log = await response.read()
        async with client.ws_connect(url_ws, headers=headers, params=params) as ws:
            ws_actual_log = await self.read_ws(ws)
        assert actual_log == ws_actual_log
        self.assert_archive_logs(
            actual_log=actual_log,
            container_count_start=2,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=r"container[c_number]_[l_number]\n",
        )

        pod = await kube_client.get_pod(pod_name)

        # test with since container1
        params = base_params.copy()
        finished_at = pod.status.container_statuses[0].finished_at
        assert finished_at
        params["since"] = (finished_at - timedelta(seconds=2)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        params["container"] = "container1"
        async with client.get(url, headers=headers, params=params) as response:
            actual_log = await response.read()
        async with client.ws_connect(url_ws, headers=headers, params=params) as ws:
            ws_actual_log = await self.read_ws(ws)
        assert actual_log == ws_actual_log
        self.assert_archive_logs(
            actual_log=actual_log,
            container_count_start=1,
            container_count_end=1,
            logs_count_start=4,
            logs_count_end=5,
            re_log_template=r"container[c_number]_[l_number]\n",
        )

        # test with since container2
        params = base_params.copy()
        finished_at = pod.status.container_statuses[1].finished_at
        assert finished_at
        params["since"] = (finished_at - timedelta(seconds=2)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        params["containers"] = "container2"
        async with client.get(url, headers=headers, params=params) as response:
            actual_log = await response.read()
        async with client.ws_connect(url_ws, headers=headers, params=params) as ws:
            ws_actual_log = await self.read_ws(ws)
        assert actual_log == ws_actual_log
        self.assert_archive_logs(
            actual_log=actual_log,
            container_count_start=2,
            container_count_end=2,
            logs_count_start=4,
            logs_count_end=5,
            re_log_template=r"container[c_number]_[l_number]\n",
        )

        # test with content-type application/json
        params = base_params.copy()
        headers_ = headers.copy()
        headers_["Content-Type"] = "application/json"
        async with client.get(url, headers=headers_, params=params) as response:
            actual_log = await response.read()
        async with client.ws_connect(url_ws, headers=headers, params=params) as ws:
            ws_actual_log = await self.read_ws(ws)
        assert actual_log == ws_actual_log
        self.assert_archive_logs_as_json(
            actual_log=actual_log,
            pod_name=pod_name,
            namespace=kube_client.namespace,
            container_count_start=1,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=r"container[c_number]_[l_number]\n",
        )

        # test with content-type application/json and prefix
        params = base_params.copy()
        params["prefix"] = "true"
        headers_ = headers.copy()
        headers_["Content-Type"] = "application/json"
        async with client.get(url, headers=headers_, params=params) as response:
            actual_log = await response.read()
        async with client.ws_connect(url_ws, headers=headers, params=params) as ws:
            ws_actual_log = await self.read_ws(ws)
        assert actual_log == ws_actual_log
        self.assert_archive_logs_as_json(
            actual_log=actual_log,
            pod_name=pod_name,
            namespace=kube_client.namespace,
            container_count_start=1,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=rf"\[{pod_name}/container[c_number]\] "
            f"container[c_number]_[l_number]\n",
        )

        # test with content-type application/json and timestamps
        params = base_params.copy()
        params["timestamps"] = "true"
        headers_ = headers.copy()
        headers_["Content-Type"] = "application/json"
        async with client.get(url, headers=headers_, params=params) as response:
            actual_log = await response.read()
        async with client.ws_connect(url_ws, headers=headers, params=params) as ws:
            ws_actual_log = await self.read_ws(ws)
        assert actual_log == ws_actual_log
        self.assert_archive_logs_as_json(
            actual_log=actual_log,
            pod_name=pod_name,
            namespace=kube_client.namespace,
            container_count_start=1,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=r"[time] container[c_number]_[l_number]\n",
        )

    @pytest.mark.usefixtures("_get_app_mock")
    async def test_apps_only_live_log(
        self,
        apps_monitoring_api: AppsMonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user1: ProjectUser,
        apps_basic_pod: MyAppsPodDescriptor,
        kube_client: MyKubeClient,
        loki_config: LokiConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        since = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        pod_name = apps_basic_pod.name

        headers = regular_user1.headers
        url = apps_monitoring_api.generate_log_url(app_id="app_instance_id")
        url_ws = apps_monitoring_api.generate_log_ws_url(app_id="app_instance_id")
        tasks = []

        # test base params
        base_params = {
            "since": since,
            "cluster_name": "default",
            "org_name": regular_user1.org_name,
            "project_name": regular_user1.project_name,
        }
        tasks.append(
            asyncio.create_task(
                self.response_read_task(client, url, headers, base_params, "stream")
            )
        )
        tasks.append(
            asyncio.create_task(
                self.response_read_task(client, url_ws, headers, base_params, "ws")
            )
        )

        # test with prefix
        params = base_params.copy()
        params["prefix"] = "true"
        tasks.append(
            asyncio.create_task(
                self.response_read_task(client, url, headers, params, "stream")
            )
        )
        tasks.append(
            asyncio.create_task(
                self.response_read_task(client, url_ws, headers, params, "ws")
            )
        )

        # test with timestamps
        params = base_params.copy()
        params["timestamps"] = "true"
        tasks.append(
            asyncio.create_task(
                self.response_read_task(client, url, headers, params, "stream")
            )
        )
        tasks.append(
            asyncio.create_task(
                self.response_read_task(client, url_ws, headers, params, "ws")
            )
        )

        # test with containers filter
        params = base_params.copy()
        params["container"] = "container2"
        tasks.append(
            asyncio.create_task(
                self.response_read_task(client, url, headers, params, "stream")
            )
        )
        tasks.append(
            asyncio.create_task(
                self.response_read_task(client, url_ws, headers, params, "ws")
            )
        )

        log_results = await asyncio.gather(*tasks)
        (
            log_base_params_stream,
            log_base_params_ws,
            log_prefix_stream,
            log_prefix_ws,
            log_ts_stream,
            log_ts_ws,
            log_container_filter_stream,
            log_container_filter_ws,
        ) = log_results

        # test base params
        self.assert_archive_logs(
            actual_log=log_base_params_stream,
            container_count_start=1,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=r"container[c_number]_[l_number]\n",
        )
        self.assert_archive_logs(
            actual_log=log_base_params_ws,
            container_count_start=1,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=r"container[c_number]_[l_number]\n",
        )

        # test with prefix
        self.assert_archive_logs(
            actual_log=log_prefix_stream,
            container_count_start=1,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=rf"\[{pod_name}/container[c_number]\] "
            rf"container[c_number]_[l_number]\n",
        )
        self.assert_archive_logs(
            actual_log=log_prefix_ws,
            container_count_start=1,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=rf"\[{pod_name}/container[c_number]\] "
            rf"container[c_number]_[l_number]\n",
        )

        # test with timestamps
        self.assert_archive_logs(
            actual_log=log_ts_stream,
            container_count_start=1,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=r"[time] container[c_number]_[l_number]\n",
        )
        self.assert_archive_logs(
            actual_log=log_ts_ws,
            container_count_start=1,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=r"[time] container[c_number]_[l_number]\n",
        )

        # test with containers filter
        self.assert_archive_logs(
            actual_log=log_container_filter_stream,
            container_count_start=2,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=r"container[c_number]_[l_number]\n",
        )
        self.assert_archive_logs(
            actual_log=log_container_filter_ws,
            container_count_start=2,
            container_count_end=2,
            logs_count_start=1,
            logs_count_end=5,
            re_log_template=r"container[c_number]_[l_number]\n",
        )

    @pytest.mark.usefixtures("_get_app_mock")
    async def test_apps_containers(
        self,
        apps_monitoring_api: AppsMonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user1: ProjectUser,
        apps_basic_pod: MyAppsPodDescriptor,
        kube_client: MyKubeClient,
    ) -> None:
        pod_name = apps_basic_pod.name
        since = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        for container_name in apps_basic_pod.containers:
            await kube_client.wait_pod_is_terminated(
                pod_name, container_name=container_name, namespace=kube_client.namespace
            )

        headers = regular_user1.headers
        url = apps_monitoring_api.generate_containers_url(app_id="app_instance_id")

        async with client.get(url, headers=headers) as response:
            assert response.status == HTTPOk.status_code
            containers = await response.json()
            assert set(containers) == set(apps_basic_pod.containers)

        params = {"since": since}
        async with client.get(url, headers=headers, params=params) as response:
            assert response.status == HTTPOk.status_code
            containers = await response.json()
            assert set(containers) == set(apps_basic_pod.containers)

    async def test_apps_logs_exceptions(
        self,
        apps_monitoring_api: AppsMonitoringApiEndpoints,
        client: aiohttp.ClientSession,
        regular_user1: ProjectUser,
        apps_basic_pod: MyAppsPodDescriptor,
        kube_client: MyKubeClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        pod_name = apps_basic_pod.name

        for container_name in apps_basic_pod.containers:
            await kube_client.wait_pod_is_running(
                pod_name, namespace=kube_client.namespace, container_name=container_name
            )

        headers = regular_user1.headers
        url = apps_monitoring_api.generate_log_url(app_id="app_instance_id")
        url_ws = apps_monitoring_api.generate_log_ws_url(app_id="app_instance_id")
        base_params = {
            "cluster_name": "default",
            "org_name": regular_user1.org_name,
            "project_name": regular_user1.project_name,
        }

        # test with not Authorized
        async with client.get(url, headers={}, params=base_params) as response:
            assert response.status == HTTPUnauthorized.status_code
        with pytest.raises(aiohttp.WSServerHandshakeError):
            async with client.ws_connect(url_ws, headers={}, params=base_params):
                pass

        # test with 401 Exception from apps_api
        async def mock_get_app(*args: Any, **kwargs: Any) -> AppInstance:
            exc_text = "Unauthorized"
            raise AppsApiException(code=401, message=exc_text)

        monkeypatch.setattr(AppsApiClient, "get_app", mock_get_app)

        async with client.get(url, headers=headers, params=base_params) as response:
            error_json = await response.json()
            assert response.status == aiohttp.web.HTTPUnauthorized.status_code
            assert error_json["error"] == "Unauthorized"
        with pytest.raises(aiohttp.WSServerHandshakeError):
            async with client.ws_connect(url_ws, headers=headers, params=base_params):
                pass

        # test with 403 Exception from apps_api
        async def mock_get_app(*args: Any, **kwargs: Any) -> AppInstance:  # type: ignore
            exc_text = "Forbidden"
            raise AppsApiException(code=403, message=exc_text)

        monkeypatch.setattr(AppsApiClient, "get_app", mock_get_app)

        async with client.get(url, headers=headers, params=base_params) as response:
            error_json = await response.json()
            assert response.status == aiohttp.web.HTTPForbidden.status_code
            assert error_json["error"] == "Forbidden"
        with pytest.raises(aiohttp.WSServerHandshakeError):
            async with client.ws_connect(url_ws, headers=headers, params=base_params):
                pass

        # test with another Exception from apps_api
        async def mock_get_app(*args: Any, **kwargs: Any) -> AppInstance:  # type: ignore
            exc_text = "Something went wrong"
            raise AppsApiException(code=500, message=exc_text)

        monkeypatch.setattr(AppsApiClient, "get_app", mock_get_app)

        async with client.get(url, headers=headers, params=base_params) as response:
            error_json = await response.json()
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            assert error_json["error"] == "Something went wrong"
        with pytest.raises(aiohttp.WSServerHandshakeError):
            async with client.ws_connect(url_ws, headers=headers, params=base_params):
                pass
