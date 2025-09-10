import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack

import aiohttp
from apolo_kube_client import (
    KubeClient,
    KubeClientAuthType,
    KubeConfig,
)
from neuro_config_client import ConfigClient
from neuro_logging import init_logging, setup_sentry

from ..config import ResourcesMonitorConfig
from ..config_factory import EnvironConfigFactory
from .monitoring import MonitoringService


LOGGER = logging.getLogger(__name__)


async def ping(request: aiohttp.web.Request) -> aiohttp.web.Response:
    return aiohttp.web.Response(text="Pong")


async def create_app(config: ResourcesMonitorConfig) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    app.router.add_get("/ping", ping)

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            LOGGER.info("Initializing Kubernetes client")
            kube_client = await exit_stack.enter_async_context(
                KubeClient(
                    config=KubeConfig(
                        endpoint_url=config.kube.endpoint_url,
                        cert_authority_data_pem=config.kube.cert_authority_data_pem,
                        cert_authority_path=config.kube.cert_authority_path,
                        auth_type=KubeClientAuthType(config.kube.auth_type),
                        auth_cert_path=config.kube.auth_cert_path,
                        auth_cert_key_path=config.kube.auth_cert_key_path,
                        token=config.kube.token,
                        token_path=config.kube.token_path,
                        client_conn_timeout_s=config.kube.client_conn_timeout_s,
                        client_read_timeout_s=config.kube.client_read_timeout_s,
                        client_conn_pool_size=config.kube.client_conn_pool_size,
                    )
                )
            )

            LOGGER.info("Initializing Config client")
            config_client = await exit_stack.enter_async_context(
                ConfigClient(config.platform_config.url, config.platform_config.token)
            )

            LOGGER.info("Initializing Monitoring service")
            service = await exit_stack.enter_async_context(
                MonitoringService(
                    kube_client=kube_client,
                    config_client=config_client,
                    cluster_name=config.cluster_name,
                )
            )

            await service.start()

            yield

    app.cleanup_ctx.append(_init_app)

    return app


def main() -> None:  # pragma: no coverage
    init_logging(health_check_url_path="/ping")
    config = EnvironConfigFactory().create_resources_monitor()
    # NOTE: If config is passed as arg all secrets will be logged in
    # log receord args attribute.
    logging.info(f"Loaded config: {config!r}")  # noqa: G004
    setup_sentry(health_check_url_path="/ping")
    aiohttp.web.run_app(
        create_app(config), host=config.server.host, port=config.server.port
    )
