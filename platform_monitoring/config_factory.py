import logging
import os
from typing import Dict, Optional

from yarl import URL

from .config import Config, PlatformApiConfig, PlatformAuthConfig, ServerConfig


logger = logging.getLogger(__name__)


class EnvironConfigFactory:
    def __init__(self, environ: Optional[Dict[str, str]] = None) -> None:
        self._environ = environ or os.environ

    def create(self) -> Config:
        return Config(
            server=self.create_server(),
            platform_api=self.create_platform_api(),
            platform_auth=self.create_platform_auth(),
        )

    def create_server(self) -> ServerConfig:
        host = self._environ.get("NP_MONITORING_API_HOST", ServerConfig.host)
        port = int(self._environ.get("NP_MONITORING_API_PORT", ServerConfig.port))
        return ServerConfig(host=host, port=port)

    def create_platform_api(self) -> PlatformApiConfig:
        url = URL(self._environ["NP_MONITORING_PLATFORM_API_URL"])
        token = self._environ["NP_MONITORING_PLATFORM_API_TOKEN"]
        return PlatformApiConfig(url=url, token=token)

    def create_platform_auth(self) -> PlatformAuthConfig:
        url = URL(self._environ["NP_MONITORING_PLATFORM_AUTH_URL"])
        token = self._environ["NP_MONITORING_PLATFORM_AUTH_TOKEN"]
        return PlatformAuthConfig(url=url, token=token)
