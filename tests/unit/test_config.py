from typing import Any, Dict

from platform_monitoring.config import (
    Config,
    PlatformApiConfig,
    PlatformAuthConfig,
    ServerConfig,
)
from platform_monitoring.config_factory import EnvironConfigFactory
from yarl import URL


def test_create() -> None:
    environ: Dict[str, Any] = {
        "NP_MONITORING_API_HOST": "0.0.0.0",
        "NP_MONITORING_API_PORT": 8080,
        "NP_MONITORING_PLATFORM_API_URL": "http://platformapi/api/v1",
        "NP_MONITORING_PLATFORM_API_TOKEN": "platform-api-token",
        "NP_MONITORING_PLATFORM_AUTH_URL": "http://platformauthapi/api/v1",
        "NP_MONITORING_PLATFORM_AUTH_TOKEN": "platform-auth-token",
    }
    config = EnvironConfigFactory(environ).create()
    assert config == Config(
        server=ServerConfig(host="0.0.0.0", port=8080),
        platform_api=PlatformApiConfig(
            url=URL("http://platformapi/api/v1"), token="platform-api-token"
        ),
        platform_auth=PlatformAuthConfig(
            url=URL("http://platformauthapi/api/v1"), token="platform-auth-token"
        ),
    )
