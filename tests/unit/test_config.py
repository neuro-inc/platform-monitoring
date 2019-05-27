from typing import Any, Dict

from platform_monitoring.config import Config, ServerConfig
from platform_monitoring.config_factory import EnvironConfigFactory


def test_create() -> None:
    environ: Dict[str, Any] = {
        "NP_MONITORING_API_HOST": "0.0.0.0",
        "NP_MONITORING_API_PORT": 8080,
    }
    config = EnvironConfigFactory(environ).create()
    assert config == Config(monitoring_server=ServerConfig(host="0.0.0.0", port=8080))
