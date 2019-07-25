from pathlib import Path
from typing import Any, Dict

import pytest
from platform_monitoring.config import (
    Config,
    DockerConfig,
    ElasticsearchConfig,
    KubeClientAuthType,
    KubeConfig,
    PlatformApiConfig,
    PlatformAuthConfig,
    RegistryConfig,
    ServerConfig,
)
from platform_monitoring.config_factory import EnvironConfigFactory
from yarl import URL


CA_DATA_PEM = "this-is-certificate-authority-public-key"
TOKEN = "this-is-token"


@pytest.fixture()
def cert_authority_path(tmp_path: Path) -> str:
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text(CA_DATA_PEM)
    return str(ca_path)


@pytest.fixture()
def token_path(tmp_path: Path) -> str:
    token_path = tmp_path / "token"
    token_path.write_text(TOKEN)
    return str(token_path)


def test_create(cert_authority_path: str, token_path: str) -> None:
    environ: Dict[str, Any] = {
        "NP_MONITORING_API_HOST": "0.0.0.0",
        "NP_MONITORING_API_PORT": 8080,
        "NP_MONITORING_PLATFORM_API_URL": "http://platformapi/api/v1",
        "NP_MONITORING_PLATFORM_API_TOKEN": "platform-api-token",
        "NP_MONITORING_PLATFORM_AUTH_URL": "http://platformauthapi/api/v1",
        "NP_MONITORING_PLATFORM_AUTH_TOKEN": "platform-auth-token",
        "NP_MONITORING_ES_HOSTS": "http://es1,http://es2",
        "NP_MONITORING_ES_AUTH_USER": "test-user",
        "NP_MONITORING_ES_AUTH_PASSWORD": "test-password",
        "NP_MONITORING_K8S_API_URL": "https://localhost:8443",
        "NP_MONITORING_K8S_AUTH_TYPE": "token",
        "NP_MONITORING_K8S_CA_PATH": cert_authority_path,
        "NP_MONITORING_K8S_TOKEN_PATH": token_path,
        "NP_MONITORING_K8S_AUTH_CERT_PATH": "/cert_path",
        "NP_MONITORING_K8S_AUTH_CERT_KEY_PATH": "/cert_key_path",
        "NP_MONITORING_K8S_NS": "other-namespace",
        "NP_MONITORING_K8S_CLIENT_CONN_TIMEOUT": "111",
        "NP_MONITORING_K8S_CLIENT_READ_TIMEOUT": "222",
        "NP_MONITORING_K8S_CLIENT_CONN_POOL_SIZE": "333",
        "NP_MONITORING_REGISTRY_URL": "http://testhost:5000",
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
        elasticsearch=ElasticsearchConfig(hosts=["http://es1", "http://es2"]),
        kube=KubeConfig(
            endpoint_url="https://localhost:8443",
            cert_authority_data_pem=CA_DATA_PEM,
            auth_type=KubeClientAuthType.TOKEN,
            token=TOKEN,
            auth_cert_path="/cert_path",
            auth_cert_key_path="/cert_key_path",
            namespace="other-namespace",
            client_conn_timeout_s=111,
            client_read_timeout_s=222,
            client_conn_pool_size=333,
        ),
        registry=RegistryConfig(url=URL("http://testhost:5000")),
        docker=DockerConfig(),
    )


@pytest.mark.parametrize(
    "url, expected_host",
    (
        (URL("https://testdomain.com"), "testdomain.com"),
        (URL("https://testdomain.com:443"), "testdomain.com:443"),
        (URL("http://localhost:5000"), "localhost:5000"),
    ),
)
def test_registry_config_host(url: URL, expected_host: str) -> None:
    config = RegistryConfig(url)
    assert config.host == expected_host
