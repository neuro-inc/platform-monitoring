import logging
import os
from pathlib import Path
from typing import Dict, Optional

from yarl import URL

from .config import (
    Config,
    ElasticsearchConfig,
    KubeClientAuthType,
    KubeConfig,
    PlatformApiConfig,
    PlatformAuthConfig,
    RegistryConfig,
    ServerConfig,
)


logger = logging.getLogger(__name__)


class EnvironConfigFactory:
    def __init__(self, environ: Optional[Dict[str, str]] = None) -> None:
        self._environ = environ or os.environ

    def create(self) -> Config:
        return Config(
            server=self._create_server(),
            platform_api=self._create_platform_api(),
            platform_auth=self._create_platform_auth(),
            elasticsearch=self._create_elasticsearch(),
            kube=self._create_kube(),
            registry=self._create_registry(),
        )

    def _create_server(self) -> ServerConfig:
        host = self._environ.get("NP_MONITORING_API_HOST", ServerConfig.host)
        port = int(self._environ.get("NP_MONITORING_API_PORT", ServerConfig.port))
        return ServerConfig(host=host, port=port)

    def _create_platform_api(self) -> PlatformApiConfig:
        url = URL(self._environ["NP_MONITORING_PLATFORM_API_URL"])
        token = self._environ["NP_MONITORING_PLATFORM_API_TOKEN"]
        return PlatformApiConfig(url=url, token=token)

    def _create_platform_auth(self) -> PlatformAuthConfig:
        url = URL(self._environ["NP_MONITORING_PLATFORM_AUTH_URL"])
        token = self._environ["NP_MONITORING_PLATFORM_AUTH_TOKEN"]
        return PlatformAuthConfig(url=url, token=token)

    def _create_elasticsearch(self) -> ElasticsearchConfig:
        hosts = self._environ["NP_MONITORING_ES_HOSTS"].split(",")
        return ElasticsearchConfig(hosts=hosts)

    def _create_kube(self) -> KubeConfig:
        endpoint_url = self._environ["NP_MONITORING_K8S_API_URL"]
        auth_type = KubeClientAuthType(
            self._environ.get("NP_MONITORING_K8S_AUTH_TYPE", KubeConfig.auth_type.value)
        )
        ca_path = self._environ.get("NP_MONITORING_K8S_CA_PATH")
        ca_data = Path(ca_path).read_text() if ca_path else None

        token_path = self._environ.get("NP_MONITORING_K8S_TOKEN_PATH")
        token = Path(token_path).read_text() if token_path else None

        return KubeConfig(
            endpoint_url=endpoint_url,
            cert_authority_data_pem=ca_data,
            auth_type=auth_type,
            auth_cert_path=self._environ.get("NP_MONITORING_K8S_AUTH_CERT_PATH"),
            auth_cert_key_path=self._environ.get(
                "NP_MONITORING_K8S_AUTH_CERT_KEY_PATH"
            ),
            token=token,
            namespace=self._environ.get("NP_MONITORING_K8S_NS", KubeConfig.namespace),
            client_conn_timeout_s=int(
                self._environ.get("NP_MONITORING_K8S_CLIENT_CONN_TIMEOUT")
                or KubeConfig.client_conn_timeout_s
            ),
            client_read_timeout_s=int(
                self._environ.get("NP_MONITORING_K8S_CLIENT_READ_TIMEOUT")
                or KubeConfig.client_read_timeout_s
            ),
            client_conn_pool_size=int(
                self._environ.get("NP_MONITORING_K8S_CLIENT_CONN_POOL_SIZE")
                or KubeConfig.client_conn_pool_size
            ),
        )

    def _create_registry(self) -> RegistryConfig:
        return RegistryConfig(url=URL(self._environ["NP_MONITORING_REGISTRY_URL"]))
