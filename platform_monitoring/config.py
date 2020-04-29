import enum
from dataclasses import dataclass
from typing import Optional, Sequence

from yarl import URL


DOCKER_API_VERSION = "v1.39"


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(frozen=True)
class PlatformApiConfig:
    url: URL
    token: str


@dataclass(frozen=True)
class PlatformAuthConfig:
    url: URL
    token: str


@dataclass(frozen=True)
class ElasticsearchConfig:
    hosts: Sequence[str]


class KubeClientAuthType(str, enum.Enum):
    NONE = "none"
    TOKEN = "token"
    CERTIFICATE = "certificate"


@dataclass(frozen=True)
class CORSConfig:
    allowed_origins: Sequence[str] = ()


@dataclass(frozen=True)
class KubeConfig:
    endpoint_url: str
    cert_authority_data_pem: Optional[str] = None
    cert_authority_path: Optional[str] = None
    auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE
    auth_cert_path: Optional[str] = None
    auth_cert_key_path: Optional[str] = None
    token: Optional[str] = None
    namespace: str = "default"
    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_conn_pool_size: int = 100

    kubelet_node_port: int = 10255


@dataclass(frozen=True)
class RegistryConfig:
    url: URL

    @property
    def host(self) -> str:
        port = self.url.explicit_port  # type: ignore
        suffix = f":{port}" if port else ""
        return f"{self.url.host}{suffix}"


@dataclass(frozen=True)
class DockerConfig:
    docker_engine_api_port: int = 2375


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    platform_api: PlatformApiConfig
    platform_auth: PlatformAuthConfig
    elasticsearch: ElasticsearchConfig
    kube: KubeConfig
    docker: DockerConfig
    registry: RegistryConfig
    cors: CORSConfig
    cluster_name: str
