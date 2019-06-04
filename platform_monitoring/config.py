import enum
from dataclasses import dataclass
from typing import Optional, Sequence

from yarl import URL


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
    user: Optional[str] = None
    password: Optional[str] = None


class KubeClientAuthType(str, enum.Enum):
    NONE = "none"
    TOKEN = "token"
    CERTIFICATE = "certificate"


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


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    platform_api: PlatformApiConfig
    platform_auth: PlatformAuthConfig
    elasticsearch: ElasticsearchConfig
    orchestrator: KubeConfig
