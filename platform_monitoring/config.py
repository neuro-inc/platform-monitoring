import enum
from dataclasses import dataclass, field
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
class PlatformConfig:
    url: URL
    token: str


@dataclass(frozen=True)
class ElasticsearchConfig:
    hosts: Sequence[str]


@dataclass(frozen=True)
class S3Config:
    region: str
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)
    job_logs_bucket_name: str
    job_logs_key_prefix_format: str
    endpoint_url: Optional[URL] = None


class LogsStorageType(str, enum.Enum):
    ELASTICSEARCH = "elasticsearch"
    S3 = "s3"


@dataclass(frozen=True)
class LogsConfig:
    storage_type: LogsStorageType
    cleanup_interval_sec: float = 15 * 60  # 15m


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
    token_path: Optional[str] = None
    token: Optional[str] = None
    namespace: str = "default"
    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_conn_pool_size: int = 100

    kubelet_node_port: int = 10250

    job_label: str = "platform.neuromation.io/job"
    node_pool_label: str = "platform.neuromation.io/nodepool"


@dataclass(frozen=True)
class RegistryConfig:
    url: URL

    @property
    def host(self) -> str:
        port = self.url.explicit_port  # type: ignore
        suffix = f":{port}" if port else ""
        return f"{self.url.host}{suffix}"


@dataclass(frozen=True)
class ContainerRuntimeConfig:
    name: str
    port: int = 9000


@dataclass(frozen=True)
class ZipkinConfig:
    url: URL
    app_name: str = "platform-monitoring"
    sample_rate: float = 0


@dataclass(frozen=True)
class SentryConfig:
    dsn: URL
    cluster_name: str
    app_name: str = "platform-monitoring"
    sample_rate: float = 0


@dataclass(frozen=True)
class Config:
    cluster_name: str
    server: ServerConfig
    platform_api: PlatformApiConfig
    platform_auth: PlatformAuthConfig
    platform_config: PlatformConfig
    logs: LogsConfig
    kube: KubeConfig
    container_runtime: ContainerRuntimeConfig
    registry: RegistryConfig
    cors: CORSConfig
    elasticsearch: Optional[ElasticsearchConfig] = None
    s3: Optional[S3Config] = None
    zipkin: Optional[ZipkinConfig] = None
    sentry: Optional[SentryConfig] = None
