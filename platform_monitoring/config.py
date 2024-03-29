import enum
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Optional

from yarl import URL


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(frozen=True)
class PlatformApiConfig:
    url: URL
    token: str = field(repr=False)


@dataclass(frozen=True)
class PlatformAuthConfig:
    url: Optional[URL]
    token: str = field(repr=False)


@dataclass(frozen=True)
class PlatformConfig:
    url: URL
    token: str = field(repr=False)


@dataclass(frozen=True)
class ElasticsearchConfig:
    hosts: Sequence[str]


@dataclass(frozen=True)
class S3Config:
    region: str
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)
    job_logs_bucket_name: str
    endpoint_url: Optional[URL] = None


class LogsStorageType(str, enum.Enum):
    ELASTICSEARCH = "elasticsearch"
    S3 = "s3"


@dataclass(frozen=True)
class LogsCompactConfig:
    run_interval: float = 300
    compact_interval: float = 3600
    cleanup_interval: float = 3600


@dataclass(frozen=True)
class LogsConfig:
    storage_type: LogsStorageType
    cleanup_interval_sec: float = 15 * 60  # 15m
    compact: LogsCompactConfig = LogsCompactConfig()


class KubeClientAuthType(str, enum.Enum):
    NONE = "none"
    TOKEN = "token"
    CERTIFICATE = "certificate"


@dataclass(frozen=True)
class KubeConfig:
    endpoint_url: str
    cert_authority_data_pem: Optional[str] = field(default=None, repr=False)
    cert_authority_path: Optional[str] = None
    auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE
    auth_cert_path: Optional[str] = field(default=None, repr=False)
    auth_cert_key_path: Optional[str] = None
    token_path: Optional[str] = None
    token: Optional[str] = field(default=None, repr=False)
    namespace: str = "default"
    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_conn_pool_size: int = 100

    kubelet_node_port: int = 10250
    nvidia_dcgm_node_port: Optional[int] = None

    job_label: str = "platform.neuromation.io/job"
    node_pool_label: str = "platform.neuromation.io/nodepool"


@dataclass(frozen=True)
class RegistryConfig:
    url: URL

    @property
    def host(self) -> str:
        port = self.url.explicit_port
        suffix = f":{port}" if port else ""
        return f"{self.url.host}{suffix}"


@dataclass(frozen=True)
class ContainerRuntimeConfig:
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
    elasticsearch: Optional[ElasticsearchConfig] = None
    s3: Optional[S3Config] = None
    zipkin: Optional[ZipkinConfig] = None
    sentry: Optional[SentryConfig] = None
