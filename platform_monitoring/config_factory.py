import logging
import os
from pathlib import Path

from yarl import URL

from .config import (
    Config,
    ContainerRuntimeConfig,
    ElasticsearchConfig,
    KubeClientAuthType,
    KubeConfig,
    LogsCompactConfig,
    LogsConfig,
    LogsStorageType,
    LokiConfig,
    PlatformApiConfig,
    PlatformAuthConfig,
    PlatformConfig,
    RegistryConfig,
    S3Config,
    ServerConfig,
)


logger = logging.getLogger(__name__)


class EnvironConfigFactory:
    def __init__(self, environ: dict[str, str] | None = None) -> None:
        self._environ = environ or os.environ

    def _get_url(self, name: str) -> URL | None:
        value = self._environ[name]
        if value == "-":
            return None
        return URL(value)

    def create(self) -> Config:
        return Config(
            cluster_name=self._environ["NP_MONITORING_CLUSTER_NAME"],
            server=self._create_server(),
            platform_api=self._create_platform_api(),
            platform_auth=self._create_platform_auth(),
            platform_config=self._create_platform_config(),
            elasticsearch=self._create_elasticsearch(),
            s3=self._create_s3(),
            loki=self._create_loki(),
            logs=self._create_logs(),
            kube=self._create_kube(),
            registry=self._create_registry(),
            container_runtime=self._create_container_runtime(),
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
        url = self._get_url("NP_MONITORING_PLATFORM_AUTH_URL")
        token = self._environ["NP_MONITORING_PLATFORM_AUTH_TOKEN"]
        return PlatformAuthConfig(url=url, token=token)

    def _create_platform_config(self) -> PlatformConfig:
        url = URL(self._environ["NP_MONITORING_PLATFORM_CONFIG_URL"])
        token = self._environ["NP_MONITORING_PLATFORM_CONFIG_TOKEN"]
        return PlatformConfig(url=url, token=token)

    def _create_elasticsearch(self) -> ElasticsearchConfig | None:
        if not any(key.startswith("NP_MONITORING_ES") for key in self._environ.keys()):
            return None
        hosts = self._environ["NP_MONITORING_ES_HOSTS"].split(",")
        return ElasticsearchConfig(hosts=hosts)

    def _create_s3(self) -> S3Config | None:
        if not any(key.startswith("NP_MONITORING_S3") for key in self._environ.keys()):
            return None
        endpoint_url = self._environ.get("NP_MONITORING_S3_ENDPOINT_URL", "")
        return S3Config(
            region=self._environ.get("NP_MONITORING_S3_REGION", ""),
            access_key_id=self._environ.get("NP_MONITORING_S3_ACCESS_KEY_ID"),
            secret_access_key=self._environ.get("NP_MONITORING_S3_SECRET_ACCESS_KEY"),
            endpoint_url=URL(endpoint_url) if endpoint_url else None,
            job_logs_bucket_name=self._environ["NP_MONITORING_S3_JOB_LOGS_BUCKET_NAME"],
        )

    def _create_loki(self) -> LokiConfig | None:
        if not any(
            key.startswith("NP_MONITORING_LOKI") for key in self._environ.keys()
        ):
            return None
        return LokiConfig(
            endpoint_url=URL(self._environ["NP_MONITORING_LOKI_ENDPOINT_URL"]),
        )
        # return LokiConfig(
        #     endpoint_url=URL(
        #         "http://observability-loki-gateway.platform.svc.cluster.local"
        #         # "http://localhost:3100"
        #     ),
        # )

    def _create_logs(self) -> LogsConfig:
        return LogsConfig(
            # storage_type=LogsStorageType.LOKI,
            storage_type=LogsStorageType(
                self._environ.get(
                    "NP_MONITORING_LOGS_STORAGE_TYPE",
                    LogsStorageType.ELASTICSEARCH.value,
                )
            ),
            cleanup_interval_sec=float(
                self._environ.get(
                    "NP_MONITORING_LOGS_CLEANUP_INTERVAL_SEC",
                    LogsConfig.cleanup_interval_sec,
                ),
            ),
            compact=self._create_logs_compact(),
        )

    def _create_logs_compact(self) -> LogsCompactConfig:
        return LogsCompactConfig(
            run_interval=float(
                self._environ.get(
                    "NP_MONITORING_LOGS_COMPACT_RUN_INTERVAL",
                    LogsCompactConfig.run_interval,
                )
            ),
            compact_interval=float(
                self._environ.get(
                    "NP_MONITORING_LOGS_COMPACT_COMPACT_INTERVAL",
                    LogsCompactConfig.compact_interval,
                )
            ),
            cleanup_interval=float(
                self._environ.get(
                    "NP_MONITORING_LOGS_COMPACT_CLEANUP_INTERVAL",
                    LogsCompactConfig.cleanup_interval,
                )
            ),
        )

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
            token_path=token_path,
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
            kubelet_node_port=int(
                self._environ.get("NP_MONITORING_K8S_KUBELET_PORT")
                or KubeConfig.kubelet_node_port
            ),
            nvidia_dcgm_node_port=(
                int(self._environ["NP_MONITORING_K8S_NVIDIA_DCGM_PORT"])
                if "NP_MONITORING_K8S_NVIDIA_DCGM_PORT" in self._environ
                else KubeConfig.nvidia_dcgm_node_port
            ),
            node_pool_label=self._environ.get(
                "NP_MONITORING_NODE_LABEL_NODE_POOL", KubeConfig.node_pool_label
            ),
        )

    def _create_registry(self) -> RegistryConfig:
        return RegistryConfig(url=URL(self._environ["NP_MONITORING_REGISTRY_URL"]))

    def _create_container_runtime(self) -> ContainerRuntimeConfig:
        return ContainerRuntimeConfig(
            port=int(
                self._environ.get(
                    "NP_MONITORING_CONTAINER_RUNTIME_PORT", ContainerRuntimeConfig.port
                )
            ),
        )
