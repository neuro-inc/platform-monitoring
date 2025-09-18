from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass

from aiohttp.web_exceptions import HTTPBadRequest, HTTPNotFound, HTTPUnprocessableEntity
from apolo_kube_client import KubeClient
from apolo_kube_client._errors import ResourceNotFound
from apolo_kube_client.apolo import generate_namespace_name
from kubernetes.client.models import V1Secret


class VClusterServiceError(Exception):
    """Base class for vcluster-related failures."""

    def __init__(
        self,
        status_code: int = HTTPBadRequest.status_code,
        message: str = "VCluster Error",
    ):
        self.status_code = status_code
        self.message = message


class VClusterSecretNotFoundError(VClusterServiceError):
    def __init__() -> None:
        super().__init__(
            status_code=HTTPNotFound.status_code,
            message="VCluster secret was not found",
        )


class VClusterKubeconfigMalformed(VClusterServiceError):
    def __init__() -> None:
        super().__init__(
            status_code=HTTPUnprocessableEntity.status_code,
            message="VCluster secret malformed",
        )


@dataclass(frozen=True)
class VClusterKubeconfig:
    certificate_authority: str
    client_certificate: str
    client_key: str
    config: str


class VClusterService:
    def __init__(self, kube_client: KubeClient) -> None:
        self._kube_client = kube_client

    async def get_kubeconfig(
        self,
        org_name: str,
        project_name: str,
    ) -> VClusterKubeconfig:
        namespace = generate_namespace_name(org_name, project_name)
        secret_name = f"vc-{project_name}"

        try:
            secret: V1Secret = await self._kube_client.core_v1.secret.get(
                name=secret_name,
                namespace=namespace,
            )
        except ResourceNotFound as exc:
            raise VClusterSecretNotFoundError() from exc

        data = secret.data or {}
        parsed_data = {}
        for key, attr in (
            (
                "certificate-authority",
                "certificate_authority",
            ),
            (
                "client-certificate",
                "client_certificate",
            ),
            (
                "client-key",
                "client_key",
            ),
            (
                "config",
                "config",
            ),
        ):
            raw_value = data.get(key)
            if not raw_value:
                raise VClusterKubeconfigMalformed()

            try:
                parsed_value = base64.b64decode(raw_value, validate=True).decode()
            except (binascii.Error, UnicodeDecodeError) as exc:
                raise VClusterKubeconfigMalformed() from exc
            else:
                parsed_data[attr] = parsed_value

        return VClusterKubeconfig(**parsed_data)
