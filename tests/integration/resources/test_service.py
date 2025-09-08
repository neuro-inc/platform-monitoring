from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any
from unittest import mock
from uuid import uuid4

import pytest
import tenacity
from apolo_kube_client import (
    KubeClient,
    KubeClientAuthType,
    KubeConfig,
    escape_json_pointer,
)
from kubernetes.client.models import (
    V1Container,
    V1Node,
    V1NodeStatus,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
)
from neuro_config_client import (
    ConfigClient,
    PatchClusterRequest,
    ResourcePoolType,
)

from platform_monitoring.cluster_resources.service import (
    APOLO_PLATFORM_JOB_LABEL_KEY,
    APOLO_PLATFORM_NODE_POOL_LABEL_KEY,
    APOLO_PLATFORM_ROLE_LABEL_KEY,
    MonitoringService,
)


@pytest.fixture
async def kube_config(
    kube_config_cluster_payload: dict[str, Any],
    kube_config_user_payload: dict[str, Any],
    cert_authority_data_pem: str,
) -> KubeConfig:
    return KubeConfig(
        endpoint_url=kube_config_cluster_payload["server"],
        auth_type=KubeClientAuthType.CERTIFICATE,
        cert_authority_data_pem=cert_authority_data_pem,
        auth_cert_path=kube_config_user_payload["client-certificate"],
        auth_cert_key_path=kube_config_user_payload["client-key"],
        namespace="default",
    )


@pytest.fixture
async def kube_client(kube_config: KubeConfig) -> AsyncIterator[KubeClient]:
    async with KubeClient(config=kube_config) as client:
        yield client


@pytest.fixture
def config_client() -> Iterator[mock.AsyncMock]:
    client = mock.AsyncMock(spec=ConfigClient)
    with mock.patch(
        "platform_monitoring.cluster_resources.service.ConfigClient",
        return_value=client,
    ):
        yield client


@pytest.fixture
def cluster_name() -> str:
    return "test-cluster"


class TestMonitoringService:
    @pytest.fixture
    async def service(
        self,
        kube_client: KubeClient,
        config_client: mock.AsyncMock,
        cluster_name: str,
    ) -> AsyncIterator[MonitoringService]:
        async with MonitoringService(
            kube_client=kube_client,
            config_client=config_client,
            cluster_name=cluster_name,
        ) as service:
            yield service

    @pytest.fixture
    async def delete_node_later(
        self, kube_client: KubeClient
    ) -> AsyncIterator[Callable[[str], None]]:
        to_delete: list[V1Node] = []

        def _delete_node(node: V1Node) -> None:
            to_delete.append(node)

        yield _delete_node

        for node in to_delete:
            try:
                await kube_client.core_v1.node.delete(node.metadata.name)
            except Exception:
                pass

    @pytest.fixture
    async def delete_pod_later(
        self, kube_client: KubeClient
    ) -> AsyncIterator[Callable[[str], None]]:
        to_delete: list[V1Pod] = []

        def _delete_pod(pod: V1Pod) -> None:
            to_delete.append(pod)

        yield _delete_pod

        for pod in to_delete:
            try:
                await kube_client.core_v1.pod.delete(
                    pod.metadata.name, namespace=pod.metadata.namespace
                )
            except Exception:
                pass

    def _has_pool_type(
        self, patch_request: PatchClusterRequest, pool_type_name: str
    ) -> bool:
        if not patch_request.orchestrator:
            return False
        pool_types = patch_request.orchestrator.resource_pool_types
        if not pool_types:
            return False
        return any(pool_type.name == pool_type_name for pool_type in pool_types)

    def _get_pool_type(
        self, patch_request: PatchClusterRequest, pool_type_name: str
    ) -> ResourcePoolType:
        if not patch_request.orchestrator:
            msg = "No orchestrator in patch request"
            raise ValueError(msg)
        pool_types = patch_request.orchestrator.resource_pool_types
        if not pool_types:
            msg = "No pool types in patch request"
            raise ValueError(msg)
        for pool_type in pool_types:
            if pool_type.name == pool_type_name:
                return pool_type
        msg = f"No pool type named {pool_type_name} in patch request"
        raise ValueError(msg)

    async def test_start__existing_nodes(
        self,
        config_client: mock.AsyncMock,
        kube_client: KubeClient,
        delete_node_later: Callable[[V1Node], None],
        service: MonitoringService,
        cluster_name: str,
    ) -> None:
        node_pool_name = str(uuid4())
        node = await kube_client.core_v1.node.create(
            V1Node(
                api_version="v1",
                kind="Node",
                metadata={
                    "name": str(uuid4()),
                    "labels": {
                        APOLO_PLATFORM_ROLE_LABEL_KEY: "workload",
                        APOLO_PLATFORM_NODE_POOL_LABEL_KEY: node_pool_name,
                    },
                },
                status=V1NodeStatus(
                    capacity={"cpu": "1", "memory": "1Gi"},
                    allocatable={"cpu": "1", "memory": "1Gi"},
                ),
            )
        )
        delete_node_later(node)

        await service.start()

        async for attempt in tenacity.AsyncRetrying(
            wait=tenacity.wait_fixed(0.1),
            stop=tenacity.stop_after_delay(5),
            reraise=True,
        ):
            with attempt:
                config_client.patch_cluster.assert_awaited()

                assert (
                    config_client.patch_cluster.call_args_list[-1][0][0] == cluster_name
                )

                patch_request = config_client.patch_cluster.call_args_list[-1][0][1]
                assert self._has_pool_type(patch_request, node_pool_name)

    async def test_start__node_added_updated_removed(
        self,
        config_client: mock.AsyncMock,
        kube_client: KubeClient,
        delete_node_later: Callable[[V1Node], None],
        service: MonitoringService,
    ) -> None:
        await service.start()

        # Add a node
        node_pool_name = str(uuid4())
        node = await kube_client.core_v1.node.create(
            V1Node(
                api_version="v1",
                kind="Node",
                metadata={
                    "name": str(uuid4()),
                    "labels": {
                        APOLO_PLATFORM_ROLE_LABEL_KEY: "workload",
                        APOLO_PLATFORM_NODE_POOL_LABEL_KEY: node_pool_name,
                    },
                },
                status=V1NodeStatus(
                    capacity={"cpu": "1", "memory": "1Gi"},
                    allocatable={"cpu": "1", "memory": "1Gi"},
                ),
            )
        )
        delete_node_later(node)

        async for attempt in tenacity.AsyncRetrying(
            wait=tenacity.wait_fixed(0.1),
            stop=tenacity.stop_after_delay(5),
            reraise=True,
        ):
            with attempt:
                config_client.patch_cluster.assert_awaited()

                patch_request = config_client.patch_cluster.call_args_list[-1][0][1]
                assert self._has_pool_type(patch_request, node_pool_name)

        config_client.reset_mock()

        # Remove all labels
        await kube_client.core_v1.node.patch_json(
            node.metadata.name, [{"op": "remove", "path": "/metadata/labels"}]
        )

        async for attempt in tenacity.AsyncRetrying(
            wait=tenacity.wait_fixed(0.1),
            stop=tenacity.stop_after_delay(5),
            reraise=True,
        ):
            with attempt:
                config_client.patch_cluster.assert_awaited()

                patch_request = config_client.patch_cluster.call_args_list[-1][0][1]
                assert not self._has_pool_type(patch_request, node_pool_name)

        config_client.reset_mock()

        # Add the labels back
        await kube_client.core_v1.node.patch_json(
            node.metadata.name,
            [
                {
                    "op": "add",
                    "path": "/metadata/labels",  # noqa: E501
                    "value": {},  # type: ignore
                },
                {
                    "op": "add",
                    "path": f"/metadata/labels/{escape_json_pointer(APOLO_PLATFORM_ROLE_LABEL_KEY)}",  # noqa: E501
                    "value": "workload",
                },
                {
                    "op": "add",
                    "path": f"/metadata/labels/{escape_json_pointer(APOLO_PLATFORM_NODE_POOL_LABEL_KEY)}",  # noqa: E501
                    "value": node_pool_name,
                },
            ],
        )

        async for attempt in tenacity.AsyncRetrying(
            wait=tenacity.wait_fixed(0.1),
            stop=tenacity.stop_after_delay(5),
            reraise=True,
        ):
            with attempt:
                config_client.patch_cluster.assert_awaited()

                patch_request = config_client.patch_cluster.call_args_list[-1][0][1]
                assert self._has_pool_type(patch_request, node_pool_name)

        config_client.reset_mock()

        # Remove the node
        await kube_client.core_v1.node.delete(node.metadata.name)

        async for attempt in tenacity.AsyncRetrying(
            wait=tenacity.wait_fixed(0.1),
            stop=tenacity.stop_after_delay(5),
            reraise=True,
        ):
            with attempt:
                config_client.patch_cluster.assert_awaited()

                patch_request = config_client.patch_cluster.call_args_list[-1][0][1]
                assert not self._has_pool_type(patch_request, node_pool_name)

    async def test_start__pod_added_updated_removed(
        self,
        config_client: mock.AsyncMock,
        kube_client: KubeClient,
        delete_pod_later: Callable[[V1Pod], None],
        service: MonitoringService,
    ) -> None:
        await service.start()

        node_pool_name = "minikube"

        async for attempt in tenacity.AsyncRetrying(
            wait=tenacity.wait_fixed(0.1),
            stop=tenacity.stop_after_delay(5),
            reraise=True,
        ):
            with attempt:
                config_client.patch_cluster.assert_awaited()

                patch_request = config_client.patch_cluster.call_args_list[-1][0][1]
                assert self._has_pool_type(patch_request, node_pool_name)

                initial_pool_type = self._get_pool_type(patch_request, node_pool_name)

        config_client.reset_mock()

        # Add a pod
        pod_name = str(uuid4())
        pod = await kube_client.core_v1.pod.create(
            V1Pod(
                api_version="v1",
                kind="Pod",
                metadata=V1ObjectMeta(
                    name=pod_name,
                    namespace="default",
                    labels={"app": "test"},
                ),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            name="busybox",
                            image="busybox",
                            resources=V1ResourceRequirements(
                                requests={"cpu": "100m", "memory": "128Mi"},
                            ),
                            command=["sh", "-c", "trap : TERM INT; sleep 3600 & wait"],
                        )
                    ]
                ),
            )
        )
        delete_pod_later(pod)

        async for attempt in tenacity.AsyncRetrying(
            wait=tenacity.wait_fixed(1),
            stop=tenacity.stop_after_delay(30),
            reraise=True,
        ):
            with attempt:
                config_client.patch_cluster.assert_awaited()

                patch_request = config_client.patch_cluster.call_args_list[-1][0][1]
                assert self._has_pool_type(patch_request, node_pool_name)

                pool_type = self._get_pool_type(patch_request, node_pool_name)
                assert pool_type.available_cpu == initial_pool_type.available_cpu - 0.1
                assert (
                    pool_type.available_memory
                    == initial_pool_type.available_memory - 128 * 1024**2
                )

        config_client.reset_mock()

        # Add job label
        await kube_client.core_v1.pod.patch_json(
            pod_name,
            [
                {
                    "op": "add",
                    "path": f"/metadata/labels/{escape_json_pointer(APOLO_PLATFORM_JOB_LABEL_KEY)}",  # noqa: E501
                    "value": "test-job",
                }
            ],
            namespace="default",
        )

        async for attempt in tenacity.AsyncRetrying(
            wait=tenacity.wait_fixed(0.1),
            stop=tenacity.stop_after_delay(5),
            reraise=True,
        ):
            with attempt:
                config_client.patch_cluster.assert_awaited()

                patch_request = config_client.patch_cluster.call_args_list[-1][0][1]
                assert self._has_pool_type(patch_request, node_pool_name)

                pool_type = self._get_pool_type(patch_request, node_pool_name)
                assert pool_type == initial_pool_type

        config_client.reset_mock()

        # Remove job label
        await kube_client.core_v1.pod.patch_json(
            pod_name,
            [
                {
                    "op": "remove",
                    "path": f"/metadata/labels/{escape_json_pointer(APOLO_PLATFORM_JOB_LABEL_KEY)}",  # noqa: E501
                }
            ],
            namespace="default",
        )

        async for attempt in tenacity.AsyncRetrying(
            wait=tenacity.wait_fixed(0.1),
            stop=tenacity.stop_after_delay(5),
            reraise=True,
        ):
            with attempt:
                config_client.patch_cluster.assert_awaited()

                patch_request = config_client.patch_cluster.call_args_list[-1][0][1]
                assert self._has_pool_type(patch_request, node_pool_name)

                pool_type = self._get_pool_type(patch_request, node_pool_name)
                assert pool_type.available_cpu == initial_pool_type.available_cpu - 0.1
                assert (
                    pool_type.available_memory
                    == initial_pool_type.available_memory - 128 * 1024**2
                )

        config_client.reset_mock()

        # Delete the pod
        await kube_client.core_v1.pod.delete(pod_name, namespace="default")

        async for attempt in tenacity.AsyncRetrying(
            wait=tenacity.wait_fixed(1),
            stop=tenacity.stop_after_delay(30),
            reraise=True,
        ):
            with attempt:
                config_client.patch_cluster.assert_awaited()

                patch_request = config_client.patch_cluster.call_args_list[-1][0][1]
                assert self._has_pool_type(patch_request, node_pool_name)

                pool_type = self._get_pool_type(patch_request, node_pool_name)
                assert pool_type == initial_pool_type
