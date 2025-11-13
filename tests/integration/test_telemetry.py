import asyncio
from unittest import mock

from apolo_kube_client import KubeClientSelector, V1Pod
from apolo_kube_client.apolo import generate_namespace_name

from platform_monitoring.kube_client import (
    KubeTelemetry,
    PodContainerStats,
    wait_pod_is_not_waiting,
)


class TestTelemetry:
    async def test_get_pod_container_stats(
        self,
        kube_client_selector: KubeClientSelector,
        job_pod: V1Pod,
        kube_node_name: str,
        org_name: str,
        project_name: str,
    ) -> None:
        assert job_pod.spec
        assert job_pod.metadata
        assert job_pod.metadata.name
        job_pod.spec.containers[0].command = [
            "/bin/bash",
            "-c",
            "for i in {1..5}; do echo $i; sleep 1; done",
        ]
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            await kube_client.core_v1.pod.create(job_pod)
            await wait_pod_is_not_waiting(
                kube_client, job_pod.metadata.name, timeout_s=60.0
            )

        pod_metrics = []
        namespace_name = generate_namespace_name(org_name, project_name)
        telemetry = KubeTelemetry(
            kube_client_selector,
            namespace_name,
            org_name=org_name,
            project_name=project_name,
            pod_name=job_pod.metadata.name,
            container_name=job_pod.spec.containers[0].name,
        )
        while True:
            stats = await telemetry._get_pod_container_stats(
                kube_node_name,
                job_pod.metadata.name,
                job_pod.spec.containers[0].name,
                namespace_name,
            )
            if stats:
                pod_metrics.append(stats)
            else:
                break
            await asyncio.sleep(1)

        assert pod_metrics
        assert pod_metrics[0] == PodContainerStats(cpu=mock.ANY, memory=mock.ANY)
        assert pod_metrics[0].cpu >= 0.0
        assert pod_metrics[0].memory > 0.0
