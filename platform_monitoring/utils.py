from aioelasticsearch import Elasticsearch
from neuromation.api import JobDescription as Job, JobStatus

from .base import LogReader
from .kube_client import KubeClient
from .logs import ElasticsearchLogReader, PodContainerLogReader


class LogReaderFactory:
    # TODO (A Yushkovskiy 07-Jun-2019) Add another abstraction layer joining together
    #  kube-client and elasticsearch-client (in platform-api it's KubeOrchestrator)
    #  and move there method `get_pod_log_reader`

    def __init__(self, kube_client: KubeClient, es_client: Elasticsearch) -> None:
        self._kube_client = kube_client
        self._es_client = es_client

    async def get_pod_log_reader(self, pod_name: str) -> LogReader:
        if await self._kube_client.check_pod_exists(pod_name):
            return PodContainerLogReader(
                client=self._kube_client, pod_name=pod_name, container_name=pod_name
            )
        return ElasticsearchLogReader(
            es_client=self._es_client,
            namespace_name=self._kube_client.namespace,
            pod_name=pod_name,
            container_name=pod_name,
        )


class JobsHelper:
    def is_job_running(self, job: Job) -> bool:
        return job.status == JobStatus.RUNNING

    def is_job_finished(self, job: Job) -> bool:
        return job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED)


class KubeHelper:
    def get_job_pod_name(self, job: Job) -> str:
        # TODO (A Danshyn 11/15/18): we will need to start storing jobs'
        #  kube pod names explicitly at some point
        return job.id
