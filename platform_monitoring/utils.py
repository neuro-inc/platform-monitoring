import abc

from aiobotocore.client import AioBaseClient
from aioelasticsearch import Elasticsearch
from neuro_sdk import JobDescription as Job, JobStatus

from .base import LogReader
from .kube_client import KubeClient
from .logs import ElasticsearchLogReader, PodContainerLogReader, S3LogReader


class LogReaderFactory(abc.ABC):
    @abc.abstractmethod
    async def get_pod_log_reader(
        self, pod_name: str, *, previous: bool = False, archive: bool = False
    ) -> LogReader:
        pass  # pragma: no cover


class ElasticsearchLogReaderFactory(LogReaderFactory):
    # TODO (A Yushkovskiy 07-Jun-2019) Add another abstraction layer joining together
    #  kube-client and elasticsearch-client (in platform-api it's KubeOrchestrator)
    #  and move there method `get_pod_log_reader`

    def __init__(self, kube_client: KubeClient, es_client: Elasticsearch) -> None:
        self._kube_client = kube_client
        self._es_client = es_client

    async def get_pod_log_reader(
        self, pod_name: str, *, previous: bool = False, archive: bool = False
    ) -> LogReader:
        if not archive and await self._kube_client.check_pod_exists(pod_name):
            return PodContainerLogReader(
                client=self._kube_client,
                pod_name=pod_name,
                container_name=pod_name,
                previous=previous,
            )
        return ElasticsearchLogReader(
            es_client=self._es_client,
            namespace_name=self._kube_client.namespace,
            pod_name=pod_name,
            container_name=pod_name,
        )


class S3LogReaderFactory(LogReaderFactory):
    def __init__(
        self,
        kube_client: KubeClient,
        s3_client: AioBaseClient,
        bucket_name: str,
        key_prefix_format: str,
    ) -> None:
        self._kube_client = kube_client
        self._s3_client = s3_client
        self._bucket_name = bucket_name
        self._key_prefix_format = key_prefix_format

    async def get_pod_log_reader(
        self, pod_name: str, *, previous: bool = False, archive: bool = False
    ) -> LogReader:
        if not archive and await self._kube_client.check_pod_exists(pod_name):
            return PodContainerLogReader(
                client=self._kube_client,
                pod_name=pod_name,
                container_name=pod_name,
                previous=previous,
            )
        return S3LogReader(
            s3_client=self._s3_client,
            bucket_name=self._bucket_name,
            prefix_format=self._key_prefix_format,
            namespace_name=self._kube_client.namespace,
            pod_name=pod_name,
            container_name=pod_name,
        )


class JobsHelper:
    def is_job_running(self, job: Job) -> bool:
        return job.status == JobStatus.RUNNING

    def is_job_finished(self, job: Job) -> bool:
        return job.status in (
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        )


class KubeHelper:
    def get_job_pod_name(self, job: Job) -> str:
        # TODO (A Danshyn 11/15/18): we will need to start storing jobs'
        #  kube pod names explicitly at some point
        return job.id
