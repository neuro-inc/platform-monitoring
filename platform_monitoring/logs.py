from aioelasticsearch import Elasticsearch
from neuromation.api import JobDescription as Job

from .kube_base import LogReader
from .kube_client import KubeClient


class MockLogReader(LogReader):
    """ Temporary class, to be removed
    """

    async def read(self, size: int = -1) -> bytes:
        return b"mock"


class LogReaderFactory:
    def __init__(self, kube_client: KubeClient, es_client: Elasticsearch) -> None:
        self._kube_client = kube_client
        self._es_client = es_client

    async def get_job_log_reader(self, job: Job) -> LogReader:
        pod_name = self._kube_client.get_job_pod_name(job)
        if await self._kube_client.check_pod_exists(pod_name):
            # TODO (A Yushkovskiy 04-Jun-2019) return PodContainerLogReader
            return MockLogReader()
        # TODO (A Yushkovskiy 04-Jun-2019) return ElasticsearchLogReader
        return MockLogReader()
