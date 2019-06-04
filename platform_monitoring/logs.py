from neuromation.api import JobDescription as Job

from .elasticsearch import Elasticsearch as ElasticsearchClient
from .kube_base import LogReader
from .kube_client import KubeClient


class LogReaderFactory:
    def __init__(self, kube_client: KubeClient, es_client: ElasticsearchClient) -> None:
        self._kube_client = kube_client
        self._es_client = es_client

    async def get_job_log_reader(self, job: Job) -> LogReader:
        pod_name = self._kube_client.get_job_pod_name(job)
        if await self._kube_client.check_pod_exists(pod_name):
            # TODO (A Yushkovskiy 04-Jun-2019) return PodContainerLogReader
            return LogReader()
        # TODO (A Yushkovskiy 04-Jun-2019) return ElasticsearchLogReader
        return LogReader()
