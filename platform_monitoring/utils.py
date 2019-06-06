from neuromation.api import JobDescription as Job


class KubeHelper:
    @classmethod
    def get_job_pod_name(cls, job: Job) -> str:
        # TODO (A Danshyn 11/15/18): we will need to start storing jobs'
        #  kube pod names explicitly at some point
        return job.id
