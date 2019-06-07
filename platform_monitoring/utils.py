from neuromation.api import JobDescription as Job, JobStatus


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
