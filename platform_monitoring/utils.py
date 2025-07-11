import functools
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, aclosing
from datetime import datetime
from typing import Any

import iso8601
from apolo_api_client import Job, JobStatus


def asyncgeneratorcontextmanager[T_co](
    func: Callable[..., AsyncGenerator[T_co, Any]],
) -> Callable[..., AbstractAsyncContextManager[AsyncGenerator[T_co, Any]]]:
    @functools.wraps(func)
    def wrapper(
        *args: Any, **kwargs: Any
    ) -> AbstractAsyncContextManager[AsyncGenerator[T_co, Any]]:
        return aclosing(func(*args, **kwargs))

    return wrapper


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


def parse_date(s: str) -> datetime:
    return iso8601.parse_date(s)


def format_date(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")
