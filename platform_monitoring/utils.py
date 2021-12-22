import functools
import sys
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from types import TracebackType
from typing import Any, Optional, TypeVar

import iso8601
from neuro_sdk import JobDescription as Job, JobStatus


T_co = TypeVar("T_co", covariant=True)

if sys.version_info >= (3, 10):
    from contextlib import aclosing
else:

    class aclosing(AbstractAsyncContextManager[T_co]):
        def __init__(self, thing: T_co):
            self.thing = thing

        async def __aenter__(self) -> T_co:
            return self.thing

        async def __aexit__(
            self,
            exc_type: Optional[type[BaseException]],
            exc: Optional[BaseException],
            tb: Optional[TracebackType],
        ) -> None:
            await self.thing.aclose()  # type: ignore


def asyncgeneratorcontextmanager(
    func: Callable[..., T_co]
) -> Callable[..., AbstractAsyncContextManager[T_co]]:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> AbstractAsyncContextManager[T_co]:
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
