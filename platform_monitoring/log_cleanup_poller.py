import asyncio
import contextlib
import logging
from typing import Any, Optional

from platform_logging import new_trace

from platform_monitoring.jobs_service import JobsService
from platform_monitoring.utils import KubeHelper, LogsService


logger = logging.getLogger(__name__)


class LogCleanupPoller:
    def __init__(
        self,
        jobs_service: JobsService,
        logs_service: LogsService,
        interval_sec: float = 60.0,
    ) -> None:
        self._jobs_service = jobs_service
        self._logs_service = logs_service
        self._interval_sec = interval_sec
        self._kube_helper = KubeHelper()

        self._task: Optional[asyncio.Task[None]] = None

    async def __aenter__(self) -> "LogCleanupPoller":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("Concurrent usage of LogCleanupPoller not allowed")
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        assert self._task is not None
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task

    async def _run(self) -> None:
        while True:
            start = asyncio.get_running_loop().time()
            await self._run_once()
            elapsed = asyncio.get_running_loop().time() - start
            delay = self._interval_sec - elapsed
            if delay < 0:
                delay = 0
            await asyncio.sleep(delay)

    @new_trace
    async def _run_once(self) -> None:
        try:
            async with self._jobs_service.get_jobs_for_log_removal() as it:
                async for job in it:
                    await self._logs_service.drop_logs(
                        self._kube_helper.get_job_pod_name(job)
                    )
                    await self._jobs_service.mark_logs_dropped(job.id)
        except asyncio.CancelledError:
            raise
        except BaseException:
            logger.exception("Failed to cleanup logs, ignoring...")
